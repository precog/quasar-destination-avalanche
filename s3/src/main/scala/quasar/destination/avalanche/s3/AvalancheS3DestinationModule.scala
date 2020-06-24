/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.destination.avalanche.s3

import quasar.destination.avalanche.TransactorPools._

import argonaut._, Argonaut._
import cats.data.EitherT
import cats.effect.{
  Concurrent,
  ConcurrentEffect,
  ContextShift,
  Resource,
  Sync,
  Timer
}
import cats.implicits._
import doobie.hikari.HikariTransactor
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.blobstore.s3.{
  S3DeleteService,
  S3PutService,
  S3StatusService,
  AccessKey,
  Bucket,
  Region,
  SecretKey
}
import quasar.blobstore.BlobstoreStatus
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, DestinationModule}
import scala.{
  Int,
  StringContext,
  Unit
}
import scala.concurrent.duration._
import scala.util.{Either, Random}
import scalaz.NonEmptyList
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => AwsRegion}

object AvalancheS3DestinationModule extends DestinationModule {
  val IngresDriverFqcn = "com.ingres.jdbc.IngresDriver"
  // Avalanche closes the connection after 4 minutes so we set a connection lifetime of 3 minutes.
  val MaxLifetime = 3.minutes
  val Redacted = "<REDACTED>"
  val PoolSize: Int = 10
  val PartSize = 10 * 1024 * 1024

  def destinationType: DestinationType =
    DestinationType("avalanche-s3", 1L)

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[AvalancheS3Config].result.fold(_ => Json.jEmptyObject, cfg =>
      cfg.copy(
        clusterPassword =
          ClusterPassword(Redacted),
        bucketConfig =
          BucketConfig(
              Bucket(cfg.bucketConfig.bucket.value),
              AccessKey(Redacted),
              SecretKey(Redacted),
              Region(Redacted))).asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    config: Json): Resource[F, Either[InitializationError[Json], Destination[F]]] =
    (for {
      cfg <- EitherT.fromEither[Resource[F, ?]](config.as[AvalancheS3Config].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((destinationType, config, err))
      }

      connectionUri = cfg.connectionUri.toString

      xa <- EitherT.right(for {
        poolSuffix <- Resource.liftF(Sync[F].delay(Random.alphanumeric.take(5).mkString))
        connectPool <- boundedPool[F](s"avalanche-dest-connect-$poolSuffix", PoolSize)
        transactPool <- unboundedPool[F](s"avalanche-dest-transact-$poolSuffix")
        transactor <- HikariTransactor.newHikariTransactor[F](
          IngresDriverFqcn,
          connectionUri,
          cfg.username.value,
          cfg.clusterPassword.value,
          connectPool,
          transactPool)
      } yield transactor)

      bucketCfg = cfg.bucketConfig

      client <- EitherT.right[InitializationError[Json]][Resource[F, ?], S3AsyncClient](
        s3Client[F](bucketCfg.accessKey, bucketCfg.secretKey, bucketCfg.region))

      _ <- EitherT(Resource.liftF(validBucket[F](client, config, bucketCfg.bucket)))

      deleteService = S3DeleteService(client, bucketCfg.bucket)

      putService = S3PutService(client, PartSize, bucketCfg.bucket)

      _ <- EitherT.right[InitializationError[Json]](Resource.liftF(
        xa.configure(ds => Sync[F].delay(ds.setMaxLifetime(MaxLifetime.toMillis)))))

      dest: Destination[F] = new AvalancheS3Destination[F](deleteService, putService, cfg, xa)

    } yield dest).value

  private def validBucket[F[_]: Concurrent: ContextShift](
      client: S3AsyncClient,
      config: Json,
      bucket: Bucket)
      : F[Either[InitializationError[Json], Unit]] = {
    S3StatusService(client, bucket) map {
      case BlobstoreStatus.Ok =>
        ().asRight
      case BlobstoreStatus.NotFound =>
        DestinationError
          .invalidConfiguration(
            (destinationType, config, NonEmptyList("Upload bucket does not exist")))
          .asLeft
      case BlobstoreStatus.NoAccess =>
        DestinationError.accessDenied(
          (destinationType, config, "Access denied to upload bucket"))
          .asLeft
      case BlobstoreStatus.NotOk(msg) =>
        DestinationError
          .invalidConfiguration(
            (destinationType, config, NonEmptyList(msg)))
          .asLeft
    }
  }

  private def s3Client[F[_]: Concurrent](
    accessKey: AccessKey,
    secretKey: SecretKey,
    region: Region)
      : Resource[F, S3AsyncClient] = {
    val client =
      Concurrent[F].delay(
        S3AsyncClient.builder
          .credentialsProvider(
            StaticCredentialsProvider.create(
              AwsBasicCredentials.create(accessKey.value, secretKey.value)))
          .region(AwsRegion.of(region.value))
          .build)

    Resource.fromAutoCloseable[F, S3AsyncClient](client)
  }
}
