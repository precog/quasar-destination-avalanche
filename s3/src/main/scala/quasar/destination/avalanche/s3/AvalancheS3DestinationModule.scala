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

import quasar.destination.avalanche._

import scala.{Int, Unit}
import scala.util.{Either, Right}

import java.lang.String

import argonaut._, Argonaut._

import cats.data.{EitherT, NonEmptyList}
import cats.effect.{
  Concurrent,
  ConcurrentEffect,
  ContextShift,
  Resource,
  Timer
}
import cats.implicits._

import doobie.Transactor

import org.slf4s.Logger

import quasar.api.destination.DestinationType
import quasar.blobstore.s3.{
  S3DeleteService,
  S3PutService,
  AccessKey,
  Bucket,
  Region,
  SecretKey
}
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, PushmiPullyu}
import quasar.plugin.jdbc.TransactorConfig

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.{Region => AwsRegion}

object AvalancheS3DestinationModule extends AvalancheDestinationModule[AvalancheS3Config] {
  val PartSize: Int = 10 * 1024 * 1024 // 10MB

  val destinationType: DestinationType =
    DestinationType("avalanche-s3", 1L)

  def transactorConfig(config: AvalancheS3Config): Either[NonEmptyList[String], TransactorConfig] =
    Right(AvalancheTransactorConfig(
      config.connectionUri,
      config.username,
      config.clusterPassword))

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[AvalancheS3Config].fold(
      (_, _) => jEmptyObject,
      _.sanitized.asJson)

  def avalancheDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: AvalancheS3Config,
      transactor: Transactor[F],
      pushPull: PushmiPullyu[F],
      log: Logger)
      : Resource[F, Either[InitError, Destination[F]]] = {

    val bucketCfg = config.bucketConfig

    val init = for {
      client <- EitherT.right[InitError](s3Client[F](
        bucketCfg.accessKey,
        bucketCfg.secretKey,
        bucketCfg.region))

      _ <- EitherT(Resource.liftF(validBucket[F](client, config.sanitized.asJson, bucketCfg.bucket)))

      deleteService = S3DeleteService(client, bucketCfg.bucket)

      putService = S3PutService(client, PartSize, bucketCfg.bucket)

      dest = AvalancheS3Destination[F](
        config.bucketConfig,
        config.writeMode,
        putService,
        deleteService,
        transactor,
        log)
    } yield dest

    init.value
  }

  // skip validation for now, see ch11655
  private def validBucket[F[_]: Concurrent: ContextShift](
      client: S3AsyncClient,
      sanitizedConfig: Json,
      bucket: Bucket)
      : F[Either[InitError, Unit]] = ().asRight[InitError].pure[F]
    // S3StatusService(client, bucket) map {
    //   case BlobstoreStatus.Ok =>
    //     ().asRight

    //   case BlobstoreStatus.NotFound =>
    //     DestinationError
    //       .invalidConfiguration[Json, InitError](
    //         destinationType,
    //         sanitizedConfig,
    //         ZNel("Upload bucket does not exist"))
    //       .asLeft

    //   case BlobstoreStatus.NoAccess =>
    //     DestinationError
    //       .accessDenied[Json, InitError](
    //         destinationType,
    //         sanitizedConfig,
    //         "Access denied to upload bucket")
    //       .asLeft

    //   case BlobstoreStatus.NotOk(msg) =>
    //     DestinationError
    //       .invalidConfiguration[Json, InitError](
    //         destinationType,
    //         sanitizedConfig,
    //         ZNel(msg))
    //       .asLeft
    // }

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
