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

package quasar.destination.avalanche.azure

import quasar.destination.avalanche.TransactorPools._

import argonaut._, Argonaut._
import cats.data.EitherT
import cats.effect.{
  ConcurrentEffect,
  ContextShift,
  Resource,
  Sync,
  Timer
}
import doobie.hikari.HikariTransactor
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{DestinationError, DestinationType}
import quasar.blobstore.azure.{
  Azure,
  AzureCredentials,
  ClientId,
  ClientSecret,
  TenantId
}
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, DestinationModule, PushmiPullyu}
import scala.{
  Int,
  StringContext
}
import scala.concurrent.duration._
import scala.util.{Either, Random}

object AvalancheAzureDestinationModule extends DestinationModule {
  val IngresDriverFqcn = "com.ingres.jdbc.IngresDriver"
  // Avalanche closes the connection after 4 minutes so we set a connection lifetime of 3 minutes.
  val MaxLifetime = 3.minutes
  val Redacted = "<REDACTED>"
  val PoolSize: Int = 10

  def destinationType: DestinationType =
    DestinationType("avalanche-azure", 1L)

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[AvalancheAzureConfig].result.fold(_ => Json.jEmptyObject, cfg =>
      cfg.copy(
        azureCredentials =
          AzureCredentials.ActiveDirectory(
            ClientId(Redacted),
            TenantId(Redacted),
            ClientSecret(Redacted)),
        password =
          ClusterPassword(Redacted)).asJson)

  def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json,
      pushPull: PushmiPullyu[F])
      : Resource[F, Either[InitializationError[Json], Destination[F]]] =
    (for {
      cfg <- EitherT.fromEither[Resource[F, ?]](config.as[AvalancheAzureConfig].result) leftMap {
        case (err, _) => DestinationError.malformedConfiguration((destinationType, config, err))
      }
      poolSuffix <- EitherT.right(Resource.liftF(Sync[F].delay(Random.alphanumeric.take(5).mkString)))
      connectPool <- EitherT.right(boundedPool[F](s"avalanche-dest-connect-$poolSuffix", PoolSize))
      transactPool <- EitherT.right(unboundedPool[F](s"avalanche-dest-transact-$poolSuffix"))

      jdbcUri = cfg.connectionUri.toString

      transactor <- EitherT.right[InitializationError[Json]](
        HikariTransactor.newHikariTransactor[F](
          IngresDriverFqcn,
          jdbcUri,
          cfg.username.value,
          cfg.password.value,
          connectPool,
          transactPool))

      _ <- EitherT.right(Resource.liftF(
          transactor.configure(ds => Sync[F].delay(ds.setMaxLifetime(MaxLifetime.toMillis)))))

      (refContainerClient, refresh) <- EitherT.right[InitializationError[Json]](
        Resource.liftF(Azure.refContainerClient(AvalancheAzureConfig.toConfig(cfg))))

      dest: Destination[F] =
        new AvalancheAzureDestination[F](transactor, refContainerClient, refresh, cfg)

    } yield dest).value
}


