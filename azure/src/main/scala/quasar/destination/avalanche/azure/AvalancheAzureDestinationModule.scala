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

import quasar.destination.avalanche._

import scala.util.{Either, Right}

import java.lang.String

import argonaut._, Argonaut._

import cats.data.NonEmptyList
import cats.effect.{
  ConcurrentEffect,
  ContextShift,
  Resource,
  Timer
}
import cats.implicits._

import doobie.Transactor

import org.slf4s.Logger

import quasar.api.destination.DestinationType
import quasar.blobstore.azure.Azure
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, PushmiPullyu}
import quasar.lib.jdbc.TransactorConfig

object AvalancheAzureDestinationModule extends AvalancheDestinationModule[AvalancheAzureConfig] {

  val destinationType: DestinationType =
    DestinationType("avalanche-azure", 1L)

  def transactorConfig(config: AvalancheAzureConfig): Either[NonEmptyList[String], TransactorConfig] =
    Right(AvalancheTransactorConfig(
      config.connectionUri,
      config.username,
      config.password))

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[AvalancheAzureConfig].fold(
      (_, _) => jEmptyObject,
      _.sanitized.asJson)

  def avalancheDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: AvalancheAzureConfig,
      transactor: Transactor[F],
      pushPull: PushmiPullyu[F],
      log: Logger)
      : Resource[F, Either[InitError, Destination[F]]] = {

    val clientMgr = Azure.refContainerClient(AvalancheAzureConfig.toConfig(config))
    val dest = AvalancheAzureDestination[F](config, clientMgr, transactor, log)

    Resource.eval(dest.asRight[InitError].pure[F])
  }
}
