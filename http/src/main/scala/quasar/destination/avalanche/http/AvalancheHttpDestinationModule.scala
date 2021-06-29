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

package quasar.destination.avalanche.http

import quasar.destination.avalanche._

import scala._

import argonaut._, Argonaut._

import cats.effect._
import cats.implicits._

import doobie.Transactor

import org.slf4s.Logger

import quasar.api.destination.DestinationType
import quasar.connector.MonadResourceErr
import quasar.connector.destination.{Destination, PushmiPullyu}

object AvalancheHttpDestinationModule extends AvalancheDestinationModule[AvalancheHttpConfig] {

  val destinationType: DestinationType =
    DestinationType("avalanche-http", 1L)

  def connectionConfig(config: AvalancheHttpConfig): AvalancheTransactorConfig = 
    AvalancheTransactorConfig(
      config.connectionUri,
      config.username,
      config.clusterPassword,
      config.googleAuth,
      config.salesforceAuth)

  def sanitizeDestinationConfig(config: Json): Json =
    config.as[AvalancheHttpConfig].fold(
      (_, _) => jEmptyObject,
      _.sanitized.asJson)

  def avalancheDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: AvalancheHttpConfig,
      transactor: Transactor[F],
      pushPull: PushmiPullyu[F],
      log: Logger)
      : Resource[F, Either[InitError, Destination[F]]] =
    new AvalancheHttpDestination(config.writeMode, config.baseUrl, transactor, pushPull, log)
      .asRight[InitError]
      .pure[Resource[F, ?]]
}
