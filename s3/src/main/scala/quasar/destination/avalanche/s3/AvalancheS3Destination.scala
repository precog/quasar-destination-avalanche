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

import scala._, Predef._

import java.net.URI

import cats.effect.{Sync, Timer}

import doobie.Transactor

import org.slf4s.Logger

import pathy.Path.FileName

import quasar.blobstore.services.{DeleteService, PutService}
import quasar.connector.destination.Destination
import quasar.connector.MonadResourceErr

object AvalancheS3Destination {
  def apply[F[_]: Sync: MonadResourceErr: Timer](
      bucketConfig: BucketConfig,
      writeMode: WriteMode,
      putService: PutService[F],
      deleteService: DeleteService[F],
      xa: Transactor[F],
      logger: Logger)
      : Destination[F] = {

    val authParams = Map(
      "AWS_ACCESS_KEY" -> bucketConfig.accessKey.value,
      "AWS_SECRET_KEY" -> bucketConfig.secretKey.value)

    val stagedUri: FileName => URI =
      n => URI.create(s"s3a://${bucketConfig.bucket.value}/${n.value}")

    new StagedAvalancheDestination[F](
      AvalancheS3DestinationModule.destinationType,
      putService,
      deleteService,
      stagedUri,
      authParams,
      writeMode,
      xa,
      logger)
  }
}
