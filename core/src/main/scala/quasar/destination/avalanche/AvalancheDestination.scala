/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.destination.avalanche

import scala.Predef._
import scala._

import java.util.UUID

import quasar.api.push.RenderConfig
import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.resource._
import quasar.blobstore.azure.AzurePutService
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.blobstore.paths.{BlobPath, PathElem, Path}

import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import com.microsoft.azure.storage.blob.ContainerURL

import doobie._
import doobie.implicits._

import org.slf4s.Logging

import pathy.{Path => PPath}, PPath.FileName

import scalaz.NonEmptyList

final class AvalancheDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  xa: Transactor[F],
  refContainerURL: Ref[F, ContainerURL],
  refreshToken: F[Unit]) extends Destination[F] with Logging {

  def destinationType: DestinationType =
    AvalancheDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] = NonEmptyList(csvSink)

  private val csvSink: ResultSink[F] = ResultSink.csv[F](RenderConfig.Csv()) {
    case (path, columns, bytes) => for {
      _ <- ensureSingleSegment(path)
      freshNameSuffix <- Sync[F].delay(UUID.randomUUID().toString)
      freshName = s"reform-$freshNameSuffix"
      _ <- refreshToken
      containerURL <- refContainerURL.get
      put = AzurePutService.mk[F](containerURL)
      _ <- put((BlobPath(List(PathElem(freshName))), bytes))
    } yield ()
  }

  private def ensureSingleSegment(r: ResourcePath): F[FileName] =
    r match {
      case file /: ResourcePath.Root => FileName(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def toBlobPath(path: ResourcePath): BlobPath =
    BlobPath(toPath(path))

  private def toPath(path: ResourcePath): Path =
    ResourcePath.resourceNamesIso.get(path).map(n => PathElem(n.value)).toList
}
