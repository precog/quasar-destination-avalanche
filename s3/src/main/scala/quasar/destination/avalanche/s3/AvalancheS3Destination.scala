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

import quasar.destination.avalanche.AvalancheQueries._

import cats.data.NonEmptyList
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import doobie._
import doobie.free.connection.createStatement
import doobie.implicits._
import fs2.{compression, Stream}
import java.util.UUID
import java.net.URI
import org.slf4s.Logging
import pathy.Path.FileName
import quasar.api.destination.DestinationType
import quasar.api.ColumnType
import quasar.blobstore.services.{DeleteService, PutService}
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.connector.destination.{LegacyDestination, ResultSink}
import quasar.connector.MonadResourceErr
import quasar.destination.avalanche.WriteMode._
import scala.{
  Int,
  List,
  RuntimeException,
  Seq,
  Some,
  StringContext,
  Unit
 }
import scala.Predef.String
import scala.util.matching.Regex

final class AvalancheS3Destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    deleteService: DeleteService[F],
    put: PutService[F],
    config: AvalancheS3Config,
    xa: Transactor[F]) extends LegacyDestination[F] with Logging {

  def destinationType: DestinationType =
    AvalancheS3DestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] = 
    NonEmptyList.one(csvSink)

  private val csvSink: ResultSink[F, ColumnType.Scalar] = 
    ResultSink.create[F, ColumnType.Scalar](AvalancheRenderConfig) {
      case (path, columns, bytes) => Stream.force(
        for {
          cols <- Sync[F].fromEither(ensureValidColumns(columns).leftMap(new RuntimeException(_)))
          tableName <- ensureValidTableName[F](path)
          compressed = bytes.through(compression.gzip(bufferSize = 1024 * 32))
          suffix <- Sync[F].delay(UUID.randomUUID().toString)
          freshName = s"precog-$suffix.gz"
          uploadPath = BlobPath(List(PathElem(freshName)))
          _ <- put((uploadPath, compressed))
          pushF = push(tableName, cols, FileName(freshName))
          pushS = Stream.eval(pushF).onFinalize(deleteFile(uploadPath))
        } yield pushS )
    }

  private def deleteFile(path: BlobPath): F[Unit] =
    deleteService(path).void

  private def push(
      tableName: String,
      cols: NonEmptyList[Fragment],
      freshName: FileName)
      : F[Unit] = {

    val dropQuery = dropTableQuery(tableName).update

    val existanceQuery = existanceTableQuery(tableName).query[Int]

    val truncateQuery = truncateTableQuery(tableName).update

    val createQuery = createTableQuery(tableName, cols).update

    val bucketCfg = config.bucketConfig
    val auth = Seq[(String, String)](
      ("AWS_ACCESS_KEY", bucketCfg.accessKey.value),
      ("AWS_SECRET_KEY", bucketCfg.secretKey.value))
    val stagingUri = URI.create(s"s3a://${bucketCfg.bucket.value}/${freshName.value}")

    val loadQuery = copyQuery(tableName, freshName.value, stagingUri, auth).update

    for {
      _ <- debug(s"Table creation query:\n${createQuery.sql}")

      _ <- config.writeMode match {
        case Replace => debug(s"Drop table query:\n${dropQuery.sql}")
        case Truncate => debug(s"Truncate table query:\n${truncateQuery.sql}")
        case Create => ().pure[F]
      }

      _ <- debugRedacted(s"Load query:\n${loadQuery.sql}")

      load = Sync[ConnectionIO].bracket(createStatement)(st =>
        for {
          _ <- Sync[ConnectionIO].delay(st.execute(loadQuery.sql))
          count <- Sync[ConnectionIO].delay(st.getUpdateCount)
        } yield count)(st => Sync[ConnectionIO].delay(st.close))

      count <- ((config.writeMode match {
          case Replace => dropQuery.run *> createQuery.run
          case Create => createQuery.run
          case Truncate => (for {
              exists <- existanceQuery.option
              result <- if (exists == Some(1)) truncateQuery.run else createQuery.run
            } yield result)
        }) *> load).transact(xa)

      _ <- debug(s"Finished load")

      _ <- debug(s"Load result count: $count")

      _ <- debug(s"Finished table copy")

    } yield ()
  }

  private def debug(msg: String): F[Unit] =
    Sync[F].delay(log.debug(msg))

  private def debugRedacted(msg: String): F[Unit] = {
    // remove everything within single quotes, to avoid leaking credentials
    Sync[F].delay(
      log.debug((new Regex("""'[^' ]{2,}'""")).replaceAllIn(msg, "<REDACTED>")))
  }
}
