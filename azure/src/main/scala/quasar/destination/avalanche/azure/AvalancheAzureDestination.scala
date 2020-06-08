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

import quasar.destination.avalanche.AvalancheQueries._


import cats.ApplicativeError
import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.azure.storage.blob.BlobContainerAsyncClient
import doobie._
import doobie.free.connection.createStatement
import doobie.implicits._
import fs2.{compression, Stream}
import java.util.UUID
import java.net.URI
import org.slf4s.Logging
import pathy.Path.FileName
import quasar.api.destination.DestinationType
import quasar.api.resource._
import quasar.api.ColumnType
import quasar.blobstore.azure.{AzureDeleteService, AzurePutService, Expires, TenantId}
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.connector.destination.{LegacyDestination, ResultSink}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.destination.avalanche.WriteMode._
import scala.{
  Byte,
  Int,
  List,
  RuntimeException,
  Seq,
  Some,
  StringContext,
  Throwable,
  Unit
 }
import scala.Predef.String
import scala.util.matching.Regex

final class AvalancheAzureDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    xa: Transactor[F],
    refContainerClient: Ref[F, Expires[BlobContainerAsyncClient]],
    refreshToken: F[Unit],
    config: AvalancheAzureConfig) extends LegacyDestination[F] with Logging {

  def destinationType: DestinationType =
    AvalancheAzureDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] = NonEmptyList.of(csvSink)

  private val csvSink: ResultSink[F, ColumnType.Scalar] = 
    ResultSink.create[F, ColumnType.Scalar](AvalancheRenderConfig) {
      case (path, columns, bytes) =>
        for {
          tableName <- Stream.eval(ensureValidTableName(path))

          cols <- ApplicativeError[Stream[F, ?], Throwable].fromEither(
            ensureValidColumns(columns).leftMap(new RuntimeException(_)))

          compressed = bytes.through(compression.gzip(bufferSize = 1024 * 32))

          freshName <- Stream.eval(upload(compressed))

          _ <- Stream.eval(copy(tableName, freshName, cols)).onFinalize(deleteBlob(freshName))

        } yield ()
  }

  private def deleteBlob(freshName: FileName): F[Unit] =
    for {
      _ <- refreshToken

      containerClient <- refContainerClient.get

      deleteService = AzureDeleteService.mk[F](containerClient.value)

      _ <- deleteService(BlobPath(List(PathElem(freshName.value))))
    } yield ()

  private def copy(tableName: String, fileName: FileName, cols: NonEmptyList[Fragment])
      : F[Unit] = {

    val dropQuery = dropTableQuery(tableName).update

    val existanceQuery = existanceTableQuery(tableName).query[Int]

    val truncateQuery = truncateTableQuery(tableName).update

    val createQuery = createTableQuery(tableName, cols).update

    val staginUri = URI.create(
      s"abfs://${config.containerName.value}@${config.accountName.value}.dfs.core.windows.net/${fileName.value}")
    val authCfg = config.azureCredentials
    val endpoint = authEndpoint(authCfg.tenantId)
    val auth = Seq[(String, String)](
      ("AZURE_CLIENT_ENDPOINT", endpoint),
      ("AZURE_CLIENT_ID", authCfg.clientId.value),
      ("AZURE_CLIENT_SECRET", authCfg.clientSecret.value))

    val loadQuery = copyQuery(tableName, fileName.value, staginUri, auth).update

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

  private def upload(bytes: Stream[F, Byte]): F[FileName] =
    for {
      freshNameSuffix <- Sync[F].delay(UUID.randomUUID().toString)
      freshName = s"precog-$freshNameSuffix"

      _ <- refreshToken

      containerClient <- refContainerClient.get

      put = AzurePutService.mk[F](containerClient.value)

      _ <- debug(s"Starting upload of $freshName")

      _ <- put((BlobPath(List(PathElem(freshName))), bytes))

      _ <- debug(s"Finished upload of $freshName")

    } yield FileName(freshName)

  private def removeSingleQuotes(str: String): String =
    str.replace("'", "")

  private def ensureValidTableName(r: ResourcePath): F[String] =
    r match {
      case file /: ResourcePath.Root => escapeIdent(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def authEndpoint(tid: TenantId): String = {
    val tenant = removeSingleQuotes(tid.value)
    
    s"https://login.microsoftonline.com/$tenant/oauth2/token"
  }

  private def debug(msg: String): F[Unit] =
    Sync[F].delay(log.debug(msg))

  private def debugRedacted(msg: String): F[Unit] = {
    // remove everything within single quotes, to avoid leaking credentials
    Sync[F].delay(
      log.debug((new Regex("""'[^' ]{2,}'""")).replaceAllIn(msg, "<REDACTED>")))
  }
}
