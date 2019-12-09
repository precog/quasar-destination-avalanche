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

import java.time.format.DateTimeFormatter
import java.util.UUID

import quasar.api.push.RenderConfig
import quasar.api.destination.{Destination, DestinationType, ResultSink}
import quasar.api.resource._
import quasar.api.table.{ColumnType, TableColumn}
import quasar.blobstore.azure.{AzureDeleteService, AzurePutService, Expires, TenantId}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.blobstore.paths.{BlobPath, PathElem}

import cats.data._
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._

import com.microsoft.azure.storage.blob.ContainerURL

import doobie._
import doobie.implicits._

import fs2.Stream

import org.slf4s.Logging

import pathy.Path.FileName

import scalaz.NonEmptyList

import shims._

final class AvalancheDestination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
  xa: Transactor[F],
  refContainerURL: Ref[F, Expires[ContainerURL]],
  refreshToken: F[Unit],
  config: AvalancheConfig) extends Destination[F] with Logging {

  private val ingresRenderConfig = RenderConfig.Csv(
    includeHeader = false,
    offsetDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS xxx"),
    offsetTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS xxx"),
    localDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS"),
    localDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd"),
    localTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))

  def destinationType: DestinationType =
    AvalancheDestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F]] = NonEmptyList(csvSink)

  private val csvSink: ResultSink[F] = ResultSink.csv[F](ingresRenderConfig) {
    case (path, columns, bytes) =>
      for {
        tableName <- Stream.eval(ensureSingleSegment(path))

        freshName <- Stream.eval(upload(bytes))

        _ <- Stream.eval(copy(tableName, freshName, columns)).onFinalize(deleteBlob(freshName))

      } yield ()
  }

  private def deleteBlob(freshName: String): F[Unit] =
    for {
      _ <- refreshToken

      containerURL <- refContainerURL.get

      deleteService = AzureDeleteService.mk[F](containerURL.value)

      _ <- deleteService(BlobPath(List(PathElem(freshName))))
    } yield ()

  private def copy(tableName: FileName, freshName: String, columns: List[TableColumn])
      : F[Unit] =
    for {
      cols0 <- columns.toNel.fold[F[NonEmptyList[TableColumn]]](
        Sync[F].raiseError(new Exception("No columns specified")))(
        _.asScalaz.pure[F])

      cols <- cols0.traverse(mkColumn(_)).fold[F[NonEmptyList[Fragment]]](
        errs => Sync[F].raiseError(
          new Exception(s"Some column types are not supported: ${mkErrorString(errs.asScalaz)}")),
        _.pure[F])

      dropQuery = dropTableQuery(tableName.value).update

      createQuery = createTableQuery(tableName.value, cols).update

      loadQuery = copyQuery(tableName.value, freshName).update

      _ <- debug(s"Drop table query:\n${dropQuery.sql}")

      _ <- debug(s"Table creation query:\n${createQuery.sql}")

      _ <- debug(s"Load query:\n${loadQuery.sql}")

      _ <- (dropQuery.run *> createQuery.run).transact(xa)

      _ <- debug(s"Finished load")

      // use JDBC directly and turn-off autocommit to run the COPY command.
      // Otherwise Avalanche refuses to load
      connection = xa.connect(xa.kernel)

      load = connection.use(cn => for {

        _ <- Sync[F].delay(cn.setAutoCommit(false))

        statement <- Sync[F].delay(cn.createStatement())

        _ <- Sync[F].delay(statement.execute(loadQuery.sql))

        count = statement.getUpdateCount

        _ <- Sync[F].delay(cn.commit())

        _ <- Sync[F].delay(statement.close())

      } yield count)

      _ <- debug(s"Load result count: $count")

      _ <- debug(s"Finished table copy")
    } yield ()

  private def upload(bytes: Stream[F, Byte]): F[String] =
    for {
      freshNameSuffix <- Sync[F].delay(UUID.randomUUID().toString)
      freshName = s"reform-$freshNameSuffix"

      _ <- refreshToken

      containerURL <- refContainerURL.get

      put = AzurePutService.mk[F](containerURL.value)

      _ <- debug(s"Starting upload of $freshName")

      _ <- put((BlobPath(List(PathElem(freshName))), bytes))

      _ <- debug(s"Finished upload of $freshName")

    } yield freshName

  private def ensureSingleSegment(r: ResourcePath): F[FileName] =
    r match {
      case file /: ResourcePath.Root => FileName(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def copyQuery(tableName: String, fileName: String): Fragment =
    fr"COPY" ++ Fragment.const0(s""""$tableName"""") ++ fr0"() VWLOAD FROM " ++ abfsPath(fileName) ++
      fr"WITH" ++ pairs(
      fr"AZURE_CLIENT_ENDPOINT" -> authEndpoint(config.azureCredentials.tenantId),
      fr"AZURE_CLIENT_ID" -> Fragment.const(s"'${config.azureCredentials.clientId.value}'"),
      fr"AZURE_CLIENT_SECRET" -> Fragment.const(s"'${config.azureCredentials.clientSecret.value}'"),
      fr"FDELIM" -> fr"','",
      fr"QUOTE" -> fr"""'"'""")

  private def authEndpoint(tid: TenantId): Fragment =
    Fragment.const(s"'https://login.microsoftonline.com/${tid.value}/oauth2/token'")

  private def pairs(ps: (Fragment, Fragment)*): Fragment =
    (ps map {
      case (lhs, rhs) => lhs ++ fr"=" ++ rhs
    }).toList.intercalate(fr",")

  private def abfsPath(file: String): Fragment =
    Fragment.const(
      s"'abfs://${config.containerName.value}@${config.accountName.value}.dfs.core.windows.net/$file'")

  private def createTableQuery(tableName: String, columns: NonEmptyList[Fragment]): Fragment =
    fr"CREATE TABLE" ++ Fragment.const(s""""$tableName"""") ++
    Fragments.parentheses(columns.intercalate(fr",")) ++ fr"with nopartition"

  private def dropTableQuery(tableName: String): Fragment =
    fr"DROP TABLE IF EXISTS" ++ Fragment.const(tableName)

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs.map(_.show).intercalate(", ")

  private def mkColumn(c: TableColumn): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToAvalanche(c.tpe).map(Fragment.const(s""""${c.name}"""") ++ _)

  private def columnTypeToAvalanche(ct: ColumnType.Scalar)
      : ValidatedNel[ColumnType.Scalar, Fragment] =
    ct match {
      case ColumnType.Null => fr0"INTEGER1".validNel
      case ColumnType.Boolean => fr0"BOOLEAN".validNel
      case ColumnType.LocalTime => fr0"TIME(3)".validNel
      case ColumnType.OffsetTime => fr0"TIME(3) WITH TIME ZONE".validNel
      case ColumnType.LocalDate => fr0"ANSIDATE".validNel
      case od @ ColumnType.OffsetDate => od.invalidNel
      case ColumnType.LocalDateTime => fr0"TIMESTAMP(3)".validNel
      case ColumnType.OffsetDateTime => fr0"TIMESTAMP(3) WITH TIME ZONE".validNel
      // Avalanche supports intervals, but not ISO 8601 intervals, which is what
      // Quasar produces
      case i @ ColumnType.Interval => i.invalidNel
      case ColumnType.Number => fr0"DECIMAL(33, 3)".validNel
      case ColumnType.String => fr0"NVARCHAR(512)".validNel
    }

  private def debug(msg: String): F[Unit] =
    Sync[F].delay(log.debug(msg))
}
