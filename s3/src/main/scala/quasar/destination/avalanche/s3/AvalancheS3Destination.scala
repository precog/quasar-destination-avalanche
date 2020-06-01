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

import cats.data.{ValidatedNel, NonEmptyList}
import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import doobie._
import doobie.free.connection.createStatement
import doobie.implicits._
import fs2.{compression, Stream}
import java.time.format.DateTimeFormatter
import java.util.UUID
import org.slf4s.Logging
import pathy.Path.FileName
import quasar.api.destination.DestinationType
import quasar.api.resource._
import quasar.api.table.TableName
import quasar.api.{Column, ColumnType}
import quasar.blobstore.services.{DeleteService, PutService}
import quasar.blobstore.paths.{BlobPath, PathElem}
import quasar.connector.destination.{LegacyDestination, ResultSink}
import quasar.connector.render.RenderConfig
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.destination.avalanche.WriteMode._
import scala.{
  Either,
  Int,
  List,
  RuntimeException,
  Some,
  StringContext,
  Unit
 }
import scala.Predef.{String, ArrowAssoc}
import scala.util.matching.Regex

final class AvalancheS3Destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
    deleteService: DeleteService[F],
    put: PutService[F],
    config: AvalancheS3Config,
    xa: Transactor[F]) extends LegacyDestination[F] with Logging {

  private val AvalancheRenderConfig = RenderConfig.Csv(
    includeHeader = false,
    offsetDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS xxx"),
    offsetTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS xxx"),
    localDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS"),
    localDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd"),
    localTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))

  def destinationType: DestinationType =
    AvalancheS3DestinationModule.destinationType

  def sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] = 
    NonEmptyList.one(csvSink)

  private val csvSink: ResultSink[F, ColumnType.Scalar] = 
    ResultSink.create[F, ColumnType.Scalar](AvalancheRenderConfig) {
      case (path, columns, bytes) => Stream.force(
        for {
          cols <- Sync[F].fromEither(ensureValidColumns(columns).leftMap(new RuntimeException(_)))
          tableName <- ensureValidTableName(path)
          compressed = bytes.through(compression.gzip(bufferSize = 1024 * 32))
          suffix <- Sync[F].delay(UUID.randomUUID().toString)
          freshName = s"precog-$suffix.gz"
          uploadPath = BlobPath(List(PathElem(freshName)))
          _ <- put((uploadPath, compressed))
          pushF = push(tableName, cols, FileName(freshName))
          pushS = Stream.eval(pushF).onFinalize(deleteFile(uploadPath))
        } yield pushS)
    }

  private def deleteFile(path: BlobPath): F[Unit] =
    deleteService(path).void

  private def push(
      tableName: TableName,
      cols: NonEmptyList[Fragment],
      freshName: FileName)
      : F[Unit] = {

    val dropQuery = dropTableQuery(tableName).update

    val existanceQuery = existanceTableQuery(tableName).query[Int]

    val truncateQuery = truncateTableQuery(tableName).update

    val createQuery = createTableQuery(tableName, cols).update

    val loadQuery = copyQuery(tableName, freshName).update

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

  // Ingres accepts double quotes as part of identifiers, but they must
  // be repeated twice. So we duplicate all quotes
  // More details:
  // https://docs.actian.com/avalanche/index.html#page/SQLLanguage%2FRegular_and_Delimited_Identifiers.htm%23ww414482
  private def escapeIdent(ident: String): String = {
    val escaped = ident.replace("\"", "\"\"")
    s""""$escaped""""
  }

  private def ensureValidTableName(r: ResourcePath): F[TableName] =
    r match {
      case file /: ResourcePath.Root => TableName(escapeIdent(file)).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def ensureValidColumns(columns: NonEmptyList[Column[ColumnType.Scalar]])
      : Either[String, NonEmptyList[Fragment]] =
    columns.traverse(mkColumn(_)).toEither leftMap { errs =>
      s"Some column types are not supported: ${mkErrorString(errs)}"
    }

  private def copyQuery(tableName: TableName, fileName: FileName): Fragment = {
    val table = tableName.name
    val auth = config.bucketConfig
    val akey = auth.accessKey
    val skey = auth.secretKey

    fr"COPY" ++ Fragment.const0(table) ++ fr0"() VWLOAD FROM " ++ s3Path(fileName) ++
      fr"WITH" ++ pairs(
      fr"AWS_ACCESS_KEY" -> Fragment.const(s"'${akey.value}'"),
      fr"AWS_SECRET_KEY" -> Fragment.const(s"'${skey.value}'"),
      fr"FDELIM" -> fr"','",
      fr"QUOTE" -> fr"""'"'""") ++ fr", AUTO_DETECT_COMPRESSION"
  }

  private def pairs(ps: (Fragment, Fragment)*): Fragment =
    (ps map {
      case (lhs, rhs) => lhs ++ fr"=" ++ rhs
    }).toList.intercalate(fr",")

  private def s3Path(file: FileName): Fragment = {
    val bucket = config.bucketConfig.bucket
    val bucketName = bucket.value
    val fileName = file.value
    Fragment.const(s"'s3a://$bucketName/$fileName'")
  }

  private def createTableQuery(tableName: TableName, columns: NonEmptyList[Fragment]): Fragment = {
    fr"CREATE TABLE" ++ Fragment.const(tableName.name) ++
      Fragments.parentheses(columns.intercalate(fr",")) ++ fr"with nopartition"
  }

  private def dropTableQuery(tableName: TableName): Fragment = {
    val table = tableName.name

    fr"DROP TABLE IF EXISTS" ++ Fragment.const(table)
  }

  private def truncateTableQuery(tableName: TableName): Fragment = {
    val table = tableName.name

    fr"MODIFY" ++ Fragment.const(table) ++ fr"TO TRUNCATED"
  }

  private def existanceTableQuery(tableName: TableName): Fragment = {
    val table = tableName.name.toLowerCase.substring(1, tableName.name.length()-1)

    fr0"SELECT COUNT(*) AS exists_flag FROM iitables WHERE table_name = '" ++ Fragment.const(table) ++ fr0"'"
  }

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Avalanche")
      .intercalate(", ")

  private def mkColumn(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToAvalanche(c.tpe).map(Fragment.const(escapeIdent(c.name)) ++ _)

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

  private def debugRedacted(msg: String): F[Unit] = {
    // remove everything within single quotes, to avoid leaking credentials
    Sync[F].delay(
      log.debug((new Regex("""'[^' ]{2,}'""")).replaceAllIn(msg, "<REDACTED>")))
  }
}
