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

package quasar.destination.avalanche

import cats.Applicative
import cats.data.{ValidatedNel, NonEmptyList}
import cats.implicits._
import doobie._
import doobie.implicits._
import java.net.URI
import java.time.format.DateTimeFormatter
import scala.{
  Either,
  Seq,
  StringContext
}
import scala.Predef.{ArrowAssoc, String}
import quasar.api.resource._
import quasar.api.{Column, ColumnType}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.render.RenderConfig

object AvalancheQueries {

  val AvalancheRenderConfig = RenderConfig.Csv(
    includeHeader = false,
    offsetDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS xxx"),
    offsetTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS xxx"),
    localDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS"),
    localDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd"),
    localTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))

  // Ingres accepts double quotes as part of identifiers, but they must
  // be repeated twice. So we duplicate all quotes
  // More details:
  // https://docs.actian.com/avalanche/index.html#page/SQLLanguage%2FRegular_and_Delimited_Identifiers.htm%23ww414482
  def escapeIdent(ident: String): String = {
    val escaped = ident.replace("\"", "\"\"")
    s""""$escaped""""
  }

  def ensureValidTableName[F[_]: Applicative](r: ResourcePath)(implicit mr: MonadResourceErr[F]): F[String] =
    r match {
      case file /: ResourcePath.Root => escapeIdent(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  def ensureValidColumns(columns: NonEmptyList[Column[ColumnType.Scalar]])
      : Either[String, NonEmptyList[Fragment]] =
    columns.traverse(mkColumn(_)).toEither leftMap { errs =>
      s"Some column types are not supported: ${mkErrorString(errs)}"
    }

  def copyQuery(tableName: String, fileName: String, staginUri: URI, parameters: Seq[(String, String)]): Fragment = {
    val auth = authFragments(parameters)
    val staging = Fragment.const(s"'${staginUri.toString}'")

    fr"COPY" ++ Fragment.const0(tableName) ++ fr0"() VWLOAD FROM " ++ 
      staging ++ fr"WITH" ++ auth ++ fr"," ++ pairs(
      fr"FDELIM" -> fr"','",
      fr"QUOTE" -> fr"""'"'""") ++ fr", AUTO_DETECT_COMPRESSION"
  }

  def authFragments(params: Seq[(String, String)]): Fragment =
    (params map {
      case (key, value) =>
        Fragment.const(s"${key}") -> Fragment.const(s"'${value}'")
    } map (f => pairs(f))).toList.intercalate(fr",")

  def pairs(ps: (Fragment, Fragment)*): Fragment =
    (ps map {
        case (lhs, rhs) => lhs ++ fr"=" ++ rhs
     }).toList.intercalate(fr",")

  def createTableQuery(tableName: String, columns: NonEmptyList[Fragment]): Fragment = {
    fr"CREATE TABLE" ++ Fragment.const(tableName) ++
      Fragments.parentheses(columns.intercalate(fr",")) ++ fr"with nopartition"
  }

  def dropTableQuery(tableName: String): Fragment = {
    fr"DROP TABLE IF EXISTS" ++ Fragment.const(tableName)
  }

  def truncateTableQuery(tableName: String): Fragment = {
    fr"MODIFY" ++ Fragment.const(tableName) ++ fr"TO TRUNCATED"
  }

  def existanceTableQuery(tableName: String): Fragment = {
    val table = tableName.toLowerCase.substring(1, tableName.length()-1)

    fr0"SELECT COUNT(*) AS exists_flag FROM iitables WHERE table_name = '" ++ Fragment.const(table) ++ fr0"'"
  }

  def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Avalanche")
      .intercalate(", ")

  def mkColumn(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToAvalanche(c.tpe).map(Fragment.const(escapeIdent(c.name)) ++ _)

  def columnTypeToAvalanche(ct: ColumnType.Scalar)
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
}