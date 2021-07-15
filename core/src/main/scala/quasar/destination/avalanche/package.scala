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

package quasar.destination

import scala._, Predef._
import scala.concurrent.duration._
import scala.util.matching.Regex

import java.lang.{String, System}
import java.net.URI
import java.time.format.DateTimeFormatter
import java.util.UUID

import cats.data.NonEmptyList
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.log.{LogHandler => _, _}

import quasar.connector.render.RenderConfig

package object avalanche {
  type TableName = String

  private[destination] val UUID0: UUID = new UUID(0L, 0L)

  val GranteeDbadminGrp: Fragment = Fragment.const("group dbadmingrp")

  val AvalancheRenderConfig: RenderConfig.Csv =
    RenderConfig.Csv(
      includeHeader = false,
      offsetDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS xxx"),
      offsetTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS xxx"),
      localDateTimeFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS"),
      localDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd"),
      offsetDateFormat = DateTimeFormatter.ofPattern("uuuu-MM-dd 00:00:00 xxx"),
      localTimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))

  // Ingres accepts double quotes as part of identifiers, but they must
  // be repeated twice. So we duplicate all quotes
  // More details:
  // https://docs.actian.com/avalanche/index.html#page/SQLLanguage%2FRegular_and_Delimited_Identifiers.htm%23ww414482
  def escapeIdent(ident: String): String = {
    val escaped = ident.replace("\"", "\"\"")
    s""""$escaped""""
  }

  def loadUris(
      tableName: TableName,
      columns: NonEmptyList[Fragment],
      writeMode: WriteMode,
      srcUris: NonEmptyList[URI],
      authParameters: Map[String, String],
      logHandler: LogHandler)
      : ConnectionIO[Int] = {

    val prepare = prepareTable(tableName, columns, writeMode, logHandler)
    val vwload = copyVWLoad(tableName, srcUris, authParameters, logHandler)

    prepare >> vwload
  }

  def prepareTable(
      tableName: TableName,
      columns: NonEmptyList[Fragment],
      writeMode: WriteMode,
      logHandler: LogHandler)
      : ConnectionIO[Int] = {

    def createTableUpdate: Update0 =
      (fr"CREATE TABLE" ++ Fragment.const(tableName) ++
        Fragments.parentheses(columns.intercalate(fr",")) ++
        fr"with nopartition").updateWithLogHandler(logHandler)

    def dropTableUpdate: Update0 =
      (fr"DROP TABLE IF EXISTS" ++ Fragment.const(tableName))
        .updateWithLogHandler(logHandler)

    def truncateTableUpdate: Update0 =
      (fr"MODIFY" ++ Fragment.const(tableName) ++ fr"TO TRUNCATED")
        .updateWithLogHandler(logHandler)

    def tableExistsQuery: Query0[Int] = {
      // TODO: yikes, we need to better control sanitization
      val table = tableName.substring(1, tableName.length() - 1)
      (fr0"SELECT COUNT(*) AS exists_flag FROM iitables WHERE table_name = '" ++ Fragment.const(table) ++ fr0"'")
        .queryWithLogHandler[Int](logHandler)
    }

    def grantAllTableUpdate: Update0 =
      (fr"GRANT ALL PRIVILEGES ON TABLE" ++ Fragment.const(tableName) ++ fr"TO" ++ GranteeDbadminGrp)
        .updateWithLogHandler(logHandler)

    val initializeTable: ConnectionIO[Int] = createTableUpdate.run >> grantAllTableUpdate.run

    writeMode match {
      case WriteMode.Replace =>
        dropTableUpdate.run >> initializeTable

      case WriteMode.Create =>
        initializeTable

      case WriteMode.Append =>
        tableExistsQuery.option flatMap { result =>
          if (result.exists(_ == 1))
            0.pure[ConnectionIO]
          else
            initializeTable
        }

      case WriteMode.Truncate =>
        tableExistsQuery.option flatMap { result =>
          if (result.exists(_ == 1))
            truncateTableUpdate.run
          else
            initializeTable
        }
    }
  }

  def redactedLogEvent(event: LogEvent): LogEvent = {
    def redactAttrValues(s: String) =
      singleQuotedValue.replaceAllIn(s, "=<REDACTED>")

    event match {
      case Success(sql, args, exec, proc) =>
        Success(redactAttrValues(sql), args, exec, proc)

      case ProcessingFailure(sql, args, exec, proc, t) =>
        ProcessingFailure(redactAttrValues(sql), args, exec, proc, t)

      case ExecFailure(sql, args, exec, t) =>
        ExecFailure(redactAttrValues(sql), args, exec, t)
    }
  }

  def copyVWLoad(
      tableName: TableName,
      srcUris: NonEmptyList[URI],
      authParameters: Map[String, String],
      logHandler: LogHandler)
      : ConnectionIO[Int] = {

    val now = FS.delay(System.nanoTime)
    val redactedHandler = logHandler.unsafeRun compose redactedLogEvent

    def diff(a: Long, b: Long) = FiniteDuration((a - b).abs, NANOSECONDS)
    def log(e: LogEvent) = FS.delay(redactedHandler(e))

    def attrs(kv: List[(Fragment, Fragment)]): Fragment =
      kv.map { case (k, v) => k ++ fr0"=" ++ v }.intercalate(fr",")

    val sourcesSql =
      srcUris.map(uri => Fragment.const0(s"'$uri'")).intercalate(fr",")

    val authAttrs =
      authParameters.toList.map(_.bimap(
        Fragment.const0(_),
        v => Fragment.const0(s"'$v'")))

    val formatAttrs =
      List(
        fr0"FDELIM" -> fr0"','",
        fr0"QUOTE" -> fr0"""'"'""")

    val vwloadSql =
      fr"COPY" ++ Fragment.const0(tableName) ++ fr"() VWLOAD FROM" ++
        sourcesSql ++
        fr" WITH" ++
        attrs(authAttrs ::: formatAttrs) ++ fr0", AUTO_DETECT_COMPRESSION"

    // NB: Unable to use `Update0` as `COPY` requires using the generic `Statement#execute` method.
    HC.createStatement(for {
      t0 <- now
      sql = vwloadSql.update.sql
      en <- (FS.execute(sql) *> FS.getUpdateCount).attempt
      t1 <- now
      n  <- en.liftTo[StatementIO] onError {
        case e => log(ExecFailure(sql, Nil, diff(t1, t0), e))
      }
      _  <- log(Success(sql, Nil, diff(t1, t0), FiniteDuration(0L, NANOSECONDS)))
    } yield n)
  }

  private val singleQuotedValue: Regex = s"""='[^' ]{2,}'""".r
}
