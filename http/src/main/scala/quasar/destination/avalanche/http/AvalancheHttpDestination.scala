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

import scala._, Predef.Map

import java.net.URI

import cats.data.NonEmptyList
import cats.effect.{Sync, Timer}
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Stream

import org.slf4s.Logger

import quasar.connector.MonadResourceErr
import quasar.connector.destination.PushmiPullyu
import quasar.plugin.jdbc.Slf4sLogHandler

final class AvalancheHttpDestination[F[_]: MonadResourceErr: Sync: Timer](
    writeMode: WriteMode,
    baseUrl: Option[URI],
    xa: Transactor[F],
    pushPull: PushmiPullyu[F],
    logger: Logger)
    extends AvalancheDestination[F](logger) {

  val destinationType =
    AvalancheHttpDestinationModule.destinationType

  def loadGzippedCsv(
      tableName: TableName,
      columns: NonEmptyList[Fragment],
      gzippedCsv: Stream[F, Byte])
      : Stream[F, Unit] = {

    val proxied = pushPull { url0 =>
      val url = baseUrl.fold(url0) { base =>
        val basePath =
          Option(base.getPath).fold("") { p =>
            if (p.endsWith("/"))
              p.substring(0, p.length - 1)
            else
              p
          }

        new URI(
          base.getScheme,
          base.getAuthority,
          Option(url0.getPath).fold(basePath)(basePath + _),
          url0.getQuery,
          url0.getFragment)
      }

      val prepare = prepareTable(tableName, columns, writeMode, logHandler)
      val vwload = copyVWLoad(tableName, NonEmptyList.one(url), Map(), logHandler)
      (prepare >> vwload.void).transact(xa)
    }

    gzippedCsv.through(proxied)
  }

  private val logHandler = Slf4sLogHandler(logger)
}
