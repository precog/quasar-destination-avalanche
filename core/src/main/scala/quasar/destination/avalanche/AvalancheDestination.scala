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

import scala.{io => _, _}, Predef._
import scala.util.Either

import cats.data._
import cats.effect.{Sync, Timer}
import cats.implicits._

import doobie._

import fs2.{compression, Stream}

import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import org.slf4s.Logger

import quasar.api.{Column, ColumnType}
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{LegacyDestination, ResultSink}

abstract class AvalancheDestination[F[_]: MonadResourceErr: Sync: Timer](
    logger: Logger)
    extends LegacyDestination[F] {

  import AvalancheDestination._

  def loadGzippedCsv(
      tableName: TableName,
      columns: NonEmptyList[Fragment],
      gzippedCsv: Stream[F, Byte])
      : Stream[F, Unit]

  ////

  val sinks: NonEmptyList[ResultSink[F, ColumnType.Scalar]] =
    NonEmptyList.one(ResultSink.create[F, ColumnType.Scalar](AvalancheRenderConfig) {
      case (path, scalarCols, bytes) =>
        Stream.force(for {
          tableName <- ensureValidTableName(path)

          columns <- Sync[F].fromEither(
            ensureValidColumns(scalarCols).leftMap(new IllegalArgumentException(_)))

          compressed = bytes.through(compression.gzip(bufferSize = CompressionBufferSize))

          _ <- log.debug(s"Avalanche load of $tableName started")

          loaded = loadGzippedCsv(tableName, columns, compressed) onError {
            case t => Stream.eval(log.debug(t)(s"Avalanche load of $tableName failed: ${t.getMessage}"))
          }

          logComplete = Stream.eval_(log.debug(s"Avalanche load of $tableName completed"))

        } yield loaded ++ logComplete)
    })

  ////

  private val log = Slf4jLogger.getLoggerFromSlf4j[F](logger.underlying)

  private def ensureValidColumns(columns: NonEmptyList[Column[ColumnType.Scalar]])
      : Either[String, NonEmptyList[Fragment]] =
    columns.traverse(mkColumn(_)).toEither leftMap { errs =>
      s"Some column types are not supported: ${mkErrorString(errs)}"
    }

  private def ensureValidTableName(r: ResourcePath): F[TableName] =
    r match {
      case file /: ResourcePath.Root => escapeIdent(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def mkColumn(c: Column[ColumnType.Scalar]): ValidatedNel[ColumnType.Scalar, Fragment] =
    columnTypeToAvalanche(c.tpe).map(Fragment.const(escapeIdent(c.name)) ++ _)

  private def mkErrorString(errs: NonEmptyList[ColumnType.Scalar]): String =
    errs
      .map(err => s"Column of type ${err.show} is not supported by Avalanche")
      .intercalate(", ")
}

object AvalancheDestination {
  val CompressionBufferSize: Int = 32 * 1024
}
