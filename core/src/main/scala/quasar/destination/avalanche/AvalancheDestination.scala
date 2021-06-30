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

import scala.{io => _, _}
import scala.util.Either

import cats.data._
import cats.effect.{Sync, Timer}
import cats.implicits._

import doobie._

import fs2.{compression, Pipe, Stream}

import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import org.slf4s.Logger

import quasar.api.{Column, ColumnType, Labeled}
import quasar.api.push.TypeCoercion
import quasar.api.push.param._
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{Constructor, Destination, ResultSink}

abstract class AvalancheDestination[F[_]: MonadResourceErr: Sync: Timer](
    logger: Logger)
    extends Destination[F] {

  import AvalancheDestination._

  type Type = AvalancheType
  val Type = AvalancheType
  type TypeId = AvalancheTypeId
  val Id = AvalancheTypeId

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId] = tpe match {
    case ColumnType.Null =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.INTEGER1))
    case ColumnType.Boolean =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.BOOLEAN))
    case ColumnType.LocalTime =>
      TypeCoercion.Satisfied(NonEmptyList.of(Id.TIME, Id.TIME_LOCAL))
    case ColumnType.OffsetTime =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.TIME_ZONED))
    case ColumnType.LocalDate =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.ANSIDATE))
    case ColumnType.OffsetDate =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.OFFSET_DATE))
    case ColumnType.LocalDateTime =>
      TypeCoercion.Satisfied(NonEmptyList.of(Id.TIMESTAMP, Id.TIMESTAMP_LOCAL))
    case ColumnType.OffsetDateTime =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.TIMESTAMP_ZONED))
    case ColumnType.Interval =>
      TypeCoercion.Unsatisfied(List(), None)
    case ColumnType.Number =>
      TypeCoercion.Satisfied(NonEmptyList.of(
        Id.FLOAT8,
        Id.FLOAT4,
        Id.INTEGER1,
        Id.INTEGER2,
        Id.INTEGER4,
        Id.INTEGER8,
        Id.DECIMAL,
        Id.MONEY))
    case ColumnType.String =>
      TypeCoercion.Satisfied(NonEmptyList.of(
        Id.NVARCHAR,
        Id.NCHAR,
        Id.CHAR,
        Id.VARCHAR,
        Id.INTERVAL_DAY,
        Id.INTERVAL_YEAR,
        Id.IPV4,
        Id.IPV6,
        Id.UUID))
  }

  def construct(id: TypeId): Either[Type, Constructor[Type]] = id match {
    case Id.NCHAR =>
      withCharLength(Type.NCHAR(_), 16000)
    case Id.NVARCHAR =>
      withCharLength(Type.NVARCHAR(_), 16000)
    case Id.CHAR => 
      withCharLength(Type.CHAR(_), 32000)
    case Id.VARCHAR => 
      withCharLength(Type.VARCHAR(_), 32000)
    case Id.INTEGER1 =>
      Left(Type.INTEGER1)
    case Id.INTEGER2 =>
      Left(Type.INTEGER2)
    case Id.INTEGER4 =>
      Left(Type.INTEGER4)
    case Id.INTEGER8 =>
      Left(Type.INTEGER8)
    case Id.DECIMAL =>
      Right(Constructor.Binary(
        Labeled("precision", Formal.integer(Some(Ior.Both(1, 38)), Some(stepOne), None)),
        Labeled("scale", Formal.integer(Some(Ior.Left(0)), Some(stepOne), None)),
        Type.DECIMAL(_, _)))
    case Id.FLOAT4 =>
      Left(Type.FLOAT4)
    case Id.FLOAT8 =>
      Left(Type.FLOAT8)
    case Id.BOOLEAN =>
      Left(Type.BOOLEAN)
    case Id.MONEY =>
      Left(Type.MONEY)
    case Id.IPV4 =>
      Left(Type.IPV4)
    case Id.IPV6 =>
      Left(Type.IPV6)
    case Id.UUID =>
      Left(Type.UUID)
    case Id.TIME =>
      withSeconds(Type.TIME(_, Type.TimeZoning.WithoutZone))
    case Id.TIME_ZONED =>
      withSeconds(Type.TIME(_, Type.TimeZoning.WithZone))
    case Id.TIME_LOCAL =>
      withSeconds(Type.TIME(_, Type.TimeZoning.WithLocalZone))
    case Id.TIMESTAMP =>
      withSeconds(Type.TIMESTAMP(_, Type.TimeZoning.WithoutZone))
    case Id.TIMESTAMP_ZONED =>
      withSeconds(Type.TIMESTAMP(_, Type.TimeZoning.WithZone))
    case Id.TIMESTAMP_LOCAL =>
      withSeconds(Type.TIMESTAMP(_, Type.TimeZoning.WithLocalZone))
    case Id.INTERVAL_DAY =>
      withSeconds(Type.INTERVAL_DAY(_))
    case Id.INTERVAL_YEAR =>
      Left(Type.INTERVAL_YEAR)
    case Id.ANSIDATE =>
      Left(Type.ANSIDATE)
    case Id.OFFSET_DATE =>
      Left(Type.OFFSET_DATE)
  }

  private def stepOne: IntegerStep = IntegerStep.Factor(0, 1)

  private def withCharLength(mkType: Int => Type, maxSize: Int): Either[Type, Constructor[Type]] =
    Right(Constructor.Unary(
      Labeled("size", Formal.integer(Some(Ior.both(1, maxSize)), Some(stepOne), Some(1024))),
      mkType))

  private def withSeconds(mkType: Int => Type): Either[Type, Constructor[Type]] =
    Right(Constructor.Unary(
      Labeled("seconds precision", Formal.integer(Some(Ior.both(0, 9)), Some(stepOne), None)),
      mkType))

  val typeIdOrdinal = Id.ordinalPrism
  implicit val typeIdLabel = Id.label

  def loadGzippedCsv(
      tableName: TableName,
      columns: NonEmptyList[Fragment],
      gzippedCsv: Stream[F, Byte])
      : Stream[F, Unit]


  private val log = Slf4jLogger.getLoggerFromSlf4j[F](logger.underlying)

  private def ensureValidTableName(r: ResourcePath): F[TableName] =
    r match {
      case file /: ResourcePath.Root => escapeIdent(file).pure[F]
      case _ => MonadResourceErr[F].raiseError(ResourceError.notAResource(r))
    }

  private def mkColumn(c: Column[Type]): Fragment =
    Fragment.const(escapeIdent(c.name)) ++ c.tpe.fragment

  val sinks: NonEmptyList[ResultSink[F, Type]] =
    NonEmptyList.one(ResultSink.create[F, Type, Byte] { (path, typedCols) =>
      val pipe: Pipe[F, Byte, Unit] = bytes => Stream.force(for {
        tableName <- ensureValidTableName(path)

        columns = typedCols.map(mkColumn)

        compressed = bytes.through(compression.gzip(bufferSize = CompressionBufferSize))

        _ <- log.debug(s"Avalanche load of $tableName started")

        loaded = loadGzippedCsv(tableName, columns, compressed) onError {
          case t => Stream.eval(log.debug(t)(s"Avalanche load of $tableName failed: ${t.getMessage}"))
        }

        logComplete = Stream.eval_(log.debug(s"Avalanche load of $tableName completed"))

      } yield loaded ++ logComplete)

      (AvalancheRenderConfig, pipe)
    })
}
object AvalancheDestination {
  val CompressionBufferSize: Int = 32 * 1024
}
