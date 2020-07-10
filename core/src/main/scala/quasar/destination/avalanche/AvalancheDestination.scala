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

import fs2.{compression, Stream}

import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import org.slf4s.Logger

import quasar.api.{Column, ColumnType, Labeled}
import quasar.api.push.TypeCoercion
import quasar.api.push.param._
import quasar.api.resource._
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.destination.{Destination, ResultSink}

import skolems.∃

abstract class AvalancheDestination[F[_]: MonadResourceErr: Sync: Timer](
    logger: Logger)
    extends Destination[F] {

  import AvalancheDestination._

  type Type = AvalancheType
  val Type = AvalancheType
  type TypeId = AvalancheTypeId
  val Id = AvalancheTypeId

  trait Constructor[P] extends ConstructorLike[P]

  object Constructor {
    final case object NCHAR extends Constructor[Int] {
      def apply(size: Int) = Type.NCHAR(size)
    }
    final case object NVARCHAR extends Constructor[Int] {
      def apply(size: Int) = Type.NVARCHAR(size)
    }
    final class TIME(zoning: Type.TimeZoning) extends Constructor[Int] {
      def apply(precision: Int) = Type.TIME(precision, zoning)
    }
    final class TIMESTAMP(zoning: Type.TimeZoning) extends Constructor[Int] {
      def apply(precision: Int) = Type.TIMESTAMP(precision, zoning)
    }
    final case object INTERVAL_DAY extends Constructor[Int] {
      def apply(precision: Int) = Type.INTERVAL_DAY(precision)
    }
    final case object DECIMAL extends Constructor[(Int, Int)] {
      def apply(params: (Int, Int)) = Type.DECIMAL(params._1, params._2)
    }
    object TIME {
      def apply(zoning: Type.TimeZoning) = new TIME(zoning)
    }
    object TIMESTAMP {
      def apply(zoning: Type.TimeZoning) = new TIMESTAMP(zoning)
    }
  }

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
      TypeCoercion.Unsatisfied(List(ColumnType.OffsetDateTime, ColumnType.LocalDate), None)
    case ColumnType.LocalDateTime =>
      TypeCoercion.Satisfied(NonEmptyList.of(Id.TIMESTAMP, Id.TIMESTAMP_LOCAL))
    case ColumnType.OffsetDateTime =>
      TypeCoercion.Satisfied(NonEmptyList.one(Id.TIMESTAMP_ZONED))
    case ColumnType.Interval =>
      TypeCoercion.Unsatisfied(List(), None)
    case ColumnType.Number =>
      TypeCoercion.Satisfied(NonEmptyList.of(
        Id.INTEGER1,
        Id.INTEGER2,
        Id.INTEGER4,
        Id.INTEGER8,
        Id.FLOAT4,
        Id.FLOAT8,
        Id.DECIMAL,
        Id.MONEY))
    case ColumnType.String =>
      TypeCoercion.Satisfied(NonEmptyList.of(
        Id.NCHAR,
        Id.NVARCHAR,
        Id.INTERVAL_DAY,
        Id.INTERVAL_YEAR,
        Id.IPV4,
        Id.IPV6,
        Id.UUID))
  }

  def construct(id: TypeId): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]] = id match {
    case Id.NCHAR =>
      withCharLength(Constructor.NCHAR)
    case Id.NVARCHAR =>
      withCharLength(Constructor.NVARCHAR)
    case Id.INTEGER1 =>
      Left(Type.INTEGER1)
    case Id.INTEGER2 =>
      Left(Type.INTEGER2)
    case Id.INTEGER4 =>
      Left(Type.INTEGER4)
    case Id.INTEGER8 =>
      Left(Type.INTEGER8)
    case Id.DECIMAL =>
      Right(formalConstructor[(Int, Int)](Constructor.DECIMAL, "decimal params",
        Formal.Pair(
          Formal.integer(Some(Ior.Both(1, 38)), Some(stepOne)),
          Formal.integer(Some(Ior.Left(0)), Some(stepOne)))))
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
      withSeconds(Constructor.TIME(Type.TimeZoning.WithoutZone))
    case Id.TIME_ZONED =>
      withSeconds(Constructor.TIME(Type.TimeZoning.WithZone))
    case Id.TIME_LOCAL =>
      withSeconds(Constructor.TIME(Type.TimeZoning.WithLocalZone))
    case Id.TIMESTAMP =>
      withSeconds(Constructor.TIMESTAMP(Type.TimeZoning.WithoutZone))
    case Id.TIMESTAMP_ZONED =>
      withSeconds(Constructor.TIMESTAMP(Type.TimeZoning.WithZone))
    case Id.TIMESTAMP_LOCAL =>
      withSeconds(Constructor.TIMESTAMP(Type.TimeZoning.WithLocalZone))
    case Id.INTERVAL_DAY =>
      withSeconds(Constructor.INTERVAL_DAY)
    case Id.INTERVAL_YEAR =>
      Left(Type.INTERVAL_YEAR)
    case Id.ANSIDATE =>
      Left(Type.ANSIDATE)
  }
  private def stepOne: IntegerStep = IntegerStep.Factor(0, 1)

  private def withCharLength(cstr: Constructor[Int]): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]] =
    Right(formalConstructor[Int](cstr, "size", Formal.integer(Some(Ior.both(1, 16000)), Some(stepOne))))

  private def withSeconds(cstr: Constructor[Int]): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]] =
    Right(formalConstructor[Int](cstr, "seconds precision", Formal.integer(Some(Ior.both(0, 9)), Some(stepOne))))

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
    NonEmptyList.one(ResultSink.create[F, Type](AvalancheRenderConfig) {
      case (path, typedCols, bytes) =>
        Stream.force(for {
          tableName <- ensureValidTableName(path)

          columns = typedCols.map(mkColumn)

          compressed = bytes.through(compression.gzip(bufferSize = CompressionBufferSize))

          _ <- log.debug(s"Avalanche load of $tableName started")

          loaded = loadGzippedCsv(tableName, columns, compressed) onError {
            case t => Stream.eval(log.debug(t)(s"Avalanche load of $tableName failed: ${t.getMessage}"))
          }

          logComplete = Stream.eval_(log.debug(s"Avalanche load of $tableName completed"))

        } yield loaded ++ logComplete)
    })
}
object AvalancheDestination {
  val CompressionBufferSize: Int = 32 * 1024
}
