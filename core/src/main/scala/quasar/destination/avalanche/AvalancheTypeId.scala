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

import scala._

import quasar.api.Label

sealed abstract class AvalancheTypeId(val ordinal: Int) extends Product with Serializable

object AvalancheTypeId {
  import monocle.Prism

  final case object NCHAR extends AvalancheTypeId(0)
  final case object NVARCHAR extends AvalancheTypeId(1)
  final case object INTEGER1 extends AvalancheTypeId(2)
  final case object INTEGER2 extends AvalancheTypeId(3)
  final case object INTEGER4 extends AvalancheTypeId(4)
  final case object INTEGER8 extends AvalancheTypeId(5)
  final case object DECIMAL extends AvalancheTypeId(6)
  final case object FLOAT4 extends AvalancheTypeId(7)
  final case object FLOAT8 extends AvalancheTypeId(8)
  final case object ANSIDATE extends AvalancheTypeId(9)
  final case object TIME extends AvalancheTypeId(10)
  final case object TIME_ZONED extends AvalancheTypeId(11)
  final case object TIME_LOCAL extends AvalancheTypeId(12)
  final case object TIMESTAMP extends AvalancheTypeId(13)
  final case object TIMESTAMP_ZONED extends AvalancheTypeId(14)
  final case object TIMESTAMP_LOCAL extends AvalancheTypeId(15)
  final case object INTERVAL_DAY extends AvalancheTypeId(16)
  final case object INTERVAL_YEAR extends AvalancheTypeId(17)
  final case object BOOLEAN extends AvalancheTypeId(18)
  final case object MONEY extends AvalancheTypeId(19)
  final case object IPV4 extends AvalancheTypeId(20)
  final case object IPV6 extends AvalancheTypeId(21)
  final case object UUID extends AvalancheTypeId(22)
  final case object OFFSET_DATE extends AvalancheTypeId(23)

  val ordinalPrism: Prism[Int, AvalancheTypeId] =
    Prism.partial[Int, AvalancheTypeId]({
      case NCHAR.ordinal => NCHAR
      case NVARCHAR.ordinal => NVARCHAR
      case INTEGER1.ordinal => INTEGER1
      case INTEGER2.ordinal => INTEGER2
      case INTEGER4.ordinal => INTEGER4
      case INTEGER8.ordinal => INTEGER8
      case DECIMAL.ordinal => DECIMAL
      case FLOAT4.ordinal => FLOAT4
      case FLOAT8.ordinal => FLOAT8
      case ANSIDATE.ordinal => ANSIDATE
      case TIME.ordinal => TIME
      case TIME_ZONED.ordinal => TIME_ZONED
      case TIME_LOCAL.ordinal => TIME_LOCAL
      case TIMESTAMP.ordinal => TIMESTAMP
      case TIMESTAMP_ZONED.ordinal => TIMESTAMP_ZONED
      case TIMESTAMP_LOCAL.ordinal => TIMESTAMP_LOCAL
      case INTERVAL_DAY.ordinal => INTERVAL_DAY
      case INTERVAL_YEAR.ordinal => INTERVAL_YEAR
      case BOOLEAN.ordinal => BOOLEAN
      case MONEY.ordinal => MONEY
      case IPV4.ordinal => IPV4
      case IPV6.ordinal => IPV6
      case UUID.ordinal => UUID
      case OFFSET_DATE.ordinal => OFFSET_DATE
    })(_.ordinal)

  val label = Label[AvalancheTypeId] {
    case NCHAR => "NCHAR/CHAR"
    case NVARCHAR => "NVARCHAR/VARCHAR"
    case INTEGER1 => "INTEGER1"
    case INTEGER2 => "INTEGER2"
    case INTEGER4 => "INTEGER4"
    case INTEGER8 => "INTEGER8"
    case DECIMAL => "DECIMAL"
    case FLOAT4 => "FLOAT4"
    case FLOAT8 => "FLOAT8"
    case ANSIDATE => "ANSIDATE"
    case TIME => "TIME WITHOUT TIME ZONE"
    case TIME_ZONED => "TIME WITH TIME ZONE"
    case TIME_LOCAL => "TIME WITH LOCAL TIME ZONE"
    case TIMESTAMP => "TIMESTAMP WITHOUT TIME ZONE"
    case TIMESTAMP_ZONED => "TIMESTAMP WITH TIME ZONE"
    case TIMESTAMP_LOCAL => "TIMESTAMP WITH LOCAL TIME ZONE"
    case INTERVAL_DAY => "INTERVAL FROM DAY TO SECOND"
    case INTERVAL_YEAR => "INTERVAL FROM YEAR TO DAY"
    case BOOLEAN => "BOOLEAN"
    case MONEY => "MONEY"
    case IPV4 => "IPV4"
    case IPV6 => "IPV6"
    case UUID => "UUID"
    case OFFSET_DATE => "TIMESTAMP(0) WITH TIME ZONE"
  }
}
