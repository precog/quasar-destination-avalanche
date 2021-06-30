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

import doobie._
import doobie.implicits._

sealed trait AvalancheType extends Product with Serializable {
  def fragment: Fragment
}

object AvalancheType {
  sealed trait TimeZoning extends Product with Serializable { self =>
    def fragment: Fragment
  }

  object TimeZoning {
    final case object WithoutZone extends TimeZoning {
      def fragment = fr0"WITHOUT TIME ZONE"
    }
    final case object WithZone extends TimeZoning {
      def fragment = fr0"WITH TIME ZONE"
    }
    final case object WithLocalZone extends TimeZoning {
      def fragment = fr0"WITH LOCAL TIME ZONE"
    }
  }

  final case class NCHAR(size: Int) extends AvalancheType {
    def fragment = Fragment.const0(s"NCHAR(${size})")
  }
  final case class NVARCHAR(size: Int) extends AvalancheType {
    def fragment = Fragment.const0(s"NVARCHAR(${size})")
  }
  final case class CHAR(size: Int) extends AvalancheType {
    def fragment = Fragment.const0(s"CHAR(${size})")
  }
  final case class VARCHAR(size: Int) extends AvalancheType {
    def fragment = Fragment.const0(s"VARCHAR(${size})")
  }
  final case object INTEGER1 extends AvalancheType {
    def fragment = fr0"INTEGER1"
  }
  final case object INTEGER2 extends AvalancheType {
    def fragment = fr0"INTEGER2"
  }
  final case object INTEGER4 extends AvalancheType {
    def fragment = fr0"INTEGER4"
  }
  final case object INTEGER8 extends AvalancheType {
    def fragment = fr0"INTEGER8"
  }
  final case class DECIMAL(precision: Int, scale: Int) extends AvalancheType {
    def fragment = Fragment.const0(s"DECIMAL(${precision}, ${scale})")
  }
  final case object FLOAT4 extends AvalancheType {
    def fragment = fr0"FLOAT4"
  }
  final case object FLOAT8 extends AvalancheType {
    def fragment = fr0"FLOAT8"
  }
  final case object ANSIDATE extends AvalancheType {
    def fragment = fr0"ANSIDATE"
  }
  final case object OFFSET_DATE extends AvalancheType {
    def fragment = fr0"TIMESTAMP(0) WITH TIME ZONE"
  }
  final case class TIME(precision: Int, zoning: TimeZoning) extends AvalancheType {
    def fragment = Fragment.const(s"TIME(${precision})") ++ zoning.fragment
  }
  final case class TIMESTAMP(precision: Int, zoning: TimeZoning) extends AvalancheType {
    def fragment = Fragment.const(s"TIMESTAMP(${precision})") ++ zoning.fragment
  }
  final case class INTERVAL_DAY(precision: Int) extends AvalancheType {
    def fragment = Fragment.const(s"INTERVAL DAY TO SECOND(${precision})")
  }
  final case object INTERVAL_YEAR extends AvalancheType {
    def fragment = fr0"INTERVAL YEAR TO DAY"
  }
  final case object BOOLEAN extends AvalancheType {
    def fragment = fr0"BOOLEAN"
  }
  final case object MONEY extends AvalancheType {
    def fragment = fr0"MONEY"
  }
  final case object IPV4 extends AvalancheType {
    def fragment = fr0"IPV4"
  }
  final case object IPV6 extends AvalancheType {
    def fragment = fr0"IPV6"
  }
  final case object UUID extends AvalancheType {
    def fragment = fr0"UUID"
  }
}
