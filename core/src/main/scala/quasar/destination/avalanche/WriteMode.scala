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

import scala.Predef.String
import scala.Product
import scala.Serializable

import argonaut._, Argonaut._

import cats._

sealed trait WriteMode extends Product with Serializable

object WriteMode {
  case object Create extends WriteMode
  case object Replace extends WriteMode
  case object Truncate extends WriteMode
  case object Append extends WriteMode

  implicit val codecJson: CodecJson[WriteMode] =
    CodecJson(
      _ match {
        case Create => jString("create")
        case Replace => jString("replace")
        case Truncate => jString("truncate")
        case Append => jString("append")
      },
      c => c.as[String] flatMap {
        case "create" => DecodeResult.ok(Create)
        case "replace" => DecodeResult.ok(Replace)
        case "truncate" => DecodeResult.ok(Truncate)
        case "append" => DecodeResult.ok(Append)
        case _ => DecodeResult.fail("Valid write modes are 'create', 'truncate', 'append' and 'replace'", c.history)
      })

  implicit val eqv: Eq[WriteMode] =
    Eq.fromUniversalEquals
}
