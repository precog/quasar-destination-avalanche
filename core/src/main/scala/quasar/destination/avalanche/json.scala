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
import scala.util.Either

import java.lang.String
import java.net.{URI, URISyntaxException}

import argonaut._, Argonaut._

import cats.implicits._

object json {
  def decodeOrDefault[A](decodeJson: DecodeJson[A], defaultValue: A): DecodeJson[A] =
    DecodeJson.withReattempt(a => a.success match {
      case None =>
        DecodeResult.ok(defaultValue)
      case Some(v) =>
        decodeJson.decode(v)
    })

  implicit val uriCodecJson: CodecJson[URI] =
    CodecJson(
      uri => jString(uri.toString),
      c => for {
        uriStr <- c.jdecode[String]
        uri0 = Either.catchOnly[URISyntaxException](new URI(uriStr))
        uri <- uri0.fold(
          ex => DecodeResult.fail(s"Invalid URI: ${ex.getMessage}", c.history),
          DecodeResult.ok(_))
      } yield uri)
}
