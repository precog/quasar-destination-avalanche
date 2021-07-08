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
import quasar.destination.avalanche.json._

import scala.Option

import java.net.URI

import argonaut._, Argonaut._

final case class AvalancheHttpConfig(
    connectionUri: URI,
    auth: AvalancheAuth,
    writeMode: WriteMode,
    baseUrl: Option[URI]) {

  def sanitized: AvalancheHttpConfig =
    copy(
      auth = auth.sanitized)
}

object AvalancheHttpConfig {

  implicit def avalancheHttpConfigCodecJson: CodecJson[AvalancheHttpConfig] =
    CodecJson({ (c: AvalancheHttpConfig) =>
        (("baseUrl" :=? c.baseUrl) ->?:
          ("connectionUri" := c.connectionUri) ->:
          ("writeMode" := c.writeMode) ->:
          jEmptyObject)
          .deepmerge(c.auth.asJson)
      },
      (c => for {
         connectionUri <- (c --\ "connectionUri").as[URI]
         auth <- c.as[AvalancheAuth]
         writeMode <- (c --\ "writeMode").as[WriteMode]
         baseUrl <- (c --\ "baseUrl").as[Option[URI]]
       } yield AvalancheHttpConfig(
         connectionUri,
         auth,
         writeMode,
         baseUrl)))

}
