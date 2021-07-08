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

import quasar.lib.jdbc.Redacted

import java.util.UUID

import scala.{Serializable, Product}

import argonaut._, Argonaut._

import org.http4s.Uri
import org.http4s.argonaut._

sealed abstract class AvalancheAuth extends Product with Serializable {
  def sanitized: AvalancheAuth
}

object AvalancheAuth {
  final case class UsernamePassword(username: Username, password: ClusterPassword) extends AvalancheAuth {
    def sanitized: AvalancheAuth = UsernamePassword(username, ClusterPassword(Redacted))
  }
  object UsernamePassword {
    implicit val codecUsernamePassword: CodecJson[UsernamePassword] = 
      casecodec2(UsernamePassword.apply, UsernamePassword.unapply)("username", "clusterPassword")
  }


  final case class ExternalAuth(authId: UUID, userinfoUri: Uri) extends AvalancheAuth {
    def sanitized: AvalancheAuth = ExternalAuth(UUID0, userinfoUri)
  } 
  object ExternalAuth {
    implicit val codecExternalAuth: CodecJson[ExternalAuth] = 
      casecodec2(ExternalAuth.apply, ExternalAuth.unapply)("authId", "userinfoUri")
  }

  implicit val encodeAuth: EncodeJson[AvalancheAuth] = EncodeJson {
    case u: UsernamePassword => 
      u.asJson
    case e: ExternalAuth =>
      ("externalAuth" := e) ->: jEmptyObject
  }

  implicit val decodeAuth: DecodeJson[AvalancheAuth] = 
    UsernamePassword.codecUsernamePassword.Decoder ||| 
      DecodeJson(c => (c --\ "externalAuth").as[ExternalAuth])

}
