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

import java.lang.String

import argonaut.{Json => _, _}, Argonaut._

final case class Username(asString: String)

object Username {
  val DbUser: Username = Username("dbuser")

  implicit val usernameDecodeJson: DecodeJson[Username] =
    json.decodeOrDefault(jdecode1(Username(_)), DbUser)

  implicit val usernameEncodeJson: EncodeJson[Username] =
    jencode1(_.asString)
}
