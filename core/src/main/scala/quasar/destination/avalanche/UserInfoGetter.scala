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

import argonaut._

import cats.effect._
import cats.implicits._

import java.lang.String
import java.nio.charset.StandardCharsets

import org.http4s.{Request, Uri, Method, Headers, AuthScheme}
import org.http4s.headers.Authorization
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.argonaut.jsonDecoder

import scala.Option
import scala.concurrent.duration.Duration

import quasar.connector.{Credentials}

object UserInfoGetter {

  private val utf8 = StandardCharsets.UTF_8

  def emailFromUserinfo[F[_]: ConcurrentEffect: Timer: ContextShift](token: Credentials.Token, userinfoUrl: Uri, userinfoField: String): F[Option[Email]] = {
    val req = Request[F](
      uri = userinfoUrl,
      method = Method.GET,
      headers = Headers.of(
        Authorization(
          org.http4s.Credentials.Token(AuthScheme.Bearer, new String(token.toByteArray, utf8)))))

    EmberClientBuilder
      .default[F]
      .withMaxTotal(400)
      .withMaxPerKey(_ => 200)
      .withTimeout(Duration.Inf)
      .build
      .use(_.expect[Json](req)
        .map(v => 
          (v -| userinfoField)
            .flatMap(
              _.as[Email].toOption)))
  }

}
