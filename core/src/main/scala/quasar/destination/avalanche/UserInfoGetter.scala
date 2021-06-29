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

import cats._
import cats.effect._
import cats.implicits._

import java.lang.String
import java.nio.charset.StandardCharsets
import java.util.UUID

import org.http4s.{Request, Uri, Method, Headers, AuthScheme}
import org.http4s.headers.Authorization
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.argonaut.jsonDecoder
import org.http4s.syntax.literals._

import scala.{Option, Some, None, StringContext, Either, Left, Right}
import scala.concurrent.duration.Duration

import quasar.connector.{Credentials, GetAuth, ExternalCredentials}

object UserInfoGetter {

  private val utf8 = StandardCharsets.UTF_8

  private def emailFromUserinfo[F[_]: ConcurrentEffect: Timer: ContextShift](token: Credentials.Token, userinfoUrl: Uri): F[Option[Email]] = {
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
          (v -| "email")
            .flatMap(
              _.as[Email].toOption)))
  }

  def fromGoogle[F[_]: ConcurrentEffect: Timer: ContextShift](token: Credentials.Token): F[Option[Email]] = 
    emailFromUserinfo[F](token, uri"https://openidconnect.googleapis.com/v1/userinfo")

  def fromSalesforce[F[_]: ConcurrentEffect: Timer: ContextShift](token: Credentials.Token): F[Option[Email]] = 
    emailFromUserinfo[F](token, uri"https://login.salesforce.com/services/oauth2/userinfo")

  def getToken[F[_]: Monad: Clock](
      getAuth: GetAuth[F], 
      key: UUID)
      : F[Either[String, Credentials.Token]] = {

    def verifyCreds(cred: Credentials): Either[String, Credentials.Token] = cred match {
      case t: Credentials.Token => Right(t)
      case _ => Left("Unsupported auth type provided by the configured auth key")
    }

    getAuth(key).flatMap {
      case Some(ExternalCredentials.Perpetual(t)) => 
        verifyCreds(t).pure[F]

      case Some(ExternalCredentials.Temporary(acquire, renew)) => 
        for {
          creds <- acquire.flatMap(_.nonExpired)
          result <- creds match {
            case None => 
              renew >> 
                acquire
                  .flatMap(_.nonExpired)
                  .map(_.toRight("Failed to acquire a non-expired token"))
            case Some(t) => 
              t.asRight[String].pure[F]
          }
        } yield result.flatMap(verifyCreds)

      case None => 
        "No auth found for the configured auth key"
          .asLeft[Credentials.Token].pure[F]

      case Some(_) => 
        "Unsupported credential type provided by the configured auth key"
          .asLeft[Credentials.Token].pure[F]
    }

  }
}
