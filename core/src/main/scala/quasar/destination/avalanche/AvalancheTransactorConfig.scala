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

import cats.effect._
import cats.{Monad}
import cats.implicits._
import cats.data.EitherT

import scala.{Int, None, Some, StringContext, Either, Right, Left}
import scala.concurrent.duration._

import java.lang.String
import java.net.URI
import java.util.UUID
import java.nio.charset.Charset

import quasar.lib.jdbc.{JdbcDriverConfig, TransactorConfig}
import quasar.connector.{GetAuth, Credentials, ExternalCredentials}
import quasar.destination.avalanche.AvalancheAuth.UsernamePassword
import quasar.destination.avalanche.AvalancheAuth.ExternalAuth

case class AvalancheTransactorConfig(
  connectionUri: URI,
  auth: AvalancheAuth) {

  private def getToken[F[_]: Monad: Clock](
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

  def transactorConfig[F[_]: ConcurrentEffect: Timer: ContextShift](
      getAuth: GetAuth[F])
      : F[Either[String, TransactorConfig]] = {

    auth match {
      case UsernamePassword(username, password) =>
        AvalancheTransactorConfig.fromUsernamePassword(connectionUri, username, password).asRight[String].pure[F]

      case ExternalAuth(authId, userinfoUri) =>
        (
          for {
            token <- EitherT(getToken[F](getAuth, authId))
            email <- EitherT.fromOptionF(
              UserInfoGetter.emailFromUserinfo(token, userinfoUri),
              "Querying user info using the token acquired via the auth key did not yield an email. Check the scopes granted to the token.")
          } yield AvalancheTransactorConfig.fromToken(connectionUri, Username(email.asString), token)
        ).value

    }
  }
}

object AvalancheTransactorConfig {
  val IngresDriverFqcn: String = "com.ingres.jdbc.IngresDriver"
  // Avalanche closes the connection after 4 minutes so we set a connection lifetime of 3 minutes.
  val MaxLifetime: FiniteDuration = 3.minutes
  val PoolSize: Int = 8

  private[this] def fromDetails(
      connectionUrl: URI,
      username: Username,
      password: String)
      : TransactorConfig = {

    val fullUrl = {
      val u = connectionUrl.toString
      val auth = s"UID=${username.asString};PWD=$password"

      if (u.endsWith(";"))
        URI.create(u + auth)
      else
        URI.create(u + ";" + auth)
    }

    val driverConfig =
      JdbcDriverConfig.JdbcDriverManagerConfig(fullUrl, Some(IngresDriverFqcn))

    TransactorConfig(driverConfig, None)
  }

  def fromUsernamePassword(
      connectionUrl: URI,
      username: Username,
      password: ClusterPassword)
      : TransactorConfig = 
    fromDetails(connectionUrl, username, password.asString) 

  private val utf8 = Charset.forName("UTF-8")

  def fromToken(
      connectionUrl: URI,
      username: Username,
      token: Credentials.Token)
      : TransactorConfig = 
    fromDetails(
      connectionUrl, 
      username, 
      s"access_token=${new String(token.toByteArray, utf8)}")

}
