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
import cats.implicits._
import cats.data.EitherT

import scala.{Int, None, Some, StringContext, Option, Either}
import scala.concurrent.duration._

import java.lang.String
import java.net.URI
import java.nio.charset.Charset
import java.util.UUID

import quasar.lib.jdbc.{JdbcDriverConfig, TransactorConfig}
import quasar.connector.{GetAuth, Credentials}

case class AvalancheTransactorConfig(
  connectionUri: URI,
  username: Username, 
  password: Option[ClusterPassword], 
  googleAuth: Option[GoogleAuth],
  salesforceAuth: Option[SalesforceAuth]) {

  def transactorConfig[F[_]: ConcurrentEffect: Timer: ContextShift](
    getAuth: GetAuth[F]): F[Either[String, TransactorConfig]] = {

    def getConfigForAuthKey(
      authKey: UUID, 
      emailGetter: Credentials.Token => F[Option[String]]
    ): F[Either[String, TransactorConfig]] = 
        (
          for {
            token <- EitherT(UserInfoGetter.getToken[F](getAuth, authKey))
            email <- EitherT.fromOptionF(
              emailGetter(token),
                "Querying user info using the token acquired via the auth key did not yield an email. Check the scopes granted to the token."
            )
          } yield AvalancheTransactorConfig.fromToken(connectionUri, Username(email), token)
        ).value

    (password, googleAuth, salesforceAuth) match {
      case (Some(password), None, None) => 
        AvalancheTransactorConfig.fromUsernamePassword(connectionUri, username, password).asRight[String].pure[F]

      case (None, Some(GoogleAuth(authKey)), None) => 
        getConfigForAuthKey(authKey, UserInfoGetter.fromGoogle[F](_))

      case (None, None, Some(SalesforceAuth(authKey))) => 
        getConfigForAuthKey(authKey, UserInfoGetter.fromSalesforce[F](_))

      case _ => 
          "Must specify exactly one of: 'username'+'clusterPassword', 'googleAuthId' or 'salesforceAuthId'".asLeft[TransactorConfig].pure[F]
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
    token: Credentials.Token
  ): TransactorConfig = 
    fromDetails(
      connectionUrl, 
      username, 
      s"access_token=${new String(token.toByteArray, utf8)}"
    )

}
