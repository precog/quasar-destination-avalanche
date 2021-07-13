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

import quasar.lib.jdbc.{JdbcDriverConfig, TransactorConfig, ManagedTransactor}
import quasar.connector.{Credentials, GetAuth}

import java.lang.String
import java.net.URI
import java.nio.charset.Charset

import scala.{Int, None, Some, StringContext, Either}
import scala.concurrent.duration._

import cats.syntax.all._

import cats.effect.{Resource, Concurrent, ContextShift, Timer}

import doobie.Transactor

case class AvalancheTransactorConfig(
  connectionUri: URI,
  auth: AvalancheAuth) {

  def transactorAcquirer[F[_]: Concurrent: ContextShift: Timer](debugId: String, getAuth: GetAuth[F]): Resource[F, Either[String, F[Transactor[F]]]] = auth match {
    case AvalancheAuth.UsernamePassword(username, password) =>

      ManagedTransactor[F](
        debugId, 
        AvalancheTransactorConfig.fromUsernamePassword(
          connectionUri, 
          username, 
          password))
        .map(_.pure[F].asRight[String])

    case ext: AvalancheAuth.ExternalAuth =>
      TransactorManager(
        ext,
        getAuth,
        (email, token) => {
          val conf = AvalancheTransactorConfig.fromToken(
            connectionUri, 
            Username(email.asString), 
            token)
          ManagedTransactor[F](debugId, conf)
        })
        .attemptNarrow[TransactorManager.Error].map(_.leftMap(_.message))

  }
}

object AvalancheTransactorConfig {
  val IngresDriverFqcn: String = "com.ingres.jdbc.IngresDriver"
  // Avalanche closes the connection after 4 minutes so we set a connection lifetime of 3 minutes.
  val MaxLifetime: FiniteDuration = 3.minutes
  val PoolSize: Int = 8

  private[this] def appendJdbcParamString(jdbcUri: URI, params: String): URI = {
      val u = jdbcUri.toString

      if (u.endsWith(";"))
        URI.create(u + params)
      else
        URI.create(u + ";" + params)
  }

  private[this] def fromDetails(
      connectionUrl: URI,
      username: Username,
      password: String)
      : TransactorConfig = {

    val fullUrl = appendJdbcParamString(connectionUrl, s"UID=${username.asString};PWD=$password")

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
      : TransactorConfig = {

    // for token auth the auth_type must be set to "browser"
    // If any 'auth_type' is already set we don't override,
    // but we do append `auth_type=browser' if it is missing
    val fullUri = 
      if (connectionUrl.toString.matches(raw"^.*(/|;)auth_type=[^;]+.*$$"))
        connectionUrl 
      else 
        appendJdbcParamString(connectionUrl, "auth_type=browser")

    fromDetails(
      fullUri, 
      username, 
      s"access_token=${new String(token.toByteArray, utf8)}")
  }

}
