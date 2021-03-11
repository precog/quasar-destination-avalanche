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

import scala.{Int, None, Some, StringContext}
import scala.concurrent.duration._

import java.lang.String
import java.net.URI

import quasar.lib.jdbc.{JdbcDriverConfig, TransactorConfig}

object AvalancheTransactorConfig {
  val IngresDriverFqcn: String = "com.ingres.jdbc.IngresDriver"
  // Avalanche closes the connection after 4 minutes so we set a connection lifetime of 3 minutes.
  val MaxLifetime: FiniteDuration = 3.minutes
  val PoolSize: Int = 8

  def apply(
      connectionUrl: URI,
      username: Username,
      password: ClusterPassword)
      : TransactorConfig = {

    val fullUrl = {
      val u = connectionUrl.toString
      val auth = s"UID=${username.asString};PWD=${password.asString}"

      if (u.endsWith(";"))
        URI.create(u + auth)
      else
        URI.create(u + ";" + auth)
    }

    val driverConfig =
      JdbcDriverConfig.JdbcDriverManagerConfig(fullUrl, Some(IngresDriverFqcn))

    TransactorConfig(driverConfig, None)
  }
}
