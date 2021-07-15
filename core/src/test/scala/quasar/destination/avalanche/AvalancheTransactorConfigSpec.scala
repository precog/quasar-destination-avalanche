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

import quasar.lib.jdbc.JdbcDriverConfig
import quasar.connector.Credentials

import org.specs2.mutable.Specification

import java.net.URI
import java.nio.charset.StandardCharsets

object AvalancheTransactorConfigSpec extends Specification {

  "AvalancheTransactorConfig fromToken" >> {

    "should format the PWD as PWD=access_token=<token>" >> {
      val conf = AvalancheTransactorConfig.fromToken(
        URI.create("jdbc:actian://potato.tomato.com:27839/db;encryption=on;auth_type=browser;"),
        Username("user"),
        Credentials.Token("token".getBytes(StandardCharsets.UTF_8)))

      conf.driverConfig must beLike { 
        case JdbcDriverConfig.JdbcDriverManagerConfig(uri, _) => 
          uri.toString() must_=== 
            "jdbc:actian://potato.tomato.com:27839/db;encryption=on;auth_type=browser;UID=user;PWD=access_token=token"
      }
    }


    "should add `auth_type` if it is not already in the url" >> {

      val conf = AvalancheTransactorConfig.fromToken(
        URI.create("jdbc:actian://potato.tomato.com:27839/db;encryption=on;"),
        Username("user"),
        Credentials.Token("token".getBytes(StandardCharsets.UTF_8)))

      conf.driverConfig must beLike { 
        case JdbcDriverConfig.JdbcDriverManagerConfig(uri, _) => 
          uri.toString() must_=== 
            "jdbc:actian://potato.tomato.com:27839/db;encryption=on;auth_type=browser;UID=user;PWD=access_token=token"
      }

    }

    "should not touch `auth_type` if it is already there" >> {
      val conf = AvalancheTransactorConfig.fromToken(
        URI.create("jdbc:actian://potato.tomato.com:27839/db;encryption=on;auth_type=potato"),
        Username("user"),
        Credentials.Token("token".getBytes(StandardCharsets.UTF_8)))

      conf.driverConfig must beLike { 
        case JdbcDriverConfig.JdbcDriverManagerConfig(uri, _) => 
          uri.toString() must_=== 
            "jdbc:actian://potato.tomato.com:27839/db;encryption=on;auth_type=potato;UID=user;PWD=access_token=token"
      }
    }
  }

  "AvalancheTransactorConfig fromUsernamePassword should format the JDBC URL correctly" >> {
    val conf = AvalancheTransactorConfig.fromUsernamePassword(
      URI.create("jdbc:actian://potato.tomato.com:27839/db;encryption=on;"),
      Username("user"),
      ClusterPassword("password"))

    conf.driverConfig must beLike { 
      case JdbcDriverConfig.JdbcDriverManagerConfig(uri, _) => 
        uri.toString() must_=== 
          "jdbc:actian://potato.tomato.com:27839/db;encryption=on;UID=user;PWD=password"
    }
  }
}
