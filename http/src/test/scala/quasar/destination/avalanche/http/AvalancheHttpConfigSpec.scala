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

import scala.{None, Some, StringContext}

import java.net.URI

import argonaut._, Argonaut._

import org.http4s.syntax.literals._

import org.specs2.mutable.Specification

object AvalancheHttpConfigSpec extends Specification {
  import WriteMode._

  "avalanche-http parses and prints a valid config" >> {
    val json = Json(
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "username" := "myuser",
      "clusterPassword" := "super secret",
      "writeMode" := "truncate",
      "baseUrl" := "http://example.com/precog")

    val cfg =
      AvalancheHttpConfig(
        URI.create("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.UsernamePassword(Username("myuser"), ClusterPassword("super secret")),
        Truncate,
        Some(URI.create("http://example.com/precog")))

    json.as[AvalancheHttpConfig].result must beRight(cfg)

    cfg.asJson.as[AvalancheHttpConfig].result must beRight(cfg)
  }

  "avalanche-http base URL is optional" >> {
    val json = Json(
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "username" := "myuser",
      "clusterPassword" := "super secret",
      "writeMode" := "create")

    val cfg =
      AvalancheHttpConfig(
        URI.create("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.UsernamePassword(Username("myuser"), ClusterPassword("super secret")),
        Create,
        None)

    json.as[AvalancheHttpConfig].result must beRight(cfg)

    cfg.asJson.as[AvalancheHttpConfig].result must beRight(cfg)
  }

  "avalanche-http valid external auth is parsed" >> {
    val json = Json(
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "externalAuth" := Json.obj(
        "authId" := "00000000-0000-0000-0000-000000000000",
        "userinfoUri" := "https://potato.tomato.com/userinfo"),
      "writeMode" := "create")

    val cfg =
      AvalancheHttpConfig(
        URI.create("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.ExternalAuth(UUID0, uri"https://potato.tomato.com/userinfo"),
        Create,
        None)

    json.as[AvalancheHttpConfig].result must beRight(cfg)

    cfg.asJson.as[AvalancheHttpConfig].result must beRight(cfg)
  }

}
