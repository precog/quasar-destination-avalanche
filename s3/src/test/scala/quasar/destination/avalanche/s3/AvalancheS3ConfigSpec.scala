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

package quasar.destination.avalanche.s3

import quasar.destination.avalanche._

import java.net.URI

import argonaut._, Argonaut._

import org.http4s.syntax.literals._

import org.specs2.mutable.Specification

import quasar.blobstore.s3._

import scala.StringContext

object AvalancheS3ConfigSpec extends Specification {
  import WriteMode._

  "avalanche-s3 encodes and decodes a valid config with write mode create" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "username" := "myuser",
      "clusterPassword" := "mypassword",
      "writeMode" := "create")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.UsernamePassword(Username("myuser"), ClusterPassword("mypassword")),
        Create)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }

  "avalanche-s3 parses and prints a valid legacy config without username" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "clusterPassword" := "super secret",
      "writeMode" := "truncate")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.UsernamePassword(Username("dbuser"), ClusterPassword("super secret")),
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }

  "avalanche-s3 parses and prints a valid config" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "username" := "myuser",
      "clusterPassword" := "super secret",
      "writeMode" := "truncate")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.UsernamePassword(Username("myuser"), ClusterPassword("super secret")),
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }


  "avalanche-s3 parses and prints a valid config with external auth" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "externalAuth" := Json.obj(
        "authId" := "00000000-0000-0000-0000-000000000000",
        "userinfoUri" := "https://potato.tomato.com/userinfo",
        "userinfoUidField" := "email"),
      "writeMode" := "truncate")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        AvalancheAuth.ExternalAuth(UUID0, uri"https://potato.tomato.com/userinfo", "email"),
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }

}
