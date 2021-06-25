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

import org.specs2.mutable.Specification

import quasar.blobstore.s3._

import scala.{Some, None}

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
        Username("myuser"),
        Some(ClusterPassword("mypassword")),
        None,
        None,
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
        Username("dbuser"),
        Some(ClusterPassword("super secret")),
        None,
        None,
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
        Username("myuser"),
        Some(ClusterPassword("super secret")),
        None,
        None,
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }


  "avalanche-s3 parses and prints a valid config with google auth" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "googleAuthId" := "00000000-0000-0000-0000-000000000000",
      "writeMode" := "truncate")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        Username.DbUser,
        None,
        Some(GoogleAuth(UUID0)),
        None,
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }

  "avalanche-s3 parses and prints a valid config with salesforce auth" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "credentials" := Json.obj(
          "accessKey" := "aws-access-key",
          "secretKey" := "aws-secret-key",
          "region" := "us-east-1")),
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "salesforceAuthId" := "00000000-0000-0000-0000-000000000000",
      "writeMode" := "truncate")

    val cfg =
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        Username.DbUser,
        None,
        None,
        Some(SalesforceAuth(UUID0)),
        Truncate)

    initialJson.as[AvalancheS3Config].result must beRight(cfg)

    cfg.asJson.as[AvalancheS3Config].result must beRight(cfg)
  }
}
