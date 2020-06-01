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

import argonaut._, Argonaut._
import java.net.URI
import org.specs2.mutable.Specification
import quasar.blobstore.s3._
import quasar.destination.avalanche.WriteMode._

object AvalancheS3ConfigSpec extends Specification {
  "avalanche-s3 encodes and decodes a valid config without write mode" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "accessKey" := "aws-access-key",
        "secretKey" := "aws-secret-key",
        "region" := "us-east-1"),
      "jdbcUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "password" := "mypassword",
      "writemode" := "create")

    initialJson.as[AvalancheS3Config].result must beRight(
      AvalancheS3Config(
        BucketConfig(
              Bucket("bucket-name"),
              AccessKey("aws-access-key"),
              SecretKey("aws-secret-key"),
              Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        ClusterPassword("mypassword"),
        Create))
  }

  "avalanche-s3 parses and prints a valid config with write mode" >> {
    val initialJson = Json.obj(
      "bucketConfig" := Json.obj(
        "bucket" := "bucket-name",
        "accessKey" := "aws-access-key",
        "secretKey" := "aws-secret-key",
        "region" := "us-east-1"),
      "jdbcUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "password" := "super secret",
      "writemode" := "truncate")

    initialJson.as[AvalancheS3Config].result must beRight(
      AvalancheS3Config(
        BucketConfig(
          Bucket("bucket-name"),
          AccessKey("aws-access-key"),
          SecretKey("aws-secret-key"),
          Region("us-east-1")),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        ClusterPassword("super secret"),
        Truncate))
  }
}
