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
import quasar.destination.avalanche.json._

import scala._, Predef._

import java.net.URI

import argonaut._, Argonaut._

import quasar.blobstore.s3.{AccessKey, Bucket, Region, SecretKey}
import quasar.lib.jdbc.Redacted

final case class BucketConfig(
    bucket: Bucket,
    accessKey: AccessKey,
    secretKey: SecretKey,
    region: Region)

final case class AvalancheS3Config(
    bucketConfig: BucketConfig,
    connectionUri: URI,
    auth: AvalancheAuth,
    writeMode: WriteMode) {

  def sanitized: AvalancheS3Config =
    copy(
      bucketConfig =
        BucketConfig(
          bucketConfig.bucket,
          AccessKey(Redacted),
          SecretKey(Redacted),
          Region(Redacted)),
      auth = auth.sanitized)
}

object AvalancheS3Config {

  private implicit val bucketConfigCodecJson: CodecJson[BucketConfig] = {
    val encode: BucketConfig => Json = {
      case BucketConfig(Bucket(bucket), AccessKey(accessKey), SecretKey(secretKey), Region(region)) =>
        Json(
          "bucket" -> jString(bucket),
          "credentials" -> Json(
            "accessKey" -> jString(accessKey),
            "secretKey" -> jString(secretKey),
            "region" -> jString(region)))
    }

    val decode: HCursor => DecodeResult[BucketConfig] = { root =>
      for {
        bucket <- (root --\ "bucket").as[String]
        creds = root --\ "credentials"
        accessKey <- (creds --\ "accessKey").as[String]
        secretKey <- (creds --\ "secretKey").as[String]
        region <- (creds --\ "region").as[String]
      } yield BucketConfig(Bucket(bucket), AccessKey(accessKey), SecretKey(secretKey), Region(region))
    }

    CodecJson[BucketConfig](encode, decode)
  }

  implicit def avalancheHttpConfigCodecJson: CodecJson[AvalancheS3Config] =
    CodecJson({ (c: AvalancheS3Config) =>
        (("bucketConfig" := c.bucketConfig) ->:
          ("connectionUri" := c.connectionUri) ->:
          ("writeMode" := c.writeMode) ->:
          jEmptyObject)
          .deepmerge(c.auth.asJson)
      },
      (c => for {
         connectionUri <- (c --\ "connectionUri").as[URI]
         auth <- c.as[AvalancheAuth]
         writeMode <- (c --\ "writeMode").as[WriteMode]
         bucketConfig <- (c --\ "bucketConfig").as[BucketConfig]
       } yield AvalancheS3Config(
         bucketConfig,
         connectionUri,
         auth,
         writeMode)))

}
