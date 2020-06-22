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

import scala._, Predef._

import java.net.{URI, URISyntaxException}

import argonaut._, Argonaut._

import cats.implicits._

import quasar.blobstore.s3.{AccessKey, Bucket, Region, SecretKey}
import quasar.destination.avalanche.{Json => J, WriteMode}
import quasar.plugin.jdbc.Redacted

final case class Username(value: String)
final case class ClusterPassword(value: String)

final case class BucketConfig(
    bucket: Bucket,
    accessKey: AccessKey,
    secretKey: SecretKey,
    region: Region)

final case class AvalancheS3Config(
    bucketConfig: BucketConfig,
    connectionUri: URI,
    username: Username,
    clusterPassword: ClusterPassword,
    writeMode: WriteMode) {

  def sanitized: AvalancheS3Config =
    copy(
      clusterPassword =
        ClusterPassword(Redacted),
      bucketConfig =
        BucketConfig(
          bucketConfig.bucket,
          AccessKey(Redacted),
          SecretKey(Redacted),
          Region(Redacted)))
}

object AvalancheS3Config {

  val DbUser: Username = Username("dbuser")

  implicit val decodeUsername: DecodeJson[Username] = J.decodeOrDefault(jdecode1(Username(_)), DbUser)
  implicit val decodeClusterPassword: DecodeJson[ClusterPassword] = jdecode1(ClusterPassword(_))

  implicit val encodeUsername: EncodeJson[Username] = jencode1(_.value)
  implicit val encodeClusterPassword: EncodeJson[ClusterPassword] = jencode1(_.value)

  private implicit val uriCodecJson: CodecJson[URI] =
    CodecJson(
      uri => Json.jString(uri.toString),
      c => for {
        uriStr <- c.jdecode[String]
        uri0 = Either.catchOnly[URISyntaxException](new URI(uriStr))
        uri <- uri0.fold(
          ex => DecodeResult.fail(s"Invalid URI: ${ex.getMessage}", c.history),
          DecodeResult.ok(_))
      } yield uri)

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

  implicit def avalancheConfigCodecJson: CodecJson[AvalancheS3Config] =
    casecodec5[BucketConfig, URI, Username, ClusterPassword, WriteMode, AvalancheS3Config](
      AvalancheS3Config.apply,
      AvalancheS3Config.unapply)(
      "bucketConfig", "connectionUri", "username", "clusterPassword", "writeMode")
}
