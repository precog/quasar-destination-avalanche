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
import cats.implicits._
import java.net.{URI, URISyntaxException}
import quasar.blobstore.s3.{
  AccessKey,
  Bucket,
  Region,
  SecretKey
}
import quasar.destination.avalanche.WriteMode, WriteMode._
import scala.{Either, StringContext}
import scala.Predef.String

final case class ClusterPassword(value: String)

// sealed trait Authorization
// object Authorization {
//   final case class Keys(accessKey: AccessKey, secretKey: SecretKey, region: Region) extends Authorization
// }

final case class BucketConfig(
  bucket: Bucket,
  accessKey: AccessKey,
  secretKey: SecretKey,
  region: Region)

final case class AvalancheS3Config(
  bucketConfig: BucketConfig,
  connectionUri: URI,
  password: ClusterPassword,
  writeMode: WriteMode)

object AvalancheS3Config {

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

  private implicit val bucketConfigCodecJson: CodecJson[BucketConfig] =
    casecodec4[String, String, String, String, BucketConfig](
      (bucket, accessKey, secretKey, region) =>
        BucketConfig(Bucket(bucket), AccessKey(accessKey), SecretKey(secretKey), Region(region)),
      bc => (bc.bucket.value, bc.accessKey.value, bc.secretKey.value, bc.region.value).some)(
        "bucket", "accessKey", "secretKey", "region")

  implicit def avalancheConfigCodecJson: CodecJson[AvalancheS3Config] =
    casecodec4[BucketConfig, URI, String, WriteMode, AvalancheS3Config](
      (bucketCfg, uri, password, writeMode) =>
        AvalancheS3Config(BucketConfig(bucketCfg.bucket, bucketCfg.accessKey, bucketCfg.secretKey, bucketCfg.region),
        uri,
        ClusterPassword(password.value),
        writeMode),
      asc => (asc.bucketConfig, asc.connectionUri, asc.password.value, asc.writeMode).some)(
      "bucketConfig", "jdbcUri", "password", "writemode")
}
