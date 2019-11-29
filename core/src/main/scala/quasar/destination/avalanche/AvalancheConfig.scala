/*
 * Copyright 2014–2019 SlamData Inc.
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

import scala.Predef._
import scala._

import java.net.{URI, URISyntaxException}

import quasar.blobstore.azure.{
  AzureCredentials,
  ClientId,
  ClientSecret,
  Config,
  ContainerName,
  DefaultConfig,
  StorageUrl,
  TenantId
}

import argonaut._, Argonaut._

import cats.implicits._

final case class ClusterPassword(value: String)

final case class AvalancheConfig(
  containerName: ContainerName,
  storageUrl: StorageUrl,
  connectionUri: URI,
  password: ClusterPassword,
  azureCredentials: AzureCredentials.ActiveDirectory)


object AvalancheConfig {
  def toConfig(config: AvalancheConfig): Config =
    DefaultConfig(
      config.containerName,
      config.azureCredentials.some,
      config.storageUrl,
      None)

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

  private implicit val activeDirectoryCodecJson: CodecJson[AzureCredentials.ActiveDirectory] =
    casecodec3[String, String, String, AzureCredentials.ActiveDirectory](
      (clientId, tenantId, clientSecret) =>
        AzureCredentials.ActiveDirectory(
          ClientId(clientId),
          TenantId(tenantId),
          ClientSecret(clientSecret)),
      ad => (ad.clientId.value, ad.tenantId.value, ad.clientSecret.value).some)(
      "clientId", "tenantId", "clientSecret")

  implicit val avalancheConfigCodecJson: CodecJson[AvalancheConfig] =
    casecodec5[String, String, URI, String, AzureCredentials.ActiveDirectory, AvalancheConfig](
      (containerName, storageUrl, uri, password, creds) =>
        AvalancheConfig(
          ContainerName(containerName),
          StorageUrl(storageUrl),
          uri,
          ClusterPassword(password),
          creds),
      cfg =>
        (cfg.containerName.value,
          cfg.storageUrl.value,
          cfg.connectionUri,
          cfg.password.value,
          cfg.azureCredentials).some)(
      "containerName", "storageUrl", "connectionUri", "clusterPassword", "azureCredentials")

}
