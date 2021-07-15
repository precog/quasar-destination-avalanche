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

package quasar.destination.avalanche.azure

import quasar.destination.avalanche._
import quasar.destination.avalanche.json._

import java.net.URI
import scala._, Predef.String

import quasar.blobstore.azure.{
  AccountName,
  AzureCredentials,
  ClientId,
  ClientSecret,
  Config,
  ContainerName,
  DefaultConfig,
  StorageUrl,
  TenantId
}
import quasar.lib.jdbc.Redacted

import argonaut._, Argonaut._
import cats.implicits._

final case class AvalancheAzureConfig(
    accountName: AccountName,
    containerName: ContainerName,
    connectionUri: URI,
    auth: AvalancheAuth,
    writeMode: WriteMode,
    azureCredentials: AzureCredentials.ActiveDirectory) {

  def sanitized: AvalancheAzureConfig =
    copy(
      azureCredentials =
        AzureCredentials.ActiveDirectory(
          ClientId(Redacted),
          TenantId(Redacted),
          ClientSecret(Redacted)),
      auth = auth.sanitized)
}

object AvalancheAzureConfig {

  def toConfig(config: AvalancheAzureConfig): Config =
    DefaultConfig(
      config.containerName,
      config.azureCredentials.some,
      storageUrl(config.accountName),
      None)

  private def storageUrl(accountName: AccountName): StorageUrl =
    StorageUrl(s"https://${accountName.value}.blob.core.windows.net")

  private implicit val activeDirectoryCodecJson: CodecJson[AzureCredentials.ActiveDirectory] =
    casecodec3[String, String, String, AzureCredentials.ActiveDirectory](
      (clientId, tenantId, clientSecret) =>
        AzureCredentials.ActiveDirectory(
          ClientId(clientId),
          TenantId(tenantId),
          ClientSecret(clientSecret)),
      ad => (ad.clientId.value, ad.tenantId.value, ad.clientSecret.value).some)(
      "clientId", "tenantId", "clientSecret")

  implicit def AvalancheAzureConfigCodecJson: CodecJson[AvalancheAzureConfig] =
    CodecJson({ (c: AvalancheAzureConfig) =>
        (("accountName" := c.accountName.value) ->:
          ("containerName" := c.containerName.value) ->:
          ("connectionUri" := c.connectionUri) ->:
          ("writeMode" := c.writeMode) ->:
          ("credentials" := c.azureCredentials) ->:
          jEmptyObject)
          .deepmerge(c.auth.asJson)
      },
      (c => for {
         accountName <- (c --\ "accountName").as[String]
         containerName <- (c --\ "containerName").as[String]
         connectionUri <- (c --\ "connectionUri").as[URI]
         auth <- c.as[AvalancheAuth]
         writeMode <- (c --\ "writeMode").as[Option[WriteMode]]
         credentials <- (c --\ "credentials").as[AzureCredentials.ActiveDirectory]
       } yield AvalancheAzureConfig(
         AccountName(accountName),
         ContainerName(containerName),
         connectionUri,
         auth,
         writeMode.getOrElse(WriteMode.Replace),
         credentials)))
}
