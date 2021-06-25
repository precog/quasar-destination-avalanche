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
    username: Username,
    password: Option[ClusterPassword],
    googleAuth: Option[GoogleAuth],
    salesforceAuth: Option[SalesforceAuth],
    writeMode: WriteMode,
    azureCredentials: AzureCredentials.ActiveDirectory) {

  def sanitized: AvalancheAzureConfig =
    copy(
      azureCredentials =
        AzureCredentials.ActiveDirectory(
          ClientId(Redacted),
          TenantId(Redacted),
          ClientSecret(Redacted)),
      password =
        password.as(ClusterPassword(Redacted)),
      googleAuth = googleAuth.map(_.sanitized),
      salesforceAuth = salesforceAuth.map(_.sanitized))
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
        ("accountName" := c.accountName.value) ->:
        ("containerName" := c.containerName.value) ->:
        ("connectionUri" := c.connectionUri) ->:
        ("username" := c.username) ->:
        ("clusterPassword" :=? c.password) ->?:
        ("googleAuthId" :=? c.googleAuth) ->?:
        ("salesforceAuthId" :=? c.salesforceAuth) ->?:
        ("writeMode" := c.writeMode) ->:
        ("credentials" := c.azureCredentials) ->:
        jEmptyObject,
      },
      (c => for {
         accountName <- (c --\ "accountName").as[String]
         containerName <- (c --\ "containerName").as[String]
         connectionUri <- (c --\ "connectionUri").as[URI]
         username <- (c --\ "username").as[Username]
         clusterPassword <- (c --\ "clusterPassword").as[Option[ClusterPassword]]
         googleAuth <- (c --\ "googleAuthId").as[Option[GoogleAuth]]
         salesforceAuth <- (c --\ "salesforceAuthId").as[Option[SalesforceAuth]]
         writeMode <- (c --\ "writeMode").as[Option[WriteMode]]
         credentials <- (c --\ "credentials").as[AzureCredentials.ActiveDirectory]
       } yield AvalancheAzureConfig(
         AccountName(accountName),
         ContainerName(containerName),
         connectionUri,
         username,
         clusterPassword,
         googleAuth,
         salesforceAuth,
         writeMode.getOrElse(WriteMode.Replace),
         credentials)))
}
