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

package quasar.destination.avalanche

import quasar.blobstore.azure.{
  AccountName,
  AzureCredentials,
  ClientId,
  ClientSecret,
  ContainerName,
  TenantId
}

import argonaut._, Argonaut._

import java.net.URI

import org.specs2.mutable.Specification

object AvalancheConfigSpec extends Specification {
  "parses a valid config" >> {
    val testConfig = Json.obj(
      "accountName" := "foo",
      "containerName" := "bar",
      "connectionUri" := "jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;",
      "clusterPassword" := "super secret",
      "credentials" := Json.obj(
        "clientId" := "client-id-uuid",
        "tenantId" := "tenant-id-uuid",
        "clientSecret" := "client-secret-string"))

    testConfig.as[AvalancheConfig].result must beRight(
      AvalancheConfig(
        AccountName("foo"),
        ContainerName("bar"),
        new URI("jdbc:ingres://cluster-id.azure.actiandatacloud.com:27839/db;encryption=on;"),
        ClusterPassword("super secret"),
        AzureCredentials.ActiveDirectory(
          ClientId("client-id-uuid"),
          TenantId("tenant-id-uuid"),
          ClientSecret("client-secret-string"))))
  }
}
