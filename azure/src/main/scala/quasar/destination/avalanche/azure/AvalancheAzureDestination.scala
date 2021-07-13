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

import scala._, Predef._

import cats.data.Kleisli
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.effect.concurrent.Ref
import cats.implicits._

import com.azure.storage.blob.BlobContainerAsyncClient

import doobie.Transactor

import fs2.Stream

import java.net.URI

import org.slf4s.Logger

import pathy.Path.FileName

import quasar.blobstore.BlobstoreStatus
import quasar.blobstore.azure.{AzureDeleteService, AzurePutService, Expires, TenantId}
import quasar.blobstore.paths.BlobPath
import quasar.connector.destination.Destination
import quasar.connector.MonadResourceErr

object AvalancheAzureDestination {
  def apply[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: AvalancheAzureConfig,
      clientMgr: F[(Ref[F,Expires[BlobContainerAsyncClient]], F[Unit])],
      acquireXa: F[Transactor[F]],
      logger: Logger)
      : Destination[F] = {

    val authCfg = config.azureCredentials

    val authParams = Map(
      "AZURE_CLIENT_ENDPOINT" -> authEndpoint(authCfg.tenantId),
      "AZURE_CLIENT_ID" -> authCfg.clientId.value,
      "AZURE_CLIENT_SECRET" -> authCfg.clientSecret.value)

    val stagedUri: FileName => URI =
      n => URI.create(s"abfs://${config.containerName.value}@${config.accountName.value}.dfs.core.windows.net/${n.value}")

    val putService =
      Kleisli[F, (BlobPath, Stream[F, Byte]), Int] { args =>
        for {
          (refClient, refreshToken) <- clientMgr
          _ <- refreshToken
          containerClient <- refClient.get
          put = AzurePutService.mk[F](containerClient.value)
          result <- put(args)
        } yield result
      }

    val deleteService =
      Kleisli[F, BlobPath, BlobstoreStatus] { path =>
        for {
          (refClient, refreshToken) <- clientMgr
          _ <- refreshToken
          containerClient <- refClient.get
          delete = AzureDeleteService.mk[F](containerClient.value)
          result <- delete(path)
        } yield result
      }

    new StagedAvalancheDestination[F](
      AvalancheAzureDestinationModule.destinationType,
      putService,
      deleteService,
      stagedUri,
      authParams,
      config.writeMode,
      acquireXa,
      logger)
  }

  ////

  private def authEndpoint(tid: TenantId): String = {
    val tenant = removeSingleQuotes(tid.value)
    s"https://login.microsoftonline.com/$tenant/oauth2/token"
  }

  private def removeSingleQuotes(str: String): String =
    str.replace("'", "")
}
