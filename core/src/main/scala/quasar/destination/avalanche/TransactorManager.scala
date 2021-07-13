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

import quasar.connector.{Credentials, GetAuth, ExternalCredentials, Expires}

import java.lang.{String, Exception, Throwable}

import scala.{Either, Right, Left, Some, None}

import cats.MonadError
import cats.implicits._
import cats.effect.{Resource, Clock, Concurrent, ContextShift, Timer}
import cats.effect.concurrent.Ref

import doobie.Transactor

import fs2.Hotswap

import org.http4s.client.Client

object TransactorManager {

  // Technically can occur at any acquisition of a transactor, but practically will most likely
  // only occur at init time
  case class Error(message: String) extends Exception(message)

  private def verifyCreds(cred: Credentials): Either[Error, Credentials.Token] = cred match {
    case t: Credentials.Token => Right(t)
    case _ => Left(Error(
      "Unsupported auth type provided by the configured auth key; only `Token` credentials are supported"))
  }

  def acquireValidToken[F[_]: MonadError[?[_], Throwable]: Clock](
      token: F[ExternalCredentials.Temporary[F]])
      : F[Expires[Credentials.Token]] = 
    for {
      ExternalCredentials.Temporary(acquire, renew) <- token
      tempCreds  <- acquire
      (creds, expiresAt) <- tempCreds.nonExpired.flatMap {
        case Some(c) => (c, tempCreds.expiresAt).pure[F]
        case None => 
          renew >>
            acquire
              .flatMap { v => 
                v.nonExpired
                  .flatMap(_.liftTo[F](Error("Failed to acquire a non-expired token using the provided external auth ID")))
                   .map((_, v.expiresAt))
              }
      }
      token <- verifyCreds(creds).liftTo[F]
    } yield Expires(token, expiresAt)
  

  def apply[F[_]: Concurrent: Timer: ContextShift](
      auth: AvalancheAuth.ExternalAuth,
      getAuth: GetAuth[F],
      buildTransactor: (Email, Credentials.Token) => Resource[F, Transactor[F]])
      : Resource[F, F[Transactor[F]]] = {

    val AvalancheAuth.ExternalAuth(authId, userinfoUri, userinfoField) = auth

    val token: F[ExternalCredentials.Temporary[F]] = 
      getAuth(authId)
        .flatMap(_.liftTo[F](Error("No credentials were returned by the configured external auth ID")))
        .map {
          case ExternalCredentials.Perpetual(t) =>
            // Shouldn't really happen, but not particularly a problem if it does
            ExternalCredentials.Temporary[F](
              Expires(t, None).pure[F], 
              ().pure[F])
            
          case t: ExternalCredentials.Temporary[F] => t
        }

    def getEmail(client: Client[F], token: Credentials.Token): F[Email] = 
      UserInfoGetter.emailFromUserinfo[F](client, token, userinfoUri, userinfoField)
        .flatMap(_.liftTo[F](Error(
          "Querying user info using the token acquired via the auth key did not yield an email. Check whether the userinfoField is correctly set.")))

    for {
      expiringToken <- Resource.eval(acquireValidToken[F](token))
      client <- Http4sEmberClient.client[F]
      email <- Resource.eval(getEmail(client, expiringToken.value))
      (hotswap, firstTransactor) <- Hotswap(buildTransactor(email, expiringToken.value))
      lastTransactorRef <- Resource.eval(Ref[F].of(firstTransactor))
      lastTokenRef <- Resource.eval(Ref[F].of(expiringToken))
    } yield 
      expiringToken.isExpired[F].flatMap(isExpired => 
        if (isExpired)
          for {
            newToken <- acquireValidToken(token)
            newEmail <- getEmail(client, newToken.value) 
            newTransactor <- hotswap.swap(buildTransactor(newEmail, newToken.value))
            _ <- lastTransactorRef.set(newTransactor)
            _ <- lastTokenRef.set(newToken)
          } yield newTransactor
        else lastTransactorRef.get)
  }

}
