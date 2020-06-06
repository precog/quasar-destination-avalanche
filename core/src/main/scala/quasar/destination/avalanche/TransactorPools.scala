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

import cats.effect.{Blocker, Resource, Sync}
import quasar.{concurrent => qc}
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.Predef.String
import scala.Int

object TransactorPools {

  def boundedPool[F[_]: Sync](name: String, threadCount: Int): Resource[F, ExecutionContext] =
    Resource.make(
      Sync[F].delay(
        Executors.newFixedThreadPool(
          threadCount,
          qc.NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor(_))

  def unboundedPool[F[_]: Sync](name: String): Resource[F, Blocker] =
    Resource.make(
      Sync[F].delay(
        Executors.newCachedThreadPool(
          qc.NamedDaemonThreadFactory(name))))(es => Sync[F].delay(es.shutdown()))
      .map(es => qc.Blocker(ExecutionContext.fromExecutor(es)))

}