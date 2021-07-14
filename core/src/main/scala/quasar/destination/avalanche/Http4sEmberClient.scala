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

import scala.concurrent.duration.Duration

import cats.effect.{Concurrent, Timer, ContextShift, Resource}

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

object Http4sEmberClient {
  
  def client[F[_]: Concurrent: Timer: ContextShift]: Resource[F, Client[F]] = 
    EmberClientBuilder
      .default[F]
      .withMaxTotal(400)
      .withMaxPerKey(_ => 200)
      .withTimeout(Duration.Inf)
      .build

}
