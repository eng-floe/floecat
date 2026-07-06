/*
 * Copyright 2026 Yellowbrick Data, Inc.
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

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;

/** Resolves Floecat Flight call context from the current shared gRPC request context. */
final class GrpcResolvedCallContexts {

  private GrpcResolvedCallContexts() {}

  static ResolvedCallContext currentOrUnauthenticated() {
    return ResolvedCallContexts.currentOrUnauthenticated();
  }
}
