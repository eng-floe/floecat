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

package ai.floedb.floecat.service.security.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import io.grpc.Context;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Resolves the principal for the current call.
 *
 * <p>The principal travels as part of the whole {@link ResolvedCallContext} carried by {@link
 * ResolvedCallContexts} (explicit scope, then the per-call Vert.x duplicated-context local written
 * by the inbound interceptor, then the {@code io.grpc.Context} keys). This method prefers that
 * carrier and falls back to reading {@link #KEY} directly for callers that drive {@link
 * io.grpc.Context} without the interceptor (e.g. unit tests), and to the empty default when no
 * channel carries a principal.
 *
 * <p>The carrier exists because {@link io.grpc.Context} is not reliable across Quarkus's worker
 * thread-hops: {@code io.grpc.override.ContextStorageOverride} stores the attached context in one
 * shared per-call slot that several threads of the same call race, so {@link #KEY} can read back
 * empty (historically surfacing as a misleading {@code PERMISSION_DENIED}) or — via its
 * thread-local fallback on a reused worker — stale. See {@link ResolvedCallContexts}.
 */
@ApplicationScoped
public class PrincipalProvider {
  public static final Context.Key<PrincipalContext> KEY = Context.key("principal");

  /** Returns the principal for the current call, or the empty default when none is carried. */
  public PrincipalContext get() {
    ResolvedCallContext resolved = ResolvedCallContexts.currentOrNull();
    if (resolved != null) {
      return resolved.principalContext();
    }
    PrincipalContext fromGrpc = KEY.get();
    if (fromGrpc != null) {
      return fromGrpc;
    }
    return PrincipalContext.getDefaultInstance();
  }
}
