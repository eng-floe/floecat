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

package ai.floedb.floecat.service.context;

import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

/**
 * Resolves the engine context declared by the current call.
 *
 * <p>Reads the {@link ResolvedCallContexts} carrier first — the {@code io.grpc.Context} keys alone
 * are unreliable across Quarkus's worker thread-hops, and an engine context that silently reads
 * back empty makes engine-gated system objects unresolvable (eng-floe/floecat#361).
 */
@ApplicationScoped
public final class EngineContextProvider {

  public String engineKind() {
    return engineContext().engineKind();
  }

  public String engineVersion() {
    return engineContext().engineVersion();
  }

  public String normalizedKind() {
    return engineContext().normalizedKind();
  }

  public String normalizedVersion() {
    return engineContext().normalizedVersion();
  }

  public String effectiveEngineKind() {
    return engineContext().effectiveEngineKind();
  }

  public boolean isPresent() {
    return engineContext().hasEngineKind();
  }

  public EngineContext engineContext() {
    ResolvedCallContext resolved = ResolvedCallContexts.currentOrNull();
    if (resolved != null) {
      return resolved.engineContext();
    }
    EngineContext ctx = InboundContextInterceptor.ENGINE_CONTEXT_KEY.get();
    if (ctx != null) {
      return ctx;
    }
    String kind = Optional.ofNullable(InboundContextInterceptor.ENGINE_KIND_KEY.get()).orElse("");
    String version =
        Optional.ofNullable(InboundContextInterceptor.ENGINE_VERSION_KEY.get()).orElse("");
    EngineContext fallback = EngineContext.of(kind, version);
    if (!fallback.hasEngineKind()) {
      // MDC carrying an engine kind proves the request declared an engine, so an empty engine
      // context here is the exact condition that silently un-resolves engine-gated system
      // objects (eng-floe/floecat#361).
      ResolvedCallContexts.warnOnChannelDisagreement(
          "floecat_engine_kind", "no channel carries an engine context");
    }
    return fallback;
  }
}
