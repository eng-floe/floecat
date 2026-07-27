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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.context.impl.ResolvedCallContexts;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.Test;

/**
 * Off-thread propagation is the whole point of {@link PropagatedContext}: a fan-out worker never
 * inherits the request thread's thread-locals, so without re-establishing them the ambient reads
 * ({@code engineContext()}, the log MDC) read empty — which is how an engine-gated multi-input
 * batch silently misclassified before this existed (eng-floe/floecat#361). These tests capture on a
 * context-bearing thread and assert the body sees that context on a plain executor thread that
 * carries none of its own.
 */
class PropagatedContextTest {

  private static ResolvedCallContext resolvedWithEngine(String engineKind) {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(),
        "query-1",
        "corr-1",
        EngineContext.of(engineKind, "16.0"),
        null,
        null);
  }

  @Test
  void reEstablishesAmbientContextOnAForeignThread() throws Exception {
    ResolvedCallContext resolved = resolvedWithEngine("duckdb");
    // A single-thread pool that never saw the request: its thread-locals are empty until supply()
    // re-establishes them, so a wrong read here would be the real regression.
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    try {
      String engineKind =
          ResolvedCallContexts.callWith(
              resolved,
              () -> {
                PropagatedContext captured = PropagatedContext.capture();
                return foreign
                    .submit(
                        () ->
                            captured.supply(
                                () -> {
                                  ResolvedCallContext seen = ResolvedCallContexts.currentOrNull();
                                  assertThat(seen).isNotNull();
                                  // MDC is derived from the call context, so off-thread log lines
                                  // carry the request's ids too.
                                  assertThat(MDC.get("floecat_engine_kind")).isEqualTo("duckdb");
                                  assertThat(MDC.get("correlation_id")).isEqualTo("corr-1");
                                  return seen.engineContext().engineKind();
                                }))
                    .get();
              });
      assertThat(engineKind).isEqualTo("duckdb");
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void clearsMdcAfterTheBodySoAPooledThreadDoesNotLeakIt() throws Exception {
    ResolvedCallContext resolved = resolvedWithEngine("spark");
    ExecutorService foreign = Executors.newSingleThreadExecutor();
    try {
      // MDC.get is typed Object, so keep the carrier Object too rather than casting.
      Object mdcAfterBody =
          ResolvedCallContexts.callWith(
              resolved,
              () -> {
                PropagatedContext captured = PropagatedContext.capture();
                return foreign
                    .submit(
                        () -> {
                          captured.run(
                              () -> assertThat(MDC.get("floecat_engine_kind")).isEqualTo("spark"));
                          // Same pooled thread, outside the body: the request's ids must be gone so
                          // the next unrelated task cannot inherit them.
                          return MDC.get("floecat_engine_kind");
                        })
                    .get();
              });
      assertThat(mdcAfterBody).isNull();
    } finally {
      foreign.shutdownNow();
    }
  }

  @Test
  void offAnyRequestTheBodyStillRuns() {
    // Captured with no ambient request (startup, unit tests): supply must run the body rather than
    // fail, and no context is fabricated.
    PropagatedContext captured = PropagatedContext.capture();
    assertThat(captured.supply(() -> "ran")).isEqualTo("ran");
    assertThat(ResolvedCallContexts.currentOrNull()).isNull();
  }
}
