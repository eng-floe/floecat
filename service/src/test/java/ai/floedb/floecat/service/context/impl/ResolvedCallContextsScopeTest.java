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
package ai.floedb.floecat.service.context.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.flight.context.ResolvedCallContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Pins the three states of the explicit call scope, which decide whose principal and engine a
 * nested body sees. They are only distinguishable by reading {@code callWith}, {@code
 * callWithoutRequestScope} and {@code currentOrNull} together, so each is asserted here rather than
 * left to a reader to infer:
 *
 * <ul>
 *   <li><b>absent</b> — no explicit scope; {@code currentOrNull} consults the fallback carriers.
 *   <li><b>present</b> — {@code callWith(resolved, …)} overrides for the body's duration.
 *   <li><b>explicitly empty</b> — {@code callWithoutRequestScope} makes {@code currentOrNull}
 *       return null even when a fallback carrier could answer.
 * </ul>
 */
class ResolvedCallContextsScopeTest {

  @Test
  void callWithOrInheritNullLeavesAnEnclosingScopeInForce() {
    // The behaviour changed here: a null argument used to CLEAR an enclosing scope (falling back to
    // the ambient carriers). It now leaves the enclosing scope alone, so a service body that grafts
    // a possibly-null context read before a thread hop cannot accidentally drop the caller's scope.
    ResolvedCallContext outer = resolved("duckdb");

    ResolvedCallContexts.callWith(
        outer,
        () -> {
          ResolvedCallContext seen =
              ResolvedCallContexts.callWithOrInherit(null, ResolvedCallContexts::currentOrNull);
          assertThat(seen)
              .as("callWithOrInherit(null, ...) must not clear the scope its caller established")
              .isSameAs(outer);
          return null;
        });
  }

  @Test
  void runWithOrInheritNullAlsoLeavesAnEnclosingScopeInForce() {
    // runWith is the void-returning twin of callWith and repeats the null check rather than
    // delegating, so callWith's coverage does not extend to it: the two can drift apart silently.
    ResolvedCallContext outer = resolved("trino");
    AtomicReference<ResolvedCallContext> seen = new AtomicReference<>();

    ResolvedCallContexts.callWith(
        outer,
        () -> {
          ResolvedCallContexts.runWithOrInherit(
              null, () -> seen.set(ResolvedCallContexts.currentOrNull()));
          return null;
        });

    assertThat(seen.get())
        .as("runWithOrInherit(null, ...) must not clear the scope its caller established")
        .isSameAs(outer);
  }

  @Test
  void callWithOrInheritNullOutsideAnyScopeLeavesTheFallbackCarriersVisible() {
    // With no enclosing scope, a null argument installs nothing, so currentOrNull still consults
    // the
    // fallback carriers — what BaseServiceImpl.run relies on when its pre-hop read returned null.
    io.grpc.Context withFallbackRequest =
        io.grpc.Context.current()
            .withValue(
                InboundContextInterceptor.PC_KEY,
                PrincipalContext.newBuilder().setSubject("from-grpc").build());

    withFallbackRequest.run(
        () -> {
          ResolvedCallContext seen =
              ResolvedCallContexts.callWithOrInherit(null, ResolvedCallContexts::currentOrNull);
          assertThat(seen).isNotNull();
          assertThat(seen.principalContext().getSubject()).isEqualTo("from-grpc");
        });
  }

  @Test
  void callWithoutRequestScopeHidesBothAnEnclosingScopeAndTheFallbackCarriers() {
    // The explicitly-empty state: distinct from "absent", and the reason the thread-local holds an
    // Optional rather than a bare context.
    ResolvedCallContext outer = resolved("spark");
    io.grpc.Context withFallbackRequest =
        io.grpc.Context.current()
            .withValue(
                InboundContextInterceptor.PC_KEY,
                PrincipalContext.newBuilder().setSubject("from-grpc").build());

    withFallbackRequest.run(
        () ->
            ResolvedCallContexts.callWith(
                outer,
                () -> {
                  ResolvedCallContext seen =
                      ResolvedCallContexts.callWithoutRequestScope(
                          ResolvedCallContexts::currentOrNull);
                  assertThat(seen)
                      .as(
                          "an explicitly empty scope beats both the enclosing scope and the carrier")
                      .isNull();
                  assertThat(ResolvedCallContexts.currentOrNull())
                      .as("and the enclosing scope is restored afterward")
                      .isSameAs(outer);
                  return null;
                }));
  }

  @Test
  void callWithAndRunWithRejectNullRatherThanSilentlyInheriting() {
    // A null used to CLEAR the enclosing scope. Keeping it a silent convention on these two would
    // mean an old caller's null flips from "isolate" to "inherit" with no error and leaks the
    // caller's scope into the body. The inherit case is now a separately named method.
    assertThatThrownBy(() -> ResolvedCallContexts.callWith(null, () -> null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> ResolvedCallContexts.runWith(null, () -> {}))
        .isInstanceOf(NullPointerException.class);
  }

  private static ResolvedCallContext resolved(String engineKind) {
    return new ResolvedCallContext(
        PrincipalContext.getDefaultInstance(),
        "query-1",
        "corr-1",
        EngineContext.of(engineKind, "1.0"),
        null,
        null);
  }
}
