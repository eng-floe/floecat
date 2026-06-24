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

package ai.floedb.floecat.flight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Context;
import org.jboss.logging.MDC;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ContextSnapshotTest {
  private static final Context.Key<String> TEST_KEY = Context.key("flight-context-test-key");

  @AfterEach
  void clearMdc() {
    MDC.clear();
  }

  @Test
  void capturePreservesGrpcValuesWithoutCancellationDomain() throws Exception {
    Context.CancellableContext cancellable =
        Context.current().withValue(TEST_KEY, "value").withCancellation();
    Context previous = cancellable.attach();
    ContextSnapshot snapshot;
    try {
      snapshot = ContextSnapshot.capture();
    } finally {
      cancellable.detach(previous);
    }
    cancellable.cancel(new RuntimeException("cancelled"));

    try (AutoCloseable ignored = snapshot.apply()) {
      assertEquals("value", TEST_KEY.get());
      assertFalse(Context.current().isCancelled());
    }
  }

  @Test
  void applyRestoresPreviousMdc() throws Exception {
    MDC.put("query_id", "before");
    ContextSnapshot snapshot;
    MDC.put("query_id", "captured");
    snapshot = ContextSnapshot.capture();
    MDC.put("query_id", "before");

    try (AutoCloseable ignored = snapshot.apply()) {
      assertEquals("captured", MDC.get("query_id"));
    }

    assertEquals("before", MDC.get("query_id"));
  }

  @Test
  void applyRestoresPreviousOpenTelemetryContext() throws Exception {
    io.opentelemetry.context.ContextKey<String> key =
        io.opentelemetry.context.ContextKey.named("flight-otel-test-key");
    ContextSnapshot snapshot;
    try (io.opentelemetry.context.Scope ignored =
        io.opentelemetry.context.Context.current().with(key, "captured").makeCurrent()) {
      snapshot = ContextSnapshot.capture();
    }

    assertNull(io.opentelemetry.context.Context.current().get(key));
    try (AutoCloseable ignored = snapshot.apply()) {
      assertEquals("captured", io.opentelemetry.context.Context.current().get(key));
    }
    assertNull(io.opentelemetry.context.Context.current().get(key));
  }
}
