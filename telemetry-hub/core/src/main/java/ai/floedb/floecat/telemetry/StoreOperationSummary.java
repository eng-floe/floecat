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

package ai.floedb.floecat.telemetry;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/** Request-local aggregate of store/repository work for compact trace summary events. */
public final class StoreOperationSummary {
  private static final ContextKey<Mutable> CURRENT =
      ContextKey.named("floecat-store-operation-summary");
  private static final Pattern UNSAFE_CHARS = Pattern.compile("[^a-z0-9]+");
  private static final Pattern EDGE_UNDERSCORES = Pattern.compile("^_+|_+$");

  private StoreOperationSummary() {}

  public static Context start(Context context, boolean enabled) {
    Context parent = context == null ? Context.current() : context;
    return enabled ? parent.with(CURRENT, new Mutable()) : parent;
  }

  public static void reset() {
    Mutable current = current();
    if (current != null) {
      current.reset();
    }
  }

  public static void record(String component, String operation, Duration elapsed, boolean success) {
    Mutable current = current();
    if (current != null) {
      current.record(component, operation, elapsed, success);
    }
  }

  /** Records physical backend work independently of nested repository scope suppression. */
  public static void recordBackend(
      String backend, String operation, Duration elapsed, boolean success, long bytes, long items) {
    Mutable current = current();
    if (current != null) {
      current.recordBackend(backend, operation, elapsed, success, bytes, items);
    }
  }

  public static boolean enterScope() {
    Mutable current = current();
    if (current == null) {
      return true;
    }
    return current.enterScope();
  }

  public static void exitScope() {
    Mutable current = current();
    if (current != null) {
      current.exitScope();
    }
  }

  public static void put(String key, String value) {
    Mutable current = current();
    if (current != null && key != null && !key.isBlank() && value != null && !value.isBlank()) {
      current.put(key, value);
    }
  }

  public static void put(String key, long value) {
    Mutable current = current();
    if (current != null && key != null && !key.isBlank()) {
      current.put(key, value);
    }
  }

  public static void nanos(String key, long nanos) {
    Mutable current = current();
    if (current != null && key != null && !key.isBlank()) {
      current.put(key + "_ms", Math.max(0L, nanos) / 1_000_000.0);
    }
  }

  public static void fallback(String key) {
    Mutable current = current();
    if (current != null && key != null && !key.isBlank()) {
      current.fallback(key);
    }
  }

  public static void addTo(PhaseDiagnostics diagnostics) {
    if (diagnostics == null) {
      return;
    }
    Mutable current = current();
    if (current == null) {
      return;
    }
    Snapshot snapshot = current.snapshot();
    if (snapshot.operations == 0 && snapshot.backends.isEmpty() && snapshot.values.isEmpty()) {
      return;
    }
    diagnostics.put("store_operations", snapshot.operations);
    diagnostics.put("store_errors", snapshot.errors);
    diagnostics.put("store_ms", snapshot.totalNanos / 1_000_000.0);
    diagnostics.put("repo_gets", snapshot.repoGets);
    diagnostics.put("repo_lists", snapshot.repoLists);
    diagnostics.put("repo_counts", snapshot.repoCounts);
    diagnostics.put("repo_writes", snapshot.repoWrites);
    diagnostics.put("fallbacks", snapshot.fallbacks);
    snapshot.operationCounts.forEach((key, value) -> diagnostics.put(key + "_count", value));
    snapshot.operationNanos.forEach((key, value) -> diagnostics.nanos(key, value));
    snapshot.backends.forEach(
        (key, value) -> {
          diagnostics.put(key + "_count", value.count());
          diagnostics.nanos(key, value.nanos());
          diagnostics.put(key + "_bytes", value.bytes());
          diagnostics.put(key + "_items", value.items());
          if (value.errors() > 0L) {
            diagnostics.put(key + "_errors", value.errors());
          }
        });
    snapshot.values.forEach(
        (key, value) -> {
          if (value instanceof String stringValue) {
            diagnostics.put(key, stringValue);
          } else if (value instanceof Integer intValue) {
            diagnostics.put(key, intValue.longValue());
          } else if (value instanceof Long longValue) {
            diagnostics.put(key, longValue);
          } else if (value instanceof Double doubleValue) {
            diagnostics.put(key, doubleValue);
          } else if (value instanceof Boolean boolValue) {
            diagnostics.put(key, boolValue);
          }
        });
  }

  private static String summarizeOperation(String component, String operation) {
    String normalizedComponent = sanitize(component);
    String normalizedOperation = sanitize(operation);
    if ("repository".equals(normalizedComponent)) {
      return "repo_" + normalizedOperation;
    }
    if (normalizedComponent.isBlank()) {
      return normalizedOperation;
    }
    return normalizedComponent + "_" + normalizedOperation;
  }

  private static String sanitize(String value) {
    if (value == null || value.isBlank()) {
      return "unknown";
    }
    String sanitized = UNSAFE_CHARS.matcher(value.trim().toLowerCase(Locale.ROOT)).replaceAll("_");
    return EDGE_UNDERSCORES.matcher(sanitized).replaceAll("");
  }

  private static Mutable current() {
    return Context.current().get(CURRENT);
  }

  private static final class Mutable {
    private final LinkedHashMap<String, Long> operationCounts = new LinkedHashMap<>();
    private final LinkedHashMap<String, Long> operationNanos = new LinkedHashMap<>();
    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();
    private final LinkedHashMap<String, BackendAggregate> backends = new LinkedHashMap<>();
    private long operations;
    private long errors;
    private long totalNanos;
    private long repoGets;
    private long repoLists;
    private long repoCounts;
    private long repoWrites;
    private long fallbacks;
    private final ThreadLocal<Integer> activeScopes = ThreadLocal.withInitial(() -> 0);

    private synchronized void reset() {
      operationCounts.clear();
      operationNanos.clear();
      values.clear();
      backends.clear();
      operations = 0L;
      errors = 0L;
      totalNanos = 0L;
      repoGets = 0L;
      repoLists = 0L;
      repoCounts = 0L;
      repoWrites = 0L;
      fallbacks = 0L;
      activeScopes.remove();
    }

    private boolean enterScope() {
      int depth = activeScopes.get();
      activeScopes.set(depth + 1);
      return depth == 0;
    }

    private void exitScope() {
      int depth = activeScopes.get();
      if (depth <= 1) {
        activeScopes.remove();
      } else {
        activeScopes.set(depth - 1);
      }
    }

    private synchronized void record(
        String component, String operation, Duration elapsed, boolean success) {
      operations++;
      if (!success) {
        errors++;
      }
      long nanos = elapsed == null ? 0L : Math.max(0L, elapsed.toNanos());
      totalNanos += nanos;
      String summaryKey = summarizeOperation(component, operation);
      operationCounts.merge(summaryKey, 1L, Long::sum);
      operationNanos.merge(summaryKey, nanos, Long::sum);
      if ("repository".equals(sanitize(component))) {
        classifyRepositoryOperation(operation);
      }
    }

    private synchronized void put(String key, Object value) {
      values.put(key, value);
    }

    /** Merges one physical operation into its bounded backend/operation aggregate. */
    private synchronized void recordBackend(
        String backend,
        String operation,
        Duration elapsed,
        boolean success,
        long bytes,
        long items) {
      String key = "backend_" + summarizeOperation(backend, operation);
      backends
          .computeIfAbsent(key, ignored -> new BackendAggregate())
          .record(elapsed, success, bytes, items);
    }

    private synchronized void fallback(String key) {
      fallbacks++;
      add("fallback_" + sanitize(key), 1L);
    }

    private void classifyRepositoryOperation(String operation) {
      String normalized = sanitize(operation);
      if (normalized.contains("list")) {
        repoLists++;
      } else if (normalized.contains("count")) {
        repoCounts++;
      } else if (normalized.contains("create")
          || normalized.contains("update")
          || normalized.contains("delete")
          || normalized.contains("put")) {
        repoWrites++;
      } else {
        repoGets++;
      }
    }

    private synchronized void add(String key, long amount) {
      values.merge(
          key,
          amount,
          (left, right) ->
              left instanceof Number number ? number.longValue() + (Long) right : right);
    }

    /** Captures immutable copies of all request totals for emission outside the mutable state. */
    private synchronized Snapshot snapshot() {
      LinkedHashMap<String, BackendSnapshot> backendSnapshot = new LinkedHashMap<>();
      backends.forEach((key, value) -> backendSnapshot.put(key, value.snapshot()));
      return new Snapshot(
          operations,
          errors,
          totalNanos,
          repoGets,
          repoLists,
          repoCounts,
          repoWrites,
          fallbacks,
          Map.copyOf(operationCounts),
          Map.copyOf(operationNanos),
          Map.copyOf(backendSnapshot),
          Map.copyOf(values));
    }
  }

  /** Mutable totals for one bounded backend/operation pair, guarded by the owning summary. */
  private static final class BackendAggregate {
    private long count;
    private long nanos;
    private long bytes;
    private long items;
    private long errors;

    /** Adds one measured call while clamping externally supplied quantities at zero. */
    private void record(Duration elapsed, boolean success, long measuredBytes, long measuredItems) {
      count++;
      nanos += elapsed == null ? 0L : Math.max(0L, elapsed.toNanos());
      bytes += Math.max(0L, measuredBytes);
      items += Math.max(0L, measuredItems);
      if (!success) {
        errors++;
      }
    }

    /** Freezes the current totals for safe publication in the enclosing request snapshot. */
    private BackendSnapshot snapshot() {
      return new BackendSnapshot(count, nanos, bytes, items, errors);
    }
  }

  /** Immutable request-summary totals for one backend/operation pair. */
  private record BackendSnapshot(long count, long nanos, long bytes, long items, long errors) {}

  private record Snapshot(
      long operations,
      long errors,
      long totalNanos,
      long repoGets,
      long repoLists,
      long repoCounts,
      long repoWrites,
      long fallbacks,
      Map<String, Long> operationCounts,
      Map<String, Long> operationNanos,
      Map<String, BackendSnapshot> backends,
      Map<String, Object> values) {}
}
