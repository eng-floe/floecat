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

import ai.floedb.floecat.telemetry.helpers.StoreMetrics;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Owns physical storage timing, result measurement, request correlation, and metric vocabulary.
 *
 * <p>Application callers cross this seam once around a complete synchronous store operation. AWS
 * SDK adapters report bounded aggregate samples through {@link #recordSdk(SdkSample)} without
 * relying on ambient request context.
 */
public final class StorageTelemetry {
  /** Physical backend families visible in production telemetry. */
  public enum Backend {
    DYNAMODB("dynamodb"),
    S3("s3");

    private final String tag;

    Backend(String tag) {
      this.tag = tag;
    }

    public String tag() {
      return tag;
    }
  }

  /** Bounded logical and AWS operation names. */
  public enum Operation {
    GET("get"),
    GET_BATCH("get_batch"),
    HEAD("head"),
    PUT("put"),
    LIST("list"),
    DELETE("delete"),
    DELETE_VERSION("delete_version"),
    DELETE_PREFIX("delete_prefix"),
    VERSIONING_STATUS("versioning_status"),
    COMPARE_AND_SET("compare_and_set"),
    COMPARE_AND_DELETE("compare_and_delete"),
    COMPARE_AND_SET_BATCH("compare_and_set_batch"),
    LIST_BY_PREFIX("list_by_prefix"),
    DELETE_BY_PREFIX("delete_by_prefix"),
    COUNT_BY_PREFIX("count_by_prefix"),
    IS_EMPTY("is_empty"),
    DUMP("dump"),
    GET_ITEM("get_item"),
    PUT_ITEM("put_item"),
    UPDATE_ITEM("update_item"),
    DELETE_ITEM("delete_item"),
    QUERY("query"),
    SCAN("scan"),
    TRANSACT_WRITE_ITEMS("transact_write_items"),
    BATCH_WRITE_ITEM("batch_write_item"),
    GET_OBJECT("get_object"),
    HEAD_OBJECT("head_object"),
    PUT_OBJECT("put_object"),
    LIST_OBJECTS_V2("list_objects_v2"),
    DELETE_OBJECT("delete_object"),
    DELETE_OBJECTS("delete_objects"),
    GET_BUCKET_VERSIONING("get_bucket_versioning"),
    OTHER("other");

    private final String tag;

    Operation(String tag) {
      this.tag = tag;
    }

    public String tag() {
      return tag;
    }
  }

  /** Bounded logical outcomes for completed storage operations. */
  public enum Result {
    SUCCESS("success"),
    NOT_FOUND("not_found");

    private final String tag;

    Result(String tag) {
      this.tag = tag;
    }

    public String tag() {
      return tag;
    }
  }

  /** Bounded AWS SDK timing phases. */
  public enum SdkPhase {
    API_CALL("api_call"),
    SERVICE_CALL("service_call"),
    CREDENTIALS_FETCH("credentials_fetch"),
    TOKEN_FETCH("token_fetch"),
    BACKOFF("backoff"),
    MARSHALLING("marshalling"),
    SIGNING("signing"),
    UNMARSHALLING("unmarshalling"),
    TIME_TO_FIRST_BYTE("ttfb"),
    TIME_TO_LAST_BYTE("ttlb"),
    ENDPOINT_RESOLVE("endpoint_resolve"),
    CONCURRENCY_ACQUIRE("concurrency_acquire");

    private final String tag;

    SdkPhase(String tag) {
      this.tag = tag;
    }

    String operationTag(Operation operation) {
      return operation.tag() + "." + tag;
    }
  }

  /** Identifies one physical storage call using bounded values only. */
  public record Call(Backend backend, Operation operation) {
    public Call {
      Objects.requireNonNull(backend, "backend");
      Objects.requireNonNull(operation, "operation");
    }
  }

  /** Bytes, items, and logical result already known by the store operation. */
  public record Measurement(long bytes, long items, Result result) {
    public Measurement {
      bytes = Math.max(0L, bytes);
      items = Math.max(0L, items);
      result = Objects.requireNonNull(result, "result");
    }

    public static Measurement none() {
      return new Measurement(0L, 0L, Result.SUCCESS);
    }

    public static Measurement of(long bytes, long items) {
      return new Measurement(bytes, items, Result.SUCCESS);
    }

    public static Measurement notFound() {
      return new Measurement(0L, 0L, Result.NOT_FOUND);
    }
  }

  /** Call-level values and independent attempt-phase samples emitted by one completed SDK call. */
  public record SdkSample(
      Call call, boolean success, int retries, Map<SdkPhase, List<Duration>> durations) {
    public SdkSample {
      Objects.requireNonNull(call, "call");
      retries = Math.max(0, retries);
      EnumMap<SdkPhase, List<Duration>> copy = new EnumMap<>(SdkPhase.class);
      if (durations != null) {
        durations.forEach(
            (phase, samples) -> {
              if (phase != null && samples != null) {
                List<Duration> valid = new ArrayList<>();
                for (Duration duration : samples) {
                  if (duration != null && !duration.isNegative()) {
                    valid.add(duration);
                  }
                }
                if (!valid.isEmpty()) {
                  copy.put(phase, List.copyOf(valid));
                }
              }
            });
      }
      durations = Map.copyOf(copy);
    }
  }

  private final Observability observability;

  public StorageTelemetry(Observability observability) {
    this.observability = Objects.requireNonNull(observability, "observability");
  }

  /**
   * Returns inert storage telemetry for tests and applications without an observability backend.
   */
  public static StorageTelemetry noop() {
    return new StorageTelemetry(new NoopObservability());
  }

  /**
   * Observes one complete synchronous physical store operation and returns its unmodified result.
   * Runtime failures are recorded and rethrown unchanged.
   */
  public <T> T observe(
      Call call, Supplier<T> work, Function<? super T, Measurement> resultMeasure) {
    Objects.requireNonNull(call, "call");
    Objects.requireNonNull(work, "work");
    Objects.requireNonNull(resultMeasure, "resultMeasure");
    StoreMetrics metrics =
        new StoreMetrics(observability, call.backend().tag(), call.operation().tag());
    long startedNanos = System.nanoTime();
    try (ObservationScope scope = metrics.observePhysical()) {
      try {
        T result = work.get();
        Measurement measurement =
            Objects.requireNonNull(resultMeasure.apply(result), "measurement");
        scope.success();
        metrics.recordRequest(measurement.result().tag());
        if (measurement.bytes() > 0L) {
          metrics.recordBytes(measurement.bytes(), measurement.result().tag());
        }
        if (measurement.items() > 0L) {
          metrics.recordItems(measurement.items(), measurement.result().tag());
        }
        recordSummary(call, startedNanos, true, measurement);
        return result;
      } catch (RuntimeException | Error failure) {
        scope.error(failure);
        metrics.recordRequest("error");
        recordSummary(call, startedNanos, false, Measurement.none());
        throw failure;
      }
    }
  }

  /** Records one completed AWS SDK call without consulting ambient request context. */
  public void recordSdk(SdkSample sample) {
    Objects.requireNonNull(sample, "sample");
    String component = "aws_sdk_" + sample.call().backend().tag();
    String result = sample.success() ? "success" : "error";
    StoreMetrics callMetrics =
        new StoreMetrics(observability, component, sample.call().operation().tag());
    callMetrics.recordRequest(result);
    if (sample.retries() > 0) {
      callMetrics.recordRetries(sample.retries());
    }
    sample
        .durations()
        .forEach(
            (phase, durations) -> {
              StoreMetrics metrics =
                  new StoreMetrics(
                      observability, component, phase.operationTag(sample.call().operation()));
              durations.forEach(duration -> metrics.recordLatency(duration, result));
            });
  }

  /** Adds one completed logical call to the active RPC's backend aggregate when present. */
  private static void recordSummary(
      Call call, long startedNanos, boolean success, Measurement measurement) {
    StoreOperationSummary.recordBackend(
        call.backend().tag(),
        call.operation().tag(),
        Duration.ofNanos(Math.max(0L, System.nanoTime() - startedNanos)),
        success,
        measurement.bytes(),
        measurement.items());
  }
}
