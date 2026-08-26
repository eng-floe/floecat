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
package ai.floedb.floecat.storage.aws;

import ai.floedb.floecat.telemetry.StorageTelemetry;
import ai.floedb.floecat.telemetry.StorageTelemetry.Backend;
import ai.floedb.floecat.telemetry.StorageTelemetry.Call;
import ai.floedb.floecat.telemetry.StorageTelemetry.Operation;
import ai.floedb.floecat.telemetry.StorageTelemetry.SdkPhase;
import ai.floedb.floecat.telemetry.StorageTelemetry.SdkSample;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.SdkMetric;

/**
 * Stateless AWS SDK metrics adapter for DynamoDB and S3.
 *
 * <p>It accepts only bounded service, operation, result, and phase values. Object keys, buckets,
 * tables, endpoints, accounts, and request identifiers never cross this seam. Closing is a no-op
 * because one application-scoped adapter is shared by refreshing client instances.
 */
@ApplicationScoped
public final class AwsStorageMetricPublisher implements MetricPublisher {
  private static final Map<SdkPhase, SdkMetric<Duration>> DURATION_METRICS = durationMetrics();

  private final StorageTelemetry telemetry;

  @Inject
  public AwsStorageMetricPublisher(StorageTelemetry telemetry) {
    this.telemetry = telemetry;
  }

  /** Translates one completed SDK call tree into bounded storage metrics. */
  @Override
  public void publish(MetricCollection collection) {
    if (collection == null) {
      return;
    }
    Optional<Backend> backend = backend(first(collection, CoreMetric.SERVICE_ID).orElse(""));
    if (backend.isEmpty()) {
      return;
    }
    Operation operation = operation(first(collection, CoreMetric.OPERATION_NAME).orElse(""));
    boolean success = first(collection, CoreMetric.API_CALL_SUCCESSFUL).orElse(false);
    int retries = first(collection, CoreMetric.RETRY_COUNT).orElse(0);
    EnumMap<SdkPhase, List<Duration>> durations = new EnumMap<>(SdkPhase.class);
    DURATION_METRICS.forEach(
        (phase, metric) -> {
          List<Duration> samples = values(collection, metric);
          if (!samples.isEmpty()) {
            durations.put(phase, samples);
          }
        });
    telemetry.recordSdk(
        new SdkSample(new Call(backend.orElseThrow(), operation), success, retries, durations));
  }

  /** Leaves the stateless shared publisher available to replacement client generations. */
  @Override
  public void close() {
    // Shared by client generations; closing one SDK client must not disable later publications.
  }

  /** Maps an SDK service identifier to a supported bounded backend. */
  private static Optional<Backend> backend(String service) {
    String normalized = service.toLowerCase(Locale.ROOT);
    if (normalized.contains("dynamodb")) {
      return Optional.of(Backend.DYNAMODB);
    }
    if (normalized.equals("s3") || normalized.contains("simple storage")) {
      return Optional.of(Backend.S3);
    }
    return Optional.empty();
  }

  /** Maps an SDK operation name to the bounded storage operation vocabulary. */
  private static Operation operation(String operation) {
    return switch (operation) {
      case "GetItem" -> Operation.GET_ITEM;
      case "PutItem" -> Operation.PUT_ITEM;
      case "UpdateItem" -> Operation.UPDATE_ITEM;
      case "DeleteItem" -> Operation.DELETE_ITEM;
      case "Query" -> Operation.QUERY;
      case "Scan" -> Operation.SCAN;
      case "TransactWriteItems" -> Operation.TRANSACT_WRITE_ITEMS;
      case "BatchWriteItem" -> Operation.BATCH_WRITE_ITEM;
      case "GetObject" -> Operation.GET_OBJECT;
      case "HeadObject" -> Operation.HEAD_OBJECT;
      case "PutObject" -> Operation.PUT_OBJECT;
      case "ListObjectsV2" -> Operation.LIST_OBJECTS_V2;
      case "DeleteObject" -> Operation.DELETE_OBJECT;
      case "DeleteObjects" -> Operation.DELETE_OBJECTS;
      case "GetBucketVersioning" -> Operation.GET_BUCKET_VERSIONING;
      default -> Operation.OTHER;
    };
  }

  /** Finds the first value for a call-level metric anywhere in the SDK collection tree. */
  private static <T> Optional<T> first(MetricCollection collection, SdkMetric<T> metric) {
    for (T value : collection.metricValues(metric)) {
      if (value != null) {
        return Optional.of(value);
      }
    }
    for (MetricCollection child : collection.children()) {
      Optional<T> value = first(child, metric);
      if (value.isPresent()) {
        return value;
      }
    }
    return Optional.empty();
  }

  /** Preserves every attempt-level duration as an independent histogram sample. */
  private static List<Duration> values(MetricCollection collection, SdkMetric<Duration> metric) {
    List<Duration> result = new ArrayList<>();
    for (Duration value : collection.metricValues(metric)) {
      if (value != null && !value.isNegative()) {
        result.add(value);
      }
    }
    for (MetricCollection child : collection.children()) {
      result.addAll(values(child, metric));
    }
    return List.copyOf(result);
  }

  /** Defines the fixed set of SDK phases retained from each collection tree. */
  private static Map<SdkPhase, SdkMetric<Duration>> durationMetrics() {
    EnumMap<SdkPhase, SdkMetric<Duration>> metrics = new EnumMap<>(SdkPhase.class);
    metrics.put(SdkPhase.API_CALL, CoreMetric.API_CALL_DURATION);
    metrics.put(SdkPhase.SERVICE_CALL, CoreMetric.SERVICE_CALL_DURATION);
    metrics.put(SdkPhase.CREDENTIALS_FETCH, CoreMetric.CREDENTIALS_FETCH_DURATION);
    metrics.put(SdkPhase.TOKEN_FETCH, CoreMetric.TOKEN_FETCH_DURATION);
    metrics.put(SdkPhase.BACKOFF, CoreMetric.BACKOFF_DELAY_DURATION);
    metrics.put(SdkPhase.MARSHALLING, CoreMetric.MARSHALLING_DURATION);
    metrics.put(SdkPhase.SIGNING, CoreMetric.SIGNING_DURATION);
    metrics.put(SdkPhase.UNMARSHALLING, CoreMetric.UNMARSHALLING_DURATION);
    metrics.put(SdkPhase.TIME_TO_FIRST_BYTE, CoreMetric.TIME_TO_FIRST_BYTE);
    metrics.put(SdkPhase.TIME_TO_LAST_BYTE, CoreMetric.TIME_TO_LAST_BYTE);
    metrics.put(SdkPhase.ENDPOINT_RESOLVE, CoreMetric.ENDPOINT_RESOLVE_DURATION);
    metrics.put(SdkPhase.CONCURRENCY_ACQUIRE, HttpMetric.CONCURRENCY_ACQUIRE_DURATION);
    return Map.copyOf(metrics);
  }
}
