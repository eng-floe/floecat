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
package ai.floedb.floecat.service.concurrent;

import ai.floedb.floecat.runtime.concurrent.ProcessWideAdmission;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.ConfigValue;
import org.jboss.logging.Logger;

/**
 * Runs blocking metadata-store reads behind the process-wide concurrency ceiling.
 *
 * <p>Each call gets one virtual thread. The call's task retains its permit until the downstream
 * operation actually returns, even when the waiting request cancels and abandons its result. No
 * reusable worker runtime exists: application generations share only the admission semaphore.
 *
 * <p>Every downstream client routed here must enforce its own request or socket timeout. A client
 * that never returns pins one permit and its application generation; Java cannot safely reclaim a
 * thread still executing foreign blocking code.
 */
@ApplicationScoped
public class MetadataIoRunner {
  static final String MAX_CONCURRENCY_PROPERTY = "floecat.query.metadata-io.max-concurrency";
  private static final int DEFAULT_CAPACITY = 64;
  private static final int MAX_CAPACITY = 256;
  private static final Logger LOG = Logger.getLogger(MetadataIoRunner.class);

  private final int capacity;
  private final Semaphore permits;
  private final Runnable saturationSink;

  /** Build the application-generation facade over the JVM-lifetime admission gate. */
  @Inject
  public MetadataIoRunner(Observability observability) {
    this(resolveProcessAdmission(), saturationSink(observability));
  }

  /** Build an isolated runner for focused same-package tests. */
  MetadataIoRunner(int capacity) {
    this(capacity, () -> {});
  }

  /** Build an isolated runner with an observable saturation callback for focused tests. */
  MetadataIoRunner(int capacity, Runnable saturationSink) {
    this(capacity, new Semaphore(requireCapacity(capacity)), saturationSink);
  }

  private MetadataIoRunner(ProcessWideAdmission.State admission, Runnable saturationSink) {
    this(admission.capacity(), admission.permits(), saturationSink);
  }

  private MetadataIoRunner(int capacity, Semaphore permits, Runnable saturationSink) {
    this.capacity = requireCapacity(capacity);
    this.permits = Objects.requireNonNull(permits, "permits");
    this.saturationSink = Objects.requireNonNull(saturationSink, "saturationSink");
  }

  /** Configured process-wide store-concurrency ceiling. */
  public int capacity() {
    return capacity;
  }

  /** Permits held by downstream calls that have not yet returned. */
  public int permitsInUse() {
    return capacity - permits.availablePermits();
  }

  /** Callers currently queued for admission. */
  public int admissionWaiters() {
    return permits.getQueueLength();
  }

  /** Run one cancellable leaf store read. */
  <T> T call(
      BooleanSupplier cancelled,
      Supplier<T> operation,
      CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.call(
        permits, cancelled, operation, failureMessages, saturationSink);
  }

  /** Run one leaf store read without cooperative request cancellation. */
  <T> T callWithoutCancellation(
      Supplier<T> operation, CancellableCallRunner.FailureMessages failureMessages) {
    return CancellableCallRunner.callWithoutCancellation(
        permits, operation, failureMessages, saturationSink);
  }

  private static ProcessWideAdmission.State resolveProcessAdmission() {
    int configured = configuredCapacity();
    ProcessWideAdmission.State admission = ProcessWideAdmission.resolve(configured);
    if (admission.capacity() != configured) {
      LOG.warnf(
          "metadata I/O capacity is fixed at %d until JVM restart; ignoring configured %d",
          admission.capacity(), configured);
    }
    return admission;
  }

  private static Runnable saturationSink(Observability observability) {
    Objects.requireNonNull(observability, "observability");
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "metadata_io");
    return () ->
        observability.counter(
            ServiceMetrics.MetadataIo.ADMISSION_SATURATED_WAITS, 1, component, operation);
  }

  private static int configuredCapacity() {
    return parseConfiguredCapacity(
        ConfigProvider.getConfig().getConfigValue(MAX_CONCURRENCY_PROPERTY));
  }

  /** Convert the deployment value to a valid admission capacity. */
  static int parseConfiguredCapacity(ConfigValue configured) {
    if (configured == null || configured.getRawValue() == null) {
      return DEFAULT_CAPACITY;
    }
    String raw = configured.getValue();
    if (raw == null) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY
              + " is set to \""
              + configured.getRawValue()
              + "\" but could not be resolved");
    }
    if (raw.isBlank()) {
      throw new IllegalStateException(MAX_CONCURRENCY_PROPERTY + " is set to a blank value");
    }
    try {
      return requireCapacity(Integer.parseInt(raw.trim()));
    } catch (NumberFormatException badValue) {
      throw new IllegalStateException(
          MAX_CONCURRENCY_PROPERTY + " must be an integer; got \"" + raw + "\"", badValue);
    } catch (IllegalArgumentException outOfRange) {
      throw new IllegalStateException(outOfRange.getMessage(), outOfRange);
    }
  }

  private static int requireCapacity(int capacity) {
    if (capacity < 1 || capacity > MAX_CAPACITY) {
      throw new IllegalArgumentException(
          MAX_CONCURRENCY_PROPERTY
              + " must be between 1 and "
              + MAX_CAPACITY
              + "; got "
              + capacity);
    }
    return capacity;
  }
}
