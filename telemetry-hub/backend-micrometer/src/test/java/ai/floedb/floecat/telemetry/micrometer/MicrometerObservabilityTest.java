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
package ai.floedb.floecat.telemetry.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;
import ai.floedb.floecat.telemetry.Observability.Category;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.StoreTraceScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryPolicy;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MicrometerObservabilityTest {
  private SimpleMeterRegistry meters;
  private TelemetryRegistry telemetryRegistry;

  @BeforeEach
  void setUp() {
    meters = new SimpleMeterRegistry();
    telemetryRegistry = Telemetry.newRegistryWithCore();
  }

  @Test
  void strictRejectsDisallowedTag() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.STRICT);
    assertThatThrownBy(
            () ->
                observability.counter(
                    Telemetry.Metrics.RPC_REQUESTS,
                    1,
                    Tag.of("component", "svc"),
                    Tag.of("operation", "op"),
                    Tag.of("account", "acc"),
                    Tag.of("status", "ok"),
                    Tag.of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void strictAllowsDuplicateCanonicalTag() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.STRICT);
    ObservationScope scope =
        observability.observe(
            Category.RPC,
            "svc",
            "op",
            Tag.of(TagKey.COMPONENT, "svc"),
            Tag.of(TagKey.OPERATION, "op"),
            Tag.of(TagKey.COMPONENT, "svc-dup"),
            Tag.of(TagKey.ACCOUNT, "acct"));
    scope.success();
    scope.close();

    assertThat(
            meters
                .find(Telemetry.Metrics.RPC_LATENCY.name())
                .tags("component", "svc", "operation", "op", "account", "acct")
                .timer())
        .isNotNull();
  }

  @Test
  void strictRejectsDuplicateNonCanonicalTag() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.STRICT);
    assertThatThrownBy(
            () ->
                observability.observe(
                    Category.RPC,
                    "svc",
                    "op",
                    Tag.of(TagKey.ACCOUNT, "acct"),
                    Tag.of(TagKey.ACCOUNT, "acct-dup")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void lenientDropsDisallowedTagAndIncrementsDroppedTagsCounter() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.counter(
        Telemetry.Metrics.RPC_REQUESTS,
        3,
        Tag.of("component", "svc"),
        Tag.of("operation", "op"),
        Tag.of("account", "acc"),
        Tag.of("status", "ok"),
        Tag.of("foo", "bar"));

    assertThat(
            meters
                .find(Telemetry.Metrics.RPC_REQUESTS.name())
                .tags("component", "svc", "operation", "op", "account", "acc", "status", "ok")
                .counter())
        .isNotNull();
    assertThat(meters.find(Telemetry.Metrics.DROPPED_TAGS.name()).counter()).isNotNull();
    assertThat(meters.find(Telemetry.Metrics.DROPPED_TAGS.name()).counter().count()).isEqualTo(1d);
  }

  @Test
  void lenientDropsMetricWhenRequiredTagsMissing() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.counter(
        Telemetry.Metrics.RPC_ERRORS, 1, Tag.of("component", "svc"), Tag.of("operation", "op"));

    assertThat(meters.find(Telemetry.Metrics.RPC_ERRORS.name()).counter()).isNull();
    assertThat(meters.find(Telemetry.Metrics.DROPPED_TAGS.name()).counter().count()).isEqualTo(0d);
  }

  @Test
  void lenientRecordsInvalidMetricCounter() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    MetricId unknownMetric =
        new MetricId("floecat.core.invalid.metric", MetricType.COUNTER, "", "v1", "core");
    observability.counter(
        unknownMetric,
        1,
        Tag.of(TagKey.COMPONENT, "svc"),
        Tag.of(TagKey.OPERATION, "op"),
        Tag.of(TagKey.STATUS, "ok"));

    Counter invalid =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_INVALID_METRIC.name())
            .tags(TagKey.REASON, "unknown_metric")
            .counter();
    assertThat(invalid).isNotNull();
    assertThat(invalid.count()).isEqualTo(1d);
  }

  @Test
  void lenientRecordsDroppedMetricCounter() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.counter(Telemetry.Metrics.RPC_ERRORS, 1, Tag.of(TagKey.COMPONENT, "svc"));

    Counter dropped =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_DROPPED_METRIC.name())
            .tags(TagKey.REASON, "required_missing")
            .counter();
    assertThat(dropped).isNotNull();
    assertThat(dropped.count()).isEqualTo(1d);
  }

  @Test
  void duplicateGaugeCounterIncrements_whenGaugeReregistered() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    Supplier<Number> supplier = () -> 1;
    Tag[] tags = new Tag[] {Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op")};

    observability.gauge(Telemetry.Metrics.RPC_ACTIVE, supplier, "desc", tags);
    observability.gauge(Telemetry.Metrics.RPC_ACTIVE, supplier, "desc2", tags);

    Counter duplicates =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_DUPLICATE_GAUGE.name())
            .tags(TagKey.REASON, "duplicate_gauge")
            .counter();
    assertThat(duplicates).isNotNull();
    assertThat(duplicates.count()).isEqualTo(1d);
  }

  @Test
  void registrySizeGaugeExposesContractSize() {
    new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);

    Gauge gauge = meters.find(Telemetry.Metrics.OBSERVABILITY_REGISTRY_SIZE.name()).gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).isEqualTo(telemetryRegistry.metrics().size());
  }

  @Test
  void timerRecordsDuration() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.timer(
        Telemetry.Metrics.RPC_LATENCY,
        Duration.ofMillis(200),
        Tag.of("component", "svc"),
        Tag.of("operation", "op"),
        Tag.of(TagKey.RESULT, "ok"));

    Timer timer =
        meters
            .find(Telemetry.Metrics.RPC_LATENCY.name())
            .tags("component", "svc", "operation", "op", TagKey.RESULT, "ok")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
    assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThanOrEqualTo(0d);
  }

  @Test
  void scopeRecordsLatencyAndErrors() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(Category.RPC, "svc", "op", Tag.of(TagKey.ACCOUNT, "acct"));
    scope.error(new IllegalStateException("boom"));
    scope.close();
    Timer timer =
        meters
            .find(Telemetry.Metrics.RPC_LATENCY.name())
            .tags(
                "component",
                "svc",
                "operation",
                "op",
                "account",
                "acct",
                TagKey.STATUS,
                "UNKNOWN",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    Counter errorsCounter =
        meters
            .find(Telemetry.Metrics.RPC_ERRORS.name())
            .tags(
                "component",
                "svc",
                "operation",
                "op",
                "account",
                "acct",
                TagKey.STATUS,
                "UNKNOWN",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .counter();
    assertThat(errorsCounter).isNotNull();
    assertThat(errorsCounter.count()).isEqualTo(1d);
    assertThat(meters.find(Telemetry.Metrics.DROPPED_TAGS.name()).counter().count()).isEqualTo(0d);
  }

  @Test
  void scopeRespectsStatusOverride() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(Category.RPC, "svc", "op", Tag.of(TagKey.ACCOUNT, "acct"));
    scope.status("INTERNAL");
    scope.error(new IllegalStateException("boom"));
    scope.close();

    Timer timer =
        meters
            .find(Telemetry.Metrics.RPC_LATENCY.name())
            .tags(
                "component",
                "svc",
                "operation",
                "op",
                "account",
                "acct",
                TagKey.STATUS,
                "INTERNAL",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  void retriesAreEmittedOnClose() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(Category.RPC, "svc", "op", Tag.of(TagKey.ACCOUNT, "acct"));
    scope.retry();
    scope.retry();
    scope.success();
    scope.close();

    assertThat(
            meters
                .find(Telemetry.Metrics.RPC_RETRIES.name())
                .tags("component", "svc", "operation", "op")
                .counter()
                .count())
        .isEqualTo(2d);
    assertThat(meters.find(Telemetry.Metrics.DROPPED_TAGS.name()).counter().count()).isEqualTo(0d);
  }

  @Test
  void storeScopeRecordsErrorsAndRetries() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(Category.STORE, "svc", "store-op", Tag.of(TagKey.ACCOUNT, "acct"));
    scope.error(new IllegalStateException("boom"));
    scope.retry();
    scope.close();

    Timer timer =
        meters
            .find(Telemetry.Metrics.STORE_LATENCY.name())
            .tags(
                "component",
                "svc",
                "operation",
                "store-op",
                "account",
                "acct",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    Counter errors =
        meters
            .find(Telemetry.Metrics.STORE_ERRORS.name())
            .tags(
                "component",
                "svc",
                "operation",
                "store-op",
                "account",
                "acct",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .counter();
    assertThat(errors).isNotNull();
    assertThat(errors.count()).isEqualTo(1d);

    assertThat(
            meters
                .find(Telemetry.Metrics.STORE_RETRIES.name())
                .tags("component", "svc", "operation", "store-op")
                .counter()
                .count())
        .isEqualTo(1d);
  }

  @Test
  void cacheScopeHonorsOperation() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(
            Category.CACHE, "svc", "cache-load", Tag.of(TagKey.CACHE_NAME, "users"));
    scope.success();
    scope.close();

    Timer timer =
        meters
            .find(Telemetry.Metrics.CACHE_LATENCY.name())
            .tags(
                "component",
                "svc",
                "operation",
                "cache-load",
                TagKey.CACHE_NAME,
                "users",
                TagKey.RESULT,
                "success")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  void gcScopeRecordsPauseAndRetries() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    ObservationScope scope =
        observability.observe(Category.GC, "svc", "gc.tick", Tag.of(TagKey.GC_NAME, "pointer"));
    scope.retry();
    scope.success();
    scope.close();

    Timer timer =
        meters
            .find(Telemetry.Metrics.GC_PAUSE.name())
            .tags("component", "svc", "operation", "gc.tick", TagKey.RESULT, "success")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    assertThat(
            meters
                .find(Telemetry.Metrics.GC_RETRIES.name())
                .tags("component", "svc", "operation", "gc.tick")
                .counter()
                .count())
        .isEqualTo(1d);
  }

  @Test
  void gaugeRegistersSupplier() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    Supplier<Double> supplier = () -> 42.0;
    observability.gauge(
        Telemetry.Metrics.RPC_ACTIVE,
        supplier,
        "active RPCs",
        Tag.of("component", "svc"),
        Tag.of("operation", "op"));

    Gauge gauge =
        meters
            .find(Telemetry.Metrics.RPC_ACTIVE.name())
            .tags("component", "svc", "operation", "op")
            .gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).isEqualTo(42.0);
  }

  @Test
  void storeTraceScopeReturnsNoopWithoutParent() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    StoreTraceScope scope = observability.storeTraceScope("svc", "op");
    assertThat(scope).isSameAs(StoreTraceScope.NOOP);
  }

  @Test
  void storeTraceScopeStartsChildSpan() {
    String rawOperation = "Store/Write#1";
    try (GlobalTelemetry telemetry = new GlobalTelemetry()) {
      MicrometerObservability observability =
          new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
      Tracer tracer = GlobalOpenTelemetry.getTracer("test");
      Span parent = tracer.spanBuilder("parent-span").startSpan();
      try (Scope ignored = parent.makeCurrent()) {
        StoreTraceScope scope =
            observability.storeTraceScope(
                "svc", rawOperation, Tag.of("telemetry.contract.version", "v1"));
        scope.success();
        scope.close();
      } finally {
        parent.end();
      }
      List<SpanData> spans = telemetry.exporter().getFinishedSpanItems();
      SpanData child =
          spans.stream()
              .filter(span -> span.getName().startsWith("store."))
              .findFirst()
              .orElseThrow();
      String sanitized = rawOperation.replaceAll("[^A-Za-z0-9_.-]", "_");
      assertThat(child.getName()).isEqualTo("store." + sanitized);
      assertThat(child.getAttributes().get(AttributeKey.stringKey("floecat.store.operation")))
          .isEqualTo(rawOperation);
      assertThat(child.getAttributes().get(AttributeKey.stringKey("floecat.component")))
          .isEqualTo("svc");
      assertThat(child.getAttributes().get(AttributeKey.stringKey("telemetry.contract.version")))
          .isEqualTo("v1");
      assertThat(child.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    }
  }

  @Test
  void storeTraceScopeRecordsException() {
    try (GlobalTelemetry telemetry = new GlobalTelemetry()) {
      MicrometerObservability observability =
          new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
      Tracer tracer = GlobalOpenTelemetry.getTracer("test");
      Span parent = tracer.spanBuilder("parent-span").startSpan();
      IllegalStateException failure = new IllegalStateException("boom");
      try (Scope ignored = parent.makeCurrent()) {
        StoreTraceScope scope = observability.storeTraceScope("svc", "store-op");
        scope.error(failure);
        scope.close();
      } finally {
        parent.end();
      }
      SpanData child =
          telemetry.exporter().getFinishedSpanItems().stream()
              .filter(span -> span.getName().startsWith("store."))
              .findFirst()
              .orElseThrow();
      assertThat(child.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
      assertThat(child.getEvents())
          .anySatisfy(
              event -> {
                assertThat(event.getAttributes().get(AttributeKey.stringKey("exception.type")))
                    .endsWith("IllegalStateException");
                assertThat(event.getAttributes().get(AttributeKey.stringKey("exception.message")))
                    .isEqualTo("boom");
              });
    }
  }

  @Test
  void histogramTimerAlwaysExposesBuckets() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.timer(
        Telemetry.Metrics.RPC_LATENCY,
        Duration.ofMillis(123),
        Tag.of(TagKey.COMPONENT, "svc"),
        Tag.of(TagKey.OPERATION, "op"),
        Tag.of(TagKey.RESULT, "ok"));

    Timer timer =
        meters
            .find(Telemetry.Metrics.RPC_LATENCY.name())
            .tags("component", "svc", "operation", "op", TagKey.RESULT, "ok")
            .timer();
    assertThat(timer).isNotNull();
    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.histogramCounts()).isNotEmpty();
  }

  @Test
  void dropMetricReasonNormalization() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.counter(Telemetry.Metrics.RPC_ERRORS, 1, Tag.of(TagKey.COMPONENT, "svc"));
    Counter missing =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_DROPPED_METRIC.name())
            .tags(TagKey.REASON, "required_missing")
            .counter();
    assertThat(missing).isNotNull();

    observability.counter(
        Telemetry.Metrics.RPC_ERRORS,
        1,
        Tag.of(TagKey.COMPONENT, "svc"),
        Tag.of(TagKey.OPERATION, "op"),
        Tag.of(TagKey.RESULT, "error"),
        Tag.of("foo", "bar"));
    Counter required =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_DROPPED_METRIC.name())
            .tags(TagKey.REASON, "required_missing")
            .counter();
    assertThat(required).isNotNull();
    assertThat(required.count()).isEqualTo(1d);

    MetricId unknownMetric =
        new MetricId("floecat.core.invalid.metric", MetricType.COUNTER, "", "v1", "core");
    observability.counter(
        unknownMetric, 1, Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"));
    Counter unknown =
        meters
            .find(Telemetry.Metrics.OBSERVABILITY_INVALID_METRIC.name())
            .tags(TagKey.REASON, "unknown_metric")
            .counter();
    assertThat(unknown).isNotNull();
  }

  private static final class GlobalTelemetry implements AutoCloseable {
    private final InMemorySpanExporter exporter;
    private final OpenTelemetrySdk openTelemetry;

    private GlobalTelemetry() {
      GlobalOpenTelemetry.resetForTest();
      exporter = InMemorySpanExporter.create();
      SdkTracerProvider tracerProvider =
          SdkTracerProvider.builder()
              .addSpanProcessor(SimpleSpanProcessor.create(exporter))
              .build();
      openTelemetry =
          OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).buildAndRegisterGlobal();
    }

    InMemorySpanExporter exporter() {
      return exporter;
    }

    @Override
    public void close() {
      openTelemetry.close();
      GlobalOpenTelemetry.resetForTest();
    }
  }
}
