package ai.floedb.floecat.telemetry.micrometer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.telemetry.Observability.Category;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryPolicy;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
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
                .find("rpc.requests")
                .tags("component", "svc", "operation", "op", "account", "acc", "status", "ok")
                .counter())
        .isNotNull();
    assertThat(meters.find("observability.dropped.tags.total").counter()).isNotNull();
    assertThat(meters.find("observability.dropped.tags.total").counter().count()).isEqualTo(1d);
  }

  @Test
  void lenientDropsMetricWhenRequiredTagsMissing() {
    MicrometerObservability observability =
        new MicrometerObservability(meters, telemetryRegistry, TelemetryPolicy.LENIENT);
    observability.counter(
        Telemetry.Metrics.RPC_ERRORS, 1, Tag.of("component", "svc"), Tag.of("operation", "op"));

    assertThat(meters.find("rpc.errors").counter()).isNull();
    assertThat(meters.find("observability.dropped.tags.total").counter().count()).isEqualTo(0d);
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
            .find("rpc.latency")
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
            .find("rpc.latency")
            .tags(
                "component",
                "svc",
                "operation",
                "op",
                "account",
                "acct",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .timer();
    assertThat(timer).isNotNull();
    assertThat(timer.count()).isEqualTo(1);

    Counter errorsCounter =
        meters
            .find("rpc.errors")
            .tags(
                "component",
                "svc",
                "operation",
                "op",
                "account",
                "acct",
                TagKey.RESULT,
                "error",
                TagKey.EXCEPTION,
                "IllegalStateException")
            .counter();
    assertThat(errorsCounter).isNotNull();
    assertThat(errorsCounter.count()).isEqualTo(1d);
    assertThat(meters.find("observability.dropped.tags.total").counter().count()).isEqualTo(0d);
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
                .find("rpc.retries")
                .tags("component", "svc", "operation", "op")
                .counter()
                .count())
        .isEqualTo(2d);
    assertThat(meters.find("observability.dropped.tags.total").counter().count()).isEqualTo(0d);
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

    Gauge gauge = meters.find("rpc.active").tags("component", "svc", "operation", "op").gauge();
    assertThat(gauge).isNotNull();
    assertThat(gauge.value()).isEqualTo(42.0);
  }
}
