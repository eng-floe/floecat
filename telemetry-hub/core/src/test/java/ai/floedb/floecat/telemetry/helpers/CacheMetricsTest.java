package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class CacheMetricsTest {
  @Test
  void recordsHitsMissesAndTracksSize() {
    TestObservability observability = new TestObservability();
    CacheMetrics metrics = new CacheMetrics(observability, "svc", "op", "users");

    metrics.recordHit();
    metrics.recordMiss();
    metrics.recordMiss();

    assertThat(observability.counterValue(Telemetry.Metrics.CACHE_HITS)).isEqualTo(1d);
    assertThat(observability.counterValue(Telemetry.Metrics.CACHE_MISSES)).isEqualTo(2d);

    List<List<Tag>> hitHistory = observability.counterTagHistory(Telemetry.Metrics.CACHE_HITS);
    List<Tag> hitTags = hitHistory.get(0);
    assertThat(hitTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.CACHE_NAME, "users"));

    AtomicInteger size = new AtomicInteger(5);
    metrics.trackSize(size::get, "cache size");
    Supplier<? extends Number> gauge = observability.gauge(Telemetry.Metrics.CACHE_SIZE);
    assertThat(gauge).isNotNull();
    assertThat(gauge.get().doubleValue()).isEqualTo(5d);
    size.set(8);
    assertThat(gauge.get().doubleValue()).isEqualTo(8d);

    List<Tag> gaugeTags = observability.gaugeTags(Telemetry.Metrics.CACHE_SIZE);
    assertThat(gaugeTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.CACHE_NAME, "users"));
  }

  @Test
  void tracksAccountsGauge() {
    TestObservability observability = new TestObservability();
    CacheMetrics metrics = new CacheMetrics(observability, "svc", "op", "users");

    AtomicInteger accounts = new AtomicInteger(2);
    metrics.trackAccounts(accounts::get, "account cache count");

    Supplier<? extends Number> gauge = observability.gauge(Telemetry.Metrics.CACHE_ACCOUNTS);
    assertThat(gauge).isNotNull();
    assertThat(gauge.get().doubleValue()).isEqualTo(2d);
    accounts.set(5);
    assertThat(gauge.get().doubleValue()).isEqualTo(5d);

    List<Tag> tags = observability.gaugeTags(Telemetry.Metrics.CACHE_ACCOUNTS);
    assertThat(tags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.CACHE_NAME, "users"));
  }

  @Test
  void tracksWeightedSizeGauge() {
    TestObservability observability = new TestObservability();
    CacheMetrics metrics = new CacheMetrics(observability, "svc", "op", "users");

    AtomicInteger weight = new AtomicInteger(1024);
    metrics.trackWeightedSize(weight::get, "weighted size");

    Supplier<? extends Number> gauge = observability.gauge(Telemetry.Metrics.CACHE_WEIGHTED_SIZE);
    assertThat(gauge).isNotNull();
    assertThat(gauge.get().doubleValue()).isEqualTo(1024d);
    weight.set(2048);
    assertThat(gauge.get().doubleValue()).isEqualTo(2048d);

    List<Tag> tags = observability.gaugeTags(Telemetry.Metrics.CACHE_WEIGHTED_SIZE);
    assertThat(tags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.CACHE_NAME, "users"));
  }
}
