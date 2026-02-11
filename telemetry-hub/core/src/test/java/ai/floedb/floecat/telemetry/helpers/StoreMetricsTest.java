package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class StoreMetricsTest {
  @Test
  void recordsStoreCountersAndTimers() {
    TestObservability observability = new TestObservability();
    StoreMetrics metrics = new StoreMetrics(observability, "svc", "op");

    metrics.recordRequest("success", Tag.of(TagKey.ACCOUNT, "acct"));
    metrics.recordLatency(Duration.ofMillis(5), "success", Tag.of(TagKey.ACCOUNT, "acct"));
    metrics.recordBytes(123, "success", Tag.of(TagKey.ACCOUNT, "acct"));

    assertThat(observability.counterValue(Telemetry.Metrics.STORE_REQUESTS)).isEqualTo(1d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_BYTES)).isEqualTo(123d);
    assertThat(observability.timerValues(Telemetry.Metrics.STORE_LATENCY)).hasSize(1);

    List<Tag> requestTags =
        observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS).get(0);
    assertThat(requestTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.RESULT, "success"));

    List<Tag> timerTags = observability.timerTagHistory(Telemetry.Metrics.STORE_LATENCY).get(0);
    assertThat(timerTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.RESULT, "success"));
  }
}
