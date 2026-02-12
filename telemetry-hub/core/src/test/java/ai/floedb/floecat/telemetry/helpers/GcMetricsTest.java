package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class GcMetricsTest {
  @Test
  void recordsCollectionsAndPauses() {
    TestObservability observability = new TestObservability();
    GcMetrics metrics = new GcMetrics(observability, "svc", "op", "gc1");

    metrics.recordCollection(1);
    metrics.recordPause(Duration.ofMillis(10));

    assertThat(observability.counterValue(Telemetry.Metrics.GC_COLLECTIONS)).isEqualTo(1d);
    assertThat(observability.timerValues(Telemetry.Metrics.GC_PAUSE)).isNotEmpty();
    assertThat(observability.timerTagHistory(Telemetry.Metrics.GC_PAUSE))
        .anySatisfy(
            tags ->
                assertThat(tags)
                    .contains(Tag.of(TagKey.GC_NAME, "gc1"))
                    .anyMatch(tag -> TagKey.COMPONENT.equals(tag.key()))
                    .anyMatch(tag -> TagKey.OPERATION.equals(tag.key()))
                    .anyMatch(tag -> TagKey.RESULT.equals(tag.key())));
  }
}
