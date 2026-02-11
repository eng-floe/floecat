package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Telemetry;
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
    assertThat(observability.timerValues(Telemetry.Metrics.GC_PAUSE)).hasSize(1);
  }
}
