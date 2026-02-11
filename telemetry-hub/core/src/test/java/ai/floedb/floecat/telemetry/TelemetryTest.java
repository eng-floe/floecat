package ai.floedb.floecat.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TelemetryTest {

  @Test
  void coreContributorRegistersMetrics() {
    TelemetryRegistry registry = Telemetry.newRegistryWithCore();
    assertThat(Telemetry.metricCatalog(registry))
        .containsKey(Telemetry.Metrics.DROPPED_TAGS.name());
  }

  @Test
  void requiredTagsSubsetAllowedTags() {
    MetricDef def =
        Telemetry.requireMetricDef(Telemetry.newRegistryWithCore(), Telemetry.Metrics.RPC_ERRORS);
    assertThat(def.allowedTags()).containsAll(def.requiredTags());
  }
}
