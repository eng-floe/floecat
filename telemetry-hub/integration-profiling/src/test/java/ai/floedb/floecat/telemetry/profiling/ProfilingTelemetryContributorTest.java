package ai.floedb.floecat.telemetry.profiling;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import ai.floedb.floecat.telemetry.TelemetryRegistry;
import org.junit.jupiter.api.Test;

class ProfilingTelemetryContributorTest {
  @Test
  void registersProfilingMetric() {
    TelemetryRegistry registry = new TelemetryRegistry();
    new ProfilingTelemetryContributor().contribute(registry);
    assertNotNull(registry.metric("floecat.profiling.captures.total"));
  }
}
