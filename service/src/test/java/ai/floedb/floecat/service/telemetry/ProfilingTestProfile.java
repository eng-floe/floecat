package ai.floedb.floecat.service.telemetry;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class ProfilingTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.profile", "telemetry-prof",
        "floecat.profiling.enabled", "true",
        "floecat.profiling.endpoints-enabled", "true",
        "floecat.profiling.capture-duration", "100MS",
        "floecat.profiling.max-capture-bytes", "1048576",
        "floecat.profiling.rate-limit", "5",
        "floecat.profiling.rate-window", "1S");
  }
}
