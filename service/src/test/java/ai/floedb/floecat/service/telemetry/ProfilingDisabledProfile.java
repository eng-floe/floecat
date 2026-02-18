package ai.floedb.floecat.service.telemetry;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class ProfilingDisabledProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "quarkus.profile", "telemetry-prof",
        "floecat.profiling.enabled", "false",
        "floecat.profiling.endpoints-enabled", "true");
  }
}
