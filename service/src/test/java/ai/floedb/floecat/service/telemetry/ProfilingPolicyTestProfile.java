package ai.floedb.floecat.service.telemetry;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class ProfilingPolicyTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.ofEntries(
        Map.entry("quarkus.profile", "telemetry-prof"),
        Map.entry("floecat.profiling.enabled", "true"),
        Map.entry("floecat.profiling.endpoints-enabled", "true"),
        Map.entry("floecat.profiling.capture-duration", "100MS"),
        Map.entry("floecat.profiling.max-capture-bytes", "1048576"),
        Map.entry("floecat.profiling.rate-limit", "10"),
        Map.entry("floecat.profiling.rate-window", "5S"),
        Map.entry("floecat.profiling.policy.enabled", "true"),
        Map.entry("floecat.profiling.policy.gc.enabled", "true"),
        Map.entry("floecat.profiling.policy.gc.threshold-bytes", "1048576"),
        Map.entry("floecat.profiling.policy.gc.gc-name", "G1 Old Generation"),
        Map.entry("floecat.profiling.policy.gc.cooldown", "PT0S"),
        Map.entry("floecat.profiling.policy-poll-interval", "PT2S"));
  }
}
