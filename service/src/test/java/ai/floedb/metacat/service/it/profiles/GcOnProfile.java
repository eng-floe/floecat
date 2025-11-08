package ai.floedb.metacat.service.it.profiles;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class GcOnProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "metacat.gc.idempotency.enabled", "true",
        "metacat.gc.idempotency.tick-every", "1s",
        "metacat.gc.idempotency.page-size", "500",
        "metacat.gc.idempotency.batch-limit", "10000",
        "metacat.gc.idempotency.slice-millis", "5000",
        "metacat.idempotency.ttl-seconds", "1");
  }

  @Override
  public String getConfigProfile() {
    return "gc-on";
  }
}
