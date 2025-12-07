package ai.floedb.floecat.service.it.profiles;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class GcOnProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "floecat.gc.idempotency.enabled", "true",
        "floecat.gc.idempotency.tick-every", "1s",
        "floecat.gc.idempotency.page-size", "500",
        "floecat.gc.idempotency.batch-limit", "10000",
        "floecat.gc.idempotency.slice-millis", "5000",
        "floecat.idempotency.ttl-seconds", "1");
  }

  @Override
  public String getConfigProfile() {
    return "gc-on";
  }
}
