package ai.floedb.metacat.gateway.iceberg.rest;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

/** Ensures auth defaults are disabled during {@link RestResourceTest}. */
public class RestResourceTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of("metacat.gateway.default-authorization", "undefined");
  }
}
