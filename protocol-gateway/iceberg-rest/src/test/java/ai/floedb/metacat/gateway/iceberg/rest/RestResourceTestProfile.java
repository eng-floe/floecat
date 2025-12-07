package ai.floedb.metacat.gateway.iceberg.rest;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class RestResourceTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "metacat.gateway.default-authorization", "undefined",
        "metacat.gateway.default-warehouse-path", "s3://warehouse/default/",
        "metacat.gateway.default-region", "us-east-1",
        "metacat.gateway.storage-credential.scope", "*",
        "metacat.gateway.storage-credential.properties.type", "s3",
        "metacat.gateway.storage-credential.properties.s3.access-key-id", "test-key",
        "metacat.gateway.storage-credential.properties.s3.secret-access-key", "test-secret",
        "metacat.gateway.storage-credential.properties.s3.region", "us-east-1");
  }
}
