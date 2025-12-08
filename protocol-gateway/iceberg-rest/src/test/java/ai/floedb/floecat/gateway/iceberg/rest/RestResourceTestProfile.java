package ai.floedb.floecat.gateway.iceberg.rest;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.Map;

public class RestResourceTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    return Map.of(
        "floecat.gateway.default-authorization",
        "undefined",
        "floecat.gateway.default-warehouse-path",
        "s3://warehouse/default/",
        "floecat.gateway.default-region",
        "us-east-1",
        "floecat.gateway.storage-credential.scope",
        "*",
        "floecat.gateway.storage-credential.properties.type",
        "s3",
        "floecat.gateway.storage-credential.properties.s3.access-key-id",
        "test-key",
        "floecat.gateway.storage-credential.properties.s3.secret-access-key",
        "test-secret",
        "floecat.gateway.storage-credential.properties.s3.region",
        "us-east-1",
        "floecat.gateway.metadata-file-io",
        "ai.floedb.floecat.gateway.iceberg.rest.support.io.InMemoryS3FileIO",
        "floecat.gateway.metadata-file-io-root",
        "target/test-fake-s3");
  }
}
