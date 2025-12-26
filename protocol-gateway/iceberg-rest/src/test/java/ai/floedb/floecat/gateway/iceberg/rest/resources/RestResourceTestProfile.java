package ai.floedb.floecat.gateway.iceberg.rest.resources;

import ai.floedb.floecat.gateway.iceberg.rest.common.InMemoryS3FileIO;
import io.quarkus.test.junit.QuarkusTestProfile;
import java.util.LinkedHashMap;
import java.util.Map;

public class RestResourceTestProfile implements QuarkusTestProfile {
  @Override
  public Map<String, String> getConfigOverrides() {
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put("floecat.gateway.default-authorization", "undefined");
    overrides.put("floecat.gateway.default-warehouse-path", "s3://warehouse/default/");
    overrides.put("floecat.gateway.default-region", "us-east-1");
    overrides.put("floecat.gateway.storage-credential.scope", "*");
    overrides.put("floecat.gateway.storage-credential.properties.type", "s3");
    overrides.put("floecat.gateway.storage-credential.properties.s3.access-key-id", "test-key");
    overrides.put(
        "floecat.gateway.storage-credential.properties.s3.secret-access-key", "test-secret");
    overrides.put("floecat.gateway.storage-credential.properties.s3.region", "us-east-1");

    if (useAwsFixtures()) {
      overrides.put("floecat.gateway.metadata-file-io", "org.apache.iceberg.aws.s3.S3FileIO");
    } else {
      overrides.put("floecat.gateway.metadata-file-io", InMemoryS3FileIO.class.getName());
      overrides.put("floecat.gateway.metadata-file-io-root", "target/test-fake-s3");
    }

    return Map.copyOf(overrides);
  }

  private boolean useAwsFixtures() {
    return Boolean.parseBoolean(System.getProperty("floecat.fixtures.use-aws-s3", "false"));
  }
}
