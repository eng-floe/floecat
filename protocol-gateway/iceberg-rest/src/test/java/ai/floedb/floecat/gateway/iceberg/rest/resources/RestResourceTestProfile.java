/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    overrides.put("floecat.gateway.default-account-id", "account1");
    overrides.put("floecat.kv.table", "floecat-test");
    overrides.put("floecat.kv.ttl-enabled", "false");
    overrides.put("quarkus.smallrye-jwt.enabled", "false");
    overrides.put("floecat.gateway.default-warehouse-path", "s3://warehouse/default/");
    overrides.put("floecat.connector.integration.default-region", "us-east-1");
    overrides.put("floecat.gateway.delta-compat.enabled", "true");
    overrides.put("floecat.gateway.delta-compat.read-only", "true");

    if (useAwsFixtures()) {
      overrides.put(
          "floecat.connector.integration.metadata-file-io", "org.apache.iceberg.aws.s3.S3FileIO");
    } else {
      overrides.put(
          "floecat.connector.integration.metadata-file-io", InMemoryS3FileIO.class.getName());
      overrides.put("floecat.connector.integration.metadata-file-io-root", "target/test-fake-s3");
    }

    return Map.copyOf(overrides);
  }

  private boolean useAwsFixtures() {
    return Boolean.parseBoolean(System.getProperty("floecat.fixtures.use-aws-s3", "false"));
  }
}
