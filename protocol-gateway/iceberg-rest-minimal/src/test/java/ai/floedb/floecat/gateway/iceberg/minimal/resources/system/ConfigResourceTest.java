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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.CatalogConfigDto;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ConfigResourceTest {
  @Test
  void returnsOnlyMinimalAdvertisedEndpoints() {
    ConfigResource resource = new ConfigResource(new TestConfig());

    CatalogConfigDto payload = (CatalogConfigDto) resource.getConfig("examples").getEntity();

    assertEquals("examples", payload.defaults().get("catalog-name"));
    assertTrue(payload.endpoints().contains("POST /v1/{prefix}/tables/rename"));
    assertTrue(payload.endpoints().contains("POST /v1/{prefix}/transactions/commit"));
    assertFalse(payload.endpoints().contains("POST /v1/{prefix}/views/rename"));
    assertFalse(
        payload
            .endpoints()
            .contains("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"));
  }

  private static final class TestConfig implements MinimalGatewayConfig {
    @Override
    public String upstreamTarget() {
      return "localhost:9100";
    }

    @Override
    public boolean upstreamPlaintext() {
      return true;
    }

    @Override
    public java.util.Optional<String> defaultAccountId() {
      return java.util.Optional.of("account1");
    }

    @Override
    public java.util.Optional<String> defaultAuthorization() {
      return java.util.Optional.of("Bearer token");
    }

    @Override
    public java.util.Optional<String> defaultPrefix() {
      return java.util.Optional.of("examples");
    }

    @Override
    public Map<String, String> catalogMapping() {
      return Map.of("examples", "examples");
    }

    @Override
    public Duration idempotencyKeyLifetime() {
      return Duration.ofMinutes(30);
    }

    @Override
    public java.util.Optional<String> metadataFileIo() {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<String> metadataFileIoRoot() {
      return java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<String> metadataS3Endpoint() {
      return java.util.Optional.of("http://localstack:4566");
    }

    @Override
    public boolean metadataS3PathStyleAccess() {
      return true;
    }

    @Override
    public java.util.Optional<String> metadataS3Region() {
      return java.util.Optional.of("us-east-1");
    }

    @Override
    public java.util.Optional<String> metadataClientRegion() {
      return java.util.Optional.of("us-east-1");
    }

    @Override
    public java.util.Optional<String> metadataS3AccessKeyId() {
      return java.util.Optional.of("test");
    }

    @Override
    public java.util.Optional<String> metadataS3SecretAccessKey() {
      return java.util.Optional.of("test");
    }

    @Override
    public java.util.Optional<String> defaultWarehousePath() {
      return java.util.Optional.of("s3://floecat/");
    }

    @Override
    public boolean logRequestBodies() {
      return false;
    }

    @Override
    public int logRequestBodyMaxChars() {
      return 8192;
    }
  }
}
