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

package ai.floedb.floecat.gateway.iceberg.rest.resources.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CatalogConfigDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConfigResourceTest {

  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final ConfigResource resource = new ConfigResource();

  @BeforeEach
  void setUp() {
    resource.config = config;
    resource.mapper = new ObjectMapper();
    when(config.idempotencyKeyLifetime()).thenReturn(Duration.ofMinutes(30));
    when(config.defaultPrefix()).thenReturn(java.util.Optional.of("floecat"));
  }

  @Test
  void configResponseIncludesIdempotencyLifetime() {
    Response response = resource.getConfig("warehouse-1");
    CatalogConfigDto dto = (CatalogConfigDto) response.getEntity();
    assertEquals("PT30M", dto.idempotencyKeyLifetime());
  }

  @Test
  void missingWarehouseUsesDefaultPrefix() {
    Response response = resource.getConfig(null);
    assertEquals(200, response.getStatus());
    CatalogConfigDto dto = (CatalogConfigDto) response.getEntity();
    assertEquals("floecat", dto.overrides().get("prefix"));
  }
}
