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
  }

  @Test
  void configResponseIncludesIdempotencyLifetime() {
    Response response = resource.getConfig(null);
    CatalogConfigDto dto = (CatalogConfigDto) response.getEntity();
    assertEquals("PT30M", dto.idempotencyKeyLifetime());
  }
}
