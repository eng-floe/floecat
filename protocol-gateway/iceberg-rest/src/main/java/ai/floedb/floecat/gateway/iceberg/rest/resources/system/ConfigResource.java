package ai.floedb.floecat.gateway.iceberg.rest.resources.system;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CatalogConfigDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@Path("/v1/config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigResource {
  private static final Logger LOG = Logger.getLogger(ConfigResource.class);
  private static final List<String> SUPPORTED_FORMATS = List.of("ICEBERG");
  private static final List<String> SUPPORTED_AUTH_TYPES = List.of("none", "bearer");

  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;

  @GET
  public Response getConfig(@QueryParam("warehouse") String warehouse) {
    Map<String, String> defaults =
        Map.of(
            "client.poll-interval-ms", "100",
            "client.retry.initial-timeout-ms", "200",
            "catalog-name", resolveCatalogName(warehouse));

    Map<String, String> overrides = Map.of("prefix", resolvePrefix(warehouse));

    List<String> endpoints =
        List.of(
            "GET /v1/{prefix}/namespaces",
            "POST /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}",
            "DELETE /v1/{prefix}/namespaces/{namespace}",
            "POST /v1/{prefix}/namespaces/{namespace}/properties",
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "POST /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "POST /v1/{prefix}/namespaces/{namespace}/register",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{planId}",
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{planId}",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
            "POST /v1/{prefix}/tables/rename",
            "POST /v1/{prefix}/transactions/commit",
            "GET /v1/{prefix}/namespaces/{namespace}/views",
            "POST /v1/{prefix}/namespaces/{namespace}/views",
            "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
            "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
            "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
            "POST /v1/{prefix}/views/rename");

    String idempotencyLifetime =
        config.idempotencyKeyLifetime() == null ? null : config.idempotencyKeyLifetime().toString();

    CatalogConfigDto payload =
        new CatalogConfigDto(defaults, overrides, endpoints, idempotencyLifetime);
    logResponse(payload);
    return Response.ok(payload).build();
  }

  private String resolvePrefix(String warehouse) {
    return (warehouse == null || warehouse.isBlank()) ? "floecat" : warehouse;
  }

  private String resolveCatalogName(String warehouse) {
    return resolvePrefix(warehouse);
  }

  private void logResponse(CatalogConfigDto payload) {
    try {
      LOG.infof("Config response: %s", mapper.writeValueAsString(payload));
    } catch (JsonProcessingException e) {
      LOG.infof("Config response (fallback): %s", payload);
    }
  }
}
