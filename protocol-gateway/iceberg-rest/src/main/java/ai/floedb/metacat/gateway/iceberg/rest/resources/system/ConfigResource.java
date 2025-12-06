package ai.floedb.metacat.gateway.iceberg.rest.resources.system;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CatalogConfigDto;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Path("/v1/config")
@Produces(MediaType.APPLICATION_JSON)
public class ConfigResource {
  private static final List<String> SUPPORTED_FORMATS = List.of("ICEBERG");
  private static final List<String> SUPPORTED_AUTH_TYPES = List.of("none", "bearer");

  @Inject IcebergGatewayConfig config;

  @GET
  public Response getConfig(@QueryParam("warehouse") String warehouse) {
    Map<String, String> defaults =
        Map.of(
            "client.poll-interval-ms", "100",
            "client.retry.initial-timeout-ms", "200",
            "catalog-name", resolveCatalogName(warehouse));

    Map<String, String> overrides = Map.of();

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

    String idempotencyLifetime = null;

    return Response.ok(new CatalogConfigDto(defaults, overrides, endpoints, idempotencyLifetime))
        .build();
  }

  private String resolveCatalogName(String warehouse) {
    return (warehouse == null || warehouse.isBlank()) ? "metacat" : warehouse;
  }
}
