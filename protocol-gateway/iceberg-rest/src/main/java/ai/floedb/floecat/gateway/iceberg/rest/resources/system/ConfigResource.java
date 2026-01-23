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

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CatalogConfigDto;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogResolver;
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
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;

  @GET
  public Response getConfig(@QueryParam("warehouse") String warehouse) {
    String prefix = warehouse == null || warehouse.isBlank() ? defaultPrefix() : warehouse.trim();
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    Map<String, String> defaults =
        Map.of(
            "client.poll-interval-ms", "100",
            "client.retry.initial-timeout-ms", "200",
            "namespace-separator", "%1F",
            "catalog-name", catalogName);

    Map<String, String> overrides = Map.of("prefix", prefix);

    List<String> endpoints =
        List.of(
            "GET /v1/{prefix}/namespaces",
            "POST /v1/{prefix}/namespaces",
            "GET /v1/{prefix}/namespaces/{namespace}",
            "HEAD /v1/{prefix}/namespaces/{namespace}",
            "DELETE /v1/{prefix}/namespaces/{namespace}",
            "POST /v1/{prefix}/namespaces/{namespace}/properties",
            "GET /v1/{prefix}/namespaces/{namespace}/tables",
            "POST /v1/{prefix}/namespaces/{namespace}/tables",
            "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
            "POST /v1/{prefix}/namespaces/{namespace}/register",
            "POST /v1/{prefix}/namespaces/{namespace}/register-view",
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
            "POST /v1/{prefix}/views/rename",
            "POST /v1/oauth/tokens");

    String idempotencyLifetime =
        config.idempotencyKeyLifetime() == null ? null : config.idempotencyKeyLifetime().toString();

    CatalogConfigDto payload =
        new CatalogConfigDto(defaults, overrides, endpoints, idempotencyLifetime);
    logResponse(payload);
    return Response.ok(payload).build();
  }

  private String defaultPrefix() {
    if (config == null) {
      return "floecat";
    }
    return config.defaultPrefix().filter(p -> p != null && !p.isBlank()).orElse("floecat");
  }

  private void logResponse(CatalogConfigDto payload) {
    try {
      LOG.infof("Config response: %s", mapper.writeValueAsString(payload));
    } catch (JsonProcessingException e) {
      LOG.infof("Config response (fallback): %s", payload);
    }
  }
}
