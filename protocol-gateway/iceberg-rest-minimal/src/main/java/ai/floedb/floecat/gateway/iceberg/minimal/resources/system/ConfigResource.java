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

import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.CatalogConfigDto;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.CatalogResolver;
import jakarta.inject.Singleton;
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
@Singleton
public class ConfigResource {
  private final MinimalGatewayConfig config;

  public ConfigResource(MinimalGatewayConfig config) {
    this.config = config;
  }

  @GET
  public Response getConfig(@QueryParam("warehouse") String warehouse) {
    String prefix = warehouse == null || warehouse.isBlank() ? defaultPrefix() : warehouse.trim();
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);

    CatalogConfigDto payload =
        new CatalogConfigDto(
            Map.of(
                "client.poll-interval-ms", "100",
                "client.retry.initial-timeout-ms", "200",
                "namespace-separator", "%1F",
                "catalog-name", catalogName),
            Map.of("prefix", prefix),
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
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials",
                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan",
                "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
                "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan/{plan-id}",
                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks",
                "POST /v1/{prefix}/namespaces/{namespace}/register",
                "GET /v1/{prefix}/namespaces/{namespace}/views",
                "POST /v1/{prefix}/namespaces/{namespace}/views",
                "GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
                "POST /v1/{prefix}/namespaces/{namespace}/views/{view}",
                "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}",
                "HEAD /v1/{prefix}/namespaces/{namespace}/views/{view}",
                "POST /v1/{prefix}/views/rename",
                "POST /v1/{prefix}/namespaces/{namespace}/register-view",
                "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
                "POST /v1/{prefix}/tables/rename",
                "POST /v1/{prefix}/transactions/commit"),
            config.idempotencyKeyLifetime().toString());
    return Response.ok(payload).build();
  }

  private String defaultPrefix() {
    return config.defaultPrefix().filter(value -> !value.isBlank()).orElse("examples");
  }
}
