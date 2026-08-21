/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.integration.rpc.BearerAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogAuthentication;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationIssue;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import ai.floedb.floecat.integration.rpc.SecretValue;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogIntegrationPolarisValidationTest {
  private HttpServer server;
  private final AtomicReference<URI> configRequest = new AtomicReference<>();

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/api/catalog", this::handleRequest);
    server.start();
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void validationSendsWarehouseOnPolarisConfigHandshake() {
    CatalogIntegration integration =
        CatalogIntegration.newBuilder()
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("http://127.0.0.1:" + server.getAddress().getPort() + "/api/catalog")
            .putProperties("warehouse", "polaris-catalog")
            .setAuthentication(
                CatalogAuthentication.newBuilder()
                    .setBearer(BearerAuthentication.getDefaultInstance())
                    .setCredentialsConfigured(true)
                    .setCredentialGeneration(1L))
            .build();
    CatalogIntegrationCredentials storedCredentials =
        CatalogIntegrationCredentials.newBuilder()
            .setBearerToken(SecretValue.newBuilder().setValue("polaris-token"))
            .build();
    CatalogIntegrationCredentialStore credentialStore =
        mock(CatalogIntegrationCredentialStore.class);
    when(credentialStore.resolve(integration)).thenReturn(Optional.of(storedCredentials));
    var access = new CatalogIntegrationAccess();
    access.credentialStore = credentialStore;
    var discovery = new CatalogIntegrationDiscovery();
    discovery.access = access;

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_PASSED, result.checks().get(0).getStatus());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_PASSED, result.checks().get(1).getStatus());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_NO_TABLES, result.checks().get(2).getIssue());
    assertNotNull(configRequest.get());
    assertEquals("warehouse=polaris-catalog", configRequest.get().getRawQuery());
  }

  private void handleRequest(HttpExchange exchange) throws IOException {
    URI request = exchange.getRequestURI();
    if (request.getPath().endsWith("/v1/config")) {
      configRequest.set(request);
      if (!"warehouse=polaris-catalog".equals(request.getRawQuery())) {
        respond(
            exchange,
            400,
            "{\"error\":{\"message\":\"Please specify a warehouse\","
                + "\"type\":\"BadRequestException\",\"code\":400}}");
        return;
      }
      respond(exchange, 200, "{\"defaults\":{},\"overrides\":{}}");
      return;
    }
    if (request.getPath().endsWith("/tables")) {
      respond(exchange, 200, "{\"identifiers\":[]}");
      return;
    }
    if (request.getPath().endsWith("/namespaces")) {
      respond(exchange, 200, "{\"namespaces\":[]}");
      return;
    }
    respond(
        exchange,
        404,
        "{\"error\":{\"message\":\"Not found\",\"type\":\"NotFoundException\"," + "\"code\":404}}");
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (var response = exchange.getResponseBody()) {
      response.write(bytes);
    }
  }
}
