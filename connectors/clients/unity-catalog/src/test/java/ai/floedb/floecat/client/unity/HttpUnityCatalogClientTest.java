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

package ai.floedb.floecat.client.unity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpUnityCatalogClientTest {
  private HttpServer server;
  private HttpUnityCatalogClient client;

  @BeforeEach
  void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.start();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer catalog-token"));
  }

  @AfterEach
  void tearDown() {
    server.stop(0);
  }

  @Test
  void listCatalogsFollowsPaginationAndRefreshesAuthentication() {
    var secondRequest = new AtomicReference<HttpExchange>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          String rawQuery = exchange.getRequestURI().getRawQuery();
          if (rawQuery != null) {
            secondRequest.set(exchange);
            respond(exchange, 200, "{\"catalogs\":[{\"name\":\"second\"}]}");
          } else {
            respond(
                exchange,
                200,
                "{\"catalogs\":[{\"name\":\"first\"}]," + "\"next_page_token\":\"next/page +\"}");
          }
        });

    assertThat(client.listCatalogs()).containsExactly("first", "second");
    assertThat(secondRequest.get().getRequestURI().getRawQuery())
        .isEqualTo("page_token=next%2Fpage%20%2B");
    assertThat(secondRequest.get().getRequestHeaders().getFirst("Authorization"))
        .isEqualTo("Bearer catalog-token");
  }

  @Test
  void listSchemasTreatsAnOmittedCollectionAsEmpty() {
    server.createContext(
        "/api/2.1/unity-catalog/schemas", exchange -> respond(exchange, 200, "{}"));

    assertThat(client.listSchemas("empty_catalog")).isEmpty();
  }

  @Test
  void listCatalogsContinuesAfterAnEmptyPageWithANextToken() {
    var requests = new AtomicInteger();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          if (requests.getAndIncrement() == 0) {
            respond(exchange, 200, "{\"next_page_token\":\"next\"}");
          } else {
            respond(exchange, 200, "{\"catalogs\":null}");
          }
        });

    assertThat(client.listCatalogs()).isEmpty();
    assertThat(requests).hasValue(2);
  }

  @Test
  void listCatalogsRejectsAPresentNonArrayCollection() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, "{\"catalogs\":{}}"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
  }

  @Test
  void getTableDecodesWireShapesIntoNormalizedMetadata() {
    var rawPath = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          rawPath.set(exchange.getRequestURI().getRawPath());
          respond(
              exchange,
              200,
              """
              {
                "name":"order lines",
                "table_id":"table-id",
                "table_type":"EXTERNAL",
                "data_source_format":"DELTA",
                "storage_location":"s3://bucket/orders",
                "columns":[{"name":"id","type_name":"INT","nullable":false}],
                "properties":{"delta.appendOnly":"true"},
                "table_properties":[
                  {"key":"delta.constraints.positive","value":"id > 0"}
                ]
              }
              """);
        });

    UnityCatalogTable table = client.getTable("cat.schema.order lines").orElseThrow();

    assertThat(rawPath.get()).endsWith("cat.schema.order%20lines");
    assertThat(table.tableId()).isEqualTo("table-id");
    assertThat(table.columns())
        .singleElement()
        .satisfies(
            column -> {
              assertThat(column.name()).isEqualTo("id");
              assertThat(column.nullable()).isFalse();
            });
    assertThat(table.properties())
        .containsEntry("delta.appendOnly", "true")
        .containsEntry("delta.constraints.positive", "id > 0");
  }

  @Test
  void getTableReturnsEmptyOnlyForNotFound() {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> respond(exchange, 404, "{\"message\":\"not found\"}"));

    assertThat(client.getTable("cat.schema.missing")).isEmpty();
  }

  @Test
  void temporaryCredentialsAreTypedAndPostReadOperation() {
    var requestBody = new AtomicReference<String>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> {
          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(
              exchange,
              200,
              """
              {
                "aws_temp_credentials":{
                  "access_key_id":"key",
                  "secret_access_key":"secret",
                  "session_token":"token",
                  "access_point":"arn:aws:s3:us-east-1:123:accesspoint/example"
                },
                "expiration_time":1893456000000,
                "url":"s3://bucket/table"
              }
              """);
        });

    TemporaryTableCredentials credentials =
        client.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ);

    assertThat(requestBody.get()).contains("\"table_id\":\"table-id\"", "\"operation\":\"READ\"");
    assertThat(credentials.awsCredentials().accessKeyId()).isEqualTo("key");
    assertThat(credentials.awsCredentials().accessPoint()).contains("accesspoint/example");
    assertThat(credentials.expiresAt()).isEqualTo(Instant.ofEpochMilli(1893456000000L));
    assertThat(credentials.storageUrl()).isEqualTo("s3://bucket/table");
  }

  @Test
  void httpFailuresExposeSemanticClassificationWithoutHttpTypes() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> respond(exchange, 403, "{\"message\":\"forbidden\"}"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.PERMISSION_DENIED);
              assertThat(failure.statusCode()).isEqualTo(403);
            });
  }

  @Test
  void malformedSuccessResponseIsAnInvalidResponse() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, "not-json"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
