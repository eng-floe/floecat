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
    client.close();
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
  void rejectsCleartextForNonLoopbackHosts() {
    assertThatThrownBy(
            () ->
                new HttpUnityCatalogClient(
                    URI.create("http://catalog.example.com"),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(5),
                    Map::of))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must use HTTPS");
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
  void getTableRejectsANonObjectSuccessResponse() {
    server.createContext(
        "/api/2.1/unity-catalog/tables/", exchange -> respond(exchange, 200, "[]"));

    assertThatThrownBy(() -> client.getTable("cat.schema.invalid"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
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
    // Carried through unparsed: the epoch-millis rule lives once, in the connector SPI.
    assertThat(credentials.expirationEpochMillis()).isEqualTo("1893456000000");
    assertThat(credentials.storageUrl()).isEqualTo("s3://bucket/table");
  }

  @Test
  void malformedCredentialResponseDoesNotLeakItsBody() {
    String secret = "credential-secret-that-must-not-be-logged";
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                200,
                "{\"aws_temp_credentials\":{\"secret_access_key\":\"" + secret + "\""));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
              assertThat(failure.getMessage()).doesNotContain(secret, "secret_access_key");
              assertThat(failure).hasNoCause();
            });
  }

  @Test
  void authenticationFailureIsTranslated() {
    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              throw new IllegalStateException("token unavailable");
            });

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.TRANSPORT);
              assertThat(failure).hasCauseInstanceOf(IllegalStateException.class);
            });
  }

  @Test
  void tablePropertiesAreCollectedFromBothWireShapesInEitherField() {
    // The mirror of getTableDecodesWireShapesIntoNormalizedMetadata: Unity Catalog has shipped
    // `properties` and `table_properties` in both the object and the key/value-array shape, so the
    // decoder has to accept either spelling in either field.
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange ->
            respond(
                exchange,
                200,
                """
                {
                  "name":"orders",
                  "table_id":"table-id",
                  "properties":[{"key":"delta.appendOnly","value":"true"}],
                  "table_properties":{"delta.constraints.positive":"id > 0"}
                }
                """));

    UnityCatalogTable table = client.getTable("cat.schema.orders").orElseThrow();

    assertThat(table.properties())
        .containsEntry("delta.appendOnly", "true")
        .containsEntry("delta.constraints.positive", "id > 0");
  }

  @Test
  void namesAreNeverNullEvenWhenTheCatalogOmitsThem() {
    // A nameless entry must not poison the listing: `name` is the sort key, and Stream.sorted()
    // uses natural ordering, so a null there fails the whole page with a NullPointerException.
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange ->
            respond(
                exchange,
                200,
                """
                {"tables":[
                  {"name":"","columns":[{"name":"","type_name":null}]},
                  {"table_id":"no-name"}
                ]}
                """));

    assertThat(client.listTables("cat", "schema"))
        .hasSize(2)
        .allSatisfy(table -> assertThat(table.name()).isNotNull());
    assertThat(client.listTables("cat", "schema").getFirst().columns())
        .singleElement()
        .satisfies(column -> assertThat(column.name()).isEmpty());
  }

  @Test
  void aDatabricksErrorCodeOutranksTheHttpStatus() {
    // A workspace without EXTERNAL USE SCHEMA answers the credentials endpoint with 400 and puts
    // the refusal in the body. Classified on status alone it is an unclassified 4xx, which the
    // connector leaves retryable -- so the reconcile job loops on a permanent condition.
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                400,
                "{\"error_code\":\"PERMISSION_DENIED\",\"message\":\"missing EXTERNAL USE SCHEMA\"}"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.PERMISSION_DENIED);
              assertThat(failure.statusCode()).isEqualTo(400);
              assertThat(failure.errorCode()).isEqualTo("PERMISSION_DENIED");
              assertThat(failure.getMessage()).doesNotContain("missing EXTERNAL USE SCHEMA");
            });
  }

  @Test
  void anUnrecognizedClientErrorIsPermanentRatherThanUnclassified() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange, 400, "{\"error_code\":\"INVALID_PARAMETER_VALUE\",\"message\":\"no\"}"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              assertThat(failure.errorCode()).isEqualTo("INVALID_PARAMETER_VALUE");
            });
  }

  /**
   * The permanent default applies only to a 4xx Databricks itself answered, which its {@code
   * error_code} envelope is what identifies. An HTML 400 from a load balancer or WAF in front of
   * the workspace is not the workspace refusing anything, and making it terminal permanently fails
   * a job that would have recovered.
   */
  @Test
  void aClientErrorWithoutADatabricksEnvelopeStaysRetryable() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 400, "<html><body>Bad Request</body></html>"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.OTHER);
              assertThat(failure.statusCode()).isEqualTo(400);
              assertThat(failure.errorCode()).isNull();
            });
  }

  /**
   * A 4xx that is transient by definition must not fall into the permanent default. 408 is what a
   * load balancer in front of a Databricks workspace emits when the upstream is slow; classifying
   * it INVALID_REQUEST would make the reconcile job give up on it for good.
   */
  @Test
  void aTransientClientErrorStaysRetryable() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 408, "gateway timed out"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.OTHER));
  }

  @Test
  void aCredentialRedirectIsNotFollowed() {
    var redirectedRequests = new AtomicInteger();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/moved/credentials");
          respond(exchange, 302, "");
        });
    server.createContext(
        "/moved/credentials",
        exchange -> {
          redirectedRequests.incrementAndGet();
          respond(exchange, 200, "{}");
        });

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
    assertThat(redirectedRequests).hasValue(0);
  }

  /**
   * A 3xx the client declined to follow is a misconfigured base URI, not an outage. Left as {@code
   * OTHER} it stays retryable, so the reconciler would loop on it forever.
   */
  @Test
  void anUnfollowableRedirectIsPermanent() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/api/2.1/unity-catalog/catalogs");
          respond(exchange, 302, "");
        });

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
  }

  @Test
  void aServerErrorStaysRetryableWhateverTheBodySays() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 503, "upstream down"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.SERVER_ERROR));
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

  /**
   * INVALID_RESPONSE stays retryable, so an unparseable body shows up as a looping vend. The status
   * and the body are what identify the proxy answering instead of the catalog, so neither is
   * discarded.
   */
  @Test
  void malformedSuccessResponseIsAnInvalidResponse() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, "not-json"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
              assertThat(failure.statusCode()).isEqualTo(200);
              assertThat(failure.getMessage()).contains("not-json");
            });
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
