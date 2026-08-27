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
    // Carried through unparsed: the epoch-millis rule lives once, in the connector SPI.
    assertThat(credentials.expirationEpochMillis()).isEqualTo("1893456000000");
    assertThat(credentials.storageUrl()).isEqualTo("s3://bucket/table");
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

  /**
   * An ordinary workspace redirect has to be followed, and the Authorization header has to survive
   * it -- which is why redirects are followed by hand: Redirect.NORMAL would have stripped it.
   */
  @Test
  void aSameOriginRedirectIsFollowedWithAuthenticationIntact() {
    var authorization = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/moved/catalogs");
          respond(exchange, 301, "");
        });
    server.createContext(
        "/moved/catalogs",
        exchange -> {
          authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
          respond(exchange, 200, "{\"catalogs\":[{\"name\":\"c\"}]}");
        });

    assertThat(client.listCatalogs()).containsExactly("c");
    assertThat(authorization.get()).isEqualTo("Bearer catalog-token");
  }

  /**
   * Redirect.NORMAL re-issues a followed 301 as a bodyless GET, which the credentials call cannot
   * survive: the redirect target sees no table_id and answers 404 or 405. Following by hand replays
   * the POST and its body.
   */
  @Test
  void aRedirectedPostKeepsItsMethodAndBody() {
    var method = new AtomicReference<String>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/moved/credentials");
          respond(exchange, 307, "");
        });
    server.createContext(
        "/moved/credentials",
        exchange -> {
          method.set(exchange.getRequestMethod());
          String body =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          assertThat(body).contains("table-id");
          respond(
              exchange,
              200,
              "{\"aws_temp_credentials\":{\"access_key_id\":\"ak\",\"secret_access_key\":\"sk\"}}");
        });

    var credentials =
        client.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ);

    assertThat(method.get()).isEqualTo("POST");
    assertThat(credentials.awsCredentials().accessKeyId()).isEqualTo("ak");
  }

  /**
   * A redirect to another origin is where the JDK drops Authorization, and following it by hand
   * would instead hand the long-lived catalog token to a host the configuration never named. It is
   * a misconfigured base URI, so it is permanent and names the Location it declined.
   */
  @Test
  void aCrossOriginRedirectIsRefusedPermanently() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "https://elsewhere.example.com/catalogs");
          respond(exchange, 302, "");
        });

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              assertThat(failure.getMessage()).contains("elsewhere.example.com");
            });
  }

  /**
   * A credentialed request goes out before any server can redirect it to HTTPS, so an http:// base
   * URI would disclose the bearer token in the clear. Refused before the send, and terminally --
   * loopback, as used throughout this test, stays allowed.
   */
  @Test
  void aCleartextRequestCarryingCredentialsIsRefused() {
    var cleartext =
        new HttpUnityCatalogClient(
            URI.create("http://workspace.cloud.databricks.com"),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer catalog-token"));

    try (cleartext) {
      assertThatThrownBy(cleartext::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
                assertThat(failure.getMessage()).contains("https");
                assertThat(failure.getMessage()).doesNotContain("catalog-token");
              });
    }
  }

  /**
   * A hostname may legally begin with a numeric label, so a textual "127." prefix test is not a
   * loopback test: it would hand the bearer token to whatever that name resolves to, in the clear.
   */
  @Test
  void aHostnameThatMerelyLooksLikeLoopbackIsNotTreatedAsLoopback() {
    var cleartext =
        new HttpUnityCatalogClient(
            URI.create("http://127.attacker.example"),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer catalog-token"));

    try (cleartext) {
      assertThatThrownBy(cleartext::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
    }
  }

  /**
   * An unauthenticated Unity Catalog OSS server over plain HTTP has no secret to leak, so it is not
   * refused: the cleartext guard is scoped to requests that actually carry credentials. The host
   * never resolves, so the call fails in transport -- which is exactly the point, since the guard
   * would have refused it before any connection was attempted.
   */
  @Test
  void anUnauthenticatedCleartextRequestIsNotRefused() {
    var anonymous =
        new HttpUnityCatalogClient(
            URI.create("http://unity-catalog.invalid:8080"),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Map::of);

    try (anonymous) {
      assertThatThrownBy(anonymous::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.TRANSPORT));
    }
  }

  /**
   * The vend response is itself the secret, so this refusal cannot be conditioned on the request
   * carrying a credential: an anonymous vend over http:// hands secret_access_key and session_token
   * to anyone on the path. Refused before the send, like the credentialed case.
   */
  @Test
  void anAnonymousCleartextVendIsRefused() {
    var anonymous =
        new HttpUnityCatalogClient(
            URI.create("http://unity-catalog.invalid:8080"),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Map::of);

    try (anonymous) {
      assertThatThrownBy(
              () ->
                  anonymous.generateTemporaryTableCredentials(
                      "table-id", UnityCatalogClient.TableOperation.READ))
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
                assertThat(failure.getMessage()).contains("https");
              });
    }
  }

  /**
   * A static-token provider is {@code () -> require(props, "token")}, which throws
   * IllegalArgumentException at request time when the secret was never materialised. That is the
   * same unclassified-runtime-exception retry loop the header guard exists to prevent, so the
   * provider call belongs inside it.
   */
  @Test
  void anAuthenticationProviderThatCannotProduceAHeaderIsTerminal() {
    var broken =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              throw new IllegalArgumentException("Missing auth property: token");
            });

    try (broken) {
      assertThatThrownBy(broken::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
    }
  }

  /**
   * HttpRequest.Builder.header rejects a value HTTP cannot carry -- a token pasted with a trailing
   * newline. Bare, that IllegalArgumentException reaches the storage service unclassified and comes
   * back retryable, so the reconciler loops on a config fault that never self-corrects.
   */
  @Test
  void anUnusableAuthenticationHeaderIsTerminal() {
    var broken =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer token-with-a-newline\n"));

    try (broken) {
      assertThatThrownBy(broken::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
    }
  }

  /**
   * The credentials response carries secret_access_key and session_token in the clear, so a body
   * truncated past those fields must not be quoted into an exception message that reaches the
   * storage service's gRPC status and the logs.
   */
  @Test
  void aMalformedCredentialsResponseDoesNotQuoteTheBody() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                200,
                "{\"aws_temp_credentials\":{\"secret_access_key\":\"super-secret\","));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
              assertThat(failure.getMessage()).doesNotContain("super-secret");
              assertThat(failure.getMessage()).contains("body withheld");
            });
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

  /**
   * A followed redirect means the response came from a URI other than the one requested, and a 404
   * from a redirect target reads as a missing endpoint unless the message says where it landed.
   */
  @Test
  void aFailureAfterAFollowedRedirectNamesTheEffectiveUri() {
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/moved/catalogs");
          respond(exchange, 301, "");
        });
    server.createContext("/moved/catalogs", exchange -> respond(exchange, 404, "no such endpoint"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> assertThat(failure.getMessage()).contains("/moved/catalogs"));
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
