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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpUnityCatalogClientTest {
  private HttpServer server;
  private HttpUnityCatalogClient client;
  private String previousAllowLoopback;

  @BeforeEach
  void setUp() throws IOException {
    // Loopback cleartext is deny-by-default; the harness opts in, and restores whatever was set.
    previousAllowLoopback =
        System.setProperty(HttpUnityCatalogClient.ALLOW_LOOPBACK_PROPERTY, "true");
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
    if (previousAllowLoopback == null) {
      System.clearProperty(HttpUnityCatalogClient.ALLOW_LOOPBACK_PROPERTY);
    } else {
      System.setProperty(HttpUnityCatalogClient.ALLOW_LOOPBACK_PROPERTY, previousAllowLoopback);
    }
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
  void authenticationHeadersReplaceClientSetOnesRatherThanAppending() {
    var accept = new AtomicReference<List<String>>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          accept.set(exchange.getRequestHeaders().get("Accept"));
          respond(exchange, 200, "{\"catalogs\":[]}");
        });

    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Accept", "application/vnd.databricks+json"));

    client.listCatalogs();

    // One value, the provider's -- not the client's default plus the provider's.
    assertThat(accept.get()).containsExactly("application/vnd.databricks+json");
  }

  @Test
  void listingRefusesToPageForever() {
    // A small cap: the bound is what is under test, not the constant's magnitude. At the default
    // this would drive 10,000 real round trips to assert the same thing.
    var previous = System.setProperty(HttpUnityCatalogClient.MAX_PAGES_PROPERTY, "3");
    try {
      var requests = new AtomicInteger();
      server.createContext(
          "/api/2.1/unity-catalog/catalogs",
          exchange ->
              // A fresh token every page, so the repeated-token guard never fires.
              respond(
                  exchange,
                  200,
                  "{\"catalogs\":[{\"name\":\"c"
                      + requests.incrementAndGet()
                      + "\"}],\"next_page_token\":\"token-"
                      + requests.get()
                      + "\"}"));

      // The cap is read at construction, so this client has to be built after the property is set.
      client.close();
      client =
          new HttpUnityCatalogClient(
              URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
              Duration.ofSeconds(1),
              Duration.ofSeconds(5),
              () -> Map.of("Authorization", "Bearer catalog-token"));

      assertThatThrownBy(() -> client.listCatalogs())
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
                assertThat(failure.getMessage()).contains("paged past 3");
              });
      assertThat(requests.get()).isEqualTo(3);
    } finally {
      if (previous == null) {
        System.clearProperty(HttpUnityCatalogClient.MAX_PAGES_PROPERTY);
      } else {
        System.setProperty(HttpUnityCatalogClient.MAX_PAGES_PROPERTY, previous);
      }
    }
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
  void rejectsLoopbackCleartextUnlessTheOperatorOptsIn() {
    // False rather than cleared: an unset property falls back to
    // FLOECAT_SECURITY_ALLOW_LOOPBACK_CATALOG_ENDPOINTS, which may be set in the environment. The
    // property takes precedence.
    System.setProperty(HttpUnityCatalogClient.ALLOW_LOOPBACK_PROPERTY, "false");

    assertThatThrownBy(
            () ->
                new HttpUnityCatalogClient(
                    URI.create("http://127.0.0.1:8080"),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(5),
                    Map::of))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(HttpUnityCatalogClient.ALLOW_LOOPBACK_PROPERTY);
  }

  @Test
  void loopbackDetectionParsesLiteralsAndNeverResolvesNames() {
    // Admitted: every encoding of loopback, not only dotted-quad.
    for (String host :
        new String[] {
          "127.0.0.1",
          "127.0.0.001",
          "2130706433",
          "[::1]",
          "[::ffff:127.0.0.1]",
          "localhost",
          "LOCALHOST"
        }) {
      var uri = URI.create("http://" + host + ":8080");
      assertThatCode(
              () ->
                  new HttpUnityCatalogClient(
                          uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of)
                      .close())
          .as(host)
          .doesNotThrowAnyException();
    }

    // Denied. The last three are all digits and dots yet parse as no address, so a character-shape
    // pre-filter would pass them to a resolver. URI.getHost() is null for "127.1", so it never
    // reaches the check; "0177.0.0.1" is read as decimal 177.0.0.1, not octal.
    for (String host :
        new String[] {
          "1.2.3.4",
          "0.0.0.0",
          "catalog.example.com",
          "127.1",
          "4294967296",
          "12345678901234567890123",
          "1."
        }) {
      var uri = URI.create("http://" + host + ":8080");
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(host)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void rejectsNonPositiveTimeoutsAtConstruction() {
    var https = URI.create("https://catalog.example.com");
    for (Duration bad : new Duration[] {Duration.ZERO, Duration.ofMillis(-1)}) {
      assertThatThrownBy(
              () -> new HttpUnityCatalogClient(https, Duration.ofSeconds(1), bad, Map::of))
          .as("requestTimeout=%s", bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("requestTimeout must be positive");

      assertThatThrownBy(
              () -> new HttpUnityCatalogClient(https, bad, Duration.ofSeconds(5), Map::of))
          .as("connectTimeout=%s", bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("connectTimeout must be positive");
    }
  }

  @Test
  void emptySuccessBodyIsAClassifiedFailureRatherThanANullDereference() {
    // ObjectMapper.readTree returns MissingNode, not null, for an empty or blank body on Jackson
    // 2.10+.
    var body = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, body.get()));

    for (String empty : new String[] {"", "   ", "\n"}) {
      body.set(empty);

      assertThatThrownBy(() -> client.listCatalogs())
          .as("body=%s", empty.replace("\n", "\\n"))
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
                // The diagnostics the malformed-JSON path also keeps.
                assertThat(failure.getMessage()).contains("/api/2.1/unity-catalog/catalogs");
              });
    }
  }

  @Test
  void rejectsBaseUrisNamingUnreachableOrProbeAddressClasses() {
    // Refused whatever the flags say: nothing serves a catalog at any of these.
    for (String host :
        new String[] {
          "169.254.169.254",
          "0.0.0.0",
          "224.0.0.1",
          "[fe80::1]",
          "[ff02::1]",
          // No InetAddress predicate covers these two: limited broadcast, and 0.0.0.0/8.
          "255.255.255.255",
          "0.1.2.3"
        }) {
      var uri = URI.create("https://" + host);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(host)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("link-local, wildcard or multicast");
    }

    // All-digit hosts that ofLiteral refuses but the transport's resolver accepts modulo 2^32:
    // 7147006462 is 169.254.169.254 and 4294967296 is 0.0.0.0. Treated as hostnames they would
    // skip this gate and reach a link-local or wildcard target over HTTPS, with no opt-in.
    // A *.localhost name is not treated as loopback: it is not resolved here, and a tenant-owned
    // zone could point it anywhere.
    for (String host : new String[] {"catalog.localhost", "evil.localhost"}) {
      var uri = URI.create("https://" + host);
      assertThatCode(
              () ->
                  new HttpUnityCatalogClient(
                          uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of)
                      .close())
          .as("%s over HTTPS is fine; it is only the cleartext exemption that is withdrawn", host)
          .doesNotThrowAnyException();

      var cleartext = URI.create("http://" + host);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      cleartext, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(host)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("must use HTTPS");
    }

    // "1." is absent on purpose: unbracket strips the trailing dot and ofLiteral reads "1" as
    // 0.0.0.1, so the 0.0.0.0/8 rule above already refuses it.
    for (String host : new String[] {"7147006462", "4294967296", "12345678901234567890123"}) {
      var uri = URI.create("https://" + host);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(host)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("numeric host");
    }

    // Zone-scoped literals. ofLiteral cannot parse a zone id, so without an explicit check these
    // read as hostnames and skip the gate; the second is site-local and would need the opt-in.
    for (String host : new String[] {"[fe80::1%25eth0]", "[fd00::1%25eth0]"}) {
      var uri = URI.create("https://" + host);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(host)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("zone-scoped");
    }
  }

  @Test
  void privateAddressBaseUrisNeedAnOperatorOptIn() {
    var privateHosts = new String[] {"10.0.0.5", "172.16.0.1", "192.168.1.10", "[fd00::1]"};

    // False rather than cleared: an unset property falls back to
    // FLOECAT_SECURITY_ALLOW_PRIVATE_CATALOG_ENDPOINTS. The property takes precedence.
    var previous = System.setProperty(HttpUnityCatalogClient.ALLOW_PRIVATE_PROPERTY, "false");
    try {
      for (String host : privateHosts) {
        assertThatThrownBy(
                () ->
                    new HttpUnityCatalogClient(
                        URI.create("https://" + host),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(5),
                        Map::of))
            .as(host)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(HttpUnityCatalogClient.ALLOW_PRIVATE_PROPERTY);
      }

      System.setProperty(HttpUnityCatalogClient.ALLOW_PRIVATE_PROPERTY, "true");
      for (String host : privateHosts) {
        var uri = URI.create("https://" + host);
        assertThatCode(
                () ->
                    new HttpUnityCatalogClient(
                            uri, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of)
                        .close())
            .as(host)
            .doesNotThrowAnyException();
      }
    } finally {
      if (previous == null) {
        System.clearProperty(HttpUnityCatalogClient.ALLOW_PRIVATE_PROPERTY);
      } else {
        System.setProperty(HttpUnityCatalogClient.ALLOW_PRIVATE_PROPERTY, previous);
      }
    }
  }

  @Test
  void publicAndLoopbackHostsNeedNoAddressOptIn() {
    // Hostnames are not resolved, so they pass regardless of where they point. Loopback over HTTPS
    // is unaffected by the private-address gate.
    for (String uri :
        new String[] {"https://catalog.example.com", "https://8.8.8.8", "https://127.0.0.1"}) {
      var target = URI.create(uri);
      assertThatCode(
              () ->
                  new HttpUnityCatalogClient(
                          target, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of)
                      .close())
          .as(uri)
          .doesNotThrowAnyException();
    }
  }

  @Test
  void definitiveStatusesAreNotReclassifiedByTheirBody() {
    // The body is the more specific signal only where the status is ambiguous: a 401 stays an auth
    // failure whatever the envelope says, and a 429 stays retryable.
    record Case(int status, String errorCode, UnityCatalogException.Failure expected) {}
    var cases =
        List.of(
            new Case(401, "RESOURCE_DOES_NOT_EXIST", UnityCatalogException.Failure.UNAUTHENTICATED),
            new Case(403, "NOT_FOUND", UnityCatalogException.Failure.PERMISSION_DENIED),
            new Case(429, "PERMISSION_DENIED", UnityCatalogException.Failure.RATE_LIMITED),
            new Case(408, "PERMISSION_DENIED", UnityCatalogException.Failure.TRANSIENT),
            // 409 is ambiguous, so its body decides: a permanent conflict, an application-level
            // retry, a recognized denial, or nothing recognized at all.
            new Case(409, "ALREADY_EXISTS", UnityCatalogException.Failure.INVALID_REQUEST),
            new Case(409, "RESOURCE_ALREADY_EXISTS", UnityCatalogException.Failure.INVALID_REQUEST),
            new Case(409, "ABORTED", UnityCatalogException.Failure.OTHER),
            new Case(409, "UNAUTHENTICATED", UnityCatalogException.Failure.UNAUTHENTICATED),
            new Case(409, null, UnityCatalogException.Failure.TRANSIENT),
            // The endpoint will not accept this method; a rate-limit envelope must not make a
            // permanent refusal look retryable.
            new Case(405, "REQUEST_LIMIT_EXCEEDED", UnityCatalogException.Failure.INVALID_REQUEST),
            new Case(422, "REQUEST_LIMIT_EXCEEDED", UnityCatalogException.Failure.INVALID_REQUEST),
            // Not named in pass 1 either, but still authoritative: an envelope on one of these
            // marks it permanent by its presence, never by its value.
            new Case(415, "REQUEST_LIMIT_EXCEEDED", UnityCatalogException.Failure.INVALID_REQUEST),
            new Case(407, "RESOURCE_DOES_NOT_EXIST", UnityCatalogException.Failure.INVALID_REQUEST),
            new Case(412, "PERMISSION_DENIED", UnityCatalogException.Failure.INVALID_REQUEST),
            // No envelope: not the workspace answering, so it stays retryable.
            new Case(415, null, UnityCatalogException.Failure.OTHER),
            // Still ambiguous, so the body still wins for these two.
            new Case(400, "PERMISSION_DENIED", UnityCatalogException.Failure.PERMISSION_DENIED),
            new Case(404, "PERMISSION_DENIED", UnityCatalogException.Failure.PERMISSION_DENIED),
            new Case(404, null, UnityCatalogException.Failure.NOT_FOUND),
            // OSS Unity Catalog sends its own not-found codes. Unrecognized by
            // failureFromErrorCode, so the 404 status decides -- absence, not a request error.
            new Case(404, "TABLE_NOT_FOUND", UnityCatalogException.Failure.NOT_FOUND),
            new Case(404, "SCHEMA_NOT_FOUND", UnityCatalogException.Failure.NOT_FOUND),
            new Case(404, "CATALOG_NOT_FOUND", UnityCatalogException.Failure.NOT_FOUND));

    var current = new AtomicReference<Case>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          Case c = current.get();
          String body = c.errorCode() == null ? "{}" : "{\"error_code\":\"" + c.errorCode() + "\"}";
          respond(exchange, c.status(), body);
        });

    for (Case c : cases) {
      current.set(c);
      assertThatThrownBy(() -> client.listCatalogs())
          .as("%d + %s", c.status(), c.errorCode())
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> assertThat(failure.failure()).isEqualTo(c.expected()));
    }
  }

  @Test
  void theCredentialsRouteIsSelectableAndKeepsItsBodySuppression() {
    var requestedPath = new AtomicReference<String>();
    String secret = "oss-route-secret-that-must-not-be-logged";
    server.createContext(
        HttpUnityCatalogClient.OSS_TEMPORARY_TABLE_CREDENTIALS_PATH,
        exchange -> {
          requestedPath.set(exchange.getRequestURI().getRawPath());
          // Truncated JSON, to exercise the failure path that echoes a body.
          respond(exchange, 200, "{\"aws_temp_credentials\":{\"secret_access_key\":\"" + secret);
        });

    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer catalog-token"),
            HttpUnityCatalogClient.OSS_TEMPORARY_TABLE_CREDENTIALS_PATH);

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
              // Redaction follows the configured route, not the default constant.
              assertThat(failure.getMessage()).doesNotContain(secret, "secret_access_key");
              assertThat(failure).hasNoCause();
            });
    assertThat(requestedPath.get())
        .isEqualTo(HttpUnityCatalogClient.OSS_TEMPORARY_TABLE_CREDENTIALS_PATH);
  }

  @Test
  void rejectsACredentialsPathThatIsNotAnAbsolutePath() {
    for (String bad :
        new String[] {
          "api/2.1/creds",
          "/creds?x=1",
          "/creds#frag",
          // Absolute, but not usable in a URI. Left unchecked these throw from URI.create inside
          // send(), where everything becomes a retryable TRANSPORT failure.
          "/temporary credentials",
          "/%ZZ",
          "/a[b]"
        }) {
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      URI.create("https://catalog.example.com"),
                      Duration.ofSeconds(1),
                      Duration.ofSeconds(5),
                      Map::of,
                      bad))
          .as(bad)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("credentials path");
    }
  }

  @Test
  void rejectsUserinfoWithoutEchoingIt() {
    // Checked before the scheme and address gates, so no validation message can carry the secret.
    for (String uri :
        new String[] {
          "https://alice:s3cr3t@catalog.example.com",
          "http://alice:s3cr3t@catalog.example.com",
          "https://alice:s3cr3t@10.0.0.5",
          "https://token@catalog.example.com",
          // Registry-based authorities: URI reports host and userInfo as null for these, so the
          // guard has to read the raw authority or the URI reaches a check that echoes it.
          "https://alice:s3cr3t@a_b.example.com",
          "http://alice:s3cr3t@a_b.example.com",
          "https://alice:s3cr3t@host:port"
        }) {
      var target = URI.create(uri);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      target, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(uri)
          .isInstanceOfSatisfying(
              IllegalArgumentException.class,
              failure -> {
                assertThat(failure.getMessage()).contains("must not contain userinfo");
                assertThat(failure.getMessage()).doesNotContain("s3cr3t", "alice", "token@");
              });
    }
  }

  @Test
  void aRejectedAuthenticationHeaderNeverCarriesItsValue() {
    String token = "Bearer sup3r-s3cret\ntoken-with-a-newline";
    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", token));

    assertThatThrownBy(() -> client.listCatalogs())
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              // setHeader quotes the offending value; neither it nor the cause may reach a log.
              assertThat(failure.getMessage()).contains("Authorization").doesNotContain("sup3r");
              assertThat(failure).hasNoCause();
            });
  }

  @Test
  void anOversizedErrorCodeIsCappedFarBelowTheBody() {
    String flood = "X".repeat(50_000);
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> respond(exchange, 400, "{\"error_code\":\"" + flood + "\"}"));

    assertThatThrownBy(() -> client.listCatalogs())
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.getMessage().length()).isLessThan(5_000);
              // Capped as a code, not as a body: a value this long is not an error code, and the
              // accessor must not become a way to read response body.
              assertThat(failure.errorCode().length()).isLessThanOrEqualTo(64);
            });
  }

  @Test
  void rejectsAPortOutsideTheUsableRange() {
    for (String uri :
        new String[] {
          "https://catalog.example:65536",
          "https://catalog.example:0",
          "https://catalog.example:99999"
        }) {
      var target = URI.create(uri);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      target, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(uri)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("port must be between 1 and 65535");
    }

    // An absent port is not a range violation.
    assertThatCode(
            () ->
                new HttpUnityCatalogClient(
                        URI.create("https://catalog.example"),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(5),
                        Map::of)
                    .close())
        .doesNotThrowAnyException();
  }

  @Test
  void aMalformedListingEntryDoesNotLoseTheRestOfThePage() {
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange ->
            respond(
                exchange,
                200,
                "{\"tables\":["
                    + "{\"name\":\"first\",\"table_type\":\"TABLE\"},"
                    + "null,"
                    + "\"a-scalar\","
                    + "{\"name\":\"second\",\"table_type\":\"TABLE\"}"
                    + "]}"));

    assertThat(client.listTables("cat", "schema"))
        .extracting(UnityCatalogTable::name)
        .containsExactly("first", "second");
  }

  @Test
  void rejectsAMalformedColumnsNode() {
    var body = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/tables/", exchange -> respond(exchange, 200, body.get()));

    for (String malformed :
        new String[] {
          // An object: Jackson iterates its values as if it were an array.
          "{\"name\":\"v\",\"columns\":{\"name\":\"c\"}}",
          // A scalar: Jackson yields nothing, so the table would arrive with an empty schema.
          "{\"name\":\"v\",\"columns\":\"c\"}",
          "{\"name\":\"v\",\"columns\":7}",
          // An array of non-objects.
          "{\"name\":\"v\",\"columns\":[\"c\"]}",
          // Not a table object at all.
          "[]"
        }) {
      body.set(malformed);

      assertThatThrownBy(() -> client.getTable("main.default.v"))
          .as(malformed)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
    }
  }

  @Test
  void acceptsAnAbsentOrNullColumnsNode() {
    var body = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/tables/", exchange -> respond(exchange, 200, body.get()));

    for (String absent : new String[] {"{\"name\":\"v\"}", "{\"name\":\"v\",\"columns\":null}"}) {
      body.set(absent);

      assertThat(client.getTable("main.default.v")).isPresent();
    }
  }

  @Test
  void listTablesKeepsThePageWhenOneEntryHasMalformedColumns() {
    var malformedColumns = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange ->
            respond(
                exchange,
                200,
                """
                {"tables":[
                  {"name":"bad","columns":%s},
                  {"name":"good","columns":[{"name":"id","type_name":"INT"}]}
                ]}
                """
                    .formatted(malformedColumns.get())));

    for (String malformed : new String[] {"\"not-an-array\"", "[\"not-an-object\"]"}) {
      malformedColumns.set(malformed);

      List<UnityCatalogTable> tables = client.listTables("main", "default");

      assertThat(tables).extracting(UnityCatalogTable::name).containsExactly("bad", "good");
      assertThat(tables.get(0).columns()).isEmpty();
      assertThat(tables.get(1).columns())
          .singleElement()
          .satisfies(column -> assertThat(column.name()).isEqualTo("id"));
    }
  }

  @Test
  void errorCodeIsSuppressedOnTheCredentialsRouteLikeTheBody() {
    String secret = "reflected-secret-that-must-not-be-logged";
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 400, "{\"error_code\":\"" + secret + "\"}"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              // A remote-controlled field must not reach the message on this route; it stays
              // available for classification and on errorCode().
              assertThat(failure.getMessage()).doesNotContain(secret);
              // Not code-shaped, so it is withheld from the accessor too, not just the message.
              assertThat(failure.errorCode()).isNull();
              // Withholding the text does not erase that the workspace answered with a reason. A
              // caller deciding whether the refusal is permanent reads this, not the code.
              assertThat(failure.hasErrorEnvelope()).isTrue();
            });
  }

  @Test
  void aWrongRouteIsNotReportedAsAMissingObject() {
    // Databricks answers an unrecognized API path with 404 plus ENDPOINT_NOT_FOUND. Read as
    // NOT_FOUND it reaches getTable's absent-table branch, so a base URI whose prefix a proxy does
    // not route answers "no such table" instead of failing -- a wrong answer, not a failed call.
    server.createContext(
        "/api/2.1/unity-catalog/tables",
        exchange -> respond(exchange, 404, "{\"error_code\":\"ENDPOINT_NOT_FOUND\"}"));

    assertThatThrownBy(() -> client.getTable("cat.schema.orders"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
  }

  @Test
  void aTableTheCatalogDoesNotHaveIsStillAbsentRatherThanAFailure() {
    // The other side of the same boundary: OSS Unity Catalog sends no envelope, and a genuine
    // missing table must keep reading as absent on both wire shapes.
    var body = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/tables", exchange -> respond(exchange, 404, body.get()));

    for (String absent :
        new String[] {
          "{}",
          "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\"}",
          "{\"error_code\":\"TABLE_NOT_FOUND\"}"
        }) {
      body.set(absent);
      assertThat(client.getTable("cat.schema.orders")).as(absent).isEmpty();
    }
  }

  @Test
  void anOverLongCodeShapedValueIsWithheldOnTheCredentialsRoute() {
    // The length half of the shape test, which the charset half hides: the sibling test's value
    // fails on its hyphens, so nothing exercised length. Judged on the capped value, 500 uppercase
    // characters become 64 that pass as a code and publish that much response body through
    // errorCode() -- on the one route whose bodies are suppressed precisely because they may carry
    // a secret.
    String flood = "A".repeat(500);
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 400, "{\"error_code\":\"" + flood + "\"}"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.errorCode()).isNull();
              assertThat(failure.getMessage()).doesNotContain("AAAA");
              // Withholding the text still leaves the refusal classifiable as permanent, which is
              // what the stacked consumer reads.
              assertThat(failure.hasErrorEnvelope()).isTrue();
            });
  }

  @Test
  void onlyRecognizedCodesAreRepeatedOnTheCredentialsRoute() {
    // Membership, not shape. [A-Z0-9_] up to the cap is also the shape of an AWS access key id, so
    // "looks like a code" let a slice of a body this route suppresses through a public accessor.
    // Both sides of the boundary, so the rule cannot collapse into "withhold everything".
    record Case(String code, String reported) {}
    var cases =
        List.of(
            new Case("INVALID_PARAMETER_VALUE", "INVALID_PARAMETER_VALUE"),
            new Case("PERMISSION_DENIED", "PERMISSION_DENIED"),
            // Code-shaped and within the cap, but not Databricks vocabulary.
            new Case("A".repeat(64), null),
            new Case("AKIAIOSFODNN7EXAMPLE", null));

    var current = new AtomicReference<Case>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 400, "{\"error_code\":\"" + current.get().code() + "\"}"));

    for (Case c : cases) {
      current.set(c);
      assertThatThrownBy(
              () ->
                  client.generateTemporaryTableCredentials(
                      "table-id", UnityCatalogClient.TableOperation.READ))
          .as(c.code())
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.errorCode()).as(c.code()).isEqualTo(c.reported());
                // Withholding the text never changes whether the workspace answered, which is what
                // the stacked consumer decides terminality on.
                assertThat(failure.hasErrorEnvelope()).as(c.code()).isTrue();
              });
    }
  }

  @Test
  void aClientErrorWithNoEnvelopeReportsNoEnvelope() {
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
              assertThat(failure.errorCode()).isNull();
              assertThat(failure.hasErrorEnvelope()).isFalse();
            });
  }

  @Test
  void rejectionMessagesDoNotEchoAQueryString() {
    // A query is a common place for a token, and the userinfo guard only covers the authority.
    // The HTTPS gate fires before the query gate, so both have to withhold it.
    record Case(String uri, String host) {}
    for (Case c :
        List.of(
            new Case("http://catalog.example.com/?token=s3cr3t", "catalog.example.com"),
            new Case("https://catalog.example.com/?token=s3cr3t", "catalog.example.com"),
            new Case("https://catalog.example.com:65536/?token=s3cr3t", "catalog.example.com"),
            new Case("https://10.0.0.5/?token=s3cr3t#f=s3cr3t", "10.0.0.5"))) {
      var target = URI.create(c.uri());
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      target, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(c.uri())
          .isInstanceOfSatisfying(
              IllegalArgumentException.class,
              failure -> {
                assertThat(failure.getMessage()).doesNotContain("s3cr3t");
                // Still names what it rejected.
                assertThat(failure.getMessage()).contains(c.host());
              });
    }
  }

  @Test
  void anOversizedResponseBodyIsRefusedRatherThanBuffered() {
    var previous = System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, "4096");
    try {
      server.createContext(
          "/api/2.1/unity-catalog/catalogs",
          exchange ->
              respond(exchange, 200, "{\"catalogs\":[" + "{\"name\":\"c\"},".repeat(2_000) + "]}"));

      client.close();
      client =
          new HttpUnityCatalogClient(
              URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
              Duration.ofSeconds(1),
              Duration.ofSeconds(5),
              () -> Map.of("Authorization", "Bearer catalog-token"));

      assertThatThrownBy(() -> client.listCatalogs())
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
                assertThat(failure.getMessage()).contains("exceeded 4096 bytes");
              });
    } finally {
      if (previous == null) {
        System.clearProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY);
      } else {
        System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, previous);
      }
    }
  }

  @Test
  void anOversizedFailureBodyStillClassifiesFromItsStatus() {
    var previous = System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, "1024");
    try {
      record Case(int status, UnityCatalogException.Failure expected) {}
      var current = new AtomicReference<Case>();
      // An error page far larger than the cap, as a proxy or WAF in front of the catalog emits.
      String page = "<html>" + "y".repeat(8_000) + "</html>";
      server.createContext(
          "/api/2.1/unity-catalog/catalogs",
          exchange -> respond(exchange, current.get().status(), page));

      client.close();
      client =
          new HttpUnityCatalogClient(
              URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
              Duration.ofSeconds(1),
              Duration.ofSeconds(5),
              () -> Map.of("Authorization", "Bearer catalog-token"));

      for (Case c :
          List.of(
              new Case(403, UnityCatalogException.Failure.PERMISSION_DENIED),
              new Case(401, UnityCatalogException.Failure.UNAUTHENTICATED),
              new Case(429, UnityCatalogException.Failure.RATE_LIMITED),
              new Case(503, UnityCatalogException.Failure.SERVER_ERROR))) {
        current.set(c);
        assertThatThrownBy(() -> client.listCatalogs())
            .as("HTTP %d with an oversized body", c.status())
            .isInstanceOfSatisfying(
                UnityCatalogException.class,
                failure -> {
                  // The status decides; the oversized body is dropped, not reported.
                  assertThat(failure.failure()).isEqualTo(c.expected());
                  assertThat(failure.statusCode()).isEqualTo(c.status());
                  assertThat(failure.getMessage()).doesNotContain("yyyy");
                });
      }
    } finally {
      if (previous == null) {
        System.clearProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY);
      } else {
        System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, previous);
      }
    }
  }

  @Test
  void aBodySizedAtTheCapIsStillRead() {
    var previous = System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, "4096");
    try {
      // Padded to just under the cap, so the boundary is exercised rather than only the overrun.
      String padding = "x".repeat(3_500);
      server.createContext(
          "/api/2.1/unity-catalog/catalogs",
          exchange ->
              respond(
                  exchange,
                  200,
                  "{\"catalogs\":[{\"name\":\"only\",\"pad\":\"" + padding + "\"}]}"));

      client.close();
      client =
          new HttpUnityCatalogClient(
              URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
              Duration.ofSeconds(1),
              Duration.ofSeconds(5),
              () -> Map.of("Authorization", "Bearer catalog-token"));

      assertThat(client.listCatalogs()).containsExactly("only");
    } finally {
      if (previous == null) {
        System.clearProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY);
      } else {
        System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, previous);
      }
    }
  }

  @Test
  void aCodeShapedErrorCodeSurvivesOnTheCredentialsRoute() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 403, "{\"error_code\":\"PERMISSION_DENIED\"}"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              // A real code stays available; only unshaped, reflected content is withheld.
              assertThat(failure.errorCode()).isEqualTo("PERMISSION_DENIED");
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.PERMISSION_DENIED);
            });
  }

  @Test
  void rejectsAnUnusableResponseSizeCap() {
    for (String bad : new String[] {"0", "-1", "abc", String.valueOf(Integer.MAX_VALUE)}) {
      var previous = System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, bad);
      try {
        assertThatThrownBy(
                () ->
                    new HttpUnityCatalogClient(
                        URI.create("https://catalog.example.com"),
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(5),
                        Map::of))
            .as(bad)
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY);
      } finally {
        if (previous == null) {
          System.clearProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY);
        } else {
          System.setProperty(HttpUnityCatalogClient.MAX_RESPONSE_BYTES_PROPERTY, previous);
        }
      }
    }
  }

  @Test
  void anEchoedBearerTokenIsRedactedFromFailureMessages() {
    // Some gateways and debug handlers reflect request headers in an error body, and that body is
    // interpolated into the exception on every route except vending.
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange ->
            respond(
                exchange,
                400,
                "{\"error\":\"bad request\",\"echo\":{\"Authorization\":\"Bearer "
                    + "catalog-token\"}}"));

    assertThatThrownBy(() -> client.listCatalogs())
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.getMessage()).doesNotContain("catalog-token");
              assertThat(failure.getMessage()).contains("Bearer <redacted>");
              // The rest of the body still reaches the message.
              assertThat(failure.getMessage()).contains("bad request");
            });
  }

  @Test
  void rejectionMessagesWithholdAnAuthorityUriCannotParse() {
    // Percent-encoding the delimiter defeats a literal '@' check: URI reports host and both
    // userinfo accessors as null while the raw authority still carries the credential.
    for (String uri :
        new String[] {
          // A literal '@' is caught earlier by the userinfo guard; these reach the host check.
          "https://alice:s3cr3t%40catalog.example.com", "http://alice:s3cr3t%40catalog.example.com"
        }) {
      var target = URI.create(uri);
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      target, Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(uri)
          .isInstanceOfSatisfying(
              IllegalArgumentException.class,
              failure -> {
                assertThat(failure.getMessage()).doesNotContain("s3cr3t", "alice");
                assertThat(failure.getMessage()).contains("<unparseable-authority>");
              });
    }
  }

  @Test
  void aBaseUriWithRepeatedTrailingSlashesIsNormalized() {
    var requestedPath = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          requestedPath.set(exchange.getRequestURI().getRawPath());
          respond(exchange, 200, "{\"catalogs\":[{\"name\":\"only\"}]}");
        });

    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "///"),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> Map.of("Authorization", "Bearer catalog-token"));

    assertThat(client.listCatalogs()).containsExactly("only");
    // Not "//api/2.1/...", which proxies answer inconsistently.
    assertThat(requestedPath.get()).isEqualTo("/api/2.1/unity-catalog/catalogs");
  }

  @Test
  void rejectsUnusableBaseUrisAtConstruction() {
    for (String uri :
        new String[] {
          "ftp://catalog.example",
          "htps://catalog.example",
          "https:/missing-authority",
          "https://catalog.example?route=wrong",
          "https://catalog.example#wrong"
        }) {
      assertThatThrownBy(
              () ->
                  new HttpUnityCatalogClient(
                      URI.create(uri), Duration.ofSeconds(1), Duration.ofSeconds(5), Map::of))
          .as(uri)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Unity Catalog base URI");
    }
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
  void getTableDoesNotHideARecognizedPermissionFailureBehind404() {
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange ->
            respond(
                exchange,
                404,
                "{\"error_code\":\"PERMISSION_DENIED\",\"message\":\"access denied\"}"));

    assertThatThrownBy(() -> client.getTable("cat.schema.hidden"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.PERMISSION_DENIED);
              assertThat(failure.statusCode()).isEqualTo(404);
              assertThat(failure.errorCode()).isEqualTo("PERMISSION_DENIED");
            });
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
  void credentialStringRepresentationNeverIncludesSecrets() {
    var withSession =
        new TemporaryTableCredentials.AwsCredentials(
            "AKIAEXAMPLE", "top-secret-key", "top-secret-token", "arn:accesspoint/example");
    var withoutSession =
        new TemporaryTableCredentials.AwsCredentials("AKIAEXAMPLE", "top-secret-key", null, null);
    var envelope = new TemporaryTableCredentials(withSession, false, "1893456000000", "s3://b/t");

    // The outer record's generated toString() delegates to the nested one, so both are covered.
    assertThat(withSession.toString())
        .doesNotContain("AKIAEXAMPLE", "top-secret-key", "top-secret-token")
        .contains("accesspoint/example");
    assertThat(envelope.toString()).doesNotContain("top-secret-key", "top-secret-token");
    assertThat(withoutSession.toString()).contains("sessionToken=<absent>");
  }

  @Test
  void temporaryCredentialsRejectANonObjectSuccessResponse() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 200, "[]"));

    assertThatThrownBy(
            () ->
                client.generateTemporaryTableCredentials(
                    "table-id", UnityCatalogClient.TableOperation.READ))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure ->
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
  }

  @Test
  void aRefusedRedirectIsNotReclassifiedByItsBody() {
    // The body code wins only for a 4xx. A 3xx is a base-URI misconfiguration, not an absent
    // table.
    server.createContext(
        "/api/2.1/unity-catalog/tables/",
        exchange -> {
          exchange.getResponseHeaders().add("Location", "/moved/tables");
          respond(exchange, 302, "{\"error_code\":\"RESOURCE_DOES_NOT_EXIST\"}");
        });

    assertThatThrownBy(() -> client.getTable("main.default.call_center"))
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              // Still reported, just not used to classify.
              assertThat(failure.errorCode()).isEqualTo("RESOURCE_DOES_NOT_EXIST");
            });
  }

  @Test
  void temporaryCredentialsRejectAWrongTypedAwsField() {
    var awsField = new AtomicReference<String>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                200,
                "{\"aws_temp_credentials\":" + awsField.get() + ",\"url\":\"s3://b/t\"}"));

    for (String wrongTyped :
        new String[] {"\"AKIAEXAMPLE\"", "[]", "[{\"access_key_id\":\"key\"}]", "42", "true"}) {
      awsField.set(wrongTyped);

      assertThatThrownBy(
              () ->
                  client.generateTemporaryTableCredentials(
                      "table-id", UnityCatalogClient.TableOperation.READ))
          .as(wrongTyped)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
    }
  }

  @Test
  void temporaryCredentialsTreatAMissingOrNullAwsFieldAsAbsent() {
    var body = new AtomicReference<String>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange -> respond(exchange, 200, body.get()));

    for (String absent :
        new String[] {
          "{\"url\":\"s3://b/t\"}", "{\"aws_temp_credentials\":null,\"url\":\"s3://b/t\"}"
        }) {
      body.set(absent);

      TemporaryTableCredentials credentials =
          client.generateTemporaryTableCredentials(
              "table-id", UnityCatalogClient.TableOperation.READ);

      assertThat(credentials.awsCredentials()).as(absent).isNull();
      assertThat(credentials.hasUnsupportedCredentials()).as(absent).isFalse();
    }
  }

  @Test
  void temporaryCredentialsRejectAnAwsObjectWithoutAKeyAndSecret() {
    var awsObjectToReturn = new AtomicReference<String>();
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                200,
                "{\"aws_temp_credentials\":" + awsObjectToReturn.get() + ",\"url\":\"s3://b/t\"}"));

    for (String awsObject :
        new String[] {
          "{}",
          "{\"access_key_id\":\"key\"}",
          "{\"secret_access_key\":\"secret\"}",
          "{\"access_key_id\":\"\",\"secret_access_key\":\"secret\"}",
          "{\"access_key_id\":\"key\",\"secret_access_key\":\"   \"}",
          "{\"session_token\":\"token\",\"access_point\":\"arn:accesspoint/example\"}"
        }) {
      awsObjectToReturn.set(awsObject);

      assertThatThrownBy(
              () ->
                  client.generateTemporaryTableCredentials(
                      "table-id", UnityCatalogClient.TableOperation.READ))
          .as(awsObject)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE));
    }
  }

  @Test
  void temporaryCredentialsAllowAnAbsentAwsObjectForUnsupportedClouds() {
    server.createContext(
        "/api/2.0/unity-catalog/temporary-table-credentials",
        exchange ->
            respond(
                exchange,
                200,
                "{\"azure_user_delegation_sas\":{\"sas_token\":\"t\"},\"url\":\"abfss://c/t\"}"));

    TemporaryTableCredentials credentials =
        client.generateTemporaryTableCredentials(
            "table-id", UnityCatalogClient.TableOperation.READ);

    assertThat(credentials.awsCredentials()).isNull();
    assertThat(credentials.hasUnsupportedCredentials()).isTrue();
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
  void invalidAuthenticationConfigurationIsPermanent() {
    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              throw new IllegalArgumentException("Missing auth property: token");
            });

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              assertThat(failure).hasCauseInstanceOf(IllegalArgumentException.class);
            });
  }

  @Test
  void anInterruptedAuthProviderIsNotClassifiedAsATransportFault() {
    // The provider restores the flag and throws when its refresh is interrupted. Read as TRANSPORT
    // this is retried, and since the flag stays set every retry fails here again at once, spending
    // the caller's budget on a cancellation instead of stopping.
    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              Thread.currentThread().interrupt();
              throw new RuntimeException("interrupted refreshing the token");
            });

    try {
      assertThatThrownBy(client::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.INTERRUPTED);
                assertThat(failure.statusCode()).isEqualTo(-1);
              });
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void aRuntimeFailureFromTheProviderThatIsNotAnInterruptStaysTransport() {
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
            failure ->
                assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.TRANSPORT));
  }

  @Test
  void aStatusOfMinusOneMarksAFailureWithNoHttpStatusRatherThanZero() {
    // Consumers separate "never reached the workspace" from "the workspace refused" with
    // statusCode() >= 0, so a throw site with no status has to report -1. A shape rejection also
    // reports -1 despite arriving on a 200, which is why -1 alone is not the reached-the-workspace
    // signal; the parse-level INVALID_RESPONSE in malformedSuccessResponseIsAnInvalidResponse
    // carries the real 200.
    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              throw new IllegalArgumentException("Missing auth property: token");
            });

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
              assertThat(failure.statusCode()).isEqualTo(-1);
            });

    client.close();
    client =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            Map::of);
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, "[]"));

    assertThatThrownBy(client::listCatalogs)
        .isInstanceOfSatisfying(
            UnityCatalogException.class,
            failure -> {
              assertThat(failure.failure())
                  .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
              assertThat(failure.statusCode()).isEqualTo(-1);
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
    // `name` is the sort key and Stream.sorted() uses natural ordering, so a null fails the whole
    // page.
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
              // Suppressed from the message on this route along with the body; the classification
              // and errorCode() above are what the caller reads.
              assertThat(failure.getMessage()).doesNotContain("PERMISSION_DENIED");
              assertThat(failure.getMessage()).doesNotContain("missing EXTERNAL USE SCHEMA");
            });
  }

  @Test
  void barePermanentClientErrorsAreInvalidRequests() {
    var status = new AtomicInteger();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> respond(exchange, status.get(), "client request rejected"));

    for (int permanentStatus : new int[] {405, 422}) {
      status.set(permanentStatus);
      assertThatThrownBy(client::listCatalogs)
          .as("HTTP %s", permanentStatus)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST);
                assertThat(failure.statusCode()).isEqualTo(permanentStatus);
              });
    }
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
   * The permanent default applies only to a 4xx carrying the {@code error_code} envelope. An HTML
   * 400 from a load balancer in front of the workspace is not the workspace refusing anything.
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
   * A 4xx that is transient by definition must not fall into the permanent default. 408 is a proxy
   * timeout ahead of a slow workspace.
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
                assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.TRANSIENT));
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
   * A 3xx the client declined to follow is a misconfigured base URI, not an outage, so it is
   * permanent rather than {@code OTHER}.
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
  void anUnusableAuthenticationHeaderIsPermanent() {
    // A refused header value is a property of what the provider returned, so a retry fails
    // identically.
    var nullValued = new java.util.HashMap<String, String>();
    nullValued.put("Authorization", null);
    try (var badAuth =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> nullValued)) {
      assertThatThrownBy(badAuth::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure())
                      .isEqualTo(UnityCatalogException.Failure.INVALID_REQUEST));
    }
  }

  @Test
  void anAuthenticationProviderFailureStaysRetryable() {
    // The other half of the split: producing the header map can fail transiently -- a token
    // endpoint briefly unreachable -- and must not be classified terminally.
    try (var failingAuth =
        new HttpUnityCatalogClient(
            URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
            Duration.ofSeconds(1),
            Duration.ofSeconds(5),
            () -> {
              throw new IllegalStateException("token endpoint unreachable");
            })) {
      assertThatThrownBy(failingAuth::listCatalogs)
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure ->
                  assertThat(failure.failure()).isEqualTo(UnityCatalogException.Failure.TRANSPORT));
    }
  }

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

  @Test
  void anEmptyTwoHundredBodyKeepsItsStatusSoItStaysRetryable() {
    // Headers then a close -- a sidecar restarting, a proxy that dropped the payload. Jackson
    // parses "" to a missing node instead of throwing, so this used to reach the caller's shape
    // check and be reported with no status, which a consumer reads as a permanent shape rejection.
    // The same connection truncated one byte later throws from readNBytes and is retryable; these
    // two must not disagree.
    var current = new AtomicReference<String>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs", exchange -> respond(exchange, 200, current.get()));

    for (String empty : new String[] {"", "   ", "\n"}) {
      current.set(empty);
      assertThatThrownBy(() -> client.listCatalogs())
          .as("[%s]", empty.replace("\n", "\\n"))
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure())
                    .isEqualTo(UnityCatalogException.Failure.INVALID_RESPONSE);
                // The real status, not -1: that is what keeps it out of the permanent arm.
                assertThat(failure.statusCode()).isEqualTo(200);
              });
    }
  }

  @Test
  void aBodyThatStopsMidStreamKeepsTheStatusThatAlreadyArrived() {
    // ofInputStream returns once the headers are in, so the status is known before the body can
    // fail. Declaring a Content-Length and closing early is what a proxy does when it gives up
    // mid-response; reading the body inside the send would report TRANSPORT with -1 and lose the
    // 403, which the reconciler retries forever.
    record Case(int status, UnityCatalogException.Failure expected, int expectedStatusCode) {}
    var cases =
        List.of(
            new Case(403, UnityCatalogException.Failure.PERMISSION_DENIED, 403),
            new Case(401, UnityCatalogException.Failure.UNAUTHENTICATED, 401),
            new Case(429, UnityCatalogException.Failure.RATE_LIMITED, 429),
            // A 2xx that never finished went out and did not complete, which is what statusCode()
            // documents -1 for, and a retry may get the rest.
            new Case(200, UnityCatalogException.Failure.TRANSPORT, -1));

    var current = new AtomicReference<Case>();
    server.createContext(
        "/api/2.1/unity-catalog/catalogs",
        exchange -> {
          exchange.sendResponseHeaders(current.get().status(), 4096);
          try (var output = exchange.getResponseBody()) {
            output.write("{\"cata".getBytes(StandardCharsets.UTF_8));
            output.flush();
          }
        });

    for (Case c : cases) {
      current.set(c);
      assertThatThrownBy(() -> client.listCatalogs())
          .as("%d", c.status())
          .isInstanceOfSatisfying(
              UnityCatalogException.class,
              failure -> {
                assertThat(failure.failure()).as("%d", c.status()).isEqualTo(c.expected());
                assertThat(failure.statusCode())
                    .as("%d", c.status())
                    .isEqualTo(c.expectedStatusCode());
              });
    }
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
