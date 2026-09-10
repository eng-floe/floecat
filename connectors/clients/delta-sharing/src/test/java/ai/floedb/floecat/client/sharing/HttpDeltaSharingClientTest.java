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
package ai.floedb.floecat.client.sharing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.client.sharing.DeltaSharingModel.AccessMode;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.CredentialCloud;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TemporaryCredentials;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpDeltaSharingClientTest {

  private HttpServer server;
  private final List<String> requestPaths = new ArrayList<>();
  private final List<String> capabilityHeaders = new ArrayList<>();
  private final List<String> authorizationHeaders = new ArrayList<>();
  private final List<String> acceptHeaders = new ArrayList<>();

  @BeforeEach
  void allowCleartextLoopback() {
    // The endpoint gates require HTTPS. A test server is loopback cleartext, which is the shape the
    // opt-in exists for.
    System.setProperty("floecat.security.allow-loopback-catalog-endpoints", "true");
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("floecat.security.allow-loopback-catalog-endpoints");
    System.clearProperty(HttpDeltaSharingClient.MAX_PAGES_PROPERTY);
    System.clearProperty(HttpDeltaSharingClient.MAX_LISTING_BYTES_PROPERTY);
    System.clearProperty(HttpDeltaSharingClient.MAX_RESPONSE_BYTES_PROPERTY);
    if (server != null) {
      server.stop(0);
      server = null;
    }
    requestPaths.clear();
    capabilityHeaders.clear();
    authorizationHeaders.clear();
  }

  // ---------------------------------------------------------------------------
  // Discovery and paging
  // ---------------------------------------------------------------------------

  @Test
  void listSharesFollowsPagesUntilTheTokenIsEmpty() throws Exception {
    AtomicInteger call = new AtomicInteger();
    startServer(
        exchange -> {
          int n = call.getAndIncrement();
          respond(
              exchange,
              200,
              n == 0
                  ? "{\"items\":[{\"name\":\"one\"}],\"nextPageToken\":\"t1\"}"
                  : "{\"items\":[{\"name\":\"two\"}],\"nextPageToken\":\"\"}");
        });

    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares())
          .extracting(DeltaSharingModel.Share::name)
          .containsExactly("one", "two");
    }
    assertThat(requestPaths.get(1)).contains("pageToken=t1");
  }

  @Test
  void anAbsentItemsArrayIsNotAnError() throws Exception {
    // The protocol says items may be absent or empty and a client has to handle either.
    startServer(exchange -> respond(exchange, 200, "{\"nextPageToken\":\"\"}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares()).isEmpty();
    }
  }

  @Test
  void aRepeatedPageTokenIsRefusedRatherThanLoopedOn() throws Exception {
    // A server returning the same cursor forever would otherwise page until the cap, issuing
    // thousands of requests against an endpoint the recipient does not control.
    startServer(
        exchange ->
            respond(exchange, 200, "{\"items\":[{\"name\":\"one\"}],\"nextPageToken\":\"same\"}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("repeated a page token")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void aServerMintingFreshTokensStopsAtThePageCap() throws Exception {
    // The other half of the bound: a repeated-token guard alone does not stop a server that issues
    // a new cursor every time.
    System.setProperty(HttpDeltaSharingClient.MAX_PAGES_PROPERTY, "3");
    AtomicInteger call = new AtomicInteger();
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[],\"nextPageToken\":\"t" + call.getAndIncrement() + "\"}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares).hasMessageContaining("exceeded 3 pages");
    }
    assertThat(requestPaths).hasSize(3);
  }

  @Test
  void anAbsentAccessModesFieldIsLeftUnresolvedRatherThanReadAsUrl() throws Exception {
    // Left as it arrived. The protocol reads an absent field as url only, but resolving it here
    // would fix that reading for every caller, and the catalog provider owns the choice for
    // backward compatibility, and it decides whether a table
    // is readable at all through this client.
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"location\":\"s3://b/t\"}],\"nextPageToken\":\"\"}"));
    try (DeltaSharingClient client = client()) {
      DeltaSharingModel.Table table = client.listTables("s", "sc").get(0);
      assertThat(table.accessModes()).isEmpty();
      assertThat(table.accessModes()).doesNotContain(DeltaSharingModel.AccessMode.DIR);
    }
  }

  @Test
  void directoryAccessIsRecognisedAndUnknownModesAreKept() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"id\":\"abc\",\"location\":\"s3://b/t\","
                    + "\"accessModes\":[\"url\",\"dir\",\"future\"]}],\"nextPageToken\":\"\"}"));
    try (DeltaSharingClient client = client()) {
      DeltaSharingModel.Table table = client.listTables("s", "sc").get(0);
      // Kept, not dropped: a list holding dir still holds dir, and keeping the unknown value is
      // what makes a table stating only unknown modes distinguishable from one stating none.
      assertThat(table.accessModes())
          .containsExactly(AccessMode.URL, AccessMode.DIR, AccessMode.OTHER);
      assertThat(table.accessModes()).contains(DeltaSharingModel.AccessMode.DIR);
      assertThat(table.id()).contains("abc");
    }
  }

  // ---------------------------------------------------------------------------
  // NDJSON metadata
  // ---------------------------------------------------------------------------

  @Test
  void metadataParsesTheTwoNdjsonActions() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":3,\"readerFeatures\":[\"deletionVectors\"]}}\n"
                    + "{\"metaData\":{\"id\":\"m1\",\"format\":{\"provider\":\"parquet\"},"
                    + "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\"}\","
                    + "\"partitionColumns\":[\"day\"],\"version\":7,"
                    + "\"location\":\"s3://b/t\"}}"));
    try (DeltaSharingClient client = client()) {
      TableDescription described = client.describeTable("s", "sc", "t");
      assertThat(described.protocol().minReaderVersion()).isEqualTo(3);
      assertThat(described.protocol().readerFeatures()).containsExactly("deletionVectors");
      assertThat(described.metadata().partitionColumns()).containsExactly("day");
      assertThat(described.metadata().location()).contains("s3://b/t");
      assertThat(described.version()).contains(7L);
    }
  }

  /**
   * The shape a server answers with when it honours the {@code responseformat=delta} capability
   * this client sends on every metadata request, taken from the protocol's own example: the table's
   * fields nest under {@code deltaMetadata} while version, location, auxiliary locations and access
   * modes stay on the wrapper. Read flat, the schema arrived blank and the model rejected it before
   * any of this class's classification could run.
   */
  @Test
  void metadataParsesTheDeltaResponseFormatEnvelope() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"deltaProtocol\":{\"minReaderVersion\":3,"
                    + "\"readerFeatures\":[\"columnMapping\"]}}}\n"
                    + "{\"metaData\":{\"version\":20,\"size\":123456,\"numFiles\":5,"
                    + "\"location\":\"s3://b/t\","
                    + "\"auxiliaryLocations\":[\"s3://b/t-aux\"],"
                    + "\"accessModes\":[\"url\",\"dir\"],"
                    + "\"deltaMetadata\":{\"id\":\"m1\","
                    + "\"format\":{\"provider\":\"parquet\"},"
                    + "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\"}\","
                    + "\"partitionColumns\":[\"date\"],"
                    + "\"configuration\":{\"enableChangeDataFeed\":\"true\"}}}}"));
    try (DeltaSharingClient client = client()) {
      TableDescription described = client.describeTable("s", "sc", "t");
      assertThat(described.protocol().minReaderVersion()).isEqualTo(3);
      assertThat(described.protocol().readerFeatures()).containsExactly("columnMapping");
      assertThat(described.metadata().schemaJson()).contains("struct");
      assertThat(described.metadata().id()).contains("m1");
      assertThat(described.metadata().format()).isEqualTo("parquet");
      assertThat(described.metadata().partitionColumns()).containsExactly("date");
      assertThat(described.metadata().configuration())
          .containsEntry("enableChangeDataFeed", "true");
      assertThat(described.metadata().location()).contains("s3://b/t");
      assertThat(described.metadata().auxiliaryLocations()).containsExactly("s3://b/t-aux");
      assertThat(described.metadata().accessModes())
          .containsExactly(DeltaSharingModel.AccessMode.URL, DeltaSharingModel.AccessMode.DIR);
      assertThat(described.version()).contains(20L);
    }
  }

  /**
   * Destructive rather than merely wrong: the reconciler reads a successful empty inventory as the
   * share having nothing left and retires the overlay's tables, so a malformed 200 deletes what a
   * recorded failure would have preserved.
   */
  @Test
  void aNonArrayItemsFieldIsAProtocolViolationNotAnEmptyListing() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":{\"name\":\"prod\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array items")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * An explicit null is not an array either, and exempting it made a malformed page authoritative.
   */
  @Test
  void anExplicitNullItemsFieldIsAProtocolViolation() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":null}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array items")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * A header value permits more than the bearer grammar does, so a token carrying a colon matched
   * the redaction pattern only up to it and the remainder reached the message -- defeating the
   * control in the echoed-header case it exists for.
   */
  @Test
  void aTokenOutsideTheBearerGrammarIsStillRedactedWhenEchoed() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                500,
                "upstream failed; headers were {Authorization=Bearer part1:part2-SECRET}"));
    try (DeltaSharingClient client =
        new HttpDeltaSharingClient(
            endpoint(), "part1:part2-SECRET", Duration.ofSeconds(2), Duration.ofSeconds(2))) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain("part2-SECRET"));
    }
  }

  @Test
  void aPageThatIsNotAnObjectIsAProtocolViolation() throws Exception {
    startServer(exchange -> respond(exchange, 200, "[\"prod\"]"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("is not an object")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** Absent stays legal, and means an empty listing. */
  @Test
  void anAbsentItemsFieldIsAnEmptyListing() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares()).isEmpty();
    }
  }

  /** So does an empty array, which means the same thing. */
  @Test
  void anEmptyItemsArrayIsAnEmptyListing() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":[]}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares()).isEmpty();
    }
  }

  /** A non-positive override made every listing fail against a server that answered correctly. */
  @Test
  void aNonPositiveLimitIsRejectedRatherThanFailingEveryListing() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":[]}"));
    System.setProperty(HttpDeltaSharingClient.MAX_PAGES_PROPERTY, "0");
    try {
      assertThatThrownBy(this::client).isInstanceOf(IllegalArgumentException.class);
    } finally {
      System.clearProperty(HttpDeltaSharingClient.MAX_PAGES_PROPERTY);
    }
  }

  /**
   * Left to the caller's own completeness check, an incomplete triad reads as "this cloud cannot be
   * published" and reports UNSUPPORTED -- a per-table skip meaning the provider will never do this,
   * when in fact the server sent a malformed envelope.
   */
  @Test
  void anIncompleteAwsSessionIsReportedAsAProtocolViolation() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\",\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"a\","
                    + "\"secretAccessKey\":\"b\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("incomplete AWS session")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * The protocol states the current version in this header for an ordinary metadata request and
   * populates the action's own version only for a versioned or change-feed query, so reading the
   * body alone left the version empty against every conforming server.
   */
  @Test
  void theTableVersionComesFromTheResponseHeader() throws Exception {
    startServer(
        exchange -> {
          exchange.getResponseHeaders().add("Delta-Table-Version", "42");
          respond(
              exchange,
              200,
              "{\"protocol\":{\"deltaProtocol\":{\"minReaderVersion\":1}}}\n"
                  + "{\"metaData\":{\"deltaMetadata\":{\"id\":\"m1\","
                  + "\"format\":{\"provider\":\"parquet\"},"
                  + "\"schemaString\":\"{\\\"type\\\":\\\"struct\\\"}\"}}}");
        });
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("s", "sc", "t").version()).contains(42L);
    }
  }

  /**
   * The features are interpolated into delta-sharing-capabilities, so a value carrying a semicolon
   * appends a capability field of its own: {@code x;responseformat=parquet} makes a server answer
   * in the flat shape, which this client then parses without complaint. Checked at the constructor
   * because that is the interpolation site and every construction path goes through it.
   */
  @Test
  void aReaderFeatureThatCouldReshapeTheCapabilityHeaderIsRefused() {
    for (String bad :
        List.of("x;responseformat=parquet", "has space", "line\nbreak", "semi;colon")) {
      assertThatThrownBy(
              () ->
                  new HttpDeltaSharingClient(
                      URI.create("https://sharing.example"),
                      "token",
                      Duration.ofSeconds(1),
                      Duration.ofSeconds(1),
                      List.of(bad)))
          .describedAs("%s", bad)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  /**
   * HttpRequest.Builder.header quotes the value it rejects, and every RuntimeException from the
   * send is classified as TRANSPORT with its cause attached -- so a token carrying a newline
   * reached the cause chain the vendor logs, verbatim, on a request that never left the process.
   */
  @Test
  void aTokenThatCannotBeSentIsRefusedWithoutQuotingIt() {
    assertThatThrownBy(
            () ->
                new HttpDeltaSharingClient(
                    URI.create("https://sharing.example"),
                    "secret-with\nnewline",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(
            e -> {
              assertThat(e.getMessage()).doesNotContain("secret-with");
              assertThat(e.getMessage()).contains("control character");
            });
  }

  /**
   * An empty 200 or a 204 is a malformed response, not something the callers should meet as an NPE.
   */
  @Test
  void anEmptyBodyIsClassifiedRatherThanReachingTheCallers() throws Exception {
    startServer(exchange -> respond(exchange, 200, ""));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * Jackson quotes the offending input in its message, so chaining a parse failure on a credentials
   * body carried the secret into the cause chain -- which the vendor logs -- even though the
   * message itself withholds the body.
   */
  @Test
  void aMalformedCredentialsBodyDoesNotCarryTheSecretIntoTheCause() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"awsTempCredentials\":{\"secretAccessKey\":"
                    + "super-secret-unquoted-value}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .satisfies(
              e -> {
                String chain = e.toString();
                for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
                  chain = chain + " | " + cause;
                }
                assertThat(chain).doesNotContain("super-secret-unquoted-value");
                // Where, without what.
                assertThat(e.getMessage()).contains("line", "column");
              });
    }
  }

  /**
   * A session with no expiry validates clean and is refused at query time, because the vendor
   * requires one for every Catalog Integration session and validation does not treat an absent
   * expiry as expired. That split is the least diagnosable shape this can take.
   */
  @Test
  void credentialsNamingNoExpiryAreReportedAsAProtocolViolation() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"a\","
                    + "\"secretAccessKey\":\"b\",\"sessionToken\":\"c\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("named no expiry")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** Same reason as the credentials guard below: the record's check is not a classified failure. */
  @Test
  void metadataCarryingNoSchemaIsReportedAsAProtocolViolation() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"id\":\"m1\",\"format\":{\"provider\":\"parquet\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("s", "sc", "t"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("carried no schemaString")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * The record's own check raises IllegalArgumentException, which is not a DeltaSharingException
   * and so escapes this class's classification entirely, reaching the vendor as a bare
   * RuntimeException naming neither the table nor the server.
   */
  @Test
  void credentialsNamingNoLocationAreReportedAsAProtocolViolation() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"awsTempCredentials\":{\"accessKeyId\":\"a\","
                    + "\"secretAccessKey\":\"b\",\"sessionToken\":\"c\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("named no location")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * {@code URLEncoder} is form encoding, where a space is {@code +}. A {@code +} in a path segment
   * is a literal plus, so the request would go to the wrong path and come back 404.
   */
  @Test
  void aNameContainingASpaceIsPercentEncodedNotPlusEncoded() throws Exception {
    java.util.List<String> paths = new java.util.ArrayList<>();
    startServer(
        exchange -> {
          paths.add(exchange.getRequestURI().getRawPath());
          respond(exchange, 200, "{\"items\":[]}");
        });
    try (DeltaSharingClient client = client()) {
      client.listTables("my share", "my schema");
    }
    assertThat(paths).hasSize(1);
    assertThat(paths.get(0)).contains("my%20share", "my%20schema").doesNotContain("+");
  }

  /**
   * A server echoing request headers in an error body would otherwise put the recipient's own token
   * into the exception, which reaches validation output and operator logs.
   */
  @Test
  void anErrorBodyEchoingTheAuthorizationHeaderIsRedacted() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                500,
                "upstream failed; request headers were "
                    + "{Authorization=Bearer super-secret-recipient-token, Accept=*/*}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listShares())
          .isInstanceOf(DeltaSharingException.class)
          .satisfies(
              e -> {
                assertThat(e.getMessage()).doesNotContain("super-secret-recipient-token");
                // The whole header value goes, scheme included -- the pass recognises the header
                // name rather than the token's shape, so how the value was encoded stops
                // mattering. The name survives, so the message still says what was echoed.
                assertThat(e.getMessage()).contains("Authorization=<redacted>");
              });
    }
  }

  @Test
  void aNonJsonBodyIsReportedAsSuchRatherThanAsMissingActions() throws Exception {
    // The shape a proxy error page takes when it arrives with a success status. It fails at the
    // line parse, which names the actual problem more precisely than the missing-actions check
    // below would.
    startServer(exchange -> respond(exchange, 200, "<html>hello</html>"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("s", "sc", "t"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("could not be read as one JSON document")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void validNdjsonMissingTheMetadataActionIsAProtocolViolation() throws Exception {
    // Parses cleanly and is still unusable: the protocol requires both actions, and a caller given
    // only a protocol has no schema to materialize.
    startServer(exchange -> respond(exchange, 200, "{\"protocol\":{\"minReaderVersion\":1}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("s", "sc", "t"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("did not contain both a protocol and a metaData action");
    }
  }

  // ---------------------------------------------------------------------------
  // Temporary credentials
  // ---------------------------------------------------------------------------

  @Test
  void awsCredentialsCarryTheCompleteSessionTriadAndExpiry() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\",\"expirationTime\":1700000000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\",\"secretAccessKey\":\"SK\","
                    + "\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      TemporaryCredentials credentials = client.temporaryTableCredentials("s", "sc", "t", null);
      assertThat(credentials.cloud()).isEqualTo(CredentialCloud.AWS);
      assertThat(credentials.hasAwsSession()).isTrue();
      assertThat(credentials.location()).isEqualTo("s3://b/t");
      assertThat(credentials.expiresAt()).isPresent();
    }
  }

  @Test
  void azureAndGcpAreNamedRatherThanReturnedEmpty() throws Exception {
    // A caller that cannot use these should be able to say which cloud answered, not that the
    // server vended nothing.
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"abfss://c/t\","
                    + "\"expirationTime\":1893456000000,\"azureUserDelegationSas\":{\"sasToken\":\"s\"}}}"));
    try (DeltaSharingClient client = client()) {
      TemporaryCredentials credentials = client.temporaryTableCredentials("s", "sc", "t", null);
      assertThat(credentials.cloud()).isEqualTo(CredentialCloud.AZURE);
      assertThat(credentials.hasAwsSession()).isFalse();
    }
  }

  @Test
  void aCredentialsResponseNamingNoCloudIsAProtocolViolation() throws Exception {
    startServer(
        exchange -> respond(exchange, 200, "{\"credentials\":{\"location\":\"s3://b/t\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .hasMessageContaining("named no supported cloud");
    }
  }

  @Test
  void anAuxiliaryLocationIsSentAsTheRequestBody() throws Exception {
    List<String> bodies = new ArrayList<>();
    startServer(
        exchange -> {
          bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(
              exchange,
              200,
              "{\"credentials\":{\"location\":\"s3://b/aux\",\"expirationTime\":1893456000000,"
                  + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\",\"secretAccessKey\":\"SK\","
                  + "\"sessionToken\":\"ST\"}}}");
        });
    try (DeltaSharingClient client = client()) {
      client.temporaryTableCredentials("s", "sc", "t", "s3://b/aux");
    }
    assertThat(bodies).containsExactly("{\"location\":\"s3://b/aux\"}");
  }

  @Test
  void theCredentialsResponseNeverReachesAnErrorMessage() throws Exception {
    // Every other endpoint includes a body snippet, which is what makes a server-side error
    // diagnosable. This one must not, because the body carries live credentials.
    startServer(
        exchange ->
            respond(
                exchange,
                403,
                "{\"credentials\":{\"awsTempCredentials\":{\"secretAccessKey\":\"LEAKED\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .hasMessageNotContaining("LEAKED")
          .hasMessageContaining("HTTP 403");
    }
  }

  @Test
  void credentialsAreNotPrintedByToString() {
    TemporaryCredentials credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://b/t",
            java.util.Optional.of("AK"),
            java.util.Optional.of("SUPERSECRET"),
            java.util.Optional.of("ST"),
            java.util.Optional.empty());
    assertThat(credentials.toString())
        .doesNotContain("SUPERSECRET")
        .doesNotContain("AK")
        .contains("redacted");
  }

  // ---------------------------------------------------------------------------
  // Classification and transport
  // ---------------------------------------------------------------------------

  @Test
  void statusesClassifyIntoActionableFailures() throws Exception {
    record Case(int status, DeltaSharingException.Failure expected) {}
    for (Case c :
        List.of(
            new Case(401, DeltaSharingException.Failure.UNAUTHENTICATED),
            new Case(403, DeltaSharingException.Failure.PERMISSION_DENIED),
            new Case(404, DeltaSharingException.Failure.NOT_FOUND),
            new Case(429, DeltaSharingException.Failure.RATE_LIMITED),
            new Case(400, DeltaSharingException.Failure.INVALID_REQUEST),
            new Case(503, DeltaSharingException.Failure.SERVER_ERROR),
            // A redirect reaching the client means the base URI names something that is not a
            // sharing server, since redirects are never followed.
            new Case(302, DeltaSharingException.Failure.INVALID_REQUEST))) {
      tearDown();
      allowCleartextLoopback();
      startServer(exchange -> respond(exchange, c.status(), "{}"));
      try (DeltaSharingClient client = client()) {
        assertThatThrownBy(client::listShares)
            .describedAs("status %s", c.status())
            .isInstanceOf(DeltaSharingException.class)
            .extracting(e -> ((DeltaSharingException) e).failure())
            .isEqualTo(c.expected());
      }
    }
  }

  @Test
  void theRecipientTokenAndCapabilitiesAreSentOnEveryRequest() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":[],\"nextPageToken\":\"\"}"));
    try (DeltaSharingClient client =
        new HttpDeltaSharingClient(
            endpoint(),
            "recipient-token",
            Duration.ofSeconds(2),
            Duration.ofSeconds(2),
            List.of("deletionVectors"))) {
      client.listShares();
    }
    assertThat(authorizationHeaders).containsExactly("Bearer recipient-token");
    assertThat(capabilityHeaders)
        .containsExactly("responseformat=delta;readerfeatures=deletionVectors");
  }

  @Test
  void aCleartextEndpointIsRefusedWithoutTheOptIn() {
    System.clearProperty("floecat.security.allow-loopback-catalog-endpoints");
    assertThatThrownBy(
            () ->
                new HttpDeltaSharingClient(
                    URI.create("http://example.invalid/sharing"),
                    "t",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Delta Sharing endpoint")
        .hasMessageContaining("HTTPS");
  }

  @Test
  void anEndpointNamingTheMetadataAddressIsRefused() {
    // Shared with the Unity client through HttpEndpointGuards. Asserted here too because this
    // client is a second caller of that rule and the point of sharing it is that both hold.
    assertThatThrownBy(
            () ->
                new HttpDeltaSharingClient(
                    URI.create("https://169.254.169.254/sharing"),
                    "t",
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("link-local");
  }

  // ---------------------------------------------------------------------------
  // Harness
  // ---------------------------------------------------------------------------

  @Test
  void aSecretStraddlingTheSnippetBoundIsStillRedacted() throws Exception {
    // The bound cuts at 2000 characters. The filler puts "Bearer " at 1980, so the colon lands at
    // 1992 and the cut falls inside "part2-SECRETSUFFIX" -- the window where bounding before
    // redacting leaked. The literal no longer matched the halved token, and the pattern stops at
    // the colon, which is the character that puts this token outside the grammar, so the fragment
    // between the two survived into the message.
    String secret = "part1:part2-SECRETSUFFIX";
    startServer(
        exchange -> respond(exchange, 500, "x".repeat(1980) + "Bearer " + secret + " trailing"));
    try (DeltaSharingClient client =
        new HttpDeltaSharingClient(
            endpoint(), secret, Duration.ofSeconds(2), Duration.ofSeconds(2))) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain("SECRETSUFFIX"))
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain("part2"));
    }
  }

  @Test
  void everyOccurrenceIsRedactedAndNotOnlyTheOneInsideTheBound() throws Exception {
    // Why the whole body is scanned rather than a window around the cut. A replacement is shorter
    // than what it replaces, so redacting the first occurrence pulls later text forward into the
    // bounded range -- text a window would not have reached.
    String secret = "part1:part2-SECRETSUFFIX";
    String body =
        "y".repeat(1900) + "Bearer " + secret + "z".repeat(60) + secret + "z".repeat(60) + secret;
    startServer(exchange -> respond(exchange, 500, body));
    try (DeltaSharingClient client =
        new HttpDeltaSharingClient(
            endpoint(), secret, Duration.ofSeconds(2), Duration.ofSeconds(2))) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .satisfies(e -> assertThat(e.getMessage()).doesNotContain("part1:"));
    }
  }

  @Test
  void aListingIsBoundedInTotalAndNotOnlyPerPage() throws Exception {
    // The page cap and the per-response cap multiply out to more than any heap. This is the bound
    // that makes a listing's size finite.
    System.setProperty(HttpDeltaSharingClient.MAX_LISTING_BYTES_PROPERTY, "40");
    startServer(exchange -> respond(exchange, 200, "{\"items\":[],\"nextPageToken\":\"t1\"}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("exceeded")
          .hasMessageContaining(HttpDeltaSharingClient.MAX_LISTING_BYTES_PROPERTY)
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void theListingBoundIsCumulativeAcrossListingsOnOneClient() throws Exception {
    // Per client, because a client's life is one reconcile pass and the pass memoises every
    // schema's tables for the whole of it. A per-listing bound leaves the pass unbounded.
    System.setProperty(HttpDeltaSharingClient.MAX_LISTING_BYTES_PROPERTY, "20");
    startServer(exchange -> respond(exchange, 200, "{\"items\":[]}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares()).isEmpty();
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("exceeded");
    }
  }

  @Test
  void aNonArrayAuxiliaryLocationsIsRefused() throws Exception {
    // Iterating a scalar yields nothing, so this read as absent -- the one shape the auxiliary
    // refusal exists to catch, reconciling with a root-only credential.
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"auxiliaryLocations\":\"s3://other/part\"}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array auxiliaryLocations")
          // Named. INVALID_RESPONSE reaches the boundary as INTERNAL, whose message GrpcErrors
          // hides, so this log line is the only place the malformed entry is identified.
          .hasMessageContaining("tables of share.schema")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void aNonArrayAccessModesIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(exchange, 200, "{\"items\":[{\"name\":\"t\",\"accessModes\":\"dir\"}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array accessModes")
          .hasMessageContaining("tables of share.schema");
    }
  }

  @Test
  void aMalformedItemFailsBeforeTheRestOfTheListingIsFetched() throws Exception {
    // Decoding as each page arrives, rather than after the last one, is what keeps a listing from
    // retaining raw pages. It also stops the walk at the page that is wrong.
    startServer(exchange -> respond(exchange, 200, "{\"items\":[{}],\"nextPageToken\":\"t1\"}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("missing name");
    }
    assertThat(requestPaths).hasSize(1);
  }

  @Test
  void aNonScalarNextPageTokenIsRefusedAndNotReadAsTheEndOfTheListing() throws Exception {
    // The destructive shape: a legitimate first page followed by a malformed cursor. asText("")
    // answers the empty string for an object, which read here as "no more pages", so this returned
    // one page as the authoritative inventory and the reconciler retires whatever it leaves out.
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"share1\"}],\"nextPageToken\":{\"cursor\":\"x\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-scalar nextPageToken")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void anArrayNextPageTokenIsRefusedToo() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":[],\"nextPageToken\":[\"x\"]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-scalar nextPageToken");
    }
  }

  @Test
  void aNullNextPageTokenEndsTheListingRatherThanFailingIt() throws Exception {
    // Absent and null both mean the listing is done. Null is the ordinary idiom for it, so
    // refusing it would fail a server that is conforming in substance.
    startServer(
        exchange ->
            respond(exchange, 200, "{\"items\":[{\"name\":\"share1\"}],\"nextPageToken\":null}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares()).hasSize(1);
    }
    assertThat(requestPaths).hasSize(1);
  }

  @Test
  void aTokenAboveLatin1IsRefusedAtConstructionAndNeverReachesACauseChain() {
    // The JDK's header validation stops at U+00FF and quotes the whole header value when it
    // refuses. The send wraps a RuntimeException as TRANSPORT with the cause attached, so without
    // this the live token reached the chain the vendor logs.
    String token = "abc\uD83D\uDE00def";
    assertThatThrownBy(
            () ->
                new HttpDeltaSharingClient(
                    URI.create("https://sharing.example.com/delta-sharing"),
                    token,
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .satisfies(
            e -> {
              for (Throwable t = e; t != null; t = t.getCause()) {
                assertThat(String.valueOf(t.getMessage())).doesNotContain(token);
                assertThat(String.valueOf(t.getMessage())).doesNotContain("abc");
              }
            });
  }

  @Test
  void aTokenOfPrintableAsciiPunctuationIsAccepted() {
    // Printable ASCII, narrower than the JDK's own U+00FF limit and deliberately so: a token
    // outside it cannot be redacted reliably, because a JSON serializer that escapes non-ASCII
    // renders it as \u00e9 in an echoed body, where the literal is not present in that form and
    // the pattern stops at the backslash. The punctuation real tokens do carry still passes, which
    // is the case the by-value redaction exists for.
    assertThatCode(
            () ->
                new HttpDeltaSharingClient(
                        URI.create("https://sharing.example.com/delta-sharing"),
                        "part1:part2-tilde~end",
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(2))
                    .close())
        .doesNotThrowAnyException();
  }

  @Test
  void aTokenOutsidePrintableAsciiIsRefused() {
    // The range that would otherwise need escape-aware redaction, refused at construction instead.
    for (String token : List.of("abc\u00e9SECRET", "abc\u00FFdef", "abc\u007Fdef")) {
      assertThatThrownBy(
              () ->
                  new HttpDeltaSharingClient(
                      URI.create("https://sharing.example.com/delta-sharing"),
                      token,
                      Duration.ofSeconds(2),
                      Duration.ofSeconds(2)))
          .as(token)
          .isInstanceOf(IllegalArgumentException.class)
          .satisfies(
              e -> {
                for (Throwable t = e; t != null; t = t.getCause()) {
                  assertThat(String.valueOf(t.getMessage())).doesNotContain("SECRET");
                }
              });
    }
  }

  @Test
  void aListingWithATrailingDocumentIsRefusedRatherThanReadAsItsFirstPage() throws Exception {
    // readTree reads the first document and discards what follows, so this parsed as an empty
    // listing and every shape guard below saw a well-formed empty page. An empty inventory with no
    // recorded skip is what the reconciler reads as the share having dropped its tables.
    startServer(
        exchange -> respond(exchange, 200, "{\"items\":[]}{\"items\":[{\"name\":\"share1\"}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("could not be read as one JSON document")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void aListingRestatingItemsIsRefusedRatherThanTakingTheLastValue() throws Exception {
    // Last key wins by default, so naming tables and then restating items as empty parsed as
    // empty -- the destructive direction, and indistinguishable from a share that really is empty.
    startServer(
        exchange -> respond(exchange, 200, "{\"items\":[{\"name\":\"share1\"}],\"items\":[]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(client::listShares)
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("could not be read as one JSON document")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  @Test
  void anNdjsonLineCarryingTwoActionsIsRefused() throws Exception {
    // The metadata path reads a line at a time, so the same leniency applied there.
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}{\"metaData\":{}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("could not be read as one JSON document");
    }
  }

  /**
   * The third array field, and the one that persists. partitionColumns becomes
   * CatalogTable.partitionKeys and then UpstreamRef.partition_keys, so a scalar read as absent
   * reconciled a partitioned table as unpartitioned -- a wrong table definition that validates
   * clean rather than a failure.
   */
  @Test
  void aNonArrayPartitionColumnsIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"partitionColumns\":\"day\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array partitionColumns")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * asText answers an object with the empty string, and an empty partition key is worse than none.
   */
  @Test
  void aPartitionColumnThatIsNotANameIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"partitionColumns\":[{}]}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("partitionColumns entry that is not a name");
    }
  }

  /** An absent field still means unpartitioned, which is the ordinary shape. */
  @Test
  void anAbsentPartitionColumnsIsAnUnpartitionedTable() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").metadata().partitionColumns())
          .isEmpty();
    }
  }

  /**
   * An explicit null is not stated either. Serializers emit one for an absent list routinely, and
   * refusing it raises INVALID_RESPONSE -- which is not one branch skipped but the end of the whole
   * reconcile pass, over a shape every other optional field in this parser already reads as absent.
   */
  @Test
  void anExplicitlyNullListFieldIsAbsent() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1,\"readerFeatures\":null}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"partitionColumns\":null}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").metadata().partitionColumns())
          .isEmpty();
    }
  }

  /**
   * Exactly one cloud, not at least one. The decode takes the first branch that matches, so an
   * envelope naming two silently became whichever is tested first -- the answer a property of the
   * decoder's ordering rather than of the response.
   */
  @Test
  void aCredentialsEnvelopeNamingTwoCloudsIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"},"
                    + "\"gcpOauthToken\":{\"oauthToken\":\"g\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("named 2 clouds")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * An explicit null is not an action. has() answers true for one and parseProtocol tolerates a
   * null node, so this satisfied the both-actions guard with a protocol this client had invented --
   * a fabricated minReaderVersion and no reader features, for a body that stated neither.
   */
  @Test
  void anExplicitlyNullProtocolActionIsNotAProtocolAction() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange, 200, "{\"protocol\":null}\n{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("did not contain both a protocol and a metaData action")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * A value past the last instant a proto Timestamp carries is a unit mismatch, not a date. Without
   * this it survives every downstream expiry check by looking far in the future and then throws
   * inside Timestamps.fromMillis while the gRPC response is built -- an unclassified
   * RuntimeException in a handler, on a share that validated clean, for every read.
   */
  @Test
  void anExpiryBeyondTheRepresentableRangeIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("unit mismatch")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * The version is published as the {@code delta.sharing.version} property, and {@code asLong}
   * answers zero for a string or a container. Unchecked, a malformed version is a table definition
   * nobody sent rather than a failure.
   */
  @Test
  void aVersionThatIsNotANumberIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"version\":\"latest\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("version that is not a number")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * Delta defines this as a map of strings, and these entries become the table's properties -- so a
   * number, a boolean or a null coercing through {@code asText} is metadata the server did not
   * send.
   */
  @Test
  void aNonStringConfigurationValueIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\","
                    + "\"configuration\":{\"delta.minReaderVersion\":5}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-string configuration value")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** A stated configuration that is not an object leaves no properties, which is not the same. */
  @Test
  void aConfigurationThatIsNotAnObjectIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"configuration\":\"oops\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("configuration that is not an object");
    }
  }

  /** A container value is refused by the same rule. */
  @Test
  void aNonScalarConfigurationValueIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\","
                    + "\"configuration\":{\"delta.enableDeletionVectors\":{\"on\":true}}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-string configuration value");
    }
  }

  /**
   * A name has to be a string. {@code asText} answers "17" for a number, so such an entry became a
   * table named 17 in an inventory the reconciler treats as authoritative -- and tables the same
   * malformed page omitted are retired against it.
   */
  @Test
  void aNonTextualNameIsRefusedRatherThanCoerced() throws Exception {
    startServer(exchange -> respond(exchange, 200, "{\"items\":[{\"name\":17}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-textual name")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * A server formatting ordinary JSON across lines. This request advertises application/json as
   * well as ndjson, so both framings have to be read; splitting on newlines ahead of a whole-body
   * parse would hand "{" to the parser alone.
   */
  @Test
  void aPrettyPrintedCredentialResponseIsRead() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\n  \"credentials\": {\n    \"location\": \"s3://b/t\",\n"
                    + "    \"expirationTime\": 1893456000000,\n"
                    + "    \"awsTempCredentials\": {\n      \"accessKeyId\": \"AK\",\n"
                    + "      \"secretAccessKey\": \"SK\",\n"
                    + "      \"sessionToken\": \"ST\"\n    }\n  }\n}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.temporaryTableCredentials("s", "sc", "t", null).location())
          .isEqualTo("s3://b/t");
    }
  }

  /**
   * The Accept header offers ndjson, so a server may answer in it -- with a protocol action ahead
   * of the credentials, the way the metadata route does on this transport. Reading the body as one
   * document failed on the second line, and that arrives as INVALID_RESPONSE, which reaches the
   * client as INTERNAL and ends the whole reconcile rather than skipping a table.
   */
  @Test
  void aCredentialResponseFramedAsNdjsonIsRead() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.temporaryTableCredentials("s", "sc", "t", null).location())
          .isEqualTo("s3://b/t");
    }
  }

  /**
   * The schema is persisted as the reconciled table's Delta schema without being parsed here, so a
   * coerced value materialises a table that fails when a query maps it.
   */
  @Test
  void aNonStringSchemaStringIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":17}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("carried no schemaString")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * A numeric AWS field otherwise passes {@code hasAwsSession} and is published as a complete
   * tuple, so every storage access with it fails.
   */
  @Test
  void aNonStringAwsSessionFieldIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":17,"
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-string accessKeyId")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** A numeric credential location is not a location, and it stands in for the table root. */
  @Test
  void aNonStringCredentialLocationIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":17,\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("named no location");
    }
  }

  /**
   * {@code asLong} answers 1 for {@code true}, which clears both bounds and yields an expiry one
   * millisecond after the epoch -- a session the vendor refuses as expired, reported as an expired
   * credential rather than a malformed response.
   */
  @Test
  void anExpiryThatIsNotANumberIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\",\"expirationTime\":true,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.temporaryTableCredentials("s", "sc", "t", null))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("expiry that is not a number")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * An expiry in epoch milliseconds overflows an int, so rendering it as a JSON string -- the
   * standard protobuf encoding for int64 -- must still be accepted. Refusing it is
   * INVALID_RESPONSE, which reaches the client as INTERNAL and ends the whole reconcile rather than
   * skipping a table.
   */
  @Test
  void anExpirySentAsAStringOfEpochMillisIsAccepted() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":\"1893456000000\","
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.temporaryTableCredentials("s", "sc", "t", null).expiresAt())
          .contains(Instant.ofEpochMilli(1893456000000L));
    }
  }

  /** The version takes the same rule as the other two int64 fields. */
  @Test
  void aVersionSentAsAStringIsAccepted() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"version\":\"12\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").version()).contains(12L);
    }
  }

  /** The boundary itself is a date, and has to stay one. */
  @Test
  void theLastRepresentableExpiryIsStillAccepted() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":253402300799999,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.temporaryTableCredentials("s", "sc", "t", null).expiresAt())
          .contains(Instant.ofEpochMilli(253402300799999L));
    }
  }

  /**
   * The bounded reader asks for the cap plus one byte to spot an overrun, and that addition
   * overflows at the largest int -- so setting the cap to its largest allowed value gave a negative
   * length and failed every response as a transport error. The sibling client already refuses this
   * value on the same property.
   */
  @Test
  void theLargestIntIsNotAUsableResponseCap() {
    System.setProperty(
        HttpDeltaSharingClient.MAX_RESPONSE_BYTES_PROPERTY, String.valueOf(Integer.MAX_VALUE));
    assertThatThrownBy(
            () ->
                new HttpDeltaSharingClient(
                    URI.create("https://sharing.example.com/delta-sharing"),
                    "token",
                    Duration.ofSeconds(2),
                    Duration.ofSeconds(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(HttpDeltaSharingClient.MAX_RESPONSE_BYTES_PROPERTY);
  }

  /**
   * asText supplies its default for a missing or container node but not for a stated empty string,
   * so this arrived blank at the record's own requireText -- an IllegalArgumentException, which is
   * not a DeltaSharingException and so escapes this class's classification entirely.
   */
  @Test
  void aStatedButEmptyFormatProviderIsClassifiedRatherThanEscaping() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"format\":{\"provider\":\"\"}}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("format provider that is not a name")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** An absent format still means parquet, which is the field's own default. */
  @Test
  void anAbsentFormatProviderIsStillParquet() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").metadata().format())
          .isEqualTo("parquet");
    }
  }

  /** The fifth array field in this parser, held to the same shape rule as the other four. */
  @Test
  void aNonArrayReaderFeaturesIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1,\"readerFeatures\":\"deletionVectors\"}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-array readerFeatures")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** A version that is not one is refused rather than defaulting silently to 1. */
  @Test
  void aMinReaderVersionThatIsNotAVersionIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":\"x\"}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("not a version");
    }
  }

  /**
   * A numeric string is a version. Refusing it would fail a working share over a field no consumer
   * reads, which is a worse trade than the silence being closed here.
   */
  @Test
  void aMinReaderVersionSentAsAStringIsStillAVersion() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":\"2\",\"readerFeatures\":[\"dv\"]}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").protocol().minReaderVersion())
          .isEqualTo(2);
    }
  }

  /**
   * The credential response is specified as x-ndjson carrying a single action, and this request
   * advertised application/json alone -- so a server honouring content negotiation could answer 406
   * to every credential request, which is every reconcile and every read.
   */
  @Test
  void theCredentialRequestAdvertisesTheProtocolResponseType() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}"));
    try (DeltaSharingClient client = client()) {
      client.temporaryTableCredentials("s", "sc", "t", null);
    }
    assertThat(acceptHeaders)
        .singleElement()
        .satisfies(
            accept -> {
              assertThat(accept).contains("application/x-ndjson");
              assertThat(accept).contains("application/json");
            });
  }

  /** And the body is read either way, since nothing here inspects the response content type. */
  @Test
  void anNdjsonFramedCredentialBodyIsStillRead() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"credentials\":{\"location\":\"s3://b/t\","
                    + "\"expirationTime\":1893456000000,"
                    + "\"awsTempCredentials\":{\"accessKeyId\":\"AK\","
                    + "\"secretAccessKey\":\"SK\",\"sessionToken\":\"ST\"}}}\n"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.temporaryTableCredentials("s", "sc", "t", null).location())
          .isEqualTo("s3://b/t");
    }
  }

  /**
   * A cursor is opaque, and only an absent, null or empty token ends pagination. Reading a
   * whitespace token as the end returned the first page as the complete authoritative inventory --
   * which is what lets the reconciler retire every table the later pages would have named.
   */
  @Test
  void aWhitespaceCursorIsSentBackRatherThanEndingTheListing() throws Exception {
    AtomicInteger call = new AtomicInteger();
    startServer(
        exchange -> {
          if (call.getAndIncrement() == 0) {
            respond(exchange, 200, "{\"items\":[{\"name\":\"one\"}],\"nextPageToken\":\" \"}");
          } else {
            respond(exchange, 200, "{\"items\":[{\"name\":\"two\"}]}");
          }
        });
    try (DeltaSharingClient client = client()) {
      assertThat(client.listShares())
          .extracting(DeltaSharingModel.Share::name)
          .containsExactly("one", "two");
    }
    // Sent back exactly, as a query value rather than a path segment: encode() rejects a blank
    // string as an unusable segment, which would have raised out of the listing unclassified.
    assertThat(requestPaths).last().asString().contains("pageToken=%20");
  }

  /**
   * asText answers an object with the empty string, so an array of objects read as an empty list --
   * and for auxiliaryLocations that makes hasAuxiliaryLocations() false, so the refusal at the load
   * never fires and the table reconciles with a credential covering only its root.
   */
  @Test
  void anAuxiliaryLocationThatIsNotATextValueIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\","
                    + "\"auxiliaryLocations\":[{\"p\":\"s3://other/part\"}]}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("auxiliaryLocations entry that is not a value")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * The metadata endpoint is where a proxy error page arriving with a 200 lands, and the message is
   * hidden again at the gRPC boundary -- so the log line was all an operator had, and it said where
   * the parse failed without ever saying what arrived.
   */
  @Test
  void anNdjsonLineParseFailureCarriesTheRedactedBody() throws Exception {
    startServer(exchange -> respond(exchange, 200, "<html>proxy says no</html>"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("proxy says no");
    }
  }

  /**
   * An element that is not a mode is refused, not coerced. asText answers an object with the empty
   * string, which mapped to OTHER -- and OTHER alongside a readable value is the unsafe direction:
   * this decoded to [OTHER, DIR], contains(DIR) answered true, and the table was treated as
   * directory-accessible on the strength of a response this client could not read.
   */
  @Test
  void anAccessModeThatIsNotATextValueIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"accessModes\":[{\"a\":1},\"dir\"]}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("accessModes entry that is not a mode")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * A table stating only modes this client does not know is not a table stating none. Dropping the
   * unknown value made the two indistinguishable, and the strict setting then refused such a table
   * with a message saying it had stated no access modes at all.
   */
  @Test
  void aTableStatingOnlyUnknownModesIsDistinguishableFromOneStatingNone() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"accessModes\":[\"dir2\"]}," + "{\"name\":\"u\"}]}"));
    try (DeltaSharingClient client = client()) {
      var tables = client.listTables("s", "sc");
      assertThat(tables.get(0).accessModes()).containsExactly(AccessMode.OTHER);
      assertThat(tables.get(1).accessModes()).isEmpty();
    }
  }

  /**
   * A stated field that is not a scalar is malformed, not absent. asText answers an object with the
   * empty string, so a location stated as an object read as missing and fell through to the
   * credential endpoint, and a shareId stated as one downgraded a stable identity to a name.
   */
  @Test
  void aNonStringStatedFieldIsRefusedRatherThanReadAsAbsent() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"items\":[{\"name\":\"t\",\"location\":{\"path\":\"s3://b/t\"}}]}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.listTables("share", "schema"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("non-string location")
          .hasMessageContaining("tables of share.schema")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /**
   * One action of each kind. A second overwrote the first, so a concatenated or malformed response
   * materialised the later schema and location on a table that reconciled clean -- wrong data on a
   * live table rather than a skip, which is the last-write-wins trap the strict parser settings
   * close for duplicate keys but cannot reach across legal NDJSON lines.
   */
  @Test
  void aSecondMetadataActionIsRefusedRatherThanOverwritingTheFirst() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"location\":\"s3://b/first\"}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\",\"location\":\"s3://b/second\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("more than one metaData action")
          .extracting(e -> ((DeltaSharingException) e).failure())
          .isEqualTo(DeltaSharingException.Failure.INVALID_RESPONSE);
    }
  }

  /** The same for a repeated protocol action. */
  @Test
  void aSecondProtocolActionIsRefused() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"protocol\":{\"minReaderVersion\":1}}\n"
                    + "{\"protocol\":{\"minReaderVersion\":3}}\n"
                    + "{\"metaData\":{\"schemaString\":\"{}\"}}"));
    try (DeltaSharingClient client = client()) {
      assertThatThrownBy(() -> client.describeTable("share", "schema", "table"))
          .isInstanceOf(DeltaSharingException.class)
          .hasMessageContaining("more than one protocol action");
    }
  }

  /**
   * Order is not enforced. The wire format states protocol then metaData, but accepting the reverse
   * costs nothing and refusing it would fail a server that is otherwise correct.
   */
  @Test
  void actionsInTheReverseOrderAreStillAccepted() throws Exception {
    startServer(
        exchange ->
            respond(
                exchange,
                200,
                "{\"metaData\":{\"schemaString\":\"{}\"}}\n"
                    + "{\"protocol\":{\"minReaderVersion\":1}}"));
    try (DeltaSharingClient client = client()) {
      assertThat(client.describeTable("share", "schema", "table").protocol().minReaderVersion())
          .isEqualTo(1);
    }
  }

  private interface Handler {
    void handle(HttpExchange exchange) throws IOException;
  }

  private void startServer(Handler handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          requestPaths.add(exchange.getRequestURI().toString());
          capabilityHeaders.add(
              exchange.getRequestHeaders().getFirst("delta-sharing-capabilities"));
          authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
          acceptHeaders.add(exchange.getRequestHeaders().getFirst("Accept"));
          handler.handle(exchange);
        });
    server.start();
  }

  private URI endpoint() {
    return URI.create("http://localhost:" + server.getAddress().getPort() + "/delta-sharing");
  }

  private DeltaSharingClient client() {
    return new HttpDeltaSharingClient(
        endpoint(), "token", Duration.ofSeconds(2), Duration.ofSeconds(2));
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }
}
