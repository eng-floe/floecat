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

import ai.floedb.floecat.client.sharing.DeltaSharingException.Failure;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.AccessMode;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.CredentialCloud;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Protocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Schema;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Share;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Table;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableMetadata;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TemporaryCredentials;
import ai.floedb.floecat.http.guards.HttpEndpointGuards;
import ai.floedb.floecat.http.guards.HttpResponseSnippets;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A Delta Sharing recipient over HTTP.
 *
 * <p>Only the operations directory access needs. The query endpoint is absent because url mode is
 * not supported here; see {@link DeltaSharingClient}.
 */
public final class HttpDeltaSharingClient implements DeltaSharingClient {

  /** Cap on pages one listing will fetch, so a server minting fresh tokens cannot loop forever. */
  static final String MAX_PAGES_PROPERTY = "floecat.delta-sharing.max-pages";

  private static final int DEFAULT_MAX_PAGES = 10_000;

  /** Cap on a single response body. A larger one is refused rather than buffered. */
  static final String MAX_RESPONSE_BYTES_PROPERTY = "floecat.delta-sharing.max-response-bytes";

  private static final int DEFAULT_MAX_RESPONSE_BYTES = 32 * 1024 * 1024;

  /**
   * The cumulative bound on everything one client lists, across pages and across listings.
   *
   * <p>Counts characters of decoded body. That is the unit that bounds heap, and it equals bytes
   * for the ASCII JSON these endpoints answer with.
   *
   * <p>Per client rather than per listing, because a client's lifetime is one reconcile pass and a
   * pass memoises each schema's tables for the whole of it. A per-listing bound would leave a share
   * of two hundred schemas retaining two hundred times the cap, which is no bound on the pass.
   *
   * <p>The size comes from what a listing costs. A table entry carrying a name, its schema and
   * share, a table id and a share id, a location and its access modes is roughly two hundred and
   * eighty bytes of JSON -- nearer four hundred with an ordinary location, six hundred with a deep
   * prefix. So this admits somewhere above sixty thousand tables across a whole share, where a
   * large real share holds thousands, and the decoded records cost about the same again, so a pass
   * peaks near twice this. Beyond it a reconcile is refused, naming this property, rather than
   * taking the heap with it.
   *
   * <p>Neither of the other two bounds gives a memory bound. The per-response cap and the page cap
   * multiply out to ten thousand pages of thirty-two mebibytes, and the endpoint is named by tenant
   * configuration, so a server answering enormous pages takes the process down rather than failing
   * its own reconcile.
   */
  static final String MAX_LISTING_BYTES_PROPERTY = "floecat.delta-sharing.max-listing-bytes";

  private static final int DEFAULT_MAX_LISTING_BYTES = 32 * 1024 * 1024;

  /** Bound on upstream text reaching an exception message. */
  private static final int MAX_BODY_SNIPPET_CHARS = 2_000;

  /**
   * The last instant a proto Timestamp can carry, 9999-12-31T23:59:59.999Z.
   *
   * <p>Duplicated from {@code FloecatConnector.VendedStorageCredentials} for the reason the Unity
   * client duplicates it: this module depends on neither. Cheap to copy, because the proto
   * specification fixes it rather than any of the three copies owning it as policy.
   */
  private static final long MAX_EXPIRY_EPOCH_MILLIS = 253402300799999L;

  /** Never requested. Only somewhere for the header probe in {@link #requireHeaderSafe} to hang. */
  private static final URI PROBE_URI = URI.create("https://probe.invalid/");

  /**
   * Strict where the defaults are lenient, because the shape guards depend on it.
   *
   * <p>Two defaults let a malformed 200 through as an authoritative listing, which is the outcome
   * every guard in this class exists to prevent: the reconciler reads a successful empty or partial
   * inventory as the share having dropped what is missing, and retires it.
   *
   * <p>Trailing tokens: {@code readTree} reads the first document and discards whatever follows, so
   * {@code {"items":[]}{"items":[...]}} parsed as an empty page. Duplicate keys: the last value
   * wins, so a body naming tables and then restating {@code items} as empty parsed as empty. Both
   * reach the page-shape checks below looking perfectly well formed, because by then the evidence
   * is gone.
   *
   * <p>The NDJSON path splits on newlines and reads each line separately, so trailing-token
   * strictness holds there too: a line carrying two concatenated actions is now refused rather than
   * half read.
   */
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);

  private final String baseUri;
  private final String bearerToken;
  private final HttpClient httpClient;
  private final Duration requestTimeout;
  private final String capabilities;
  private final int maxResponseBytes;
  private final int maxPages;
  private final int maxListingBytes;
  private final AtomicLong listedChars = new AtomicLong();

  public HttpDeltaSharingClient(
      URI endpoint, String bearerToken, Duration connectTimeout, Duration requestTimeout) {
    this(endpoint, bearerToken, connectTimeout, requestTimeout, List.of());
  }

  /**
   * @param readerFeatures Delta reader features this caller can process. Sent with {@code
   *     responseformat=delta}, which is required for a table whose {@code minReaderVersion} exceeds
   *     1. Claiming a feature the reader cannot handle turns a protocol-level refusal into a
   *     failure during a scan, so the caller names what it actually supports rather than
   *     everything.
   */
  public HttpDeltaSharingClient(
      URI endpoint,
      String bearerToken,
      Duration connectTimeout,
      Duration requestTimeout,
      List<String> readerFeatures) {
    // Ahead of the transport, because these raise on a value they cannot use and anything built
    // before the throw is unreachable and never closed. A zero or negative override made every
    // listing fail with "exceeded 0 pages" and every response read as oversized.
    this.maxResponseBytes = configuredMaxResponseBytes();
    this.maxPages = positiveIntProperty(MAX_PAGES_PROPERTY, DEFAULT_MAX_PAGES);
    this.maxListingBytes =
        positiveIntProperty(MAX_LISTING_BYTES_PROPERTY, DEFAULT_MAX_LISTING_BYTES);
    this.capabilities = buildCapabilities(readerFeatures);
    URI validated =
        HttpEndpointGuards.requireAllowedEndpoint(
            Objects.requireNonNull(endpoint, "endpoint"), "Delta Sharing endpoint");
    this.baseUri = stripTrailingSlash(validated.toString());
    // Checked here, and the value never appears in the failure. HttpRequest.Builder.header quotes
    // the value it rejects, and every RuntimeException from the send is classified as TRANSPORT
    // with its cause attached -- so a token carrying a newline reached the cause chain the vendor
    // logs, verbatim, on a request that never left the process.
    this.bearerToken = requireHeaderSafe(requireText(bearerToken, "bearerToken"));
    this.requestTimeout = requirePositive(requestTimeout, "requestTimeout");
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(requirePositive(connectTimeout, "connectTimeout"))
            // A sharing endpoint is tenant-supplied. Following a redirect would let it move the
            // request, and the recipient token, to a host none of the transport gates saw.
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
  }

  @Override
  public List<Share> listShares() {
    String target = "shares";
    return List.copyOf(
        paginate(
            "/shares",
            target,
            item -> new Share(text(item, "name", target), optionalText(item, "id", target))));
  }

  @Override
  public List<Schema> listSchemas(String share) {
    String path = "/shares/" + encode(share) + "/schemas";
    String target = "schemas of " + share;
    return List.copyOf(
        paginate(path, target, item -> new Schema(share, text(item, "name", target))));
  }

  @Override
  public List<Table> listTables(String share, String schema) {
    String path = "/shares/" + encode(share) + "/schemas/" + encode(schema) + "/tables";
    String target = "tables of " + share + "." + schema;
    return List.copyOf(
        paginate(
            path,
            target,
            item ->
                new Table(
                    share,
                    schema,
                    text(item, "name", target),
                    optionalText(item, "id", target),
                    optionalText(item, "shareId", target),
                    optionalText(item, "location", target),
                    textList(item, "auxiliaryLocations", target),
                    accessModes(item, target))));
  }

  @Override
  public TableDescription describeTable(String share, String schema, String table) {
    String path = tablePath(share, schema, table) + "/metadata";
    String target = share + "." + schema + "." + table;
    NdjsonResponse response = sendNdjson(HttpRequest.newBuilder().GET(), path, target, true);
    List<JsonNode> actions = response.actions();

    Protocol protocol = null;
    TableMetadata metadata = null;
    for (JsonNode action : actions) {
      // hasNonNull, not has. An explicit null answers has() with true, and parseProtocol tolerates
      // a null node -- path() on it yields a missing node, so minReaderVersion defaults and the
      // reader features come back empty. A body of {"protocol":null} therefore satisfied the
      // both-actions guard below with a protocol action this client had invented. Same explicit
      // null the page shape was hardened against, and the same test parseCredentials already used.
      if (action.hasNonNull("protocol")) {
        // One of each. A second action of the same kind overwrote the first, so a concatenated or
        // malformed response materialised the later schema and location on a table that reconciled
        // clean -- the last-write-wins trap FAIL_ON_READING_DUP_TREE_KEY closes for duplicate keys,
        // one level up at the action level, where multi-line framing is legal and so that setting
        // does not reach it. Order is not enforced: the wire format states protocol then metaData,
        // but accepting the reverse costs nothing and refusing it would fail a correct server.
        requireFirst(protocol, "protocol", target);
        protocol = parseProtocol(action.get("protocol"), target);
      } else if (action.hasNonNull("metaData")) {
        requireFirst(metadata, "metaData", target);
        metadata = parseMetadata(action.get("metaData"), target);
      }
    }
    // The protocol requires both, in that order. A proxy error page arriving with a success status
    // is the shape that reaches here with neither, and reporting it as a protocol violation rather
    // than a null later is the difference between a diagnosable failure and one that is not.
    if (protocol == null || metadata == null) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing metadata for "
              + target
              + " did not contain both a protocol and a metaData action");
    }
    // The header first: it states the current version for an ordinary metadata request, and the
    // action's own version is populated only for a versioned or change-feed query.
    return new TableDescription(protocol, metadata, response.version().or(metadata::version));
  }

  @Override
  public TemporaryCredentials temporaryTableCredentials(
      String share, String schema, String table, String location) {
    String path = tablePath(share, schema, table) + "/temporary-table-credentials";
    String target = share + "." + schema + "." + table;
    String body =
        location == null || location.isBlank()
            ? "{}"
            : "{\"location\":" + quoteJson(location) + "}";

    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .header("Content-Type", "application/json; charset=utf-8");
    // The response carries live credentials, so no part of it reaches an exception message. Every
    // other endpoint includes a body snippet, which is what makes a server-side error diagnosable.
    // Both media types. The protocol specifies this response as x-ndjson carrying a single JSON
    // action, and this request advertised application/json alone -- so a server honouring content
    // negotiation could answer 406 to every credential request, which is every reconcile and every
    // read. The parser is unaffected either way: nothing here inspects the response content type,
    // and a one-action ndjson body is valid JSON. Advertising both is what a client that can read
    // both should say, and it cannot be refused for either.
    JsonNode root =
        credentialsEnvelope(
            sendForBody(builder, path, target, false, "application/x-ndjson, application/json"),
            target);

    JsonNode credentials = root.path("credentials");
    if (credentials.isMissingNode() || !credentials.isObject()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for " + target + " carried no credentials object");
    }
    return parseCredentials(credentials, target);
  }

  @Override
  public void close() {
    httpClient.close();
  }

  // ---------------------------------------------------------------------------
  // Paging
  // ---------------------------------------------------------------------------

  /**
   * Every item across every page of a list endpoint.
   *
   * <p>Three independent bounds, because a page token is server-supplied and none alone is enough.
   * A repeated token is refused, which catches a server returning the same cursor forever; a page
   * cap catches one minting a fresh cursor each time; and {@link #MAX_LISTING_BYTES_PROPERTY}
   * bounds what this client will pull in total. Without the first two, a listing is an unbounded
   * loop against an endpoint the recipient does not control. Without the third it is a bounded
   * number of unbounded pages, which is no memory bound at all.
   *
   * <p>Each item is decoded as its page arrives rather than after the last one. A raw node is
   * retained whole, including fields this client does not read, so a listing of ten thousand pages
   * held ten thousand pages of unread JSON; the record it decodes into holds only the fields the
   * protocol defines. It also fails a malformed item on the first page rather than after fetching
   * every remaining one.
   */
  private <T> List<T> paginate(String path, String target, Function<JsonNode, T> decode) {
    List<T> collected = new ArrayList<>();
    Set<String> seenTokens = new LinkedHashSet<>();
    String pageToken = null;

    for (int page = 0; page < maxPages; page++) {
      String pagePath =
          pageToken == null ? path : path + "?pageToken=" + encodeQueryValue(pageToken);
      String body =
          sendForBody(HttpRequest.newBuilder().GET(), pagePath, target, true, "application/json");
      // Across every listing this client has made, not just this one. A pass keeps each schema's
      // tables for the whole of its life, so the total is what bounds the pass.
      if (listedChars.addAndGet(body.length()) > maxListingBytes) {
        // Not retryable, and not one branch of the walk. INVALID_RESPONSE reaches the catalog
        // client as INTERNAL, which CatalogTraversalFailures does not treat as describing a single
        // branch, so a listing this size ends the pass instead of being recorded as one skipped
        // schema and leaving the share looking healthy.
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing listings exceeded "
                + maxListingBytes
                + " bytes ("
                + MAX_LISTING_BYTES_PROPERTY
                + ") while listing "
                + target);
      }
      JsonNode root = parseJson(body, target, true);

      // A page has to be an object, and items has to be an array where it appears at all. Absent
      // and empty are both legal and mean the same thing; anything else is a malformed response.
      // Reading a non-array items as an empty listing is destructive rather than merely wrong: the
      // reconciler treats a successful empty inventory as the share having nothing left and retires
      // the overlay's tables, so a malformed 200 deletes what a failure would have preserved.
      if (!root.isObject()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing page for " + target + " is not an object");
      }
      JsonNode items = root.path("items");
      // Present and not an array, including an explicit null. The protocol allows the field to be
      // omitted or to be an array; a null is neither, and exempting it made a malformed page an
      // authoritative empty inventory again -- the shape that lets the reconciler retire what a
      // classified failure would have preserved.
      if (!items.isMissingNode() && !items.isArray()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing page for " + target + " carried a non-array items field");
      }
      if (items.isArray()) {
        for (JsonNode item : items) {
          collected.add(decode.apply(item));
        }
      }

      // A cursor has to be a scalar. A stated object or array answers asText("") with the empty
      // string, which reads here as "there are no more pages", so a malformed cursor after a
      // legitimate first page reported a partial listing as the whole inventory -- and an
      // authoritative listing that leaves tables out is what makes the reconciler retire them.
      // This is the shape the items guard above exists for, on the other field. Absent and null
      // both legitimately mean the listing is done, and a number is a cursor dialect there is no
      // reason to refuse.
      JsonNode cursor = root.path("nextPageToken");
      if (cursor.isObject() || cursor.isArray()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing page for " + target + " carried a non-scalar nextPageToken");
      }
      String next = cursor.asText("");
      // Empty, not blank. A cursor is opaque, and the protocol ends pagination on an absent, null
      // or empty token -- nothing else. Reading a whitespace token as the end returned the first
      // page as the complete authoritative inventory, which is what lets the reconciler retire
      // every table the later pages would have named.
      if (next.isEmpty()) {
        return collected;
      }
      if (!seenTokens.add(next)) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing repeated a page token while listing " + target);
      }
      pageToken = next;
    }
    throw new DeltaSharingException(
        Failure.INVALID_RESPONSE,
        200,
        "Delta Sharing listing of " + target + " exceeded " + maxPages + " pages");
  }

  // ---------------------------------------------------------------------------
  // Transport
  // ---------------------------------------------------------------------------

  /**
   * The credential envelope, from a body framed as one JSON document or as ndjson lines.
   *
   * <p>The Accept header offers ndjson, so a server may legitimately answer in it -- with a
   * protocol action ahead of the credentials, the way the metadata route does on this same
   * transport. Reading the body as a single document would then fail on the second line, since
   * FAIL_ON_TRAILING_TOKENS refuses one, and that arrives as INVALID_RESPONSE which reaches the
   * client as INTERNAL and ends the whole reconcile rather than skipping a table. Advertising a
   * framing this could not read was the mistake; a single JSON body is one line of ndjson, so both
   * shapes go through the same reader.
   */
  private JsonNode credentialsEnvelope(String body, String target) {
    // The whole body first. This request advertises application/json as well as ndjson, and a
    // single-action ndjson body is itself valid JSON, so one parse covers both the compact and the
    // formatted shape. Splitting on newlines ahead of that would hand "{" to the parser alone for
    // any server that formats its JSON across lines.
    try {
      return parseJson(body, target, false);
    } catch (DeltaSharingException notOneDocument) {
      // Only a body carrying more than one document lands here, which is what ndjson framing is.
      // A body that is simply malformed has one line, so its own failure is what gets reported.
      if (body.lines().filter(line -> !line.isBlank()).count() < 2) {
        throw notOneDocument;
      }
      return credentialsAction(body, target);
    }
  }

  /**
   * The credentials action from an ndjson body.
   *
   * <p>The protocol frames this response as ndjson, so a server may legitimately put a protocol
   * action ahead of the credentials the way the metadata route does on this same transport.
   */
  private JsonNode credentialsAction(String body, String target) {
    JsonNode last = null;
    for (String line : body.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      JsonNode action = parseJson(trimmed, target, false);
      if (action.hasNonNull("credentials")) {
        return action;
      }
      last = action;
    }
    // No line named credentials. The last one read is answered so the caller's own envelope check
    // stays the single place that reports what was missing.
    return last == null ? parseJson(body, target, false) : last;
  }

  /** The JSON body, classified. Separate from the send so a caller can weigh the body first. */
  private JsonNode parseJson(String body, String target, boolean includeBody) {
    try {
      JsonNode root = MAPPER.readTree(body);
      // Jackson 2.20 answers an empty or blank body with MissingNode, which the callers' own shape
      // checks already reject; older versions answered null, which would reach them as an NPE and
      // bypass this transport's classification entirely. Stated here so which it is does not
      // matter.
      if (root == null || root.isMissingNode()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing returned an empty response for " + target);
      }
      return root;
    } catch (IOException e) {
      // The cause is never attached. Jackson quotes the offending input in its message, and the
      // recipient token that authorises the credential endpoint authorises the listing and metadata
      // ones too -- so a server echoing it inside a malformed 200 would carry it into the cause
      // chain the vendor logs, on any path.
      //
      // A redacted snippet is appended where the caller allows a body. Without one a malformed 200
      // reaches an operator as a parse position and nothing else, and the message is hidden again
      // at the gRPC boundary. Withheld on the credential route, which is the route that passes
      // includeBodyInErrors false -- redaction covers the token this client sent, not the keys a
      // credential response carries.
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing returned a response for "
              + target
              + " that could not be read as one JSON document"
              + parseLocation(e)
              + (includeBody ? ": " + snippet(body) : ""),
          null);
    }
  }

  /** The decoded NDJSON actions together with the version the server stated in its headers. */
  private record NdjsonResponse(List<JsonNode> actions, Optional<Long> version) {}

  private NdjsonResponse sendNdjson(
      HttpRequest.Builder builder, String path, String target, boolean includeBodyInErrors) {
    BodyAndVersion response =
        sendForBodyAndVersion(builder, path, target, includeBodyInErrors, "application/x-ndjson");
    String body = response.body();
    List<JsonNode> actions = new ArrayList<>();
    for (String line : body.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      try {
        actions.add(MAPPER.readTree(trimmed));
      } catch (IOException e) {
        // Never attached, for the reason the JSON path states: Jackson quotes the offending input,
        // and the same recipient token authorises every endpoint.
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing returned a line for "
                + target
                + " that could not be read as one JSON document"
                + parseLocation(e)
                + (includeBodyInErrors ? ": " + snippet(trimmed) : ""),
            null);
      }
    }
    return new NdjsonResponse(actions, response.version());
  }

  /** A response body together with the table version the server stated in its headers. */
  private record BodyAndVersion(String body, Optional<Long> version) {}

  private String sendForBody(
      HttpRequest.Builder builder,
      String path,
      String target,
      boolean includeBodyInErrors,
      String accept) {
    return sendForBodyAndVersion(builder, path, target, includeBodyInErrors, accept).body();
  }

  private BodyAndVersion sendForBodyAndVersion(
      HttpRequest.Builder builder,
      String path,
      String target,
      boolean includeBodyInErrors,
      String accept) {
    HttpResponse<InputStream> response;
    try {
      builder
          .uri(URI.create(baseUri + path))
          .timeout(requestTimeout)
          .header("Accept", accept)
          .header("Authorization", "Bearer " + bearerToken)
          .header("delta-sharing-capabilities", capabilities);
      response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DeltaSharingException(
          Failure.INTERRUPTED, -1, "Delta Sharing request interrupted for " + target, e);
    } catch (DeltaSharingException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new DeltaSharingException(
          Failure.TRANSPORT, -1, "Delta Sharing request failed for " + target, e);
    }

    int status = response.statusCode();

    // Read outside the send above. ofInputStream returns once the headers arrive, so by the time
    // this can fail the status is already known; reading it in the same try would let a body that
    // stops mid-stream erase that status and report TRANSPORT, so a permanent 403 from a
    // consistently truncating proxy would be retried forever.
    String body;
    try {
      body = readBounded(response);
    } catch (IOException | RuntimeException e) {
      // Ahead of the status, so a cancellation is not dressed up as a refusal from the server.
      if (Thread.currentThread().isInterrupted()) {
        throw new DeltaSharingException(
            Failure.INTERRUPTED,
            -1,
            "Delta Sharing request interrupted reading the response for " + target,
            e);
      }
      if (status < 200 || status >= 300) {
        throw httpFailure(status, target, "", includeBodyInErrors);
      }
      throw new DeltaSharingException(
          Failure.TRANSPORT,
          -1,
          "Delta Sharing response body for " + target + " did not complete",
          e);
    }

    if (status < 200 || status >= 300) {
      throw httpFailure(status, target, body == null ? "" : body, includeBodyInErrors);
    }
    if (body == null) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          status,
          "Delta Sharing response for " + target + " exceeded " + maxResponseBytes + " bytes");
    }
    return new BodyAndVersion(body, tableVersion(response));
  }

  /**
   * The response cap, which cannot be the largest int.
   *
   * <p>The bounded reader asks for the cap plus one byte to spot an overrun, and that addition
   * overflows at {@code Integer.MAX_VALUE} -- so a deployment setting the cap to its largest
   * allowed value got a negative length and every response failed as a transport error. The Unity
   * client already refuses this value on the same property; this one copied the pattern without the
   * guard.
   */
  private static int configuredMaxResponseBytes() {
    int value = positiveIntProperty(MAX_RESPONSE_BYTES_PROPERTY, DEFAULT_MAX_RESPONSE_BYTES);
    if (value == Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          MAX_RESPONSE_BYTES_PROPERTY + " must be a positive integer below " + Integer.MAX_VALUE);
    }
    return value;
  }

  /**
   * A positive integer override, or the default.
   *
   * <p>Raises on a value it cannot use rather than falling back. A zero or negative bound made
   * every listing fail with "exceeded 0 pages" and every response read as oversized, which reports
   * a protocol violation for a server that answered correctly.
   */
  private static int positiveIntProperty(String name, int defaultValue) {
    String configured = System.getProperty(name);
    if (configured == null || configured.isBlank()) {
      return defaultValue;
    }
    int value;
    try {
      value = Integer.parseInt(configured.trim());
    } catch (NumberFormatException notAnInteger) {
      throw new IllegalArgumentException(
          name + " must be a positive integer: " + configured, notAnInteger);
    }
    if (value <= 0) {
      throw new IllegalArgumentException(name + " must be a positive integer: " + configured);
    }
    return value;
  }

  /**
   * The table version the server stated in {@code Delta-Table-Version}.
   *
   * <p>The protocol puts the current version here for an ordinary metadata request and populates
   * the metaData action's own {@code version} only for a versioned, timestamped or change-feed
   * query. Reading the body alone left the version empty against every conforming server.
   *
   * <p>A header that is present but not a long is ignored rather than raised on: it is a detail
   * beside the metadata itself, and refusing the whole table over it would trade a missing property
   * for an unusable share.
   */
  private static Optional<Long> tableVersion(HttpResponse<InputStream> response) {
    return response
        .headers()
        .firstValue("Delta-Table-Version")
        .flatMap(
            value -> {
              try {
                return Optional.of(Long.parseLong(value.trim()));
              } catch (NumberFormatException notALong) {
                return Optional.empty();
              }
            });
  }

  /**
   * The body, or null when it exceeded the cap.
   *
   * <p>Null rather than an exception, because for a non-2xx an oversized body means nothing: the
   * status already answered. Throwing would classify a permanent 403 whose error page happens to be
   * large as a retryable protocol violation.
   */
  private String readBounded(HttpResponse<InputStream> response) throws IOException {
    try (InputStream stream = response.body()) {
      // Under its own deadline. HttpRequest.timeout gates receipt of the headers only, because
      // ofInputStream returns as soon as they arrive, so a server that sends headers and then
      // stalls would hold a reconcile worker here with nothing to interrupt it.
      byte[] bytes =
          HttpResponseSnippets.readWithin(
              stream, maxResponseBytes, requestTimeout, "Delta Sharing");
      return bytes.length > maxResponseBytes ? null : new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Classification from the status alone.
   *
   * <p>Delta Sharing defines no error envelope, unlike Unity Catalog, so there is no body code to
   * consult and nothing to weigh against the status.
   */
  private DeltaSharingException httpFailure(
      int status, String target, String body, boolean includeBody) {
    Failure failure =
        switch (status) {
          case 401 -> Failure.UNAUTHENTICATED;
          case 403 -> Failure.PERMISSION_DENIED;
          case 404 -> Failure.NOT_FOUND;
          case 429 -> Failure.RATE_LIMITED;
          case 400, 405, 422 -> Failure.INVALID_REQUEST;
          case 408 -> Failure.TRANSIENT;
          default -> {
            if (status >= 500) {
              yield Failure.SERVER_ERROR;
            }
            // A 3xx reaching here means the endpoint tried to redirect and the client refused, so
            // the base URI names something other than a sharing server.
            yield status >= 300 && status < 400 ? Failure.INVALID_REQUEST : Failure.OTHER;
          }
        };
    StringBuilder message =
        new StringBuilder("Delta Sharing request for ")
            .append(target)
            .append(" failed with HTTP ")
            .append(status);
    if (includeBody && body != null && !body.isBlank()) {
      message.append(": ").append(snippet(body));
    }
    return new DeltaSharingException(failure, status, message.toString());
  }

  // ---------------------------------------------------------------------------
  // Decoding
  // ---------------------------------------------------------------------------

  /** Refuses a second action of a kind already seen, rather than letting it overwrite the first. */
  private static void requireFirst(Object seen, String action, String target) {
    if (seen != null) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing sent more than one " + action + " action for " + target);
    }
  }

  /**
   * The protocol action, in either response format.
   *
   * <p>The same two shapes the metaData action comes in: a server honouring {@code
   * responseformat=delta} nests the delta protocol under {@code deltaProtocol}, and one ignoring
   * the header answers flat. Read flat only, every table decoded as minReaderVersion 1 with no
   * reader features whatever it actually required.
   */
  private static Protocol parseProtocol(JsonNode node, String target) {
    JsonNode delta = node.path("deltaProtocol");
    JsonNode source = delta.isObject() ? delta : node;
    // The fifth array field in this parser and the only one that was not held to the rule, because
    // nothing reads what it produces: the Protocol record is written and never consulted. That is
    // what made it easy to leave out, and what would make a feature-negotiation reader added later
    // treat an unsupported feature as absent -- the same silence a scalar read as empty produces
    // everywhere else here.
    requireArrayWhereStated(source, "readerFeatures", target);
    List<String> features = new ArrayList<>();
    for (JsonNode feature : source.path("readerFeatures")) {
      if (!feature.isTextual() || feature.asText().isBlank()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing stated a readerFeatures entry that is not a feature name for " + target);
      }
      features.add(feature.asText());
    }
    // Numbers and numeric strings both, since a server sending "2" is stating a version and
    // refusing it would fail a working share over a field no consumer reads. Anything that is not
    // a version at all is refused, rather than defaulting silently to 1.
    JsonNode version = source.path("minReaderVersion");
    if (!version.isMissingNode() && !version.isNull() && !isIntegerValued(version)) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a minReaderVersion that is not a version for " + target);
    }
    return new Protocol(version.asInt(1), features);
  }

  /** Whether a node states an integer, as a number or as a string holding one. */
  private static boolean isIntegerValued(JsonNode node) {
    if (node.isIntegralNumber()) {
      return true;
    }
    if (!node.isTextual()) {
      return false;
    }
    try {
      // Long, not int. These are int64 fields, and an expiry in epoch milliseconds overflows an
      // int -- so a server rendering it as a JSON string, which is the standard protobuf JSON
      // encoding for int64, is refused. That refusal is INVALID_RESPONSE, which reaches the client
      // as INTERNAL and ends the whole reconcile rather than skipping one table.
      Long.parseLong(node.asText().trim());
      return true;
    } catch (NumberFormatException notAnInteger) {
      return false;
    }
  }

  /**
   * The metaData action, in either response format.
   *
   * <p>Every request here sends {@code responseformat=delta}, and a server honouring it nests the
   * table's own fields under {@code deltaMetadata}, leaving version, size, location, auxiliary
   * locations and access modes on the wrapper. A server that ignores the header answers with the
   * flat legacy shape instead. Reading the nested object where it exists and falling back to the
   * wrapper covers both, and reading it flat covered only the second: the schema arrived blank and
   * the model rejected it before this class could classify anything.
   */
  private static TableMetadata parseMetadata(JsonNode node, String target) {
    JsonNode delta = node.path("deltaMetadata");
    JsonNode table = delta.isObject() ? delta : node;
    return parseMetadata(node, table, target);
  }

  private static TableMetadata parseMetadata(JsonNode node, JsonNode table, String target) {
    // Checked before the record, for the reason the credentials envelope is: requireText raises
    // IllegalArgumentException, which is not a DeltaSharingException and so escapes this class's
    // classification and the translate() in the catalog client, reaching the reconciler as a bare
    // RuntimeException naming neither the table nor the server.
    if (!table.path("schemaString").isTextual() || table.path("schemaString").asText().isBlank()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing metadata for " + target + " carried no schemaString");
    }
    // The same rule the other two array fields follow, and this is the one that persists. These
    // become CatalogTable.partitionKeys and then UpstreamRef.partition_keys, so a scalar read as
    // absent reconciled a partitioned table as unpartitioned -- a wrong table definition that
    // validates clean, rather than a failure. Absent stays legal and means unpartitioned; a
    // non-text element is refused, because asText answers an object with the empty string and an
    // empty partition key is worse than none.
    requireArrayWhereStated(table, "partitionColumns", target);
    List<String> partitions = new ArrayList<>();
    for (JsonNode column : table.path("partitionColumns")) {
      if (!column.isTextual() || column.asText().isBlank()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing stated a partitionColumns entry that is not a name for " + target);
      }
      partitions.add(column.asText());
    }
    // Delta defines this as a map of strings, and these entries are published as the table's
    // properties. A stated value that is not an object leaves no properties at all, and a value
    // that is a number, a boolean or a null coerces through asText -- either way the table carries
    // metadata the server did not send. Absence still means no configuration.
    Map<String, String> configuration = new HashMap<>();
    JsonNode config = table.path("configuration");
    if (!config.isMissingNode() && !config.isNull() && !config.isObject()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a configuration that is not an object for " + target);
    }
    if (config.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fields = config.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        if (!field.getValue().isTextual()) {
          throw new DeltaSharingException(
              Failure.INVALID_RESPONSE,
              200,
              "Delta Sharing stated a non-string configuration value for "
                  + field.getKey()
                  + " in "
                  + target);
        }
        configuration.put(field.getKey(), field.getValue().asText());
      }
    }
    List<String> auxiliary = textList(node, "auxiliaryLocations", target);
    return new TableMetadata(
        optionalText(table, "id", target),
        optionalText(table, "name", target),
        formatProvider(table, target),
        table.path("schemaString").asText(),
        partitions,
        configuration,
        tableVersion(node, target),
        optionalText(node, "location", target),
        auxiliary,
        accessModes(node, target));
  }

  private static TemporaryCredentials parseCredentials(JsonNode node, String target) {
    String location = node.path("location").isTextual() ? node.path("location").asText() : "";
    // Checked here rather than left to the record. requireText raises IllegalArgumentException,
    // which is not a DeltaSharingException, so it escapes this class's classification and reaches
    // the vendor as a bare RuntimeException naming neither the table nor the server.
    if (location.isBlank()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for " + target + " named no location");
    }
    // Which cloud first, because that is the more basic question about the envelope: a body naming
    // none is not a credential at all, and saying so is more use than complaining about its expiry.
    // Exactly one, not at least one. The protocol makes these a choice, and the decode below takes
    // the first branch that matches -- so an envelope naming two clouds silently became whichever
    // one is tested first, making the answer a property of this method's ordering rather than of
    // the response. Two clouds is not a credential this client can be confident it read correctly,
    // and a message saying so is more use than a session token from the wrong one.
    int clouds = 0;
    for (String field : List.of("awsTempCredentials", "azureUserDelegationSas", "gcpOauthToken")) {
      if (node.hasNonNull(field)) {
        clouds++;
      }
    }
    if (clouds == 0) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for " + target + " named no supported cloud");
    }
    if (clouds > 1) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for "
              + target
              + " named "
              + clouds
              + " clouds, and the protocol allows one");
    }

    // Required, not optional. A Catalog Integration session without one is refused at query time by
    // SourceCatalogCredentialVendor, while validation does not treat an absent expiry as expired --
    // so a share omitting it would validate clean and then fail every read, which is the least
    // diagnosable shape this can take.
    JsonNode statedExpiry = node.path("expirationTime");
    if (!statedExpiry.isMissingNode() && !statedExpiry.isNull()) {
      if (!isIntegerValued(statedExpiry)) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing temporary credentials for "
                + target
                + " named an expiry that is not a number");
      }
    }
    long expiresAtMillis = statedExpiry.asLong(0L);
    if (!node.hasNonNull("expirationTime") || expiresAtMillis <= 0L) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for " + target + " named no expiry");
    }
    // The upper bound the other two parsers of this field already apply. A value past the last
    // instant a proto Timestamp carries is a unit mismatch rather than a date -- microseconds where
    // milliseconds were meant, which Unity deployments have been seen to send -- and without this
    // it survives every downstream expiry check by looking far in the future and then throws inside
    // Timestamps.fromMillis while the gRPC response is built. That is an unclassified
    // RuntimeException in a handler, on a share that validated clean, for every read.
    if (expiresAtMillis > MAX_EXPIRY_EPOCH_MILLIS) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing temporary credentials for "
              + target
              + " named an expiry beyond the last representable instant, which is a unit mismatch"
              + " rather than a date");
    }
    Optional<Instant> expiresAt = Optional.of(Instant.ofEpochMilli(expiresAtMillis));

    if (node.hasNonNull("awsTempCredentials")) {
      JsonNode aws = node.get("awsTempCredentials");
      Optional<String> accessKeyId = optionalText(aws, "accessKeyId", target);
      Optional<String> secretAccessKey = optionalText(aws, "secretAccessKey", target);
      Optional<String> sessionToken = optionalText(aws, "sessionToken", target);
      // All three, checked here. Left to the caller's hasAwsSession() an incomplete triad reads as
      // "this cloud cannot be published", which reports UNSUPPORTED -- a per-table skip meaning the
      // provider will never do this. A 200 missing a key is a malformed envelope like any other,
      // and belongs with the rest of them.
      if (!aws.isObject()
          || accessKeyId.isEmpty()
          || secretAccessKey.isEmpty()
          || sessionToken.isEmpty()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing temporary credentials for "
                + target
                + " carried an incomplete AWS session");
      }
      return new TemporaryCredentials(
          CredentialCloud.AWS, location, accessKeyId, secretAccessKey, sessionToken, expiresAt);
    }
    // Recognised and reported by cloud rather than returned empty. A caller that cannot use an
    // Azure or GCP credential should say which one arrived, not that the server vended nothing.
    if (node.hasNonNull("azureUserDelegationSas")) {
      return new TemporaryCredentials(
          CredentialCloud.AZURE,
          location,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          expiresAt);
    }
    if (node.hasNonNull("gcpOauthToken")) {
      return new TemporaryCredentials(
          CredentialCloud.GCP,
          location,
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          expiresAt);
    }
    throw new DeltaSharingException(
        Failure.INVALID_RESPONSE,
        200,
        "Delta Sharing temporary credentials for " + target + " named no supported cloud");
  }

  /** Where a parse failed, without any of what it was parsing. */
  private static String parseLocation(IOException failure) {
    if (failure instanceof com.fasterxml.jackson.core.JsonProcessingException json
        && json.getLocation() != null) {
      return " at line "
          + json.getLocation().getLineNr()
          + ", column "
          + json.getLocation().getColumnNr();
    }
    return "";
  }

  /**
   * The table version the metaData action stated, refused where it is not a number.
   *
   * <p>{@code asLong} answers zero for a string or a container, and this value is published as the
   * {@code delta.sharing.version} property. Left unchecked, a malformed version is a table
   * definition nobody sent rather than a failure.
   */
  private static Optional<Long> tableVersion(JsonNode node, String target) {
    JsonNode version = node.path("version");
    if (version.isMissingNode() || version.isNull()) {
      return Optional.empty();
    }
    if (!isIntegerValued(version)) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a version that is not a number for " + target);
    }
    return Optional.of(version.asLong());
  }

  private static String formatProvider(JsonNode table, String target) {
    JsonNode provider = table.path("format").path("provider");
    if (provider.isMissingNode() || provider.isNull()) {
      return "parquet";
    }
    // asText supplies its default for a missing or container node but not for a stated empty
    // string, so {"provider":""} arrived blank at the record's own requireText -- which raises
    // IllegalArgumentException, not a DeltaSharingException, and so escapes this class's
    // classification and the provider's translate() to reach the reconciler naming neither the
    // table nor the server. The same escape the schemaString and credentials-envelope checks close.
    if (!provider.isTextual() || provider.asText().isBlank()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a format provider that is not a name for " + target);
    }
    return provider.asText();
  }

  /**
   * Refuses a field that is stated but is not an array.
   *
   * <p>Iterating a scalar node yields nothing, so {@code "auxiliaryLocations": "s3://other/part"}
   * read as absent -- which is the one shape the auxiliary-location refusal exists to catch, and it
   * would have reconciled with a credential covering the root alone and failed at scan time on the
   * files it does not reach. A stated {@code accessModes} scalar read as unstated for the same
   * reason. This is the rule the page shape already enforces for {@code items}; the fields inside
   * an item were not held to it.
   */
  private static void requireArrayWhereStated(JsonNode node, String field, String target) {
    JsonNode value = node.path(field);
    // Null counts as not stated, as it does for every other optional field here -- configuration,
    // version, minReaderVersion, the page cursor and optionalText all read it that way. Refusing it
    // ends the whole reconcile over an ordinary JSON rendering of an absent list. The page's own
    // items field is the one place null is refused, because there it would mean an empty inventory.
    if (!value.isMissingNode() && !value.isNull() && !value.isArray()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a non-array " + field + " for " + target);
    }
  }

  private static List<String> textList(JsonNode node, String field, String target) {
    requireArrayWhereStated(node, field, target);
    List<String> values = new ArrayList<>();
    for (JsonNode value : node.path(field)) {
      // A non-text element is refused rather than dropped. asText answers an object with the empty
      // string, so an array of objects read as an empty list -- and for auxiliaryLocations that
      // means hasAuxiliaryLocations() answers false and the refusal at the load never fires, which
      // is the silence the array guard was added to end one level up.
      if (!value.isTextual() || value.asText().isBlank()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing stated a " + field + " entry that is not a value for " + target);
      }
      values.add(value.asText().trim());
    }
    return List.copyOf(values);
  }

  private static List<AccessMode> accessModes(JsonNode item, String target) {
    requireArrayWhereStated(item, "accessModes", target);
    List<AccessMode> modes = new ArrayList<>();
    for (JsonNode mode : item.path("accessModes")) {
      // Refused, not coerced. asText answers an object with the empty string, which mapped to
      // OTHER -- and OTHER alongside a readable value is the unsafe direction: ["{...}", "dir"]
      // decoded to [OTHER, DIR], contains(DIR) answered true, and the table was treated as
      // directory-accessible on the strength of a response this client could not read.
      if (!mode.isTextual()) {
        throw new DeltaSharingException(
            Failure.INVALID_RESPONSE,
            200,
            "Delta Sharing stated an accessModes entry that is not a mode for " + target);
      }
      String value = mode.asText("").trim().toLowerCase(Locale.ROOT);
      if ("url".equals(value)) {
        modes.add(AccessMode.URL);
      } else if ("dir".equals(value)) {
        modes.add(AccessMode.DIR);
      } else {
        // Kept as OTHER rather than dropped. A server adding a third mode still must not make an
        // otherwise readable table undiscoverable -- a list holding dir and something unknown still
        // holds dir -- but dropping it made a table stating only unknown modes indistinguishable
        // from one stating none, and the strict setting then refused it saying it had stated none.
        modes.add(AccessMode.OTHER);
      }
    }
    return modes;
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** What a Delta reader feature name may contain, so it cannot reshape the capability header. */
  private static final java.util.regex.Pattern FEATURE_TOKEN =
      java.util.regex.Pattern.compile("[A-Za-z0-9_-]+");

  /**
   * The capability header, with each feature checked where it is interpolated.
   *
   * <p>Checked here rather than only in the provider that reads the operator's property, because
   * this constructor is public and is itself the provider's factory seam. A value carrying a
   * semicolon appends a capability field of its own -- {@code x;responseformat=parquet} makes a
   * server answer in the flat shape, which this client then parses without complaint -- and one
   * carrying a newline is refused by the request builder at send time instead of here.
   */
  private static String buildCapabilities(List<String> readerFeatures) {
    StringBuilder value = new StringBuilder("responseformat=delta");
    if (readerFeatures != null && !readerFeatures.isEmpty()) {
      for (String feature : readerFeatures) {
        if (feature == null || !FEATURE_TOKEN.matcher(feature).matches()) {
          throw new IllegalArgumentException(
              "Delta reader feature must be letters, digits, hyphens or underscores: " + feature);
        }
      }
      value.append(";readerfeatures=").append(String.join(",", readerFeatures));
    }
    return value.toString();
  }

  private static String tablePath(String share, String schema, String table) {
    return "/shares/" + encode(share) + "/schemas/" + encode(schema) + "/tables/" + encode(table);
  }

  /**
   * A query value, encoded without the path-segment rules.
   *
   * <p>A page token is opaque and may be anything the server chose, including a value {@code
   * encode} refuses: that method rejects a blank string as an unusable path segment, which for a
   * cursor would raise an unclassified IllegalArgumentException out of a listing rather than send
   * the token back. Same escaping, no judgement about the content.
   */
  private static String encodeQueryValue(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /**
   * Percent-encodes one path segment.
   *
   * <p>{@code URLEncoder} is form encoding, where a space becomes {@code +}. In a path segment a
   * {@code +} is a literal plus, so a share, schema or table name containing a space would be
   * requested at the wrong path and come back 404. A literal plus in the input is already {@code
   * %2B} by the time of the substitution, so anything still {@code +} stands for a space.
   */
  private static String encode(String value) {
    return URLEncoder.encode(requireText(value, "path segment"), StandardCharsets.UTF_8)
        .replace("+", "%20");
  }

  private static String text(JsonNode node, String field, String target) {
    JsonNode stated = node.path(field);
    // Textual, not merely coercible. asText answers "17" for a number and "true" for a boolean, so
    // {"name": 17} became a table named 17 in an inventory the reconciler treats as authoritative
    // -- and tables the same malformed page omitted are retired against it. The protocol defines
    // these as strings.
    if (!stated.isMissingNode() && !stated.isNull() && !stated.isTextual()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a non-textual " + field + " in " + target);
    }
    String value = stated.asText("");
    if (value.isBlank()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing item is missing " + field + " in " + target);
    }
    return value;
  }

  private static Optional<String> optionalText(JsonNode node, String field, String target) {
    JsonNode value = node.path(field);
    // Every field decoded through here is a string in the protocol. asText answers "17" for a
    // number and the empty string for a container, so a stated non-string is either metadata the
    // server did not send or a field read as absent.
    if (!value.isMissingNode() && !value.isNull() && !value.isTextual()) {
      throw new DeltaSharingException(
          Failure.INVALID_RESPONSE,
          200,
          "Delta Sharing stated a non-string " + field + " for " + target);
    }
    return optionalText(node, field);
  }

  private static Optional<String> optionalText(JsonNode node, String field) {
    String value = node.path(field).asText("");
    return value.isBlank() ? Optional.empty() : Optional.of(value);
  }

  /**
   * A response snippet with the recipient's own token taken out of it.
   *
   * <p>The Authorization header goes out on every request, and a server that echoes request headers
   * in an error body would otherwise put a live recipient token into an exception message, which
   * reaches validation output and operator logs.
   */
  private String snippet(String body) {
    // The token this client sent, by value. The generic pattern covers a token shaped the way the
    // grammar says one should be; this one need not be, since a header value permits more than the
    // grammar does, and a suffix past the first unmatched character reached the message.
    return HttpResponseSnippets.boundedSingleLine(body, MAX_BODY_SNIPPET_CHARS, bearerToken);
  }

  private static String quoteJson(String value) {
    StringBuilder quoted = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> quoted.append("\\\"");
        case '\\' -> quoted.append("\\\\");
        case '\n' -> quoted.append("\\n");
        case '\r' -> quoted.append("\\r");
        case '\t' -> quoted.append("\\t");
        default -> {
          if (c < 0x20) {
            quoted.append(String.format("\\u%04x", (int) c));
          } else {
            quoted.append(c);
          }
        }
      }
    }
    return quoted.append('"').toString();
  }

  private static String stripTrailingSlash(String value) {
    String trimmed = value;
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  /**
   * A recipient token that can be sent as a header value, described without being quoted.
   *
   * <p>Field-value rules, not a guess: a header value may not carry a control character, and a
   * newline in a bearer token is the shape a copied secret usually takes. Nor anything above {@code
   * U+00FF}, which is where the JDK's own header validation stops -- a token carrying an emoji
   * passed this check and was refused by {@code HttpRequest.Builder.header} instead, whose
   * IllegalArgumentException quotes the whole header value. The send wraps a RuntimeException as
   * TRANSPORT with the cause attached, so that put the live token into the chain the vendor logs,
   * on a request that never left the process.
   *
   * <p>Then the JDK is asked directly, because matching another component's private validation by
   * restating its rules is a thing that goes stale. The probe builds the header it would send and
   * discards it; a refusal is reported without the cause and without the value, since that
   * exception is itself the leak.
   */
  private static String requireHeaderSafe(String token) {
    for (int i = 0; i < token.length(); i++) {
      char c = token.charAt(i);
      if (c < 0x20 || c == 0x7F) {
        throw new IllegalArgumentException(
            "bearerToken carries a control character at index " + i + " and cannot be sent");
      }
      if (c > 0x7E) {
        // Printable ASCII, which is narrower than the JDK's own limit of U+00FF, and deliberately.
        // A JSON serializer that escapes non-ASCII -- Python's default among them -- renders such a
        // token as Bearer abc\u00e9SECRET in an echoed error body, where neither redaction pass
        // reaches it: the literal is not present in that form, and the pattern stops at the
        // backslash. Closing that by unescaping, redacting and re-escaping puts more machinery in
        // the one control that must not be subtly wrong; refusing the range instead removes the
        // case. A bearer token outside printable ASCII is not a shape any deployment sends, while
        // the punctuation this still accepts -- a colon among it -- is the shape that does occur.
        throw new IllegalArgumentException(
            "bearerToken carries a character outside printable ASCII at index "
                + i
                + ", which cannot be redacted reliably where a server echoes it");
      }
    }
    try {
      HttpRequest.newBuilder(PROBE_URI).header("Authorization", "Bearer " + token);
    } catch (IllegalArgumentException rejected) {
      // Deliberately not chained. rejected.getMessage() is the header value in full.
      throw new IllegalArgumentException("bearerToken cannot be sent as a header value");
    }
    return token;
  }

  private static String requireText(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(field + " must not be blank");
    }
    return value;
  }

  private static Duration requirePositive(Duration value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(field + " must be positive");
    }
    return value;
  }
}
