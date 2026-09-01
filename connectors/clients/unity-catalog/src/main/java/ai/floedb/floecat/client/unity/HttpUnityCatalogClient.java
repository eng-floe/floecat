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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/** JDK HTTP implementation of the small Unity Catalog client boundary. */
public final class HttpUnityCatalogClient implements UnityCatalogClient {
  private static final String API_ROOT = "/api/2.1/unity-catalog";

  /** How much of a response body a failure message may quote. */
  private static final int MAX_BODY_SNIPPET_CHARS = 2_000;

  /**
   * How much of an error_code the accessor may carry. Databricks' longest is around forty.
   *
   * <p>The same bound the Delta connector uses when it puts a code in a message; a third number
   * here would mean a value truncated twice at two different sizes for no reason. A length only --
   * what may be repeated on the route that suppresses bodies is decided by {@link
   * #RECOGNIZED_ERROR_CODES}, not by size.
   */
  private static final int MAX_ERROR_CODE_CHARS = 64;

  /**
   * Every {@code error_code} this client will repeat on the route that suppresses response bodies.
   *
   * <p>The ten this client classifies on, plus {@code INVALID_PARAMETER_VALUE}: Databricks answers
   * the credentials endpoint with that one when a workspace lacks {@code EXTERNAL USE SCHEMA} or a
   * table has external access off, which is the flagship refusal this feature exists to report.
   */
  private static final Set<String> RECOGNIZED_ERROR_CODES =
      Set.of(
          "UNAUTHENTICATED",
          "PERMISSION_DENIED",
          "CUSTOMER_UNAUTHORIZED",
          "RESOURCE_DOES_NOT_EXIST",
          "NOT_FOUND",
          "ENDPOINT_NOT_FOUND",
          "REQUEST_LIMIT_EXCEEDED",
          "ALREADY_EXISTS",
          "RESOURCE_ALREADY_EXISTS",
          "ABORTED",
          "INVALID_PARAMETER_VALUE");

  /** Credential vending as served by Databricks. The default; every other operation is 2.1. */
  public static final String DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH =
      "/api/2.0/unity-catalog/temporary-table-credentials";

  /** The same operation as served by OSS Unity Catalog 0.6.0 and later. */
  public static final String OSS_TEMPORARY_TABLE_CREDENTIALS_PATH =
      "/api/2.1/unity-catalog/temporary-table-credentials";

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final Pattern BEARER_TOKEN = Pattern.compile("(?i)bearer\\s+[A-Za-z0-9._~+/=-]+");

  /**
   * Page limit for one listing. The repeated-token check in {@link #listAll} catches a server that
   * reuses a token, not one that mints a new token per page, and nothing else bounds the loop:
   * {@code requestTimeout} applies per request. Set far above any real catalog.
   */
  static final int DEFAULT_MAX_PAGES = 10_000;

  /**
   * Override for {@link #DEFAULT_MAX_PAGES}. Read the same way as the response-size cap so a test
   * or a catalog with unusual paging can set it without a separate constructor.
   */
  static final String MAX_PAGES_PROPERTY = "floecat.unity.max-pages";

  /**
   * Opt-in for cleartext HTTP to a loopback Unity Catalog. Deny by default, mirroring {@code
   * floecat.security.allow-loopback-token-endpoints}. Read from system properties and the
   * environment at construction; this module does not depend on the connector config layer.
   */
  static final String ALLOW_LOOPBACK_PROPERTY = "floecat.security.allow-loopback-catalog-endpoints";

  private static final String ALLOW_LOOPBACK_ENV =
      "FLOECAT_SECURITY_ALLOW_LOOPBACK_CATALOG_ENDPOINTS";

  /**
   * Cap on a single response body, in bytes. Override with {@code floecat.unity.max-response-bytes}
   * for a catalog whose listing pages genuinely exceed it.
   */
  static final String MAX_RESPONSE_BYTES_PROPERTY = "floecat.unity.max-response-bytes";

  private static final int DEFAULT_MAX_RESPONSE_BYTES = 32 * 1024 * 1024;

  /** Opt-in for a base URI naming a private (site-local) address literal. Deny by default. */
  static final String ALLOW_PRIVATE_PROPERTY = "floecat.security.allow-private-catalog-endpoints";

  private static final String ALLOW_PRIVATE_ENV =
      "FLOECAT_SECURITY_ALLOW_PRIVATE_CATALOG_ENDPOINTS";

  private final String baseUri;
  private final String credentialsPath;
  private final int maxResponseBytes;
  private final int maxPages;

  private final Duration requestTimeout;
  private final UnityCatalogAuthentication authentication;
  private final HttpClient httpClient;

  public HttpUnityCatalogClient(
      URI baseUri,
      Duration connectTimeout,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication) {
    this(
        baseUri,
        connectTimeout,
        requestTimeout,
        authentication,
        DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH);
  }

  /**
   * A client whose credential-vending route is chosen by the caller.
   *
   * @param credentialsPath {@link #DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH}, {@link
   *     #OSS_TEMPORARY_TABLE_CREDENTIALS_PATH}, or the route a proxy exposes.
   */
  public HttpUnityCatalogClient(
      URI baseUri,
      Duration connectTimeout,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication,
      String credentialsPath) {
    // Arguments evaluate left to right; every check runs before newHttpClient allocates a
    // transport.
    this(
        stripTrailingSlash(validateBaseUri(baseUri).toString()),
        requirePositive(requestTimeout, "requestTimeout"),
        Objects.requireNonNull(authentication, "authentication"),
        requireAbsolutePath(credentialsPath),
        configuredMaxResponseBytes(),
        configuredMaxPages(),
        newHttpClient(connectTimeout));
  }

  HttpUnityCatalogClient(
      URI baseUri,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication,
      HttpClient httpClient) {
    this(
        baseUri,
        requestTimeout,
        authentication,
        DATABRICKS_TEMPORARY_TABLE_CREDENTIALS_PATH,
        httpClient);
  }

  HttpUnityCatalogClient(
      URI baseUri,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication,
      String credentialsPath,
      HttpClient httpClient) {
    this(
        stripTrailingSlash(validateBaseUri(baseUri).toString()),
        requirePositive(requestTimeout, "requestTimeout"),
        Objects.requireNonNull(authentication, "authentication"),
        requireAbsolutePath(credentialsPath),
        configuredMaxResponseBytes(),
        configuredMaxPages(),
        Objects.requireNonNull(httpClient, "httpClient"));
  }

  /**
   * Assigns already-validated values. Every check runs in a delegating argument list, ahead of
   * {@link #newHttpClient}; re-checking here would run after the transport exists, and the base-URI
   * gates read system properties that can change between the two passes.
   */
  private HttpUnityCatalogClient(
      String baseUri,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication,
      String credentialsPath,
      int maxResponseBytes,
      int maxPages,
      HttpClient httpClient) {
    this.baseUri = baseUri;
    this.maxResponseBytes = maxResponseBytes;
    this.maxPages = maxPages;
    this.requestTimeout = requestTimeout;
    this.authentication = authentication;
    this.credentialsPath = credentialsPath;
    this.httpClient = httpClient;
  }

  @Override
  public List<String> listCatalogs() {
    return listAll(API_ROOT + "/catalogs", "catalogs", node -> text(node, "name"));
  }

  @Override
  public List<String> listSchemas(String catalogName) {
    return listAll(
        API_ROOT + "/schemas?catalog_name=" + encode(catalogName),
        "schemas",
        node -> text(node, "name"));
  }

  @Override
  public List<UnityCatalogTable> listTables(String catalogName, String schemaName) {
    return listAll(
        API_ROOT
            + "/tables?catalog_name="
            + encode(catalogName)
            + "&schema_name="
            + encode(schemaName),
        "tables",
        HttpUnityCatalogClient::listedTable);
  }

  @Override
  public Optional<UnityCatalogTable> getTable(String fullName) {
    return getTable(fullName, true);
  }

  @Override
  public Optional<UnityCatalogTable> getTableWithLenientColumns(String fullName) {
    return getTable(fullName, false);
  }

  private Optional<UnityCatalogTable> getTable(String fullName, boolean strictColumns) {
    try {
      String path = API_ROOT + "/tables/" + encode(fullName);
      JsonNode response = get(path);
      if (!response.isObject()) {
        throw invalidResponse("Expected object response from " + path, null);
      }
      return Optional.of(table(response, strictColumns));
    } catch (UnityCatalogException e) {
      if (e.failure() == UnityCatalogException.Failure.NOT_FOUND) {
        return Optional.empty();
      }
      throw e;
    }
  }

  @Override
  public TemporaryTableCredentials generateTemporaryTableCredentials(
      String tableId, TableOperation operation) {
    var body = JSON.createObjectNode();
    body.put("table_id", tableId);
    body.put("operation", operation.name());
    JsonNode response = post(credentialsPath, body.toString());
    if (!response.isObject()) {
      throw invalidResponse("Expected object response from " + credentialsPath, null);
    }
    TemporaryTableCredentials.AwsCredentials aws =
        awsCredentials(response.path("aws_temp_credentials"));
    boolean unsupported =
        response.hasNonNull("azure_user_delegation_sas")
            || response.hasNonNull("azure_aad")
            || response.hasNonNull("gcp_oauth_token")
            || response.hasNonNull("r2_temp_credentials");
    return new TemporaryTableCredentials(
        aws, unsupported, text(response, "expiration_time"), text(response, "url"));
  }

  /**
   * Reads {@code aws_temp_credentials}, if the response carried a usable one.
   *
   * <p>{@link #text} returns null for a missing, JSON-null or blank field, so a present object says
   * nothing about whether it holds a credential. An access key and a secret are required; the
   * session token is absent for long-lived keys and the access point is an optional routing hint. A
   * present but unusable value -- {@code {}}, half a tuple, or a non-object -- is {@code
   * INVALID_RESPONSE}. Only a missing field or an explicit JSON null means no AWS credentials.
   */
  private static TemporaryTableCredentials.AwsCredentials awsCredentials(JsonNode awsNode) {
    if (awsNode.isMissingNode() || awsNode.isNull()) {
      return null;
    }
    if (!awsNode.isObject()) {
      throw invalidResponse("Unity Catalog returned a non-object aws_temp_credentials", null);
    }
    String accessKeyId = text(awsNode, "access_key_id");
    String secretAccessKey = text(awsNode, "secret_access_key");
    if (accessKeyId == null || secretAccessKey == null) {
      throw invalidResponse(
          "Unity Catalog returned aws_temp_credentials without an access key and secret", null);
    }
    return new TemporaryTableCredentials.AwsCredentials(
        accessKeyId,
        secretAccessKey,
        text(awsNode, "session_token"),
        text(awsNode, "access_point"));
  }

  @Override
  public void close() {
    httpClient.close();
  }

  private <T> List<T> listAll(String path, String arrayField, Function<JsonNode, T> mapper) {
    List<T> result = new ArrayList<>();
    Set<String> tokens = new HashSet<>();
    String pageToken = null;
    int pages = 0;
    do {
      if (++pages > maxPages) {
        throw invalidResponse("Unity Catalog paged past " + maxPages + " pages for " + path, null);
      }
      String pagePath =
          pageToken == null
              ? path
              : path + (path.contains("?") ? "&" : "?") + "page_token=" + encode(pageToken);
      JsonNode page = get(pagePath);
      if (!page.isObject()) {
        throw invalidResponse("Expected object response from " + pagePath, null);
      }
      JsonNode items = page.get(arrayField);
      if (items != null && !items.isNull()) {
        if (!items.isArray()) {
          throw invalidResponse("Expected array field '" + arrayField + "' from " + pagePath, null);
        }
        for (JsonNode item : items) {
          T mapped = mapper.apply(item);
          if (mapped != null) {
            result.add(mapped);
          }
        }
      }
      pageToken = text(page, "next_page_token");
      if (pageToken != null && !tokens.add(pageToken)) {
        throw invalidResponse("Unity Catalog repeated page token for " + path, null);
      }
    } while (pageToken != null);
    return List.copyOf(result);
  }

  private JsonNode get(String path) {
    return send(HttpRequest.newBuilder().GET(), path);
  }

  private JsonNode post(String path, String jsonBody) {
    return send(
        HttpRequest.newBuilder()
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonBody)),
        path);
  }

  private JsonNode send(HttpRequest.Builder builder, String path) {
    HttpResponse<InputStream> response;
    try {
      builder
          .uri(URI.create(baseUri + path))
          .timeout(requestTimeout)
          .header("Accept", "application/json");
      applyAuthentication(builder);
      response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UnityCatalogException(
          UnityCatalogException.Failure.INTERRUPTED, -1, "Unity Catalog request interrupted", e);
    } catch (UnityCatalogException e) {
      // Already classified; must precede the RuntimeException catch below.
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new UnityCatalogException(
          UnityCatalogException.Failure.TRANSPORT,
          -1,
          "Unity Catalog request failed for " + path,
          e);
    }

    int status = response.statusCode();
    String target = describeTarget(path, response.uri());
    // The configured path, not the constant: suppression follows whichever route is in use.
    boolean includeResponseBody = !credentialsPath.equals(path);

    // The body is read outside the send above, because ofInputStream returns as soon as the headers
    // arrive: by the time this can fail, the status is already known. Reading it in the same try
    // would let a body that stops mid-stream -- a declared Content-Length a proxy does not finish,
    // a reset after the headers -- erase that status and report TRANSPORT with -1, so a permanent
    // 403 from a consistently truncating proxy would be retried forever. That is the same mistake
    // readBounded avoids by returning null instead of throwing on an oversized body.
    String body;
    try {
      body = readBounded(response);
    } catch (IOException | RuntimeException e) {
      // Ahead of the status, because a cancellation must not be dressed up as a workspace refusal:
      // a terminal PERMISSION_DENIED here would permanently fail a job because a service restarted.
      if (Thread.currentThread().isInterrupted()) {
        throw new UnityCatalogException(
            UnityCatalogException.Failure.INTERRUPTED,
            -1,
            "Unity Catalog request interrupted reading the response for " + path,
            e);
      }
      // A non-2xx already answered, so it takes the same route an oversized body does -- a
      // truncated 403 and a 403 behind a megabyte of error page classify alike.
      if (status < 200 || status >= 300) {
        throw httpFailure(status, target, "", includeResponseBody);
      }
      // A 2xx whose body never arrived went out and did not complete, which is what statusCode()
      // documents -1 for, and a retry may well get the rest of it.
      throw new UnityCatalogException(
          UnityCatalogException.Failure.TRANSPORT,
          -1,
          "Unity Catalog response body could not be read for " + path,
          e);
    }

    if (status < 200 || status >= 300) {
      // Status stays authoritative even when the body was too large to keep, so an oversized proxy
      // error page cannot make a 401, 403, 429, 405, 408, 422, 423 or 425 read as anything else.
      //
      // For an unlisted 4xx it costs more than diagnostics, and the earlier claim that it did not
      // was wrong: the dropped body takes the error_code with it, so a 400 the envelope would have
      // made a permanent INVALID_REQUEST degrades to OTHER and hasErrorEnvelope() reports false for
      // a response that carried one. Retaining a prefix for non-2xx would close that, at the cost
      // of buffering part of every error page; at a 32 MiB cap the trade is not worth it.
      throw httpFailure(status, target, body == null ? "" : body, includeResponseBody);
    }
    if (body == null) {
      // A success this large is unusable: there is nothing to parse and nothing to fall back on.
      throw invalidResponse(
          status,
          "Unity Catalog response for " + target + " exceeded " + maxResponseBytes + " bytes",
          null,
          null);
    }
    try {
      JsonNode parsed = JSON.readTree(body);
      // An empty or whitespace-only body parses to a missing node rather than throwing, so without
      // this it would reach the callers' shape checks and be reported through the status-less
      // invalidResponse -- the form that means "the shape of a body that already parsed", which a
      // consumer is entitled to treat as permanent. A 2xx with no body is the opposite: headers
      // then a close, a sidecar restarting mid-rollout, a proxy that dropped the payload. It gets
      // the real status here so it stays as retryable as the same connection truncated a byte
      // later, which throws from readNBytes and classifies TRANSPORT.
      if (parsed == null || parsed.isMissingNode()) {
        throw invalidResponse(
            status, "Empty response body from Unity Catalog for " + target, null, null);
      }
      return parsed;
    } catch (JsonProcessingException e) {
      throw invalidResponse(
          status,
          "Invalid JSON from Unity Catalog for " + target,
          includeResponseBody ? body : null,
          includeResponseBody ? e : null);
    }
  }

  /**
   * Reads the response body, refusing one larger than {@link #maxResponseBytes}.
   *
   * <p>{@code BodyHandlers.ofString} buffers whatever arrives, and the whole body then reaches the
   * JSON parser. A connector URI is tenant-supplied, so a tenant pointing one at a server they
   * control could return a body large enough to exhaust the shared service's heap. The request
   * timeout is no defence: the body arrives inside it.
   *
   * <p>One byte past the limit is enough to detect the overrun without holding the rest.
   */
  private String readBounded(HttpResponse<InputStream> response) throws IOException {
    try (InputStream stream = response.body()) {
      byte[] bytes = stream.readNBytes(maxResponseBytes + 1);
      // Null, not an exception: the caller decides what an oversized body means, and for a non-2xx
      // it means nothing -- the status already answered. Throwing here would classify a permanent
      // 403 whose error page happens to be large as a retryable INVALID_RESPONSE.
      return bytes.length > maxResponseBytes ? null : new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Puts the authentication headers on a request.
   *
   * <p>{@link IllegalArgumentException} from the provider is invalid configuration and permanent.
   * Other provider failures may be transient and fall through to the transport classification.
   */
  private void applyAuthentication(HttpRequest.Builder builder) {
    Map<String, String> headers;
    try {
      headers = authentication.headers();
    } catch (IllegalArgumentException e) {
      throw new UnityCatalogException(
          UnityCatalogException.Failure.INVALID_REQUEST,
          -1,
          "Unity Catalog authentication is misconfigured",
          e);
    } catch (RuntimeException e) {
      // A provider interrupted mid-refresh restores the flag and throws. Without this the failure
      // falls through to the transport classification and reads as TRANSPORT, so the caller retries
      // a cancellation -- and because the flag is still set every retry fails here again
      // immediately, spending the whole budget in a tight loop during the shutdown that was asked
      // for. That is what INTERRUPTED exists to prevent.
      if (Thread.currentThread().isInterrupted()) {
        throw new UnityCatalogException(
            UnityCatalogException.Failure.INTERRUPTED,
            -1,
            "Unity Catalog authentication interrupted",
            e);
      }
      throw e;
    }
    if (headers == null || headers.isEmpty()) {
      return;
    }
    try {
      // setHeader, not header: header() appends, and Accept and Content-Type are already set.
      for (Map.Entry<String, String> header : headers.entrySet()) {
        try {
          builder.setHeader(header.getKey(), header.getValue());
        } catch (RuntimeException rejected) {
          // setHeader quotes the rejected value in its message, so neither it nor the cause may
          // be attached. Headers are set individually to report the name, which is not a secret.
          throw new UnityCatalogException(
              UnityCatalogException.Failure.INVALID_REQUEST,
              -1,
              "Unity Catalog authentication produced a header this request cannot carry: "
                  + header.getKey(),
              null);
        }
      }
    } catch (UnityCatalogException alreadyClassified) {
      throw alreadyClassified;
    } catch (RuntimeException e) {
      throw new UnityCatalogException(
          UnityCatalogException.Failure.INVALID_REQUEST,
          -1,
          "Unity Catalog authentication produced headers this request cannot carry",
          e);
    }
  }

  /**
   * The request target for a failure message: the path, plus where a followed redirect landed. A
   * caller of the package-private constructor may supply a redirecting client.
   */
  private String describeTarget(String path, URI effectiveUri) {
    String requested = baseUri + path;
    if (effectiveUri == null || requested.equals(effectiveUri.toString())) {
      return path;
    }
    return path + " (redirected to " + effectiveUri + ")";
  }

  private static UnityCatalogTable listedTable(JsonNode node) {
    // Null rather than a throw: listAll drops an unmapped entry, so one null or scalar element in
    // the array cannot lose the rest of the page. getTable stays strict, where the caller asked
    // for one specific table and an unreadable answer is a failure.
    return node.isObject() ? table(node, false) : null;
  }

  private static UnityCatalogTable table(JsonNode node, boolean strictColumns) {
    if (!node.isObject()) {
      throw invalidResponse("Expected a table object from Unity Catalog", null);
    }
    List<UnityCatalogTable.Column> columns = parseColumns(node, strictColumns);
    return new UnityCatalogTable(
        text(node, "name"),
        text(node, "table_id"),
        text(node, "table_type"),
        text(node, "data_source_format"),
        text(node, "storage_location"),
        text(node, "view_definition"),
        columns,
        tableProperties(node));
  }

  private static List<UnityCatalogTable.Column> parseColumns(JsonNode node, boolean strict) {
    // Jackson iterates an object's values as though it were an array and yields nothing for a
    // scalar, so a detail response must reject either shape. A listing can still identify its
    // other entries without that schema, so only the malformed entry gets an empty column list.
    JsonNode columnsNode = node.path("columns");
    if (columnsNode.isMissingNode() || columnsNode.isNull()) {
      return List.of();
    }
    if (!columnsNode.isArray()) {
      return malformedColumns(strict, "Expected 'columns' to be an array from Unity Catalog");
    }
    List<UnityCatalogTable.Column> columns = new ArrayList<>();
    for (JsonNode column : columnsNode) {
      if (!column.isObject()) {
        return malformedColumns(
            strict, "Expected 'columns' entries to be objects from Unity Catalog");
      }
      columns.add(
          new UnityCatalogTable.Column(
              text(column, "name"),
              text(column, "type_name"),
              text(column, "type_text"),
              text(column, "type_json"),
              column.path("nullable").asBoolean(true)));
    }
    return columns;
  }

  private static List<UnityCatalogTable.Column> malformedColumns(boolean strict, String message) {
    if (strict) {
      throw invalidResponse(message, null);
    }
    return List.of();
  }

  private static Map<String, String> tableProperties(JsonNode node) {
    Map<String, String> properties = new LinkedHashMap<>();
    collectStringMap(properties, node.path("properties"));
    collectStringMap(properties, node.path("table_properties"));
    collectKeyValueArray(properties, node.path("properties"));
    collectKeyValueArray(properties, node.path("table_properties"));
    return Map.copyOf(properties);
  }

  private static void collectStringMap(Map<String, String> out, JsonNode node) {
    if (!node.isObject()) {
      return;
    }
    node.fields()
        .forEachRemaining(
            entry -> {
              String value = entry.getValue().isTextual() ? entry.getValue().asText() : null;
              if (value != null && !value.isBlank()) {
                out.put(entry.getKey(), value);
              }
            });
  }

  private static void collectKeyValueArray(Map<String, String> out, JsonNode node) {
    if (!node.isArray()) {
      return;
    }
    for (JsonNode item : node) {
      String key = Optional.ofNullable(text(item, "key")).orElse(text(item, "name"));
      String value = text(item, "value");
      if (key != null && value != null) {
        out.put(key, value);
      }
    }
  }

  /**
   * Classifies a non-2xx response.
   *
   * <p>Status alone is not enough on Databricks: a workspace without {@code EXTERNAL USE SCHEMA}
   * answers the credentials endpoint with {@code 400} and puts the reason in {@code error_code}.
   * Classification runs in three passes -- statuses that are definitive on their own, then {@code
   * error_code} for the ambiguous 4xx, then status for the rest.
   *
   * <p>An unlisted 4xx is permanent only when it carries the {@code error_code} envelope. Without
   * one it may be an error page from a proxy rather than the workspace, so it stays {@code OTHER}.
   */
  private static UnityCatalogException httpFailure(
      int status, String path, String body, boolean includeResponseBody) {
    String responseBody = includeResponseBody ? truncate(body) : "";
    // Capped like the body: error_code comes from that body but is interpolated outside the
    // vending-path suppression.
    String rawErrorCode = errorCode(body);
    // Capped far below the body's allowance. An error_code is a short token -- the longest
    // Databricks sends is around forty characters -- so this bounds what a legitimate code may
    // carry. Independent of route: a consumer cannot see which route produced a failure, so a field
    // whose size depends on that is a field it cannot reason about.
    String errorCode = rawErrorCode == null ? null : truncate(rawErrorCode, MAX_ERROR_CODE_CHARS);
    // What reaches the exception. Where the body is suppressed the field is too, unless the value
    // is one this client recognizes: errorCode() is a public accessor a caller may log, and it is
    // read from the same body the vending route keeps out of messages. Recognition rather than
    // shape, because shape is not a security property -- [A-Z0-9_] within the cap is also what an
    // AWS access key id looks like.
    String reportedErrorCode =
        includeResponseBody || isRecognizedErrorCode(errorCode) ? errorCode : null;
    // 1. Definitive statuses. No body code makes a 401 anything but unauthenticated, or a 429 or
    // 408 terminal.
    UnityCatalogException.Failure failure =
        switch (status) {
          case 401 -> UnityCatalogException.Failure.UNAUTHENTICATED;
          case 403 -> UnityCatalogException.Failure.PERMISSION_DENIED;
          case 429 -> UnityCatalogException.Failure.RATE_LIMITED;
          // The endpoint will not accept this method or entity whatever the body reports; a
          // REQUEST_LIMIT_EXCEEDED envelope here would make a permanent refusal look retryable.
          case 405, 422 -> UnityCatalogException.Failure.INVALID_REQUEST;
          // Transient by definition: a proxy timeout, and two forms of "retry shortly". 409 is
          // absent because Databricks uses it for both a permanent conflict (ALREADY_EXISTS) and a
          // retryable one, so its body decides.
          case 408, 423, 425 -> UnityCatalogException.Failure.TRANSIENT;
          default -> null;
        };

    // 2. The ambiguous statuses, and only those. A 400 carries its real reason in the envelope, and
    // a 404 may be hiding a denial. Every other status stays authoritative: letting the body decide
    // a 415 or a 407 would take a server-controlled value over a status that already answers.
    if (failure == null && (status == 400 || status == 404 || status == 409)) {
      failure = failureFromErrorCode(errorCode);
    }

    // 3. Status alone for everything left. errorCode still reaches the exception for diagnostics.
    if (failure == null) {
      failure =
          switch (status) {
            case 404 -> UnityCatalogException.Failure.NOT_FOUND;
            // A 409 whose body named no recognized code: conflicts are usually momentary.
            case 409 -> UnityCatalogException.Failure.TRANSIENT;
            default -> {
              if (status >= 500) {
                yield UnityCatalogException.Failure.SERVER_ERROR;
              }
              // A 3xx reached here was declined rather than followed: a base-URI
              // misconfiguration.
              if (status >= 300 && status < 400) {
                yield UnityCatalogException.Failure.INVALID_REQUEST;
              }
              // An unlisted 4xx is permanent only with the workspace's error envelope.
              yield status >= 400 && errorCode != null
                  ? UnityCatalogException.Failure.INVALID_REQUEST
                  : UnityCatalogException.Failure.OTHER;
            }
          };
    }
    return new UnityCatalogException(
        failure,
        status,
        reportedErrorCode,
        // Envelope presence survives the redaction above: whether the workspace gave a reason is a
        // different question from whether its text is safe to show.
        errorCode != null,
        "Unity Catalog returned HTTP "
            + status
            + (reportedErrorCode == null || !includeResponseBody
                ? ""
                : " (" + reportedErrorCode + ")")
            + " for "
            + path
            + (responseBody.isEmpty() ? "" : ": " + responseBody),
        null);
  }

  /** Databricks' {@code error_code}, or null when the body is not its error envelope. */
  private static String errorCode(String body) {
    if (body == null || body.isBlank()) {
      return null;
    }
    try {
      JsonNode parsed = JSON.readTree(body);
      return parsed.isObject() ? text(parsed, "error_code") : null;
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  /**
   * Whether a code is one this client recognizes as Unity Catalog or Databricks vocabulary.
   *
   * <p>What the vending route reports instead of anything code-shaped. Shape is not a security
   * property: {@code [A-Z0-9_]} up to the cap is also what an AWS access key id looks like, so
   * "looks like a code" let 64 characters of a body the route deliberately suppresses through a
   * public accessor. Membership is a property; shape is a guess.
   *
   * <p>The cost is real and one-directional: a legitimate code Databricks adds later is withheld on
   * this route until it is listed here, and the operator reads "(error code withheld)" instead of
   * the reason. {@code hasErrorEnvelope()} still reports that the workspace answered, so nothing
   * about terminality changes -- only how much the message can say.
   */
  private static boolean isRecognizedErrorCode(String errorCode) {
    return errorCode != null
        && RECOGNIZED_ERROR_CODES.contains(errorCode.trim().toUpperCase(Locale.ROOT));
  }

  /**
   * The failure a Databricks {@code error_code} names, or null to fall back to the HTTP status.
   * Only codes whose retry behaviour differs from what the status implies are listed; anything
   * unrecognised falls through rather than being guessed terminal.
   */
  private static UnityCatalogException.Failure failureFromErrorCode(String errorCode) {
    if (errorCode == null) {
      return null;
    }
    return switch (errorCode.trim().toUpperCase(Locale.ROOT)) {
      case "UNAUTHENTICATED" -> UnityCatalogException.Failure.UNAUTHENTICATED;
      case "PERMISSION_DENIED", "CUSTOMER_UNAUTHORIZED" ->
          UnityCatalogException.Failure.PERMISSION_DENIED;
      case "RESOURCE_DOES_NOT_EXIST", "NOT_FOUND" -> UnityCatalogException.Failure.NOT_FOUND;
      // A wrong route, not a missing object: the same request will never succeed here. Separating
      // the two matters most on the metadata path, where getTable turns NOT_FOUND into an empty
      // Optional -- a base URI whose path prefix a proxy does not route would otherwise read as
      // "the catalog has no such table". Same reading as a declined 3xx below.
      case "ENDPOINT_NOT_FOUND" -> UnityCatalogException.Failure.INVALID_REQUEST;
      case "REQUEST_LIMIT_EXCEEDED" -> UnityCatalogException.Failure.RATE_LIMITED;
      // A conflict the same request cannot resolve: the object is already there.
      case "ALREADY_EXISTS", "RESOURCE_ALREADY_EXISTS" ->
          UnityCatalogException.Failure.INVALID_REQUEST;
      // Retryable, but by the caller's operation rather than by resending this request, which is
      // not something TRANSIENT promises.
      case "ABORTED" -> UnityCatalogException.Failure.OTHER;
      default -> null;
    };
  }

  private static UnityCatalogException invalidResponse(String message, Throwable cause) {
    return new UnityCatalogException(
        UnityCatalogException.Failure.INVALID_RESPONSE, -1, message, cause);
  }

  /**
   * An {@code INVALID_RESPONSE} that keeps what the catalog actually sent. The status and a snippet
   * of the body identify which proxy is answering when a body never becomes JSON.
   */
  private static UnityCatalogException invalidResponse(
      int status, String message, String body, Throwable cause) {
    String snippet = truncate(body);
    return new UnityCatalogException(
        UnityCatalogException.Failure.INVALID_RESPONSE,
        status,
        snippet.isEmpty() ? message : message + ": " + snippet,
        cause);
  }

  private static String truncate(String body) {
    return truncate(body, MAX_BODY_SNIPPET_CHARS);
  }

  private static String truncate(String value, int maxChars) {
    return value == null
        ? ""
        : redactBearerTokens(value.substring(0, Math.min(value.length(), maxChars)));
  }

  /**
   * Removes bearer tokens from text on its way into a failure message.
   *
   * <p>The Authorization header goes out on every request, and a debug handler or a misconfigured
   * gateway will echo request headers in an error body. That body is interpolated into the
   * exception for every route except vending, so without this the tenant's token reaches operator
   * logs.
   */
  private static String redactBearerTokens(String text) {
    return BEARER_TOKEN.matcher(text).replaceAll("Bearer <redacted>");
  }

  private static String text(JsonNode node, String field) {
    JsonNode value = node.path(field);
    if (value.isMissingNode() || value.isNull()) {
      return null;
    }
    String text = value.asText();
    return text.isBlank() ? null : text;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String stripTrailingSlash(String value) {
    // Every trailing slash: the client normalizes its own base URI rather than relying on a caller
    // having done it. "https://host//" would otherwise request "https://host//api/2.1/...", which
    // proxies treat inconsistently and often answer 404 -- read here as an absent table.
    String stripped = value;
    while (stripped.endsWith("/")) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }

  /**
   * Whether the authority embeds userinfo that {@link URI#getUserInfo()} does not report. {@code
   * java.net.URI} populates it only for a server-based authority; a host it rejects as a hostname
   * -- an underscore, a non-numeric port -- yields a registry-based authority with the credential
   * still in the raw string. {@code @} delimits userinfo and has no other role there.
   */
  private static boolean authorityCarriesUserInfo(URI baseUri) {
    String authority = baseUri.getRawAuthority();
    return authority != null && authority.indexOf('@') >= 0;
  }

  /**
   * The base URI as it may appear in a rejection message: scheme, authority and path only.
   *
   * <p>A query is a common place for a token, and the userinfo guard covers only credentials in the
   * authority. Every gate here reports the value it rejected, so the display form drops the two
   * components that carry data rather than address the endpoint.
   */
  private static String display(URI baseUri) {
    if (baseUri == null) {
      return "null";
    }
    StringBuilder shown = new StringBuilder();
    if (baseUri.getScheme() != null) {
      shown.append(baseUri.getScheme()).append("://");
    }
    // Parsed components only, never the raw authority. URI reports a host it cannot parse as
    // server-based -- an underscore, a bad port, or a percent-encoded delimiter such as
    // alice:s3cr3t%40host -- by leaving getHost() and both userinfo accessors null while the raw
    // authority keeps the credential. Echoing that here would undo the userinfo guard for exactly
    // the inputs it cannot recognise.
    if (baseUri.getHost() == null) {
      shown.append("<unparseable-authority>");
    } else {
      shown.append(baseUri.getHost());
      if (baseUri.getPort() != -1) {
        shown.append(':').append(baseUri.getPort());
      }
    }
    if (baseUri.getRawPath() != null) {
      shown.append(baseUri.getRawPath());
    }
    return shown.isEmpty() ? "<empty>" : shown.toString();
  }

  private static URI validateBaseUri(URI baseUri) {
    Objects.requireNonNull(baseUri, "baseUri");
    // Before any check whose message interpolates the URI. Credentials belong in
    // UnityCatalogAuthentication, and the JDK client does not transmit them.
    if (baseUri.getUserInfo() != null
        || baseUri.getRawUserInfo() != null
        || authorityCarriesUserInfo(baseUri)) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must not contain userinfo; supply credentials through "
              + "UnityCatalogAuthentication instead");
    }
    if (!baseUri.isAbsolute()) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must be absolute: " + display(baseUri));
    }
    String scheme = baseUri.getScheme();
    boolean https = "https".equalsIgnoreCase(scheme);
    boolean loopbackHttp =
        "http".equalsIgnoreCase(scheme)
            && allowLoopbackCleartext()
            && isLoopbackHost(baseUri.getHost());
    if (!https && !loopbackHttp) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must use HTTPS, except HTTP is allowed for loopback hosts when "
              + ALLOW_LOOPBACK_PROPERTY
              + " is set: "
              + display(baseUri));
    }
    if (baseUri.getHost() == null) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must include a host: " + display(baseUri));
    }
    if (baseUri.getRawQuery() != null || baseUri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must not include a query or fragment: " + display(baseUri));
    }
    // -1 is absent. URI and HttpRequest.Builder both accept 65536; InetSocketAddress rejects it at
    // send time, where it is reported as TRANSPORT.
    int port = baseUri.getPort();
    if (port != -1 && (port < 1 || port > 65535)) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI port must be between 1 and 65535: " + display(baseUri));
    }
    assertAddressClassAllowed(baseUri);
    return baseUri;
  }

  /**
   * Rejects address classes a tenant-supplied connector URI must not name.
   *
   * <p>Link-local, wildcard, multicast, broadcast and {@code 0.0.0.0/8} literals are refused
   * outright; no catalog is reachable at one, and {@code https://169.254.169.254} is otherwise a
   * well-formed HTTPS URI for a cloud metadata service. Site-local literals require {@link
   * #ALLOW_PRIVATE_PROPERTY}, since an internal catalog is an ordinary deployment. Loopback is
   * allowed; cleartext to it is governed by {@link #ALLOW_LOOPBACK_PROPERTY}.
   *
   * <p>Literals only: resolving a hostname here would disagree with the resolution {@code
   * HttpClient} performs at connect time, so a hostname is out of scope.
   */
  private static void assertAddressClassAllowed(URI baseUri) {
    String host = unbracket(baseUri.getHost());
    // A zone id names a local interface, and ofLiteral cannot parse one. Left to the catch below a
    // scoped literal reads as a hostname and skips this gate entirely, so fe80::1%eth0 admits a
    // link-local address. The transport cannot carry one either.
    if (host.indexOf('%') >= 0) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must not name a zone-scoped address: " + display(baseUri));
    }
    InetAddress address;
    try {
      address = InetAddress.ofLiteral(host);
    } catch (IllegalArgumentException notALiteral) {
      // A host of only digits and dots is not a hostname -- a DNS name cannot be entirely numeric
      // -- and the resolver the transport uses accepts these modulo 2^32 even though ofLiteral
      // refuses them: 7147006462 resolves to 169.254.169.254 and 4294967296 to 0.0.0.0. Returning
      // here would hand the transport a link-local or wildcard target this gate refuses by name.
      if (isNumericHost(host)) {
        throw new IllegalArgumentException(
            "Unity Catalog base URI must not name a numeric host that is not an address literal: "
                + display(baseUri));
      }
      return;
    }
    if (address.isLoopbackAddress()) {
      return;
    }
    if (address.isLinkLocalAddress()
        || address.isAnyLocalAddress()
        || address.isMulticastAddress()
        || isBroadcastOrThisNetwork(address)) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI must not name a link-local, wildcard or multicast address: "
              + display(baseUri));
    }
    if (isSiteLocal(address) && !allowPrivateAddresses()) {
      throw new IllegalArgumentException(
          "Unity Catalog base URI names a private address, which requires "
              + ALLOW_PRIVATE_PROPERTY
              + ": "
              + display(baseUri));
    }
  }

  /** Whether every character is an ASCII digit or a dot. Deliberately not {@code isDigit}. */
  private static boolean isNumericHost(String host) {
    if (host.isEmpty()) {
      return false;
    }
    for (int i = 0; i < host.length(); i++) {
      char c = host.charAt(i);
      if ((c < '0' || c > '9') && c != '.') {
        return false;
      }
    }
    return true;
  }

  /**
   * IPv4 limited broadcast and {@code 0.0.0.0/8}. {@code isMulticastAddress} covers 224/4 and
   * {@code isAnyLocalAddress} only the exact wildcard, so neither address is caught by them.
   */
  private static boolean isBroadcastOrThisNetwork(InetAddress address) {
    byte[] bytes = address.getAddress();
    if (bytes.length != 4) {
      return false;
    }
    boolean broadcast = true;
    for (byte octet : bytes) {
      broadcast &= octet == (byte) 0xFF;
    }
    return broadcast || bytes[0] == 0;
  }

  /** IPv4 RFC 1918 and IPv6 unique-local, which {@code isSiteLocalAddress} misses for fc00::/7. */
  private static boolean isSiteLocal(InetAddress address) {
    if (address.isSiteLocalAddress()) {
      return true;
    }
    byte[] bytes = address.getAddress();
    return bytes.length == 16 && (bytes[0] & 0xFE) == 0xFC;
  }

  private static int configuredMaxPages() {
    return positiveIntProperty(MAX_PAGES_PROPERTY, DEFAULT_MAX_PAGES);
  }

  private static int configuredMaxResponseBytes() {
    int value = positiveIntProperty(MAX_RESPONSE_BYTES_PROPERTY, DEFAULT_MAX_RESPONSE_BYTES);
    if (value == Integer.MAX_VALUE) {
      // readBounded reads the cap plus one byte to spot an overrun.
      throw new IllegalArgumentException(
          MAX_RESPONSE_BYTES_PROPERTY + " must be a positive integer below " + Integer.MAX_VALUE);
    }
    return value;
  }

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

  private static boolean allowPrivateAddresses() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_PRIVATE_PROPERTY, System.getenv().getOrDefault(ALLOW_PRIVATE_ENV, "false")));
  }

  private static String unbracket(String host) {
    String value = host == null ? "" : host.trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      value = value.substring(1, value.length() - 1);
    }
    return value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
  }

  private static HttpClient newHttpClient(Duration connectTimeout) {
    return HttpClient.newBuilder()
        .connectTimeout(requirePositive(connectTimeout, "connectTimeout"))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
  }

  /**
   * Rejects a credentials path this client could not turn into a request. Called from the
   * delegating constructor's argument list, ahead of {@link #newHttpClient}.
   *
   * <p>An absolute path is not sufficient: {@link #send} concatenates it onto the base URI, and a
   * value that is a legal string but not a legal URI -- a space, a malformed escape, a bracket --
   * throws from {@code URI.create} inside the catch that reports {@code TRANSPORT}.
   */
  private static String requireAbsolutePath(String credentialsPath) {
    Objects.requireNonNull(credentialsPath, "credentialsPath");
    boolean usable =
        credentialsPath.startsWith("/")
            && credentialsPath.indexOf('?') < 0
            && credentialsPath.indexOf('#') < 0;
    if (usable) {
      try {
        URI.create("https://placeholder.invalid" + credentialsPath);
      } catch (IllegalArgumentException notUsableInAUri) {
        usable = false;
      }
    }
    if (!usable) {
      throw new IllegalArgumentException(
          "Unity Catalog credentials path must be an absolute path usable in a URI, without a "
              + "query or fragment: "
              + credentialsPath);
    }
    return credentialsPath;
  }

  /**
   * Rejects a non-positive timeout at construction. {@link HttpRequest.Builder#timeout} refuses one
   * too, but not until a request is built -- inside {@link #send}, where it is reported as {@code
   * TRANSPORT} on every call rather than as the configuration error it is.
   */
  private static Duration requirePositive(Duration value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isZero() || value.isNegative()) {
      throw new IllegalArgumentException(field + " must be positive: " + value);
    }
    return value;
  }

  private static boolean allowLoopbackCleartext() {
    return Boolean.parseBoolean(
        System.getProperty(
            ALLOW_LOOPBACK_PROPERTY, System.getenv().getOrDefault(ALLOW_LOOPBACK_ENV, "false")));
  }

  private static boolean isLoopbackHost(String host) {
    if (host == null) {
      return false;
    }
    String normalized = host.toLowerCase(Locale.ROOT);
    if (normalized.startsWith("[") && normalized.endsWith("]")) {
      normalized = normalized.substring(1, normalized.length() - 1);
    }
    if (normalized.endsWith(".")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    // Exactly "localhost", not any *.localhost name. RFC 6761 says such names should resolve to
    // loopback, but nothing here enforces that and this gate does not resolve: a zone the tenant
    // controls can point catalog.localhost at a public address, and the Authorization header would
    // then go out in cleartext to it. CredentialResolverSupport, which this mirrors, has no suffix
    // rule either -- it resolves and requires every answer to be loopback.
    if (normalized.equals("localhost")) {
      return true;
    }
    // ofLiteral, never getByName: a host this cannot decide is denied, not resolved. A resolver
    // here disagrees with the one HttpClient uses at connect time. Character-shape pre-filters are
    // no substitute: "4294967296" and "1." are all digits and dots yet parse as no address.
    try {
      return InetAddress.ofLiteral(normalized).isLoopbackAddress();
    } catch (IllegalArgumentException notAnAddressLiteral) {
      return false;
    }
  }
}
