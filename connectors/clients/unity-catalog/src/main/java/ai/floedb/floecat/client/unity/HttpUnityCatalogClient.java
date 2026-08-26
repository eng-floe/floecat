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

/** JDK HTTP implementation of the small Unity Catalog client boundary. */
public final class HttpUnityCatalogClient implements UnityCatalogClient {
  private static final String API_ROOT = "/api/2.1/unity-catalog";
  private static final String TEMPORARY_TABLE_CREDENTIALS_PATH =
      "/api/2.0/unity-catalog/temporary-table-credentials";
  private static final ObjectMapper JSON = new ObjectMapper();

  private final String baseUri;
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
        requestTimeout,
        authentication,
        // The JDK default is Redirect.NEVER, which turns an ordinary workspace redirect -- an
        // http:// base URI upgraded to HTTPS, a renamed workspace host -- into a bare 3xx that no
        // caller can act on. NORMAL follows it, and still refuses an HTTPS-to-HTTP downgrade; a
        // 3xx that survives that is classified permanent below rather than retried forever.
        //
        // NORMAL re-issues a followed 301/302/303 as a bodyless GET, so the one POST here -- the
        // credentials call -- does not survive a redirect: what surfaces is the redirect target's
        // 404 or 405, not the 3xx. Both are permanent, so retry behaviour is unchanged, but the
        // status alone would read as a missing endpoint; the failure message therefore names the
        // effective URI whenever it differs from the one requested.
        HttpClient.newBuilder()
            .connectTimeout(connectTimeout)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build());
  }

  HttpUnityCatalogClient(
      URI baseUri,
      Duration requestTimeout,
      UnityCatalogAuthentication authentication,
      HttpClient httpClient) {
    Objects.requireNonNull(baseUri, "baseUri");
    if (!baseUri.isAbsolute()) {
      throw new IllegalArgumentException("Unity Catalog base URI must be absolute: " + baseUri);
    }
    this.baseUri = stripTrailingSlash(baseUri.toString());
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    this.authentication = Objects.requireNonNull(authentication, "authentication");
    this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
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
        HttpUnityCatalogClient::table);
  }

  @Override
  public Optional<UnityCatalogTable> getTable(String fullName) {
    try {
      return Optional.of(table(get(API_ROOT + "/tables/" + encode(fullName))));
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
    JsonNode response = post(TEMPORARY_TABLE_CREDENTIALS_PATH, body.toString());
    JsonNode awsNode = response.path("aws_temp_credentials");
    TemporaryTableCredentials.AwsCredentials aws =
        awsNode.isObject()
            ? new TemporaryTableCredentials.AwsCredentials(
                text(awsNode, "access_key_id"),
                text(awsNode, "secret_access_key"),
                text(awsNode, "session_token"),
                text(awsNode, "access_point"))
            : null;
    boolean unsupported =
        response.hasNonNull("azure_user_delegation_sas")
            || response.hasNonNull("azure_aad")
            || response.hasNonNull("gcp_oauth_token")
            || response.hasNonNull("r2_temp_credentials");
    return new TemporaryTableCredentials(
        aws, unsupported, text(response, "expiration_time"), text(response, "url"));
  }

  @Override
  public void close() {
    httpClient.close();
  }

  private <T> List<T> listAll(String path, String arrayField, Function<JsonNode, T> mapper) {
    List<T> result = new ArrayList<>();
    Set<String> tokens = new HashSet<>();
    String pageToken = null;
    do {
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
    builder
        .uri(URI.create(baseUri + path))
        .timeout(requestTimeout)
        .header("Accept", "application/json");
    Map<String, String> headers = authentication.headers();
    if (headers != null) {
      headers.forEach(builder::header);
    }

    HttpResponse<String> response;
    try {
      response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UnityCatalogException(
          UnityCatalogException.Failure.TRANSPORT, -1, "Unity Catalog request interrupted", e);
    } catch (IOException | RuntimeException e) {
      throw new UnityCatalogException(
          UnityCatalogException.Failure.TRANSPORT,
          -1,
          "Unity Catalog request failed for " + path,
          e);
    }

    int status = response.statusCode();
    String target = describeTarget(path, response.uri());
    if (status < 200 || status >= 300) {
      throw httpFailure(status, target, response.body());
    }
    try {
      return JSON.readTree(response.body());
    } catch (JsonProcessingException e) {
      throw invalidResponse(
          status, "Invalid JSON from Unity Catalog for " + target, response.body(), e);
    }
  }

  /**
   * The request target for a failure message: the path, plus where a followed redirect landed.
   *
   * <p>With {@link HttpClient.Redirect#NORMAL} the response can come from somewhere other than the
   * URI built here, and a 404 from a redirect target reads as a missing endpoint unless the message
   * says which URI actually answered.
   */
  private String describeTarget(String path, URI effectiveUri) {
    String requested = baseUri + path;
    if (effectiveUri == null || requested.equals(effectiveUri.toString())) {
      return path;
    }
    return path + " (redirected to " + effectiveUri + ")";
  }

  private static UnityCatalogTable table(JsonNode node) {
    List<UnityCatalogTable.Column> columns = new ArrayList<>();
    for (JsonNode column : node.path("columns")) {
      columns.add(
          new UnityCatalogTable.Column(
              text(column, "name"),
              text(column, "type_name"),
              text(column, "type_text"),
              text(column, "type_json"),
              column.path("nullable").asBoolean(true)));
    }
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
   * <p>The HTTP status alone is not enough on Databricks: a workspace without {@code EXTERNAL USE
   * SCHEMA}, or a table with external access turned off, answers the credentials endpoint with
   * {@code 400} and puts the real reason in the body's {@code error_code}. Read on status alone
   * that is an unclassified {@code OTHER}, which the connector leaves retryable -- so the reconcile
   * job retries a refusal that will never change. The body's code is consulted first, and any
   * remaining 4xx that <em>came from Databricks</em> -- or a 3xx the client declined to follow --
   * becomes {@link UnityCatalogException.Failure#INVALID_REQUEST} rather than {@code OTHER} so the
   * connector can treat it as the permanent condition it is.
   *
   * <p>"From Databricks" means the body carries its {@code error_code} envelope. A 4xx without one
   * need not be the workspace's answer at all -- an HTML {@code 400} or {@code 404} from a load
   * balancer or WAF in front of it is transient -- and permanently failing the job on it would be
   * exactly the over-eager terminal {@link #failureFromErrorCode} refuses to guess at. Those stay
   * {@code OTHER}, retryable. The 4xx statuses that are never permanent are still named explicitly
   * first, so an envelope-bearing 408 or 429 is not swept into the default either.
   */
  private static UnityCatalogException httpFailure(int status, String path, String body) {
    String responseBody = truncate(body);
    String errorCode = errorCode(body);
    // Only for 4xx. A 5xx is transient whatever its body says, and letting an error_code override
    // it would turn a retryable outage into a terminal refusal.
    UnityCatalogException.Failure failure = status >= 500 ? null : failureFromErrorCode(errorCode);
    if (failure == null) {
      failure =
          switch (status) {
            case 401 -> UnityCatalogException.Failure.UNAUTHENTICATED;
            case 403 -> UnityCatalogException.Failure.PERMISSION_DENIED;
            case 404 -> UnityCatalogException.Failure.NOT_FOUND;
            case 429 -> UnityCatalogException.Failure.RATE_LIMITED;
            // 4xx codes that are transient by definition, listed so they do not fall into the
            // permanent default below: 408 is what a load balancer or proxy in front of a
            // Databricks workspace emits when the upstream is slow, and 409/423/425 all mean
            // "the same request will work again shortly". OTHER, like an unrecognised
            // error_code, leaves the connector's retry behaviour unchanged.
            case 408, 409, 423, 425 -> UnityCatalogException.Failure.OTHER;
            default -> {
              if (status >= 500) {
                yield UnityCatalogException.Failure.SERVER_ERROR;
              }
              // A 3xx here is one the client declined to follow -- an HTTPS-to-HTTP downgrade, or
              // a redirect loop past the JDK's limit. That is a misconfigured base URI, which no
              // amount of retrying fixes, so it is permanent whatever the body looks like.
              if (status >= 300 && status < 400) {
                yield UnityCatalogException.Failure.INVALID_REQUEST;
              }
              // An unlisted 4xx is permanent only when Databricks answered it: its error envelope
              // is what distinguishes "this workspace refuses this request" from an error page
              // served by something in front of the workspace, which will recover.
              yield status >= 400 && errorCode != null
                  ? UnityCatalogException.Failure.INVALID_REQUEST
                  : UnityCatalogException.Failure.OTHER;
            }
          };
    }
    return new UnityCatalogException(
        failure,
        status,
        errorCode,
        "Unity Catalog returned HTTP " + status + " for " + path + ": " + responseBody,
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
   * The failure a Databricks {@code error_code} names, or null to fall back to the HTTP status.
   *
   * <p>Only codes whose retry behaviour differs from what the status implies are listed. Anything
   * unrecognised deliberately falls through: guessing terminal from an unknown code would stop the
   * reconciler retrying something that would have recovered.
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
      case "REQUEST_LIMIT_EXCEEDED" -> UnityCatalogException.Failure.RATE_LIMITED;
      default -> null;
    };
  }

  private static UnityCatalogException invalidResponse(String message, Throwable cause) {
    return new UnityCatalogException(
        UnityCatalogException.Failure.INVALID_RESPONSE, -1, message, cause);
  }

  /**
   * An {@code INVALID_RESPONSE} that keeps what the catalog actually sent.
   *
   * <p>{@code INVALID_RESPONSE} stays retryable, so a body that never becomes JSON -- usually a
   * proxy error page rather than the catalog -- shows up as a looping vend. The status and a
   * snippet of the body are the two things needed to tell which proxy is answering, and both are in
   * hand at the call site.
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
    return body == null ? "" : body.substring(0, Math.min(body.length(), 2_000));
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
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }
}
