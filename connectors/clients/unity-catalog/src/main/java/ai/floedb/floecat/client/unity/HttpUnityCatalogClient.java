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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
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
        HttpClient.newBuilder().connectTimeout(connectTimeout).build());
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
        aws, unsupported, expiration(response.path("expiration_time")), text(response, "url"));
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
    if (status < 200 || status >= 300) {
      throw httpFailure(status, path, response.body());
    }
    try {
      return JSON.readTree(response.body());
    } catch (JsonProcessingException e) {
      throw invalidResponse("Invalid JSON from Unity Catalog for " + path, e);
    }
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

  private static Instant expiration(JsonNode node) {
    String raw = node.isMissingNode() || node.isNull() ? null : node.asText();
    if (raw == null || raw.isBlank()) {
      return null;
    }
    try {
      long epochMillis = Long.parseLong(raw.trim());
      return epochMillis > 0 ? Instant.ofEpochMilli(epochMillis) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static UnityCatalogException httpFailure(int status, String path, String body) {
    UnityCatalogException.Failure failure =
        switch (status) {
          case 401 -> UnityCatalogException.Failure.UNAUTHENTICATED;
          case 403 -> UnityCatalogException.Failure.PERMISSION_DENIED;
          case 404 -> UnityCatalogException.Failure.NOT_FOUND;
          case 429 -> UnityCatalogException.Failure.RATE_LIMITED;
          default ->
              status >= 500
                  ? UnityCatalogException.Failure.SERVER_ERROR
                  : UnityCatalogException.Failure.OTHER;
        };
    String responseBody = body == null ? "" : body.substring(0, Math.min(body.length(), 2_000));
    return new UnityCatalogException(
        failure,
        status,
        "Unity Catalog returned HTTP " + status + " for " + path + ": " + responseBody);
  }

  private static UnityCatalogException invalidResponse(String message, Throwable cause) {
    return new UnityCatalogException(
        UnityCatalogException.Failure.INVALID_RESPONSE, -1, message, cause);
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
