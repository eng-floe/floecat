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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.delta.kernel.engine.Engine;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;
import org.jboss.logging.Logger;

public final class UnityDeltaConnector extends DeltaConnector {
  private static final Logger LOG = Logger.getLogger(UnityDeltaConnector.class);

  private final UcHttp ucHttp;
  private final SqlStmtClient sql;

  UnityDeltaConnector(
      String connectorId,
      UcHttp ucHttp,
      SqlStmtClient sql,
      Engine engine,
      Function<String, InputFile> parquetInput,
      boolean ndvEnabled,
      double ndvSampleFraction,
      long ndvMaxFiles) {
    super(connectorId, engine, parquetInput, ndvEnabled, ndvSampleFraction, ndvMaxFiles);
    this.ucHttp = ucHttp;
    this.sql = sql;
  }

  @Override
  public List<String> listNamespaces() {
    try {
      List<String> out = new ArrayList<>();
      for (var c : ucGetAll("/api/2.1/unity-catalog/catalogs", "catalogs")) {
        String catalogName = c.path("name").asText();
        for (var s :
            ucGetAll(
                "/api/2.1/unity-catalog/schemas?catalog_name=" + UcBaseSupport.url(catalogName),
                "schemas")) {
          out.add(catalogName + "." + s.path("name").asText());
        }
      }
      out.sort(String::compareTo);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("listNamespaces failed", e);
    }
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    try {
      var tables = listTablesNode(namespaceFq);
      List<String> out = new ArrayList<>();
      for (var t : tables) {
        String fmt = t.path("data_source_format").asText("");
        if ("DELTA".equalsIgnoreCase(fmt)) {
          out.add(t.path("name").asText());
        }
      }
      out.sort(String::compareTo);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("listTables failed", e);
    }
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    try {
      String full = namespaceFq + "." + tableName;
      var response = ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full));
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new RuntimeException("UC returned HTTP " + response.statusCode() + " for " + full);
      }
      var meta = M.readTree(response.body());

      Map<String, String> props = new LinkedHashMap<>();
      putIfPresent(props, meta, "table_type");
      putIfPresent(props, meta, "data_source_format");
      putIfPresent(props, meta, "storage_location");

      String location = meta.path("storage_location").asText(null);
      String schemaJson = buildSchemaJson(meta);
      if (location != null && !location.isBlank()) {
        try {
          schemaJson = describeTableSchemaJson(location);
        } catch (Exception ignored) {
          // Fall back to UC column metadata when Delta snapshot metadata is unavailable.
        }
      }
      return new TableDescriptor(
          namespaceFq,
          tableName,
          location,
          schemaJson,
          List.of(),
          ColumnIdAlgorithm.CID_PATH_ORDINAL,
          props);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("describe failed: " + e.getMessage(), e);
    }
  }

  @Override
  protected String storageLocation(String namespaceFq, String tableName) {
    return loadStorageLocation(namespaceFq + "." + tableName);
  }

  @Override
  protected Map<String, String> fallbackTablePropertiesForConstraints(
      String namespaceFq, String tableName) {
    try {
      String full = namespaceFq + "." + tableName;
      var response = ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full));
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        return Map.of();
      }
      JsonNode meta = M.readTree(response.body());
      return extractConstraintProperties(meta);
    } catch (Exception ignored) {
      return Map.of();
    }
  }

  @Override
  public List<String> listViews(String namespaceFq) {
    try {
      var tables = listTablesNode(namespaceFq);
      List<String> out = new ArrayList<>();
      for (var t : tables) {
        if ("VIEW".equalsIgnoreCase(t.path("table_type").asText(""))) {
          out.add(t.path("name").asText());
        }
      }
      out.sort(String::compareTo);
      return out;
    } catch (Exception e) {
      throw new RuntimeException("listViews failed", e);
    }
  }

  /**
   * Overrides the default one-call-per-view implementation. UC's list-tables response already
   * contains {@code view_definition} and {@code columns} for VIEW entries, so this method builds
   * full descriptors in a single HTTP call instead of N additional describe calls.
   */
  @Override
  public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String namespaceFq) {
    try {
      var tables = listTablesNode(namespaceFq);
      List<FloecatConnector.ViewDescriptor> out = new ArrayList<>();
      // creation_search_path is the schema portion only — the catalog is handled separately via
      // NameRef.catalog / default-catalog enrichment in QueryInputResolver.  Including the catalog
      // here would cause enrichForViewContext to prepend it a second time, resolving unqualified
      // names as catalog.catalog.schema.table.
      String[] nsParts = namespaceFq.split("\\.", 2);
      List<String> searchPath = nsParts.length > 1 ? List.of(nsParts[1].split("\\.")) : List.of();
      for (var t : tables) {
        if (!"VIEW".equalsIgnoreCase(t.path("table_type").asText(""))) {
          continue;
        }
        String viewName = t.path("name").asText();
        String sql = t.path("view_definition").asText("");
        out.add(
            new FloecatConnector.ViewDescriptor(
                namespaceFq, viewName, sql, "spark", searchPath, buildSchemaJson(t)));
      }
      out.sort((a, b) -> a.name().compareTo(b.name()));
      return out;
    } catch (Exception e) {
      // Include the namespace and underlying cause; a bare "listViewDescriptors
      // failed" hides the real error (e.g. a UC API auth/HTTP failure) and makes
      // connector-planning failures undiagnosable.
      throw new RuntimeException(
          "listViewDescriptors failed for namespace=" + namespaceFq + ": " + e, e);
    }
  }

  @Override
  public Optional<FloecatConnector.ViewDescriptor> describeView(
      String namespaceFq, String viewName) {
    try {
      String full = namespaceFq + "." + viewName;
      var response = ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full));
      if (response.statusCode() == 404) {
        return Optional.empty();
      }
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new RuntimeException(
            "UC API returned HTTP " + response.statusCode() + " for " + full);
      }
      var meta = M.readTree(response.body());
      String sql = meta.path("view_definition").asText("");
      // creation_search_path is the schema portion only (same reasoning as listViewDescriptors).
      String[] nsParts = namespaceFq.split("\\.", 2);
      List<String> searchPath = nsParts.length > 1 ? List.of(nsParts[1].split("\\.")) : List.of();
      return Optional.of(
          new FloecatConnector.ViewDescriptor(
              namespaceFq, viewName, sql, "spark", searchPath, buildSchemaJson(meta)));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("describeView failed", e);
    }
  }

  /**
   * Builds a schema JSON string (compatible with DeltaSchemaMapper) from a UC table/view JSON node
   * that contains a {@code columns} array.
   *
   * <p>Note: unlike {@link #describe}, this method intentionally omits the {@code comment} field
   * from each column's {@code metadata} block. UC exposes column comments on table entries but not
   * on VIEW entries (the {@code columns} array in a view response has no {@code comment} field).
   * Adding an empty or missing {@code comment} to view schema JSON would be noise.
   *
   * <p>Column types prefer UC's {@code type_json} (the JSON-serialized Delta StructField, which
   * keeps nested array/map/struct structure intact). {@code type_text} is only a display string —
   * for complex columns it reads {@code array<string>}, which no Delta schema parser accepts — so
   * it is used only as a fallback for entries without a usable {@code type_json}.
   */
  private String buildSchemaJson(JsonNode meta) {
    var fields = M.createArrayNode();
    for (var c : meta.path("columns")) {
      var n = M.createObjectNode();
      n.put("name", c.path("name").asText());
      JsonNode type = typeFromTypeJson(c);
      if (type != null) {
        n.set("type", type);
      } else {
        n.put("type", c.path("type_text").asText(c.path("type_name").asText()));
      }
      n.put("nullable", c.path("nullable").asBoolean(true));
      fields.add(n);
    }
    var schemaNode = M.createObjectNode();
    schemaNode.put("type", "struct");
    schemaNode.set("fields", fields);
    return schemaNode.toString();
  }

  /**
   * Extracts the Delta type node from a UC column's {@code type_json} (a JSON string holding the
   * serialized StructField, e.g. {@code {"name":"c","type":{"type":"array",...},"nullable":true}}).
   * Returns null when absent or unparseable so the caller can fall back to {@code type_text}.
   */
  private JsonNode typeFromTypeJson(JsonNode column) {
    String typeJson = column.path("type_json").asText("");
    if (typeJson.isEmpty()) {
      return null;
    }
    try {
      JsonNode structField = M.readTree(typeJson);
      JsonNode type = structField.get("type");
      return type == null || type.isNull() ? null : type;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Fetches all entries from the UC tables endpoint for the given {@code "catalog.schema"}
   * namespace, following {@code next_page_token} pagination until exhausted. Returns an empty array
   * node if the namespace contains no dot separator.
   */
  private JsonNode listTablesNode(String namespaceFq) throws Exception {
    int dot = namespaceFq.indexOf('.');
    if (dot < 0) {
      return M.createArrayNode();
    }
    String catalog = namespaceFq.substring(0, dot);
    String schema = namespaceFq.substring(dot + 1);
    return ucGetAll(
        "/api/2.1/unity-catalog/tables?catalog_name="
            + UcBaseSupport.url(catalog)
            + "&schema_name="
            + UcBaseSupport.url(schema),
        "tables");
  }

  /**
   * Fetches all items from a paginated UC REST endpoint, following {@code next_page_token} links
   * until exhausted. Accumulates the {@code arrayField} array from each page into a single {@link
   * ArrayNode}.
   *
   * @param baseUrl the endpoint URL (path + query, without a {@code page_token} param)
   * @param arrayField the JSON key that holds the array of items on each page
   */
  private ArrayNode ucGetAll(String baseUrl, String arrayField) throws Exception {
    ArrayNode all = M.createArrayNode();
    String pageToken = null;
    do {
      String url =
          pageToken == null
              ? baseUrl
              : baseUrl
                  + (baseUrl.contains("?") ? "&" : "?")
                  + "page_token="
                  + UcBaseSupport.url(pageToken);
      var resp = ucHttp.get(url);
      if (resp.statusCode() / 100 != 2) {
        throw new RuntimeException(
            "UC list returned HTTP " + resp.statusCode() + " for " + url + ": " + resp.body());
      }
      JsonNode page = M.readTree(resp.body());
      page.path(arrayField).forEach(all::add);
      String next = page.path("next_page_token").asText(null);
      pageToken = (next == null || next.isBlank()) ? null : next;
    } while (pageToken != null);
    return all;
  }

  private static void putIfPresent(Map<String, String> props, JsonNode n, String field) {
    if (!n.path(field).isMissingNode()) {
      props.put(field, n.path(field).asText());
    }
  }

  static Map<String, String> extractConstraintProperties(JsonNode tableMeta) {
    if (tableMeta == null || tableMeta.isMissingNode()) {
      return Map.of();
    }
    Map<String, String> out = new LinkedHashMap<>();
    // UC exposes table properties in two shapes depending on the API version and table type:
    // either a JSON object {"key": "value", ...} or a JSON array [{"key": ..., "value": ...}].
    // Each parser handles one shape and no-ops on the other, so both are applied to each field.
    collectStringMap(out, tableMeta.path("properties"));
    collectStringMap(out, tableMeta.path("table_properties"));
    collectKeyValueArray(out, tableMeta.path("properties"));
    collectKeyValueArray(out, tableMeta.path("table_properties"));
    return Map.copyOf(out);
  }

  private static void collectStringMap(Map<String, String> out, JsonNode node) {
    if (node == null || !node.isObject()) {
      return;
    }
    node.fields()
        .forEachRemaining(
            entry -> {
              JsonNode value = entry.getValue();
              if (value != null && value.isTextual() && !value.asText().isBlank()) {
                out.put(entry.getKey(), value.asText());
              }
            });
  }

  private static void collectKeyValueArray(Map<String, String> out, JsonNode node) {
    if (node == null || !node.isArray()) {
      return;
    }
    for (JsonNode item : node) {
      if (!item.isObject()) {
        continue;
      }
      String key = item.path("key").asText(item.path("name").asText(""));
      String value = item.path("value").asText("");
      if (!key.isBlank() && !value.isBlank()) {
        out.put(key, value);
      }
    }
  }

  private String loadStorageLocation(String full) {
    try {
      var response = ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full));
      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw new RuntimeException("UC returned HTTP " + response.statusCode() + " for " + full);
      }
      var meta = M.readTree(response.body());
      String loc = meta.path("storage_location").asText(null);
      if (loc == null || loc.isBlank()) {
        throw new IllegalStateException("Table has no storage_location: " + full);
      }
      return loc;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve storage_location for " + full, e);
    }
  }

  /**
   * Storage credentials vended by Unity Catalog for a single table, via the temporary-table-
   * credentials API.
   *
   * <p>This is the Unity analog of Iceberg REST access delegation: given the table's id, UC returns
   * short-lived cloud credentials scoped to that table's storage location, removing the need for a
   * separately configured storage authority. It requires the {@code EXTERNAL USE SCHEMA} privilege
   * on the schema; a workspace that supports vending but has not granted it fails the underlying
   * call with 403, and that failure propagates rather than being flattened into an empty result.
   *
   * <p>Scoped to AWS. On an Azure- or GCP-backed workspace UC returns a SAS token or an OAuth token
   * rather than an access-key tuple; those shapes are not yet mapped, so this returns empty ("use a
   * storage authority") instead of a partial credential the reader cannot use.
   */
  @Override
  public Optional<FloecatConnector.VendedStorageCredentials> vendStorageCredentials(
      String namespaceFq, String tableName) {
    String full = namespaceFq + "." + tableName;
    String tableId = resolveTableId(full);
    if (tableId == null) {
      return Optional.empty();
    }
    JsonNode response = requestTemporaryTableCredentials(tableId, full);

    JsonNode aws = response.path("aws_temp_credentials");
    if (aws.isMissingNode() || aws.isNull()) {
      LOG.warnf(
          "Unity Catalog vended non-AWS credentials for %s; only AWS is supported, "
              + "falling back to a storage authority",
          full);
      return Optional.empty();
    }

    String accessKeyId = text(aws, "access_key_id");
    String secretAccessKey = text(aws, "secret_access_key");
    String sessionToken = text(aws, "session_token");
    if (accessKeyId == null || secretAccessKey == null || sessionToken == null) {
      // A partial tuple is unusable: the reader needs the whole set, and the reconcile path
      // additionally needs the session token to register a refresh. Fall back rather than hand out
      // credentials that fail partway through a read.
      LOG.warnf("Unity Catalog AWS credentials for %s are incomplete; falling back", full);
      return Optional.empty();
    }

    Map<String, String> props = new LinkedHashMap<>();
    props.put("s3.access-key-id", accessKeyId);
    props.put("s3.secret-access-key", secretAccessKey);
    props.put("s3.session-token", sessionToken);

    Instant expiresAt =
        FloecatConnector.VendedStorageCredentials.expiryFromEpochMillis(
            response.path("expiration_time").asText(null));
    return Optional.of(new FloecatConnector.VendedStorageCredentials(props, expiresAt));
  }

  /** Resolves the UC table id (a uuid) that the temporary-table-credentials call is keyed on. */
  private String resolveTableId(String full) {
    try {
      var response = ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full));
      int status = response.statusCode();
      if (status == 404) {
        // The table does not exist (dropped/renamed since it was resolved). "Unavailable" per the
        // vend contract -- fall back to a storage authority rather than fail the read.
        LOG.warnf("Unity Catalog table %s not found; cannot vend credentials", full);
        return null;
      }
      if (status / 100 != 2) {
        throw ucHttpFailure(status, "tables lookup for " + full, response.body());
      }
      String tableId = M.readTree(response.body()).path("table_id").asText(null);
      if (tableId == null || tableId.isBlank()) {
        LOG.warnf("Unity Catalog table %s has no table_id; cannot vend credentials", full);
        return null;
      }
      return tableId;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to resolve table_id for " + full, e);
    }
  }

  /**
   * Requests read-scoped credentials from the UC temporary-table-credentials endpoint. A 401/403 is
   * raised as a {@link SourceCatalogAccessException} so the storage service classifies it
   * terminally -- the 403 a workspace returns when {@code EXTERNAL USE SCHEMA} is missing is a
   * permanent configuration failure, and a generic error would be retried forever.
   */
  private JsonNode requestTemporaryTableCredentials(String tableId, String full) {
    try {
      String body =
          M.createObjectNode().put("table_id", tableId).put("operation", "READ").toString();
      var response = ucHttp.post("/api/2.1/unity-catalog/temporary-table-credentials", body);
      if (response.statusCode() / 100 != 2) {
        throw ucHttpFailure(
            response.statusCode(), "temporary-table-credentials for " + full, response.body());
      }
      return M.readTree(response.body());
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to vend credentials for " + full, e);
    }
  }

  /**
   * Maps a non-2xx Unity Catalog response to a throwable the storage service can classify. A
   * 401/403 is a permanent authentication/authorization refusal and becomes a {@link
   * SourceCatalogAccessException} (terminal); anything else stays a plain {@link RuntimeException}
   * so the reconciler keeps treating it as transient and retryable.
   */
  private static RuntimeException ucHttpFailure(int status, String context, String body) {
    String message = "UC " + context + " returned HTTP " + status + ": " + body;
    return switch (status) {
      case 401 ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.UNAUTHENTICATED, message);
      case 403 ->
          new SourceCatalogAccessException(
              SourceCatalogAccessException.Denial.PERMISSION_DENIED, message);
      default -> new RuntimeException(message);
    };
  }

  private static String text(JsonNode node, String field) {
    String value = node.path(field).asText(null);
    return (value == null || value.isBlank()) ? null : value;
  }
}
