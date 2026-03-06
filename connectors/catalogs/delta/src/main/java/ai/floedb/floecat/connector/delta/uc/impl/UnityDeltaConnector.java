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
import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.parquet.io.InputFile;

public final class UnityDeltaConnector extends DeltaConnector {

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
  public String id() {
    return super.id();
  }

  @Override
  public List<String> listNamespaces() {
    try {
      var cats = M.readTree(ucHttp.get("/api/2.1/unity-catalog/catalogs").body()).path("catalogs");
      List<String> out = new ArrayList<>();
      for (var c : cats) {
        String catalogName = c.path("name").asText();
        var schemas =
            M.readTree(
                    ucHttp
                        .get(
                            "/api/2.1/unity-catalog/schemas?catalog_name="
                                + UcBaseSupport.url(catalogName))
                        .body())
                .path("schemas");
        for (var s : schemas) {
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

      var fields = M.createArrayNode();
      for (var c : meta.path("columns")) {
        var n = M.createObjectNode();
        n.put("name", c.path("name").asText());
        n.put("type", c.path("type_text").asText(c.path("type_name").asText()));
        n.put("nullable", c.path("nullable").asBoolean(true));
        var md = M.createObjectNode();
        if (!c.path("comment").isMissingNode()) {
          md.put("comment", c.path("comment").asText());
        }
        n.set("metadata", md);
        fields.add(n);
      }
      var schemaNode = M.createObjectNode();
      schemaNode.put("type", "struct");
      schemaNode.set("fields", fields);

      Map<String, String> props = new LinkedHashMap<>();
      putIfPresent(props, meta, "table_type");
      putIfPresent(props, meta, "data_source_format");
      putIfPresent(props, meta, "storage_location");

      String location = meta.path("storage_location").asText(null);
      return new TableDescriptor(
          namespaceFq,
          tableName,
          location,
          schemaNode.toString(),
          List.of(),
          ColumnIdAlgorithm.CID_PATH_ORDINAL,
          props);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("describe failed", e);
    }
  }

  @Override
  protected String storageLocation(String namespaceFq, String tableName) {
    try {
      String full = namespaceFq + "." + tableName;
      var meta =
          M.readTree(ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full)).body());
      String loc = meta.path("storage_location").asText(null);
      if (loc == null || loc.isBlank()) {
        throw new IllegalStateException("Table has no storage_location: " + full);
      }

      return loc;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to resolve storage_location for " + namespaceFq + "." + tableName, e);
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
      throw new RuntimeException("listViewDescriptors failed", e);
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
   */
  private String buildSchemaJson(JsonNode meta) {
    var fields = M.createArrayNode();
    for (var c : meta.path("columns")) {
      var n = M.createObjectNode();
      n.put("name", c.path("name").asText());
      n.put("type", c.path("type_text").asText(c.path("type_name").asText()));
      n.put("nullable", c.path("nullable").asBoolean(true));
      fields.add(n);
    }
    var schemaNode = M.createObjectNode();
    schemaNode.put("type", "struct");
    schemaNode.set("fields", fields);
    return schemaNode.toString();
  }

  /**
   * Fetches the raw {@code tables} JSON array from the UC tables endpoint for the given {@code
   * "catalog.schema"} namespace, or returns an empty array node if the namespace contains no dot
   * separator.
   *
   * <p>TODO: UC returns paginated results; this method only fetches the first page. Namespaces with
   * more entries than UC's default page size will silently return incomplete results.
   */
  private JsonNode listTablesNode(String namespaceFq) throws Exception {
    int dot = namespaceFq.indexOf('.');
    if (dot < 0) {
      return M.createArrayNode();
    }
    String catalog = namespaceFq.substring(0, dot);
    String schema = namespaceFq.substring(dot + 1);
    return M.readTree(
            ucHttp
                .get(
                    "/api/2.1/unity-catalog/tables?catalog_name="
                        + UcBaseSupport.url(catalog)
                        + "&schema_name="
                        + UcBaseSupport.url(schema))
                .body())
        .path("tables");
  }

  private static void putIfPresent(Map<String, String> props, JsonNode n, String field) {
    if (!n.path(field).isMissingNode()) {
      props.put(field, n.path(field).asText());
    }
  }
}
