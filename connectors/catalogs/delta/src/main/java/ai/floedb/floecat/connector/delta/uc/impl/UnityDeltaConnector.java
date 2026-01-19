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

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.engine.Engine;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    int dot = namespaceFq.indexOf('.');
    if (dot < 0) {
      return List.of();
    }

    String catalog = namespaceFq.substring(0, dot);
    String schema = namespaceFq.substring(dot + 1);
    try {
      var tables =
          M.readTree(
                  ucHttp
                      .get(
                          "/api/2.1/unity-catalog/tables?catalog_name="
                              + UcBaseSupport.url(catalog)
                              + "&schema_name="
                              + UcBaseSupport.url(schema))
                      .body())
              .path("tables");
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
      var meta =
          M.readTree(ucHttp.get("/api/2.1/unity-catalog/tables/" + UcBaseSupport.url(full)).body());

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
          namespaceFq, tableName, location, schemaNode.toString(), List.of(), props);
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

  private static void putIfPresent(Map<String, String> props, JsonNode n, String field) {
    if (!n.path(field).isMissingNode()) {
      props.put(field, n.path(field).asText());
    }
  }
}
