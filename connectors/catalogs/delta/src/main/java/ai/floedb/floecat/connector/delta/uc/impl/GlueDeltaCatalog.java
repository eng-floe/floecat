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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;

final class GlueDeltaCatalog implements AutoCloseable {
  private final GlueClient glue;

  GlueDeltaCatalog(GlueClient glue) {
    this.glue = glue;
  }

  List<String> deltaNamespaces() {
    List<String> out = new ArrayList<>();
    String token = null;
    do {
      var request = GetDatabasesRequest.builder().nextToken(token).maxResults(100).build();
      var response = glue.getDatabases(request);
      for (Database db : response.databaseList()) {
        String name = db.name();
        if (name != null && !name.isBlank() && databaseHasDelta(name)) {
          out.add(name);
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());
    Collections.sort(out);
    return out;
  }

  List<String> deltaTables(String namespace) {
    if (namespace == null || namespace.isBlank()) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    String token = null;
    do {
      var request =
          GetTablesRequest.builder()
              .databaseName(namespace)
              .nextToken(token)
              .maxResults(100)
              .build();
      var response = glue.getTables(request);
      for (var table : response.tableList()) {
        if (isDeltaTable(table.parameters())) {
          out.add(table.name());
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());
    Collections.sort(out);
    return out;
  }

  String storageLocation(String namespace, String tableName) {
    var response =
        glue.getTable(GetTableRequest.builder().databaseName(namespace).name(tableName).build());
    var table = response.table();
    if (!isDeltaTable(table.parameters())) {
      throw new IllegalStateException("Glue table is not DELTA: " + namespace + "." + tableName);
    }
    String storageLocation =
        table.storageDescriptor() == null ? null : table.storageDescriptor().location();
    String location = resolveLocation(table.parameters(), storageLocation);
    if (location == null || location.isBlank()) {
      throw new IllegalStateException(
          "Glue table has no storage location: " + namespace + "." + tableName);
    }
    return location;
  }

  @Override
  public void close() {
    try {
      glue.close();
    } catch (Exception ignore) {
    }
  }

  private boolean databaseHasDelta(String database) {
    String token = null;
    do {
      var request =
          GetTablesRequest.builder().databaseName(database).nextToken(token).maxResults(50).build();
      var response = glue.getTables(request);
      for (var table : response.tableList()) {
        if (isDeltaTable(table.parameters())) {
          return true;
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());
    return false;
  }

  private static boolean isDeltaTable(Map<String, String> parameters) {
    if (parameters == null || parameters.isEmpty()) {
      return false;
    }
    String tableType = parameters.get("table_type");
    if ("DELTA".equalsIgnoreCase(tableType)) {
      return true;
    }
    String provider = firstNonBlank(parameters, "spark.sql.sources.provider", "provider");
    if ("delta".equalsIgnoreCase(provider)) {
      return true;
    }
    return parameters.containsKey("delta.lastUpdateVersion")
        || parameters.containsKey("delta.lastCommitTimestamp");
  }

  private static String resolveLocation(Map<String, String> parameters, String storageLocation) {
    if (storageLocation != null && !storageLocation.isBlank()) {
      return storageLocation;
    }
    if (parameters == null || parameters.isEmpty()) {
      return null;
    }
    String value = firstNonBlank(parameters, "location", "path", "storage_location");
    if (value == null) {
      return null;
    }
    return value;
  }

  private static String firstNonBlank(Map<String, String> map, String... keys) {
    for (String key : keys) {
      String value = map.get(key);
      if (value != null && !value.isBlank()) {
        return value.trim();
      }
      String upper = map.get(key.toUpperCase(Locale.ROOT));
      if (upper != null && !upper.isBlank()) {
        return upper.trim();
      }
    }
    return null;
  }
}
