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

package ai.floedb.floecat.connector.iceberg.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;

final class GlueIcebergFilter {
  private final GlueClient glue;

  GlueIcebergFilter(GlueClient glue) {
    this.glue = glue;
  }

  List<String> icebergTables(String database) {
    var request = GetTablesRequest.builder().databaseName(database).build();

    var out = new ArrayList<String>(64);
    String token = null;
    do {
      var builder = request.toBuilder();
      if (token != null) {
        builder.nextToken(token);
      }

      var response = glue.getTables(builder.build());
      for (var table : response.tableList()) {
        var parameters = table.parameters();
        if (parameters != null) {
          var tableType = parameters.get("table_type");
          if ("ICEBERG".equalsIgnoreCase(tableType)
              || parameters.containsKey("metadata-location")) {
            out.add(table.name());
          }
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());

    Collections.sort(out);
    return out;
  }

  boolean databaseHasIceberg(String db) {
    var req = GetTablesRequest.builder().databaseName(db).maxResults(50).build();

    String token = null;
    do {
      var builder = req.toBuilder();
      if (token != null) {
        builder.nextToken(token);
      }

      var response = glue.getTables(builder.build());
      for (var table : response.tableList()) {
        var parameters = table.parameters();
        if (parameters != null) {
          var tableType = parameters.get("table_type");
          if ("ICEBERG".equalsIgnoreCase(tableType)
              || parameters.containsKey("metadata-location")) {
            return true;
          }
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());

    return false;
  }
}
