package ai.floedb.metacat.connector.iceberg.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
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
              || parameters.containsKey("metadata_location")) {
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
              || parameters.containsKey("metadata_location")) {
            return true;
          }
        }
      }
      token = response.nextToken();
    } while (token != null && !token.isEmpty());

    return false;
  }

  String metadataLocation(String database, String table) {
    var req = GetTableRequest.builder().databaseName(database).name(table).build();
    var response = glue.getTable(req);
    if (response == null
        || response.table() == null
        || response.table().parameters() == null) {
      return null;
    }
    var params = response.table().parameters();
    var loc = params.get("metadata_location");
    if (loc == null) {
      loc = params.get("metadata-location");
    }
    return (loc == null || loc.isBlank()) ? null : loc;
  }
}
