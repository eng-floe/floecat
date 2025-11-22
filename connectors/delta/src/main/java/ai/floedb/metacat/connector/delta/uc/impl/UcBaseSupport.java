package ai.floedb.metacat.connector.delta.uc.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class UcBaseSupport {
  protected final ObjectMapper M = new ObjectMapper();
  protected final UcHttp uc;
  protected final SqlStmtClient sql;

  protected UcBaseSupport(UcHttp uc, SqlStmtClient sql) {
    this.uc = uc;
    this.sql = sql;
  }

  protected JsonNode getJson(String path) {
    try {
      return M.readTree(uc.get(path).body());
    } catch (Exception e) {
      throw new RuntimeException("UC GET " + path + " failed", e);
    }
  }

  protected static String url(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8);
  }

  protected static String[] splitNs(String ns) {
    int dot = ns.indexOf('.');
    if (dot < 0) {
      throw new IllegalArgumentException("Namespace must be 'catalog.schema': " + ns);
    }

    return new String[] {ns.substring(0, dot), ns.substring(dot + 1)};
  }

  protected static String backtick(String id) {
    return "`" + id.replace("`", "``") + "`";
  }

  public List<String> ucListNamespaces() {
    var out = new ArrayList<String>();
    var catalogs = getJson("/api/2.1/unity-catalog/catalogs").path("catalogs");
    for (var c : catalogs) {
      String catalogName = c.path("name").asText();
      var schemas =
          getJson("/api/2.1/unity-catalog/schemas?catalog_name=" + url(catalogName))
              .path("schemas");
      for (var s : schemas) {
        out.add(catalogName + "." + s.path("name").asText());
      }
    }
    out.sort(Comparator.naturalOrder());
    return out;
  }

  public List<String> ucListTables(String ns) {
    var parts = splitNs(ns);
    String catalog = parts[0], schema = parts[1];
    var tables =
        getJson(
                "/api/2.1/unity-catalog/tables?catalog_name="
                    + url(catalog)
                    + "&schema_name="
                    + url(schema))
            .path("tables");
    var out = new ArrayList<String>();
    for (var t : tables) {
      if (!"TABLE".equalsIgnoreCase(t.path("table_type").asText("TABLE"))) {
        continue;
      }

      out.add(t.path("name").asText());
    }
    out.sort(Comparator.naturalOrder());
    return out;
  }

  protected long[] ucSqlBasicStats(String catalog, String schema, String table) {
    if (sql == null) {
      return new long[] {0, 0, 0};
    }

    String fq = backtick(catalog) + "." + backtick(schema) + "." + backtick(table);
    JsonNode resp = sql.run("DESCRIBE DETAIL " + fq);

    JsonNode cols = resp.path("manifest").path("columns");
    Map<String, Integer> idx = new HashMap<>();
    for (int i = 0; cols.isArray() && i < cols.size(); i++) {
      idx.put(cols.get(i).path("name").asText(), i);
    }
    JsonNode data = resp.path("data_array");
    if (!(data.isArray() && data.size() > 0)) {
      return new long[] {0, 0, 0};
    }

    JsonNode row = data.get(0);

    long files = getLong(row, idx, "numFiles", 0);
    long bytes = getLong(row, idx, "sizeInBytes", 1);
    long rows = getLong(row, idx, "numRows", 2);
    return new long[] {files, bytes, rows};
  }

  private static long getLong(
      JsonNode row, Map<String, Integer> idx, String name, int fallbackPos) {
    Integer i = idx.get(name);
    int pos = (i != null ? i : fallbackPos);
    if (pos >= 0 && pos < row.size() && !row.get(pos).isNull()) {
      try {
        return Long.parseLong(row.get(pos).asText());
      } catch (Exception ignore) {
      }
    }
    return 0L;
  }
}
