package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.connector.common.ndv.Hll;
import ai.floedb.metacat.connector.common.ndv.NdvProvider;
import ai.floedb.metacat.connector.common.ndv.PrecomputedNdv;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.puffin.StandardBlobTypes;

final class PuffinNdvProvider implements PrecomputedNdv {

  private static final ObjectMapper M = new ObjectMapper();
  private final Map<String, Long> ndvByName;

  private PuffinNdvProvider(Map<String, Long> ndvByName) {
    this.ndvByName = ndvByName;
  }

  static NdvProvider maybeCreate(Table table, long snapshotId, IntFunction<String> idToName) {
    try {
      Map<String, Long> byName = readPuffinNdv(table, snapshotId, idToName);
      if (byName.isEmpty()) {
        return null;
      }

      return new PuffinNdvProvider(byName);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void contributeNdv(String filePath, Map<String, Hll> sinks) throws Exception {}

  @Override
  public Map<String, Long> estimatesByName() {
    return ndvByName;
  }

  private static Map<String, Long> readPuffinNdv(
      Table table, long snapshotId, IntFunction<String> idToName) throws Exception {
    Map<String, Long> out = new HashMap<>();

    var ops = ((HasTableOperations) table).operations();
    var meta = ops.current();
    String metaPath = meta.metadataFileLocation();

    try (InputStream in = table.io().newInputFile(metaPath).newStream()) {
      JsonNode root = M.readTree(in);
      JsonNode statistics = root.path("statistics");
      if (!statistics.isArray()) {
        return out;
      }

      for (JsonNode stat : statistics) {
        if (stat.path("snapshot-id").asLong(Long.MIN_VALUE) != snapshotId) continue;

        JsonNode blobs = stat.path("blob-metadata");
        if (!blobs.isArray()) {
          continue;
        }

        for (JsonNode bm : blobs) {
          String type = bm.path("type").asText("");
          if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equalsIgnoreCase(
              type.trim().toLowerCase(Locale.ROOT))) {
            continue;
          }

          JsonNode fields = bm.path("fields");
          if (!fields.isArray() || fields.size() == 0) {
            continue;
          }

          JsonNode props = bm.path("properties");
          if (!props.isObject() || !props.has("ndv")) {
            continue;
          }

          Long ndv = parseLongLenient(props.get("ndv"));
          if (ndv == null) {
            continue;
          }

          for (JsonNode fidNode : fields) {
            int fid = fidNode.asInt();
            String name = idToName.apply(fid);
            if (name != null && !name.isBlank()) {
              out.merge(name, ndv, Math::max);
            }
          }
        }
      }
    }
    return out;
  }

  private static Long parseLongLenient(JsonNode n) {
    if (n == null || n.isNull()) {
      return null;
    }

    if (n.isIntegralNumber()) {
      return n.asLong();
    }

    try {
      String s = n.asText().trim().replace("_", "");
      if (s.isEmpty()) {
        return null;
      }
      return Long.parseLong(s);
    } catch (Exception ignore) {
      return null;
    }
  }
}
