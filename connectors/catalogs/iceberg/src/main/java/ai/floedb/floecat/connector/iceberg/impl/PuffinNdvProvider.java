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

import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.common.ndv.NdvApprox;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketches;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.FileMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.util.Pair;

public class PuffinNdvProvider {

  static Map<String, ColumnNdv> readPuffinNdvWithSketches(
      Table table, long snapshotId, IntFunction<String> idToName) throws IOException {

    Map<String, ColumnNdv> out = new HashMap<>();
    final ObjectMapper M = new ObjectMapper();

    var ops = ((HasTableOperations) table).operations();
    var meta = ops.current();
    String metaPath = meta.metadataFileLocation();

    JsonNode root;
    try (InputStream in = table.io().newInputFile(metaPath).newStream()) {
      root = M.readTree(in);
    }

    JsonNode statistics = root.path("statistics");
    if (statistics.isArray()) {
      for (JsonNode stat : statistics) {
        if (stat.path("snapshot-id").asLong(Long.MIN_VALUE) != snapshotId) {
          continue;
        }

        String puffinPath = stat.path("statistics-path").asText(null);
        if (puffinPath != null && !puffinPath.isBlank()) {
          readSketchesFromPuffinFile(table, puffinPath, out, idToName);
        } else {
          readNdvApproximationsFromJson(stat.path("blob-metadata"), out, idToName);
        }
      }
    }
    return out;
  }

  private static void readSketchesFromPuffinFile(
      Table table, String puffinPath, Map<String, ColumnNdv> out, IntFunction<String> idToName)
      throws IOException {

    InputFile puffin = table.io().newInputFile(puffinPath);
    try (PuffinReader reader = Puffin.read(puffin).build()) {
      FileMetadata fileMetadata = reader.fileMetadata();
      Iterable<Pair<BlobMetadata, ByteBuffer>> allBlobs = reader.readAll(fileMetadata.blobs());

      for (Pair<BlobMetadata, ByteBuffer> pair : allBlobs) {
        BlobMetadata blobMetadata = pair.first();
        ByteBuffer blobData = pair.second();
        String type = blobMetadata.type();

        if (!StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equalsIgnoreCase(type.trim())) {
          continue;
        }

        List<Integer> fieldIds = blobMetadata.inputFields();
        if (fieldIds == null || fieldIds.isEmpty()) {
          continue;
        }

        byte[] data = new byte[blobData.remaining()];
        blobData.get(data);

        try {
          var sketch = Sketches.heapifyCompactSketch(Memory.wrap(data));
          double estimate = sketch.getEstimate();

          for (Integer fid : fieldIds) {
            String name = idToName.apply(fid);
            if (name == null || name.isBlank()) {
              continue;
            }

            ColumnNdv col = out.computeIfAbsent(name, k -> new ColumnNdv());
            col.mergeTheta(data);

            if (col.approx == null) col.approx = new NdvApprox();
            col.approx.estimate = estimate;
            col.approx.method = "apache-datasketches-theta";
            if (blobMetadata.properties() != null) {
              blobMetadata
                  .properties()
                  .forEach((k, v) -> col.approx.params.putIfAbsent(k, String.valueOf(v)));
            }
          }
        } catch (Exception e) {
          System.err.println("Failed to read theta sketch for blob type " + type);
        }
      }
    }

    for (var col : out.values()) {
      col.finalizeTheta();
    }
  }

  private static void readNdvApproximationsFromJson(
      JsonNode blobMetadata, Map<String, ColumnNdv> out, IntFunction<String> idToName) {

    if (blobMetadata.isArray()) {
      for (JsonNode bm : blobMetadata) {
        String type = bm.path("type").asText("");
        JsonNode fields = bm.path("fields");
        JsonNode props = bm.path("properties");

        boolean isTheta = StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1.equalsIgnoreCase(type);
        Long ndv = (props != null) ? parseLongLenient(props.get("ndv")) : null;

        if (ndv != null && fields != null && fields.isArray() && fields.size() > 0) {
          for (JsonNode fidNode : fields) {
            int fid = fidNode.asInt();
            String name = idToName.apply(fid);
            if (name == null || name.isBlank()) {
              continue;
            }

            ColumnNdv col = out.computeIfAbsent(name, k -> new ColumnNdv());
            if (col.approx == null) {
              col.approx = new NdvApprox();
              col.approx.estimate = ndv.doubleValue();
              if (isTheta) col.approx.method = "apache-datasketches-theta-json";
              if (props != null && props.isObject()) {
                props
                    .fields()
                    .forEachRemaining(
                        e -> col.approx.params.putIfAbsent(e.getKey(), e.getValue().asText()));
              }
            }
          }
        }
      }
    }
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
