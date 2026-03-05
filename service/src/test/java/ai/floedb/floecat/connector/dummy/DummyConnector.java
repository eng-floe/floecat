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

package ai.floedb.floecat.connector.dummy;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class DummyConnector implements FloecatConnector {
  private final String id;

  private static final class Col {
    final int colId;
    final String name;
    final String logical;
    final int ordinal;

    Col(int colId, String name, String logical, int ordinal) {
      this.colId = colId;
      this.name = name;
      this.logical = logical;
      this.ordinal = ordinal;
    }
  }

  private static final List<Col> LEAF_COLS =
      List.of(
          new Col(1, "id", "INT64", 1),
          new Col(2, "ts", "TIMESTAMP", 2),
          // struct user (3) -> id(4), name(5)
          new Col(4, "user.id", "INT64", 3),
          new Col(5, "user.name", "STRING", 4),
          // list items (6) -> element(7 struct) -> sku(8), qty(9)
          new Col(8, "items.element.sku", "STRING", 5),
          new Col(9, "items.element.qty", "INT32", 6),
          // map attrs (10) -> key(11), value(12)
          new Col(11, "attrs.key", "STRING", 7),
          new Col(12, "attrs.value", "STRING", 8));

  private DummyConnector(String id) {
    this.id = id;
  }

  public static FloecatConnector create() {
    return new DummyConnector("dummy");
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public ConnectorFormat format() {
    return ConnectorFormat.CF_ICEBERG;
  }

  @Override
  public List<String> listNamespaces() {
    return List.of("db", "examples.iceberg");
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return switch (namespaceFq) {
      case "db" -> List.of("events", "users");
      case "examples.iceberg" -> List.of("orders");
      default -> List.of();
    };
  }

  @Override
  public TableDescriptor describe(String namespaceFq, String tableName) {
    String schemaJson =
        """
        {"type":"struct","fields":[
          {"id":1,"name":"id","type":"long","required":true},
          {"id":2,"name":"ts","type":"timestamp","required":false},
          {"id":3,"name":"user","required":false,"type":
            {"type":"struct","fields":[
              {"id":4,"name":"id","type":"long","required":false},
              {"id":5,"name":"name","type":"string","required":false}
            ]}
          },
          {"id":6,"name":"items","required":false,"type":
            {"type":"list","element-id":7,"element-required":false,"element":
              {"type":"struct","fields":[
                {"id":8,"name":"sku","type":"string","required":false},
                {"id":9,"name":"qty","type":"int","required":false}
              ]}
            }
          },
          {"id":10,"name":"attrs","required":false,"type":
            {"type":"map",
             "key-id":11,"key":"string",
             "value-id":12,"value-required":false,"value":"string"}
          }
        ]}
        """
            .trim();

    String location = "s3://dummy/" + namespaceFq.replace('.', '/') + "/" + tableName;
    Map<String, String> props = Map.of("upstream", "dummy", "owner", "tests");
    List<String> pks = List.of("ts");

    return new TableDescriptor(
        namespaceFq, tableName, location, schemaJson, pks, ColumnIdAlgorithm.CID_FIELD_ID, props);
  }

  @Override
  public List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespace, String table, ResourceId destinationTableId, Set<String> includeColumns) {

    long snapshotId = 42L;
    long createdAt = 1_700_000_000_000L;

    var tStats =
        TableStats.newBuilder()
            .setSnapshotId(snapshotId)
            .setRowCount(100)
            .setDataFileCount(3)
            .setTotalSizeBytes(2048)
            .build();

    Set<String> normalized =
        (includeColumns == null || includeColumns.isEmpty())
            ? null
            : includeColumns.stream()
                .filter(s -> s != null && !s.isBlank())
                .collect(Collectors.toSet());

    List<Col> selected =
        (normalized == null)
            ? LEAF_COLS
            : LEAF_COLS.stream()
                .filter(c -> normalized.contains("#" + c.colId) || normalized.contains(c.name))
                .collect(Collectors.toList());

    List<ColumnStatsView> cstats = new ArrayList<>();
    for (Col c : selected) {
      var ref =
          new ColumnRef(
              c.name,
              c.name, // physical path for nested refs in tests (dot-separated)
              c.ordinal, // stable 1-based ordinal for PATH_ORDINAL algorithms
              c.colId // fieldId for Iceberg-style schemas
              );

      cstats.add(new ColumnStatsView(ref, c.logical, null, 0L, null, null, null, null, Map.of()));
    }

    String basePath =
        "s3://dummy/" + namespace.replace('.', '/') + "/" + table + "/snapshot-" + snapshotId;

    Function<Integer, List<ColumnStatsView>> perFileCols =
        fileIndex -> {
          List<ColumnStatsView> cols = new ArrayList<>();
          for (Col c : selected) {
            long valueCount =
                switch (fileIndex) {
                  case 0 -> 10L;
                  case 1 -> 20L;
                  default -> 30L;
                };

            var ref =
                new ColumnRef(
                    c.name,
                    c.name, // physical path for nested refs in tests (dot-separated)
                    c.ordinal, // stable 1-based ordinal for PATH_ORDINAL algorithms
                    c.colId // fieldId for Iceberg-style schemas
                    );

            cols.add(
                new ColumnStatsView(
                    ref,
                    c.logical,
                    valueCount,
                    0L,
                    null,
                    "f" + fileIndex + "_min_" + c.colId,
                    "f" + fileIndex + "_max_" + c.colId,
                    null,
                    Map.of()));
          }
          return cols;
        };

    var f1 =
        new FileColumnStatsView(
            basePath + "/part-00000.parquet",
            "PARQUET",
            30,
            512,
            FileContent.FC_DATA,
            "",
            0,
            List.of(),
            null,
            perFileCols.apply(0));

    var f2 =
        new FileColumnStatsView(
            basePath + "/part-00001.parquet",
            "PARQUET",
            30,
            512,
            FileContent.FC_DATA,
            "",
            0,
            List.of(),
            null,
            perFileCols.apply(1));

    var f3 =
        new FileColumnStatsView(
            basePath + "/part-00002.parquet",
            "PARQUET",
            40,
            1024,
            FileContent.FC_DATA,
            "",
            0,
            List.of(),
            null,
            perFileCols.apply(2));

    List<FileColumnStatsView> fileStats = List.of(f1, f2, f3);

    String schemaJson = describe(namespace, table).schemaJson();

    return List.of(
        new SnapshotBundle(
            snapshotId,
            0L,
            createdAt,
            tStats,
            cstats,
            fileStats,
            schemaJson,
            null,
            0L,
            null,
            Map.of(),
            0,
            Map.of()));
  }

  @Override
  public void close() {}
}
