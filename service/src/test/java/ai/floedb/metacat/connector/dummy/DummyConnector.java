package ai.floedb.metacat.connector.dummy;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class DummyConnector implements MetacatConnector {
  private final String id;

  private static final class Col {
    final String colId;
    final String name;
    final String logical;

    Col(String colId, String name, String logical) {
      this.colId = colId;
      this.name = name;
      this.logical = logical;
    }
  }

  private static final List<Col> LEAF_COLS =
      List.of(
          new Col("1", "id", "INT64"),
          new Col("2", "ts", "TIMESTAMP"),
          // struct user (3) -> id(4), name(5)
          new Col("4", "user.id", "INT64"),
          new Col("5", "user.name", "STRING"),
          // list items (6) -> element(7 struct) -> sku(8), qty(9)
          new Col("8", "items.element.sku", "STRING"),
          new Col("9", "items.element.qty", "INT32"),
          // map attrs (10) -> key(11), value(12)
          new Col("11", "attrs.key", "STRING"),
          new Col("12", "attrs.value", "STRING"));

  private DummyConnector(String id) {
    this.id = id;
  }

  public static MetacatConnector create() {
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
    return List.of("db", "analytics.sales");
  }

  @Override
  public List<String> listTables(String namespaceFq) {
    return switch (namespaceFq) {
      case "db" -> List.of("events", "users");
      case "analytics.sales" -> List.of("orders");
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

    return new TableDescriptor(namespaceFq, tableName, location, schemaJson, pks, props);
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

    List<ColumnStats> cstats = new ArrayList<>();
    for (Col c : selected) {
      cstats.add(
          ColumnStats.newBuilder()
              .setSnapshotId(snapshotId)
              .setColumnId(c.colId)
              .setColumnName(c.name)
              .setLogicalType(c.logical)
              .setNullCount(0)
              .build());
    }

    String basePath =
        "s3://dummy/" + namespace.replace('.', '/') + "/" + table + "/snapshot-" + snapshotId;

    Function<Integer, List<ColumnStats>> perFileCols =
        fileIndex -> {
          List<ColumnStats> cols = new ArrayList<>();
          for (Col c : selected) {
            long valueCount =
                switch (fileIndex) {
                  case 0 -> 10L;
                  case 1 -> 20L;
                  default -> 30L;
                };
            cols.add(
                ColumnStats.newBuilder()
                    .setTableId(destinationTableId)
                    .setSnapshotId(snapshotId)
                    .setColumnId(c.colId)
                    .setColumnName(c.name)
                    .setLogicalType(c.logical)
                    .setValueCount(valueCount)
                    .setNullCount(0L)
                    .setMin("f" + fileIndex + "_min_" + c.colId)
                    .setMax("f" + fileIndex + "_max_" + c.colId)
                    .build());
          }
          return cols;
        };

    var f1 =
        FileColumnStats.newBuilder()
            .setTableId(destinationTableId)
            .setSnapshotId(snapshotId)
            .setFilePath(basePath + "/part-00000.parquet")
            .setRowCount(30)
            .setSizeBytes(512)
            .addAllColumns(perFileCols.apply(0))
            .build();

    var f2 =
        FileColumnStats.newBuilder()
            .setTableId(destinationTableId)
            .setSnapshotId(snapshotId)
            .setFilePath(basePath + "/part-00001.parquet")
            .setRowCount(30)
            .setSizeBytes(512)
            .addAllColumns(perFileCols.apply(1))
            .build();

    var f3 =
        FileColumnStats.newBuilder()
            .setTableId(destinationTableId)
            .setSnapshotId(snapshotId)
            .setFilePath(basePath + "/part-00002.parquet")
            .setRowCount(40)
            .setSizeBytes(1024)
            .addAllColumns(perFileCols.apply(2))
            .build();

    List<FileColumnStats> fileStats = List.of(f1, f2, f3);

    return List.of(
        new SnapshotBundle(
            snapshotId,
            0L,
            createdAt,
            tStats,
            cstats,
            fileStats,
            "{}",
            null,
            0L,
            null,
            Map.of(),
            0));
  }

  @Override
  public void close() {}
}
