package ai.floedb.metacat.connector.dummy;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.List;
import java.util.Map;

public final class DummyConnector implements MetacatConnector {
  private final String id;

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
          {"id":2,"name":"ts","type":"timestamp","required":false}
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
      String namespace, String table, ResourceId destinationTableId) {
    long snapshotId = 42L;
    long createdAt = 1_700_000_000_000L;

    var tStats =
        ai.floedb.metacat.catalog.rpc.TableStats.newBuilder()
            .setSnapshotId(snapshotId)
            .setRowCount(100)
            .setDataFileCount(3)
            .setTotalSizeBytes(2048)
            .build();

    var cStat =
        ai.floedb.metacat.catalog.rpc.ColumnStats.newBuilder()
            .setSnapshotId(snapshotId)
            .setColumnId("c1")
            .setColumnName("dummy_col")
            .setLogicalType("INT64")
            .setNullCount(0)
            .build();

    return List.of(new SnapshotBundle(snapshotId, 0L, createdAt, tStats, java.util.List.of(cStat)));
  }

  @Override
  public void close() {}
}
