package ai.floedb.metacat.connector.dummy;

import ai.floedb.metacat.connector.spi.ConnectorFormat;
import ai.floedb.metacat.connector.spi.MetacatConnector;
import java.util.*;

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
  public UpstreamTable describe(String namespaceFq, String tableName) {
    String schemaJson =
        """
      {"type":"struct","fields":[
        {"id":1,"name":"id","type":"long","required":true},
        {"id":2,"name":"ts","type":"timestamp","required":false}
      ]}
        """
            .trim();

    String location = "s3://dummy/" + namespaceFq.replace('.', '/') + "/" + tableName;

    Optional<Long> sid = Optional.of(42L);
    Optional<Long> ts = Optional.of(1_700_000_000_000L);

    Map<String, String> props = Map.of("upstream", "dummy", "owner", "tests");

    List<String> pks = List.of("ts");

    return new UpstreamTable(namespaceFq, tableName, location, schemaJson, sid, ts, props, pks);
  }

  @Override
  public boolean supportsTableStats() {
    return false;
  }

  @Override
  public void close() {}
}
