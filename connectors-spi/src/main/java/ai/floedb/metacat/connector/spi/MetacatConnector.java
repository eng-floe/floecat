package ai.floedb.metacat.connector.spi;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface MetacatConnector extends Closeable {
  String id();

  TableFormat format();

  List<String> listNamespaces();

  List<String> listTables(String namespaceFq);

  record UpstreamTable(
      String namespaceFq,
      String tableName,
      String location,
      String schemaJson,
      Optional<Long> currentSnapshotId,
      Optional<Long> currentSnapshotTsMillis,
      Map<String,String> properties,
      List<String> partitionKeys
  ) {}

  UpstreamTable describe(String namespaceFq, String tableName);

  default boolean supportsTableStats() { return false; }

  @Override void close();
}
