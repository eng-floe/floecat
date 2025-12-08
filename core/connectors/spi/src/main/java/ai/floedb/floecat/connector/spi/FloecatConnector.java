package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface FloecatConnector extends Closeable {
  String id();

  ConnectorFormat format();

  List<String> listNamespaces();

  List<String> listTables(String namespaceFq);

  TableDescriptor describe(String namespaceFq, String tableName);

  List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns);

  default List<SnapshotBundle> enumerateSnapshotsWithStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      Set<String> includeColumns,
      boolean includeStatistics) {
    return enumerateSnapshotsWithStats(namespaceFq, tableName, destinationTableId, includeColumns);
  }

  @Override
  void close();

  record TableDescriptor(
      String namespaceFq,
      String tableName,
      String location,
      String schemaJson,
      List<String> partitionKeys,
      Map<String, String> properties) {}

  record SnapshotBundle(
      long snapshotId,
      long parentId,
      long upstreamCreatedAtMs,
      TableStats tableStats,
      List<ColumnStats> columnStats,
      List<FileColumnStats> fileStats,
      String schemaJson,
      PartitionSpecInfo partitionSpec,
      long sequenceNumber,
      String manifestList,
      Map<String, String> summary,
      int schemaId) {}

  record ScanBundle(List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}
}
