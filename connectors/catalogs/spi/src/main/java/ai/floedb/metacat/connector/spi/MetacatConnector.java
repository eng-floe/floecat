package ai.floedb.metacat.connector.spi;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.execution.rpc.ScanFile;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface MetacatConnector extends Closeable {
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

  ScanBundle plan(String namespaceFq, String tableName, long snapshotId, long asOfTime);

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
      int schemaId,
      ai.floedb.metacat.catalog.rpc.IcebergMetadata icebergMetadata) {}

  record ScanBundle(List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}
}
