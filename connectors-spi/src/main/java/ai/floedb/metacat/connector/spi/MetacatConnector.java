package ai.floedb.metacat.connector.spi;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.planning.rpc.PlanFile;

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

  PlanBundle plan(String namespaceFq, String tableName, long snapshotId, long asOfTime);

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
      List<ColumnStats> columnStats) {}

  record PlanBundle(
     List<PlanFile> dataFiles,
     List<PlanFile> deleteFiles
  ) {}
}
