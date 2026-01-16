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

package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import com.google.protobuf.ByteString;
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

  /**
   * columnIdAlgorithm is the *persisted* policy for how column_id must be computed for this table.
   * Reconciler copies it into TableSpec.upstream (so service layer can run without connectors).
   */
  record TableDescriptor(
      String namespaceFq,
      String tableName,
      String location,
      String schemaJson,
      List<String> partitionKeys,
      ColumnIdAlgorithm columnIdAlgorithm,
      Map<String, String> properties) {}

  /**
   * Column identity inputs. Fields may be unknown: - physicalPath may be blank if unknown - ordinal
   * may be 0 if unknown - fieldId may be 0 if unknown
   */
  record ColumnRef(
      String name, // leaf name (required for many engines)
      String physicalPath, // canonical leaf path (recommended; may be blank)
      int ordinal, // 1-based within parent struct, or 0 if unknown
      int fieldId // 0 if unknown
      ) {}

  record ColumnStatsView(
      ColumnRef ref,
      String logicalType,
      Long valueCount,
      Long nullCount,
      Long nanCount,
      String min,
      String max,
      Ndv ndv,
      Map<String, String> properties) {}

  record FileColumnStatsView(
      String filePath,
      String fileFormat,
      long rowCount,
      long sizeBytes,
      FileContent fileContent,
      String partitionDataJson,
      int partitionSpecId,
      List<Integer> equalityFieldIds,
      Long sequenceNumber,
      List<ColumnStatsView> columns) {}

  record SnapshotBundle(
      long snapshotId,
      long parentId,
      long upstreamCreatedAtMs,
      TableStats tableStats,
      List<ColumnStatsView> columnStats,
      List<FileColumnStatsView> fileStats,
      String schemaJson,
      PartitionSpecInfo partitionSpec,
      long sequenceNumber,
      String manifestList,
      Map<String, String> summary,
      int schemaId,
      Map<String, ByteString> metadata) {}

  record ScanBundle(List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}
}
