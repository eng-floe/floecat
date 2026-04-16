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
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import com.google.protobuf.ByteString;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface FloecatConnector extends Closeable {
  String id();

  ConnectorFormat format();

  List<String> listNamespaces();

  List<String> listTables(String namespaceFq);

  TableDescriptor describe(String namespaceFq, String tableName);

  List<SnapshotBundle> enumerateSnapshots(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      SnapshotEnumerationOptions options);

  default List<SnapshotBundle> enumerateSnapshots(
      String namespaceFq, String tableName, ResourceId destinationTableId, boolean fullRescan) {
    return enumerateSnapshots(
        namespaceFq, tableName, destinationTableId, SnapshotEnumerationOptions.full(fullRescan));
  }

  /**
   * Captures target stats for one snapshot and optional selector scope.
   *
   * <p>Selector semantics are best-effort and connector-native:
   *
   * <ul>
   *   <li>`#<id>` selectors target stable destination column ids when available.
   *   <li>name/path selectors are interpreted as connector-specific display or physical paths.
   *   <li>unknown selectors are ignored; connectors may return a strict subset of requested
   *       targets.
   * </ul>
   */
  List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns);

  /**
   * Captures target stats for one snapshot and optional selector scope with explicit target-kind
   * hints.
   *
   * <p>Default behavior delegates to {@link #captureSnapshotTargetStats(String, String, ResourceId,
   * long, Set)} and ignores {@code includeTargetKinds}. Connectors may override this to skip
   * unnecessary work (for example file-level planning when only column targets are requested).
   *
   * <p>When {@link StatsTargetKind#TABLE} is included, callers may also include {@link
   * StatsTargetKind#COLUMN} and {@link StatsTargetKind#FILE} so full-bundle capture and persistence
   * can happen in one pass. Connectors may return any subset of the requested kinds.
   */
  default List<TargetStatsRecord> captureSnapshotTargetStats(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds) {
    return captureSnapshotTargetStats(
        namespaceFq, tableName, destinationTableId, snapshotId, includeColumns);
  }

  enum StatsTargetKind {
    TABLE,
    COLUMN,
    FILE,
    EXPRESSION
  }

  /**
   * Returns per-snapshot constraints for a table snapshot, if available.
   *
   * <p>Default: no constraints emitted.
   */
  default Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq, String tableName, ResourceId destinationTableId, long snapshotId) {
    return Optional.empty();
  }

  /**
   * Returns per-snapshot constraints for the provided snapshot bundle, if available.
   *
   * <p>Default: delegates to {@link #snapshotConstraints(String, String, ResourceId, long)}.
   */
  default Optional<SnapshotConstraints> snapshotConstraints(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return Optional.empty();
    }
    return snapshotConstraints(
        namespaceFq, tableName, destinationTableId, snapshotBundle.snapshotId());
  }

  record SnapshotEnumerationOptions(
      boolean fullRescan, Set<Long> knownSnapshotIds, Set<Long> targetSnapshotIds) {
    public SnapshotEnumerationOptions {
      knownSnapshotIds =
          knownSnapshotIds == null ? Set.of() : Set.copyOf(new LinkedHashSet<>(knownSnapshotIds));
    }

    public static SnapshotEnumerationOptions full(boolean fullRescan) {
      return new SnapshotEnumerationOptions(fullRescan, Set.of(), Set.of());
    }

    public static SnapshotEnumerationOptions full(boolean fullRescan, Set<Long> targetSnapshotIds) {
      return new SnapshotEnumerationOptions(fullRescan, Set.of(), targetSnapshotIds);
    }

    public static SnapshotEnumerationOptions incremental(Set<Long> knownSnapshotIds) {
      return new SnapshotEnumerationOptions(false, knownSnapshotIds, Set.of());
    }

    public static SnapshotEnumerationOptions incremental(
        Set<Long> knownSnapshotIds, Set<Long> targetSnapshotIds) {
      return new SnapshotEnumerationOptions(false, knownSnapshotIds, targetSnapshotIds);
    }
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
      String schemaJson,
      PartitionSpecInfo partitionSpec,
      long sequenceNumber,
      String manifestList,
      Map<String, String> summary,
      int schemaId,
      Map<String, ByteString> metadata) {}

  record ScanBundle(List<ScanFile> dataFiles, List<ScanFile> deleteFiles) {}

  /**
   * Describes a SQL view sourced from the external catalog.
   *
   * @param namespaceFq fully-qualified namespace (e.g. "catalog.schema")
   * @param name view display name
   * @param sql the view's defining SQL query
   * @param dialect SQL dialect (e.g. "spark")
   * @param searchPath default namespace search path used when the view's SQL was authored (e.g.
   *     {@code ["catalog", "schema"]})
   * @param schemaJson schema JSON in the same format as {@link TableDescriptor#schemaJson()} —
   *     interpreted by {@code LogicalSchemaMapper.mapRaw()} using the connector's {@link
   *     ConnectorFormat}
   */
  record ViewDescriptor(
      String namespaceFq,
      String name,
      String sql,
      String dialect,
      List<String> searchPath,
      String schemaJson) {}

  /**
   * Returns the names of views in the given namespace. Default: empty list.
   *
   * @implSpec Prefer overriding {@link #listViewDescriptors} directly when the underlying catalog
   *     can return view definitions in a single request, to avoid an additional round-trip per
   *     view.
   */
  default List<String> listViews(String namespaceFq) {
    return List.of();
  }

  /**
   * Describes a view by name. Returns {@link Optional#empty()} if the view does not exist or cannot
   * be described. Default: empty.
   *
   * @implSpec Prefer overriding {@link #listViewDescriptors} directly when the underlying catalog
   *     can return all view definitions in a single request, to avoid an additional round-trip per
   *     view.
   */
  default Optional<ViewDescriptor> describeView(String namespaceFq, String name) {
    return Optional.empty();
  }

  /**
   * Returns full descriptors for all views in the given namespace in a single operation. Connectors
   * that can batch this more efficiently than one {@link #describeView} call per name should
   * override this method.
   *
   * <p><b>Default implementation:</b> chains {@link #listViews} and {@link #describeView} per name.
   * This default is <em>fail-fast</em>: if any {@code describeView} call throws, the exception
   * propagates immediately and remaining views are skipped.
   */
  default List<ViewDescriptor> listViewDescriptors(String namespaceFq) {
    List<ViewDescriptor> result = new ArrayList<>();
    for (String name : listViews(namespaceFq)) {
      describeView(namespaceFq, name).ifPresent(result::add);
    }
    return result;
  }
}
