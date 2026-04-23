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
package ai.floedb.floecat.reconciler.spi;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Front door used by the reconciler regardless of deployment mode. */
public interface ReconcilerBackend {
  String SOURCE_NAMESPACE_PROPERTY = "floecat.reconciler.source_namespace";
  String SOURCE_NAME_PROPERTY = "floecat.reconciler.source_name";
  String SOURCE_CONNECTOR_ID_PROPERTY = "floecat.reconciler.source_connector_id";

  ResourceId ensureNamespace(ReconcileContext ctx, ResourceId catalogId, NameRef namespace);

  Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespace);

  ResourceId ensureTable(
      ReconcileContext ctx, ResourceId namespaceId, NameRef table, TableSpecDescriptor descriptor);

  /**
   * Updates an existing destination table by stable resource ID.
   *
   * <p>This method must not create a table when {@code tableId} is missing and must not resolve the
   * target by display name. Implementations should refresh reconciler-owned metadata, including the
   * persisted source identity carried in {@code descriptor}.
   *
   * @return whether the persisted table changed
   */
  default boolean updateTableById(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId namespaceId,
      NameRef table,
      TableSpecDescriptor descriptor) {
    throw new UnsupportedOperationException("updateTableById is not supported");
  }

  Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table);

  default Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
    return Optional.empty();
  }

  default Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
      ReconcileContext ctx, ResourceId tableId) {
    return Optional.empty();
  }

  SnapshotPin snapshotPinFor(
      ReconcileContext ctx, ResourceId tableId, SnapshotRef ref, Optional<Timestamp> asOf);

  Optional<Snapshot> fetchSnapshot(ReconcileContext ctx, ResourceId tableId, long snapshotId);

  Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId);

  void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot);

  /** Returns whether the snapshot has persisted stats for a specific target kind. */
  boolean statsAlreadyCapturedForTargetKind(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, StatsTargetKind targetKind);

  /**
   * Returns whether all requested column selectors are covered by persisted column stats.
   *
   * <p>Selectors use the same syntax as {@code StatsCaptureRequest.columnSelectors} (for example
   * {@code #123} for stable column id, or connector-native column name/path strings). Malformed id
   * selectors (for example {@code #abc}) are treated as unsatisfiable and must return {@code
   * false}.
   */
  boolean statsCapturedForColumnSelectors(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> selectors);

  /**
   * Returns whether all requested explicit stats targets are present for the snapshot.
   *
   * <p>Each target is checked by exact identity ({@code table_id, snapshot_id, target}).
   * Implementations should return {@code false} when any target is missing.
   */
  boolean statsCapturedForTargets(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<StatsTarget> targets);

  void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats);

  /**
   * Persists snapshot-scoped constraints and returns whether storage changed.
   *
   * <p>No-op default for backends that predate constraints support.
   */
  default boolean putSnapshotConstraints(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, SnapshotConstraints constraints) {
    return false;
  }

  String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId);

  String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId);

  Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId);

  void updateConnectorDestination(
      ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination);

  Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view);

  default Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
    return Optional.empty();
  }

  default Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
      ReconcileContext ctx, ResourceId viewId) {
    return Optional.empty();
  }

  /**
   * Ensures a view with the given spec exists at the destination.
   *
   * <p>The idempotency key is used for deduplication across reconciler runs. Implementations should
   * create the view when missing and update the existing view when the persisted definition has
   * drifted from {@code spec}.
   */
  ViewMutationResult ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey);

  /**
   * Updates an existing destination view by stable resource ID.
   *
   * <p>This method must not create a view when {@code viewId} is missing and must not resolve the
   * target by display name. Implementations should reject catalog, namespace, or display-name
   * mismatches because those fields identify the existing destination resource in single-view
   * reconcile.
   *
   * @return whether the persisted view changed
   */
  boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec);

  record ViewMutationResult(ResourceId viewId, boolean changed) {}

  record DestinationTableMetadata(
      ResourceId catalogId,
      ResourceId namespaceId,
      String displayName,
      String sourceNamespace,
      String sourceName,
      ResourceId sourceConnectorId) {
    public DestinationTableMetadata(
        ResourceId catalogId, ResourceId namespaceId, String displayName) {
      this(catalogId, namespaceId, displayName, "", "", null);
    }
  }

  record DestinationViewMetadata(
      ResourceId catalogId,
      ResourceId namespaceId,
      String displayName,
      String sourceNamespace,
      String sourceName,
      ResourceId sourceConnectorId) {
    public DestinationViewMetadata(
        ResourceId catalogId, ResourceId namespaceId, String displayName) {
      this(catalogId, namespaceId, displayName, "", "", null);
    }
  }

  record TableSpecDescriptor(
      String namespaceFq,
      String displayName,
      String schemaJson,
      Map<String, String> properties,
      List<String> partitionKeys,
      ColumnIdAlgorithm columnIdAlgorithm,
      ConnectorFormat connectorFormat,
      ResourceId connectorId,
      String connectorUri,
      String sourceNamespace,
      String sourceTable) {}
}
