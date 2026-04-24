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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.impl.GrpcReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class RuntimeSelectedReconcilerBackend implements ReconcilerBackend {
  private final ReconcilerBackend delegate;

  @Inject
  public RuntimeSelectedReconcilerBackend(
      DirectReconcilerBackend directBackend,
      GrpcReconcilerBackend grpcBackend,
      @ConfigProperty(name = "floecat.reconciler.backend", defaultValue = "local") String backend) {
    this.delegate =
        "remote".equalsIgnoreCase(backend == null ? "" : backend.trim())
            ? grpcBackend
            : directBackend;
  }

  @Override
  public ResourceId ensureNamespace(ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
    return delegate.ensureNamespace(ctx, catalogId, namespace);
  }

  @Override
  public Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespace) {
    return delegate.lookupNamespace(ctx, namespace);
  }

  @Override
  public ResourceId ensureTable(
      ReconcileContext ctx, ResourceId namespaceId, NameRef table, TableSpecDescriptor descriptor) {
    return delegate.ensureTable(ctx, namespaceId, table, descriptor);
  }

  @Override
  public boolean updateTableById(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId namespaceId,
      NameRef table,
      TableSpecDescriptor descriptor) {
    return delegate.updateTableById(ctx, tableId, namespaceId, table, descriptor);
  }

  @Override
  public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
    return delegate.lookupTable(ctx, table);
  }

  @Override
  public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
    return delegate.lookupTableDisplayName(ctx, tableId);
  }

  @Override
  public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
      ReconcileContext ctx, ResourceId tableId) {
    return delegate.lookupDestinationTableMetadata(ctx, tableId);
  }

  @Override
  public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view) {
    return delegate.lookupView(ctx, view);
  }

  @Override
  public Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
    return delegate.lookupViewDisplayName(ctx, viewId);
  }

  @Override
  public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
      ReconcileContext ctx, ResourceId viewId) {
    return delegate.lookupDestinationViewMetadata(ctx, viewId);
  }

  @Override
  public SnapshotPin snapshotPinFor(
      ReconcileContext ctx, ResourceId tableId, SnapshotRef ref, Optional<Timestamp> asOf) {
    return delegate.snapshotPinFor(ctx, tableId, ref, asOf);
  }

  @Override
  public Optional<Snapshot> fetchSnapshot(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    return delegate.fetchSnapshot(ctx, tableId, snapshotId);
  }

  @Override
  public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
    return delegate.existingSnapshotIds(ctx, tableId);
  }

  @Override
  public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
    delegate.ingestSnapshot(ctx, tableId, snapshot);
  }

  @Override
  public Optional<FloecatConnector.SnapshotFilePlan> fetchSnapshotFilePlan(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    return delegate.fetchSnapshotFilePlan(ctx, tableId, snapshotId);
  }

  @Override
  public List<TargetStatsRecord> capturePlannedFileGroupStats(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, List<String> plannedFilePaths) {
    return delegate.capturePlannedFileGroupStats(ctx, tableId, snapshotId, plannedFilePaths);
  }

  @Override
  public List<FloecatConnector.ParquetPageIndexEntry> capturePlannedFileGroupPageIndexEntries(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, List<String> plannedFilePaths) {
    return delegate.capturePlannedFileGroupPageIndexEntries(
        ctx, tableId, snapshotId, plannedFilePaths);
  }

  @Override
  public List<IndexArtifactRecord> materializePlannedFileGroupIndexArtifacts(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      List<String> plannedFilePaths,
      List<TargetStatsRecord> stats) {
    return delegate.materializePlannedFileGroupIndexArtifacts(
        ctx, tableId, snapshotId, plannedFilePaths, stats);
  }

  @Override
  public List<IndexArtifactRecord> materializePlannedFileGroupIndexArtifacts(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      List<String> plannedFilePaths,
      List<TargetStatsRecord> stats,
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    return delegate.materializePlannedFileGroupIndexArtifacts(
        ctx, tableId, snapshotId, plannedFilePaths, stats, pageIndexEntries);
  }

  @Override
  public boolean statsAlreadyCapturedForTargetKind(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, StatsTargetKind targetKind) {
    return delegate.statsAlreadyCapturedForTargetKind(ctx, tableId, snapshotId, targetKind);
  }

  @Override
  public boolean statsCapturedForColumnSelectors(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> selectors) {
    return delegate.statsCapturedForColumnSelectors(ctx, tableId, snapshotId, selectors);
  }

  @Override
  public boolean statsCapturedForTargets(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<StatsTarget> targets) {
    return delegate.statsCapturedForTargets(ctx, tableId, snapshotId, targets);
  }

  @Override
  public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
    delegate.putTargetStats(ctx, stats);
  }

  @Override
  public void putIndexArtifacts(ReconcileContext ctx, List<IndexArtifactRecord> artifacts) {
    delegate.putIndexArtifacts(ctx, artifacts);
  }

  @Override
  public boolean putSnapshotConstraints(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, SnapshotConstraints constraints) {
    return delegate.putSnapshotConstraints(ctx, tableId, snapshotId, constraints);
  }

  @Override
  public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
    return delegate.lookupCatalogName(ctx, catalogId);
  }

  @Override
  public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
    return delegate.resolveNamespaceFq(ctx, namespaceId);
  }

  @Override
  public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
    return delegate.lookupConnector(ctx, connectorId);
  }

  @Override
  public void updateConnectorDestination(
      ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
    delegate.updateConnectorDestination(ctx, connectorId, destination);
  }

  @Override
  public ViewMutationResult ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
    return delegate.ensureView(ctx, spec, idempotencyKey);
  }

  @Override
  public boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec) {
    return delegate.updateViewById(ctx, viewId, spec);
  }
}
