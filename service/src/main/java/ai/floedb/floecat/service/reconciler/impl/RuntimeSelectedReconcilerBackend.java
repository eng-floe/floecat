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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
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
  public ResourceId ensureTable(
      ReconcileContext ctx, ResourceId namespaceId, NameRef table, TableSpecDescriptor descriptor) {
    return delegate.ensureTable(ctx, namespaceId, table, descriptor);
  }

  @Override
  public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
    return delegate.lookupTable(ctx, table);
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
  public boolean statsAlreadyCaptured(ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    return delegate.statsAlreadyCaptured(ctx, tableId, snapshotId);
  }

  @Override
  public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
    delegate.putTargetStats(ctx, stats);
  }

  @Override
  public void putSnapshotConstraints(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, SnapshotConstraints constraints) {
    delegate.putSnapshotConstraints(ctx, tableId, snapshotId, constraints);
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
  public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
    return delegate.ensureView(ctx, spec, idempotencyKey);
  }
}
