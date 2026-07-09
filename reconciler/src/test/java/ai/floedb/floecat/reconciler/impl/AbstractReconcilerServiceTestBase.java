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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractReconcilerServiceTestBase {
  protected ReconcilerService service;
  protected LogicalSchemaMapper schemaMapper;
  protected PrincipalContext principal;
  protected ResourceId connectorId;

  @BeforeEach
  void setUp() {
    service = new ReconcilerService();
    schemaMapper = new LogicalSchemaMapper();
    service.credentialResolver = new InMemoryCredentialResolver();
    principal =
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr-id").build();
    connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
  }

  protected QueuedReconcileWorkerSupport queuedWorkerSupport() {
    QueuedReconcileWorkerSupport support = new QueuedReconcileWorkerSupport();
    support.reconcilerService = service;
    support.backend = service.backend;
    support.schemaMapper = schemaMapper;
    return support;
  }

  protected Connector activeConnector() {
    ResourceId destCatalogId = ResourceId.newBuilder().setAccountId("acct").setId("cat-1").build();
    ResourceId destNamespaceId = ResourceId.newBuilder().setAccountId("acct").setId("ns-1").build();
    return Connector.newBuilder()
        .setResourceId(connectorId)
        .setState(ConnectorState.CS_ACTIVE)
        .setKind(ConnectorKind.CK_DELTA)
        .setSource(
            SourceSelector.newBuilder()
                .setNamespace(
                    NamespacePath.newBuilder().addSegments("src_cat").addSegments("src_ns")))
        .setDestination(
            DestinationTarget.newBuilder()
                .setCatalogId(destCatalogId)
                .setNamespaceId(destNamespaceId))
        .build();
  }

  protected static StatsCaptureControlPlane capturedControlPlane(AtomicInteger captureCalls) {
    return capturedControlPlane(captureCalls, null);
  }

  protected static StatsCaptureControlPlane capturedControlPlane(
      AtomicInteger captureCalls, AtomicReference<StatsCaptureRequest> capturedRequest) {
    return new StatsCaptureControlPlane() {
      @Override
      public StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
        List<StatsCaptureBatchItemResult> items = new ArrayList<>();
        for (StatsCaptureRequest request : batchRequest.requests()) {
          if (capturedRequest != null) {
            capturedRequest.set(request);
          }
          captureCalls.incrementAndGet();
          items.add(StatsCaptureBatchItemResult.captured(request, capturedResult(request)));
        }
        return StatsCaptureBatchResult.of(items);
      }
    };
  }

  private static StatsCaptureResult capturedResult(StatsCaptureRequest request) {
    return StatsCaptureResult.forRecord(
        "test-engine",
        TargetStatsRecord.newBuilder()
            .setTableId(request.tableId())
            .setSnapshotId(request.snapshotId())
            .setTarget(StatsTargetIdentity.tableTarget())
            .setTable(
                ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder().setRowCount(1L).build())
            .build(),
        Map.of());
  }

  protected static final class ThrowingBackend extends DefaultBackend {
    private final RuntimeException failure;

    protected ThrowingBackend(RuntimeException failure) {
      this.failure = failure;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      throw failure;
    }
  }

  protected static final class ReturningBackend extends DefaultBackend {
    private final Connector connector;

    protected ReturningBackend(Connector connector) {
      this.connector = connector;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      return connector;
    }
  }

  protected abstract static class DefaultBackend implements ReconcilerBackend {
    @Override
    public ResourceId ensureNamespace(
        ReconcileContext ctx, ResourceId catalogId, NameRef namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ResourceId ensureTable(
        ReconcileContext ctx,
        ResourceId namespaceId,
        NameRef table,
        TableSpecDescriptor descriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean updateTableById(
        ReconcileContext ctx,
        ResourceId tableId,
        ResourceId namespaceId,
        NameRef table,
        TableSpecDescriptor descriptor) {
      return false;
    }

    @Override
    public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef view) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
      return tableId == null || tableId.getId().isBlank() ? Optional.empty() : Optional.of("tbl");
    }

    @Override
    public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
        ReconcileContext ctx, ResourceId tableId) {
      if (tableId == null || tableId.getId().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(
          new DestinationTableMetadata(
              ResourceId.newBuilder()
                  .setAccountId(tableId.getAccountId())
                  .setKind(ResourceKind.RK_CATALOG)
                  .setId("cat-1")
                  .build(),
              ResourceId.newBuilder()
                  .setAccountId(tableId.getAccountId())
                  .setKind(ResourceKind.RK_NAMESPACE)
                  .setId("ns-1")
                  .build(),
              "tbl"));
    }

    @Override
    public Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
      return viewId == null || viewId.getId().isBlank() ? Optional.empty() : Optional.of("view");
    }

    @Override
    public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
        ReconcileContext ctx, ResourceId viewId) {
      if (viewId == null || viewId.getId().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(
          new DestinationViewMetadata(
              ResourceId.newBuilder()
                  .setAccountId(viewId.getAccountId())
                  .setKind(ResourceKind.RK_CATALOG)
                  .setId("cat-1")
                  .build(),
              ResourceId.newBuilder()
                  .setAccountId(viewId.getAccountId())
                  .setKind(ResourceKind.RK_NAMESPACE)
                  .setId("ns-1")
                  .build(),
              "view"));
    }

    @Override
    public SnapshotPin snapshotPinFor(
        ReconcileContext ctx,
        ResourceId tableId,
        ai.floedb.floecat.common.rpc.SnapshotRef ref,
        Optional<com.google.protobuf.Timestamp> asOf) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Snapshot> fetchSnapshot(
        ReconcileContext ctx, ResourceId tableId, long snapshotId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
      return Set.of();
    }

    @Override
    public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean statsAlreadyCapturedForTargetKind(
        ReconcileContext ctx,
        ResourceId tableId,
        long snapshotId,
        ai.floedb.floecat.catalog.rpc.StatsTargetKind targetKind) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean statsCapturedForColumnSelectors(
        ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> selectors) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean statsCapturedForTargets(
        ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<StatsTarget> targets) {
      return false;
    }

    @Override
    public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean indexArtifactsCapturedForFilePaths(
        ReconcileContext ctx,
        ResourceId tableId,
        long snapshotId,
        List<String> filePaths,
        Set<String> selectors) {
      return false;
    }

    @Override
    public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
      if (catalogId == null || catalogId.getId().isBlank()) {
        throw new IllegalArgumentException("catalogId is required");
      }
      return "cat-1";
    }

    @Override
    public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateConnectorDestination(
        ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ReconcilerBackend.ViewMutationResult ensureView(
        ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec) {
      throw new UnsupportedOperationException();
    }
  }

  protected static final class InMemoryCredentialResolver implements CredentialResolver {
    private final Map<String, AuthCredentials> store = new HashMap<>();

    @Override
    public Optional<AuthCredentials> resolve(String accountId, String credentialId) {
      return Optional.ofNullable(store.get(key(accountId, credentialId)));
    }

    @Override
    public void store(String accountId, String credentialId, AuthCredentials credentials) {
      store.put(key(accountId, credentialId), credentials);
    }

    @Override
    public void delete(String accountId, String credentialId) {
      store.remove(key(accountId, credentialId));
    }

    private String key(String accountId, String credentialId) {
      return accountId + ":" + credentialId;
    }
  }

  protected static class ViewCapturingBackend extends DefaultBackend {
    private final Connector connector;
    final List<ViewSpec> capturedViews = new ArrayList<>();
    final List<String> capturedIdempotencyKeys = new ArrayList<>();

    protected ViewCapturingBackend(Connector connector) {
      this.connector = connector;
    }

    @Override
    public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
      return connector;
    }

    @Override
    public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
      return "dest_cat";
    }

    @Override
    public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
      return "dest_ns";
    }

    @Override
    public ReconcilerBackend.ViewMutationResult ensureView(
        ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
      capturedViews.add(spec);
      capturedIdempotencyKeys.add(idempotencyKey);
      return new ReconcilerBackend.ViewMutationResult(
          ResourceId.newBuilder().setId("view-1").build(), true);
    }

    @Override
    public boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec) {
      capturedViews.add(spec);
      capturedIdempotencyKeys.add(viewId.getId());
      return true;
    }

    @Override
    public Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
      return viewId == null || viewId.getId().isBlank()
          ? Optional.empty()
          : Optional.of("revenue_view");
    }

    @Override
    public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
        ReconcileContext ctx, ResourceId viewId) {
      if (viewId == null || viewId.getId().isBlank()) {
        return Optional.empty();
      }
      return Optional.of(
          new DestinationViewMetadata(
              connector.getDestination().getCatalogId(),
              connector.getDestination().getNamespaceId(),
              "revenue_view"));
    }

    @Override
    public void updateConnectorDestination(
        ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
      // no-op
    }
  }

  protected static class FakeConnector implements FloecatConnector {
    private final List<FloecatConnector.ViewDescriptor> viewDescriptors;

    protected FakeConnector(List<FloecatConnector.ViewDescriptor> viewDescriptors) {
      this.viewDescriptors = viewDescriptors;
    }

    @Override
    public String id() {
      return "fake";
    }

    @Override
    public ConnectorFormat format() {
      return ConnectorFormat.CF_DELTA;
    }

    @Override
    public List<String> listNamespaces() {
      return List.of();
    }

    @Override
    public List<String> listTables(String namespaceFq) {
      return List.of();
    }

    @Override
    public TableDescriptor describe(String namespaceFq, String tableName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<SnapshotBundle> enumerateSnapshots(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        SnapshotEnumerationOptions options) {
      return List.of();
    }

    @Override
    public List<TargetStatsRecord> captureSnapshotTargetStats(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        long snapshotId,
        Set<String> includeColumns) {
      return List.of();
    }

    @Override
    public FileGroupCaptureResult capturePlannedFileGroup(
        String namespaceFq,
        String tableName,
        ResourceId destinationTableId,
        long snapshotId,
        Set<String> plannedFilePaths,
        Set<String> includeColumns,
        Set<StatsTargetKind> includeTargetKinds,
        boolean captureIndexes) {
      return FileGroupCaptureResult.empty();
    }

    @Override
    public List<FloecatConnector.ViewDescriptor> listViewDescriptors(String namespaceFq) {
      return viewDescriptors;
    }

    @Override
    public Optional<FloecatConnector.ViewDescriptor> describeView(String namespaceFq, String name) {
      return viewDescriptors.stream()
          .filter(view -> namespaceFq.equals(view.namespaceFq()))
          .filter(view -> name.equals(view.name()))
          .findFirst();
    }

    @Override
    public void close() {}
  }
}
