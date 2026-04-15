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
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
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
  protected PrincipalContext principal;
  protected ResourceId connectorId;

  @BeforeEach
  void setUp() {
    service = new ReconcilerService();
    service.schemaMapper = new LogicalSchemaMapper();
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
      public StatsTriggerResult trigger(StatsCaptureRequest request) {
        if (capturedRequest != null) {
          capturedRequest.set(request);
        }
        captureCalls.incrementAndGet();
        return capturedResult(request);
      }

      @Override
      public StatsCaptureBatchResult triggerBatch(StatsCaptureBatchRequest batchRequest) {
        List<StatsCaptureBatchItemResult> items = new ArrayList<>();
        for (StatsCaptureRequest request : batchRequest.requests()) {
          StatsTriggerResult result = trigger(request);
          items.add(
              StatsCaptureBatchItemResult.captured(
                  request,
                  result
                      .captureResult()
                      .orElseThrow(
                          () ->
                              new IllegalStateException(
                                  "captured trigger missing capture result"))));
        }
        return StatsCaptureBatchResult.of(items);
      }
    };
  }

  private static StatsTriggerResult capturedResult(StatsCaptureRequest request) {
    return StatsTriggerResult.captured(
        StatsCaptureResult.forRecord(
            "test-engine",
            TargetStatsRecord.newBuilder()
                .setTableId(request.tableId())
                .setSnapshotId(request.snapshotId())
                .setTarget(StatsTargetIdentity.tableTarget())
                .setTable(
                    ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder()
                        .setRowCount(1L)
                        .build())
                .build(),
            Map.of()));
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
    public ResourceId ensureTable(
        ReconcileContext ctx,
        ResourceId namespaceId,
        NameRef table,
        TableSpecDescriptor descriptor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef table) {
      throw new UnsupportedOperationException();
    }

    public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
      throw new UnsupportedOperationException();
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
    public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
      throw new UnsupportedOperationException();
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
    public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
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
    public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
      capturedViews.add(spec);
      capturedIdempotencyKeys.add(idempotencyKey);
      return ResourceId.newBuilder().setId("view-1").build();
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
