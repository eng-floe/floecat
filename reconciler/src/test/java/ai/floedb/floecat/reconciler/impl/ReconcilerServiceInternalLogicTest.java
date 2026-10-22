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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotSelection;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationTableMetadata;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.schema.identity.HistoryTier;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.PreferredCanonicalIds;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcilerServiceInternalLogicTest extends AbstractReconcilerServiceTestBase {

  private static ColumnIdentityMap emptyIdentityMap(long sourceVersion) {
    return ColumnIdentityMaps.toProto(
        SchemaIdentityReconciler.reconcile(
            ResolvedSchema.of(List.of()),
            sourceVersion,
            IdentityMode.STRUCTURED_PATH,
            Optional.empty(),
            0L,
            PreferredCanonicalIds.NONE,
            HistoryTier.VERSIONED_LOG));
  }

  @Test
  void canonicalDeltaTableCannotInheritAnExistingMapWhenBundleOmitsIt() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    Snapshot existing =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .setColumnIdentityMapRef(
                ColumnIdentityMaps.reference(
                    "/accounts/acct/tables/tbl/column-identities/old.pb", emptyIdentityMap(123L)))
            .build();
    var bundle =
        new FloecatConnector.SnapshotBundle(
            123L,
            122L,
            Instant.now().toEpochMilli(),
            "{}",
            null,
            0L,
            null,
            Map.of(),
            0,
            null,
            ColumnIdentityMap.getDefaultInstance());
    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());

    assertThatThrownBy(
            () ->
                queuedWorkerSupport()
                    .buildSnapshot(
                        ctx,
                        tableId,
                        QueuedReconcileWorkerSupport.IdentityEnumerationContext.canonical(),
                        bundle,
                        existing))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("without a column identity map");
  }

  @Test
  void canonicalDeltaTableCannotPersistAnInvalidIdentityMap() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    ColumnIdentityMap invalidMap =
        ColumnIdentityMap.newBuilder()
            .setFormatVersion(ColumnIdentityMaps.FORMAT_VERSION)
            .setSourceVersion(123L)
            .setHistoryTier(
                ai.floedb.floecat.catalog.rpc.ColumnIdentityHistoryTier
                    .COLUMN_IDENTITY_HISTORY_TIER_VERSIONED_LOG)
            .setMode(
                ai.floedb.floecat.catalog.rpc.ColumnIdentityMode
                    .COLUMN_IDENTITY_MODE_STRUCTURED_PATH)
            .setFingerprint("sha256:tampered")
            .setStateChecksum("sha256:tampered")
            .build();
    var bundle =
        new FloecatConnector.SnapshotBundle(
            123L,
            122L,
            Instant.now().toEpochMilli(),
            "{}",
            null,
            0L,
            null,
            Map.of(),
            0,
            null,
            invalidMap);
    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());

    assertThatThrownBy(
            () ->
                queuedWorkerSupport()
                    .buildSnapshot(
                        ctx,
                        tableId,
                        QueuedReconcileWorkerSupport.IdentityEnumerationContext.canonical(),
                        bundle,
                        null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("fingerprint does not match");
  }

  @Test
  void deltaFormatSelectsCanonicalIdentityAlgorithm() {
    FloecatConnector.TableDescriptor descriptor =
        new FloecatConnector.TableDescriptor(
            "ns",
            "tbl",
            "s3://table",
            "{}",
            List.of(),
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            Map.of());
    assertThat(
            QueuedReconcileWorkerSupport.withFormatCanonicalIdentity(deltaConnector(), descriptor)
                .columnIdAlgorithm())
        .isEqualTo(ColumnIdAlgorithm.CID_CANONICAL_MAP);
  }

  @Test
  void canonicalDeltaDefaultsToCurrentSnapshotWithoutOverridingExplicitHistory() {
    assertThat(
            QueuedReconcileWorkerSupport.effectiveEnumerationSelection(
                true, ReconcileSnapshotSelection.unspecified()))
        .isEqualTo(ReconcileSnapshotSelection.current());
    assertThat(
            QueuedReconcileWorkerSupport.effectiveEnumerationSelection(
                true, ReconcileSnapshotSelection.all()))
        .isEqualTo(ReconcileSnapshotSelection.all());
    assertThat(
            QueuedReconcileWorkerSupport.effectiveEnumerationSelection(
                false, ReconcileSnapshotSelection.unspecified()))
        .isEqualTo(ReconcileSnapshotSelection.unspecified());
  }

  private static FloecatConnector deltaConnector() {
    FloecatConnector connector = mock(FloecatConnector.class);
    when(connector.format()).thenReturn(ConnectorFormat.CF_DELTA);
    return connector;
  }

  @Test
  void buildSnapshotRetainsExistingDataWhenBundleOmitsFields() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    Snapshot existing =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .setSchemaJson("existing-schema")
            .setManifestList("existing-manifest")
            .putSummary("existing-key", "existing-val")
            .setMetadataLocation("s3://bucket/old.metadata.json")
            .build();

    var bundle =
        new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
            existing.getSnapshotId(),
            -1L,
            Instant.now().toEpochMilli(),
            "",
            null,
            0L,
            null,
            Map.of("new-key", "new-val"),
            0,
            "s3://bucket/new.metadata.json",
            ColumnIdentityMap.getDefaultInstance());

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.<String>empty());
    Snapshot result =
        queuedWorkerSupport()
            .buildSnapshot(
                ctx,
                tableId,
                QueuedReconcileWorkerSupport.IdentityEnumerationContext.disabled(Set.of()),
                bundle,
                existing)
            .orElseThrow();

    assertThat(result.getManifestList()).isEqualTo(existing.getManifestList());
    assertThat(result.getSchemaJson()).isEqualTo(existing.getSchemaJson());
    assertThat(result.getSummaryMap()).containsEntry("existing-key", "existing-val");
    assertThat(result.getSummaryMap()).containsEntry("new-key", "new-val");
    assertThat(result.getMetadataLocation()).isEqualTo("s3://bucket/new.metadata.json");
  }

  @Test
  void buildSnapshotPreservesZeroAsExplicitParentSnapshotId() {
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();
    var bundle =
        new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
            1L,
            0L,
            Instant.now().toEpochMilli(),
            "{}",
            null,
            0L,
            null,
            Map.of(),
            0,
            null,
            ColumnIdentityMap.getDefaultInstance());
    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());

    Snapshot result =
        queuedWorkerSupport()
            .buildSnapshot(
                ctx,
                tableId,
                QueuedReconcileWorkerSupport.IdentityEnumerationContext.disabled(Set.of()),
                bundle,
                null)
            .orElseThrow();

    assertThat(result.hasParentSnapshotId()).isTrue();
    assertThat(result.getParentSnapshotId()).isZero();
  }

  @Test
  void activeConnectorRehydratesStoredCredentialsIntoResolvedConfig() {
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("s3://bucket/table/metadata/00001.metadata.json")
            .putProperties("iceberg.source", "filesystem")
            .setAuth(AuthConfig.newBuilder().setScheme("aws-sigv4"))
            .build();
    ((InMemoryCredentialResolver) service.credentialResolver)
        .store(
            connectorId.getAccountId(),
            connectorId.getId(),
            AuthCredentials.newBuilder()
                .setAws(
                    AuthCredentials.AwsCredentials.newBuilder()
                        .setAccessKeyId("access-key")
                        .setSecretAccessKey("secret-key")
                        .setSessionToken("session-token"))
                .build());
    service.backend = new ReturningBackend(connector);

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());
    try (ReconcilerService.ActiveConnector active =
        service.activeConnectorForResult(ctx, connectorId)) {
      assertThat(active.config().options())
          .doesNotContainKeys("s3.access-key-id", "s3.secret-access-key", "s3.session-token");
      assertThat(active.resolvedConfig().options())
          .containsEntry("s3.access-key-id", "access-key")
          .containsEntry("s3.secret-access-key", "secret-key")
          .containsEntry("s3.session-token", "session-token");
    }
  }

  @Test
  void activeConnectorPassesFilesystemIcebergMetadataLocationForServerSideResolution() {
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("s3://bucket/table/metadata/00001.metadata.json")
            .putProperties("iceberg.source", "filesystem")
            .setAuth(AuthConfig.newBuilder().setScheme("aws-sigv4"))
            .build();
    ServerSideStorageConfigResolver storageResolver = mock(ServerSideStorageConfigResolver.class);
    service.serverSideStorageConfigResolver = storageResolver;
    service.backend = new ReturningBackend(connector);
    when(storageResolver.resolveManagedWithAuthorization(
            any(), any(), any(), any(), any(), eq(connector), any(ConnectorConfig.class)))
        .thenAnswer(
            invocation ->
                new ServerSideStorageConfigResolver.ResolvedConnectorConfig(
                    invocation.getArgument(6), () -> {}));

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());
    try (var ignored = service.activeConnectorForResult(ctx, connectorId)) {
      // The managed storage configuration remains live for the active connector lifetime.
    }

    @SuppressWarnings("unchecked")
    var locationCaptor = org.mockito.ArgumentCaptor.forClass(Optional.class);
    verify(storageResolver)
        .resolveManagedWithAuthorization(
            any(),
            any(),
            any(),
            locationCaptor.capture(),
            eq(Optional.empty()),
            eq(connector),
            any());
    assertThat(locationCaptor.getValue())
        .contains("s3://bucket/table/metadata/00001.metadata.json");
  }

  @Test
  void tableScopedConfigPinsFilesystemIcebergToCommittedMetadataLocation() {
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setState(ConnectorState.CS_ACTIVE)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri("s3://bucket/table")
            .putProperties("iceberg.source", "filesystem")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    service.backend =
        new DefaultBackend() {
          @Override
          public Connector lookupConnector(ReconcileContext ctx, ResourceId requestedConnectorId) {
            return connector;
          }

          @Override
          public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
              ReconcileContext ctx, ResourceId requestedTableId) {
            return Optional.of(
                new DestinationTableMetadata(
                    ResourceId.newBuilder()
                        .setAccountId(requestedTableId.getAccountId())
                        .setKind(ResourceKind.RK_CATALOG)
                        .setId("cat-1")
                        .build(),
                    ResourceId.newBuilder()
                        .setAccountId(requestedTableId.getAccountId())
                        .setKind(ResourceKind.RK_NAMESPACE)
                        .setId("ns-1")
                        .build(),
                    "orders",
                    "iceberg",
                    "orders",
                    connectorId,
                    "s3://bucket/table",
                    "s3://bucket/table/metadata/00001-committed.metadata.json"));
          }
        };

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.empty());
    try (ReconcilerService.ActiveConnector active =
            service.activeConnectorForResult(ctx, connectorId);
        var scoped = service.tableScopedResolvedConfig(ctx, active, tableId)) {
      assertThat(scoped.config().options())
          .containsEntry(
              "metadata-location", "s3://bucket/table/metadata/00001-committed.metadata.json");
    }
  }
}
