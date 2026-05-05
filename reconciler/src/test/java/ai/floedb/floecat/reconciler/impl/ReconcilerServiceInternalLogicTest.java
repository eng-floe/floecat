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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReconcilerServiceInternalLogicTest extends AbstractReconcilerServiceTestBase {

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
            .putFormatMetadata("meta-key", ByteString.copyFromUtf8("old"))
            .build();

    var bundle =
        new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
            existing.getSnapshotId(),
            existing.getParentSnapshotId(),
            Instant.now().toEpochMilli(),
            "",
            null,
            0L,
            null,
            Map.of("new-key", "new-val"),
            0,
            Map.of(
                "meta-key", ByteString.copyFromUtf8("new"),
                "extra", ByteString.copyFromUtf8("value")));

    ReconcileContext ctx =
        new ReconcileContext("ctx", principal, "svc-test", Instant.now(), Optional.<String>empty());
    Snapshot result =
        queuedWorkerSupport().buildSnapshot(ctx, tableId, bundle, existing).orElseThrow();

    assertThat(result.getManifestList()).isEqualTo(existing.getManifestList());
    assertThat(result.getSchemaJson()).isEqualTo(existing.getSchemaJson());
    assertThat(result.getSummaryMap()).containsEntry("existing-key", "existing-val");
    assertThat(result.getSummaryMap()).containsEntry("new-key", "new-val");
    assertThat(result.getFormatMetadataMap())
        .containsEntry("meta-key", ByteString.copyFromUtf8("new"));
    assertThat(result.getFormatMetadataMap())
        .containsEntry("extra", ByteString.copyFromUtf8("value"));
  }

  @Test
  void filterBundlesForModeSkipsAlreadyIngestedSnapshotsForIncremental() {
    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                12L, 11L, 3L, "", null, 0L, null, Map.of(), 0, Map.of()));

    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, false, false, Set.of(10L, 12L), (ts, tc, vs, vc, e, sp, stp, m) -> {});

    assertThat(filtered)
        .extracting(ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(11L);
  }

  @Test
  void filterBundlesForModeKeepsAllSnapshotsForFullRescan() {
    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, "", null, 0L, null, Map.of(), 0, Map.of()));

    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, true, false, Set.of(10L, 12L), (ts, tc, vs, vc, e, sp, stp, m) -> {});

    assertThat(filtered)
        .extracting(ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void filterBundlesForModeAppliesIncrementalPruningWithinExplicitSnapshotScope() {
    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                12L, 11L, 3L, "", null, 0L, null, Map.of(), 0, Map.of()));

    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> filtered =
        QueuedReconcileWorkerSupport.filterBundlesForMode(
            bundles, false, false, Set.of(11L), (ts, tc, vs, vc, e, sp, stp, m) -> {});

    assertThat(filtered)
        .extracting(ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 12L);
  }

  @Test
  void knownSnapshotIdsForEnumerationIsStatsAwareForStatsModes() {
    Set<Long> metadataOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, false, Set.of(10L, 11L), snapshotId -> false);
    Set<Long> metadataAndStats =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, true, Set.of(10L, 11L), snapshotId -> snapshotId == 10L);
    Set<Long> statsOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            false, true, Set.of(10L, 11L), snapshotId -> false);
    Set<Long> fullRescan =
        ReconcilerService.knownSnapshotIdsForEnumeration(
            true, true, Set.of(10L, 11L), snapshotId -> true);

    assertThat(metadataOnly).containsExactlyInAnyOrder(10L, 11L);
    assertThat(metadataAndStats).containsExactly(10L);
    assertThat(statsOnly).isEmpty();
    assertThat(fullRescan).isEmpty();
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
    ReconcilerService.ActiveConnector active = service.activeConnectorForResult(ctx, connectorId);

    assertThat(active.config().options())
        .doesNotContainKeys("s3.access-key-id", "s3.secret-access-key", "s3.session-token");
    assertThat(active.resolvedConfig().options())
        .containsEntry("s3.access-key-id", "access-key")
        .containsEntry("s3.secret-access-key", "secret-key")
        .containsEntry("s3.session-token", "session-token");
  }
}
