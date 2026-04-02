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
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
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
    Snapshot result = service.buildSnapshot(ctx, tableId, bundle, existing).orElseThrow();

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
  void effectiveSelectorsPreferScopeColumnsOverConnectorSourceColumns() {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of("barf", "#2"));
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = ReconcilerService.effectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("barf", "#2");
  }

  @Test
  void effectiveSelectorsFallbackToConnectorSourceColumnsWhenScopeHasNoColumns() {
    ReconcileScope scope = ReconcileScope.of(List.of(List.of("ns")), "tbl", List.of());
    SourceSelector source = SourceSelector.newBuilder().addColumns("i").addColumns("#1").build();

    Set<String> selectors = ReconcilerService.effectiveSelectors(scope, source);

    assertThat(selectors).containsExactlyInAnyOrder("i", "#1");
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
        service.filterBundlesForMode(
            bundles, false, false, Set.of(10L, 12L), Set.of(), (s, c, e, sp, stp, m) -> {});

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
        service.filterBundlesForMode(
            bundles, true, false, Set.of(10L, 12L), Set.of(), (s, c, e, sp, stp, m) -> {});

    assertThat(filtered)
        .extracting(ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(10L, 11L);
  }

  @Test
  void filterBundlesForModeSkipsIncrementalPruningWhenSkipExistingSnapshotsEnabled() {
    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> bundles =
        List.of(
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                10L, 0L, 1L, "", null, 0L, null, Map.of(), 0, Map.of()),
            new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                11L, 10L, 2L, "", null, 0L, null, Map.of(), 0, Map.of()));

    List<ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle> filtered =
        service.filterBundlesForMode(
            bundles, false, true, Set.of(10L), Set.of(), (s, c, e, sp, stp, m) -> {});

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
        service.filterBundlesForMode(
            bundles, false, false, Set.of(11L), Set.of(11L, 12L), (s, c, e, sp, stp, m) -> {});

    assertThat(filtered)
        .extracting(ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle::snapshotId)
        .containsExactly(12L);
  }

  @Test
  void knownSnapshotIdsForEnumerationPrunesAllIncrementalRuns() {
    Set<Long> metadataOnly =
        ReconcilerService.knownSnapshotIdsForEnumeration(false, Set.of(10L, 11L));
    Set<Long> metadataAndStats =
        ReconcilerService.knownSnapshotIdsForEnumeration(false, Set.of(10L, 11L));
    Set<Long> statsOnly = ReconcilerService.knownSnapshotIdsForEnumeration(false, Set.of(10L, 11L));
    Set<Long> fullRescan = ReconcilerService.knownSnapshotIdsForEnumeration(true, Set.of(10L, 11L));

    assertThat(metadataOnly).containsExactlyInAnyOrder(10L, 11L);
    assertThat(metadataAndStats).containsExactlyInAnyOrder(10L, 11L);
    assertThat(statsOnly).containsExactlyInAnyOrder(10L, 11L);
    assertThat(fullRescan).isEmpty();
  }

  @Test
  void tableChangedIsFalseWhenNoBundlesRemain() {
    assertThat(ReconcilerService.tableChanged(List.of())).isFalse();
  }

  @Test
  void tableChangedIsTrueWhenBundlesRemain() {
    assertThat(
            ReconcilerService.tableChanged(
                List.of(
                    new ai.floedb.floecat.connector.spi.FloecatConnector.SnapshotBundle(
                        11L, 10L, 2L, "", null, 0L, null, Map.of(), 0, Map.of()))))
        .isTrue();
  }
}
