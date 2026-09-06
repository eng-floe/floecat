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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMode;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The canonical ID counter must keep advancing across a full rescan.
 *
 * <p>A full rescan deliberately reports an empty "known snapshots" set so every snapshot is
 * re-ingested. Feeding that same set to identity resolution would restart the counter at zero and
 * reissue IDs that already name different columns in previously captured statistics — so identity
 * resolution reads the snapshots that exist, independent of rescan filtering.
 */
class PreviousColumnIdentityMapTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder().setAccountId("acct").setId("table-1").build();

  @Test
  void picksTheHighestNumberedSnapshotThatCarriesAMap() {
    QueuedReconcileWorkerSupport support = support();
    ReconcileContext ctx = mock(ReconcileContext.class);

    when(support.backend.fetchSnapshot(any(), eq(TABLE), eq(3L)))
        .thenReturn(Optional.of(snapshotWithMap(3L, "sha256:newest", 9L)));
    when(support.backend.fetchSnapshot(any(), eq(TABLE), eq(1L)))
        .thenReturn(Optional.of(snapshotWithMap(1L, "sha256:older", 4L)));

    ColumnIdentityMap resolved = support.previousColumnIdentityMap(ctx, TABLE, Set.of(1L, 3L));

    assertThat(resolved.getFingerprint()).isEqualTo("sha256:newest");
    assertThat(resolved.getHighWaterMark()).isEqualTo(9L);
  }

  @Test
  void skipsSnapshotsWithoutAMapRatherThanGivingUp() {
    QueuedReconcileWorkerSupport support = support();
    ReconcileContext ctx = mock(ReconcileContext.class);

    when(support.backend.fetchSnapshot(any(), eq(TABLE), eq(5L)))
        .thenReturn(Optional.of(Snapshot.newBuilder().setSnapshotId(5L).build()));
    when(support.backend.fetchSnapshot(any(), eq(TABLE), eq(2L)))
        .thenReturn(Optional.of(snapshotWithMap(2L, "sha256:carried", 7L)));

    ColumnIdentityMap resolved = support.previousColumnIdentityMap(ctx, TABLE, Set.of(2L, 5L));

    assertThat(resolved.getFingerprint()).isEqualTo("sha256:carried");
    assertThat(resolved.getHighWaterMark()).isEqualTo(7L);
  }

  @Test
  void yieldsNoMapWhenTheTableHasNoSnapshotsAtAll() {
    QueuedReconcileWorkerSupport support = support();

    assertThat(support.previousColumnIdentityMap(mock(ReconcileContext.class), TABLE, Set.of()))
        .isEqualTo(ColumnIdentityMap.getDefaultInstance());
  }

  private static QueuedReconcileWorkerSupport support() {
    QueuedReconcileWorkerSupport support = new QueuedReconcileWorkerSupport();
    support.backend = mock(ReconcilerBackend.class);
    return support;
  }

  private static Snapshot snapshotWithMap(long snapshotId, String fingerprint, long highWaterMark) {
    ColumnIdentityMap map =
        ColumnIdentityMap.newBuilder()
            .setFormatVersion(1)
            .setSourceVersion(snapshotId)
            .setHighWaterMark(highWaterMark)
            .setMode(ColumnIdentityMode.COLUMN_IDENTITY_MODE_STRUCTURED_PATH)
            .setFingerprint(fingerprint)
            .build();
    return Snapshot.newBuilder()
        .setSnapshotId(snapshotId)
        .setColumnIdentityMap(map)
        .setColumnIdentityFingerprint(fingerprint)
        .build();
  }
}
