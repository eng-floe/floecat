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

package ai.floedb.floecat.service.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.TablePin;
import com.google.protobuf.Timestamp;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

class QueryPinsTest {

  private static ResourceId tableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static TablePin current(String id, long snapshotId) {
    return TablePin.newBuilder()
        .setTableId(tableId(id))
        .setPinKind(PinKind.PIN_KIND_CURRENT)
        .setSnapshotId(snapshotId)
        .build();
  }

  private static TablePin explicit(String id, long snapshotId) {
    return TablePin.newBuilder()
        .setTableId(tableId(id))
        .setPinKind(PinKind.PIN_KIND_SNAPSHOT_ID)
        .setSnapshotId(snapshotId)
        .build();
  }

  private static TablePin asOf(String id, long resolvedSnapshotId) {
    return TablePin.newBuilder()
        .setTableId(tableId(id))
        .setPinKind(PinKind.PIN_KIND_AS_OF)
        .setSnapshotId(resolvedSnapshotId)
        .build();
  }

  @Test
  void resolvedAsOfMatchingPhysicalIdentityIsCompatible() {
    // AS_OF is resolved to a concrete snapshot at pin time; a later reference that resolves to the
    // same physical identity (here the same snapshot id) is compatible regardless of pin kind.
    assertThat(QueryPins.compatible(asOf("t", 7), asOf("t", 7))).isTrue();
    assertThat(QueryPins.compatible(explicit("t", 7), asOf("t", 7))).isTrue();
    // A CURRENT incoming reference carries no temporal intent, so it reuses an existing AS_OF pin.
    assertThat(QueryPins.compatible(asOf("t", 7), current("t", 9))).isTrue();
  }

  @Test
  void resolvedAsOfToDifferentSnapshotConflicts() {
    assertThat(QueryPins.compatible(explicit("t", 7), asOf("t", 5))).isFalse();
    assertThat(QueryPins.compatible(asOf("t", 5), explicit("t", 7))).isFalse();
  }

  @Test
  void sameSnapshotDifferentTableBlobVersionStaysCompatible() {
    // Same immutable snapshot, different table blob version (e.g. a concurrent ALTER moved the
    // current table pointer between two references). The table blob version is per-touch
    // provenance,
    // not identity, so this must NOT conflict — first-touch keeps the first pin.
    TablePin existing = explicit("t", 7).toBuilder().setTableBlobVersion("tbl-a").build();
    TablePin incoming = explicit("t", 7).toBuilder().setTableBlobVersion("tbl-b").build();
    assertThat(QueryPins.compatible(existing, incoming)).isTrue();
  }

  @Test
  void currentIncomingAlwaysReusesExistingPin() {
    // A later reference with no temporal intent reuses whatever was pinned first, even if the
    // current catalog state has since advanced to a different snapshot.
    assertThat(QueryPins.compatible(explicit("t", 7), current("t", 9))).isTrue();
    assertThat(QueryPins.compatible(current("t", 7), current("t", 9))).isTrue();
  }

  @Test
  void sameResolvedSnapshotIsCompatible() {
    assertThat(QueryPins.compatible(current("t", 7), explicit("t", 7))).isTrue();
  }

  @Test
  void differentResolvedSnapshotConflicts() {
    assertThat(QueryPins.compatible(current("t", 7), explicit("t", 5))).isFalse();
    assertThat(QueryPins.compatible(explicit("t", 5), explicit("t", 9))).isFalse();
  }

  @Test
  void snapshotZeroIsARealSnapshotId() {
    // Snapshot 0 is a valid snapshot, never an "unresolved" sentinel.
    assertThat(QueryPins.compatible(explicit("t", 0), explicit("t", 0))).isTrue();
    assertThat(QueryPins.compatible(current("t", 0), explicit("t", 5))).isFalse();
    assertThat(QueryPins.compatible(current("t", 5), explicit("t", 0))).isFalse();
  }

  @Test
  void sameSnapshotDifferentBlobVersionConflicts() {
    // Same snapshot id but different resolved physical blob identity is not compatible.
    TablePin existing = explicit("t", 7).toBuilder().setSnapshotBlobVersion("etag-a").build();
    TablePin incoming = explicit("t", 7).toBuilder().setSnapshotBlobVersion("etag-b").build();
    assertThat(QueryPins.compatible(existing, incoming)).isFalse();
  }

  @Test
  void uncapturedBlobVersionCannotProveConflict() {
    // A pin that never captured a blob version (empty etag) cannot manufacture a blob conflict.
    TablePin captured = explicit("t", 7).toBuilder().setSnapshotBlobVersion("etag-a").build();
    TablePin uncaptured = explicit("t", 7); // no blob version
    assertThat(QueryPins.compatible(captured, uncaptured)).isTrue();
  }

  @Test
  void mergeSetsKeepsFirstPinAndAppendsNewTables() {
    RelationPinSet existing =
        RelationPinSet.newBuilder().addPins(QueryPins.ofTable(explicit("a", 1))).build();
    RelationPinSet incoming =
        RelationPinSet.newBuilder()
            .addPins(QueryPins.ofTable(current("a", 9))) // compatible: reuses first
            .addPins(QueryPins.ofTable(explicit("b", 2))) // new table
            .build();

    RelationPinSet merged = QueryPins.mergeSets(existing, incoming, "cid");

    assertThat(merged.getPinsCount()).isEqualTo(2);
    assertThat(QueryPins.findTablePin(merged, tableId("a")).orElseThrow().getSnapshotId())
        .isEqualTo(1);
    assertThat(QueryPins.findTablePin(merged, tableId("a")).orElseThrow().getPinKind())
        .isEqualTo(PinKind.PIN_KIND_SNAPSHOT_ID);
    assertThat(QueryPins.findTablePin(merged, tableId("b")).orElseThrow().getSnapshotId())
        .isEqualTo(2);
  }

  @Test
  void mergeSetsFailsOnIncompatibleTemporalIntent() {
    RelationPinSet existing =
        RelationPinSet.newBuilder().addPins(QueryPins.ofTable(explicit("a", 1))).build();
    RelationPinSet incoming =
        RelationPinSet.newBuilder().addPins(QueryPins.ofTable(explicit("a", 2))).build();

    assertThatThrownBy(() -> QueryPins.mergeSets(existing, incoming, "cid"))
        .isInstanceOf(StatusRuntimeException.class);
  }

  @Test
  void identityFingerprintIsStableAndPhysical() {
    TablePin pin =
        TablePin.newBuilder()
            .setTableId(tableId("t"))
            .setPinKind(PinKind.PIN_KIND_CURRENT)
            .setSnapshotId(42)
            .setTableBlobVersion("etag-t")
            .setSnapshotBlobVersion("etag-s")
            .setTableBlobUri("s3://secret/blob.pb") // must not affect fingerprint
            .build();

    RelationPinIdentity identity = QueryPins.identity(pin);

    assertThat(identity.getPinFingerprint()).isNotEmpty();
    assertThat(identity.getSnapshotId()).isEqualTo(42);
    assertThat(identity.getTableBlobVersion()).isEqualTo("etag-t");
    assertThat(identity.getSnapshotBlobVersion()).isEqualTo("etag-s");
    // Same physical identity → same fingerprint, regardless of server-side blob URI.
    assertThat(QueryPins.identity(pin.toBuilder().setTableBlobUri("s3://other").build()))
        .isEqualTo(identity);
    // pin_kind is provenance, not physical identity: the same physical state reached via AS_OF
    // shares the fingerprint (so it maps to one cache entry, not two).
    assertThat(
            QueryPins.identity(pin.toBuilder().setPinKind(PinKind.PIN_KIND_AS_OF).build())
                .getPinFingerprint())
        .isEqualTo(identity.getPinFingerprint());
    // Different snapshot → different fingerprint.
    assertThat(QueryPins.identity(pin.toBuilder().setSnapshotId(43).build()).getPinFingerprint())
        .isNotEqualTo(identity.getPinFingerprint());
    // The snapshot blob version (immutable data identity) changes the fingerprint.
    assertThat(
            QueryPins.identity(pin.toBuilder().setSnapshotBlobVersion("etag-s2").build())
                .getPinFingerprint())
        .isNotEqualTo(identity.getPinFingerprint());
    // The table blob version is per-touch provenance, not identity: it must NOT change the
    // fingerprint (two references to the same snapshot across an ALTER map to one cache entry).
    assertThat(
            QueryPins.identity(pin.toBuilder().setTableBlobVersion("etag-t2").build())
                .getPinFingerprint())
        .isEqualTo(identity.getPinFingerprint());
  }

  @Test
  void identityCarriesTheConstraintsRefVersionButKeepsItOutOfTheFingerprint() {
    TablePin pin = explicit("t", 7).toBuilder().setSnapshotBlobVersion("etag-s").build();
    TablePin withConstraints = pin.toBuilder().setConstraintsRefVersion("etag-c42").build();

    RelationPinIdentity base = QueryPins.identity(pin); // no bundle at pin time
    RelationPinIdentity versioned = QueryPins.identity(withConstraints);

    assertThat(base.getConstraintsRefVersion()).isEmpty();
    assertThat(versioned.getConstraintsRefVersion()).isEqualTo("etag-c42");
    // The constraints ref version is a separate provenance field, not identity: it must not move
    // the fingerprint, or a constraints write would fragment the schema cache.
    assertThat(versioned.getPinFingerprint()).isEqualTo(base.getPinFingerprint());
  }

  @Test
  void resolvedAsOfProjectsItsSnapshotIdNotTheTimestamp() {
    // A real AS_OF pin resolved to a concrete snapshot blob (URI captured) projects that resolved
    // snapshot id — the original timestamp is provenance only, not what downstream reads use.
    TablePin pin =
        asOf("t", 11).toBuilder()
            .setOriginalAsOf(Timestamp.newBuilder().setSeconds(123).build())
            .setSnapshotBlobUri("s3://t/snap-11.pb")
            .build();

    SnapshotPin projected = QueryPins.toSnapshotPin(pin);
    assertThat(projected.hasAsOf()).isFalse();
    assertThat(projected.getSnapshotId()).isEqualTo(11);
  }

  @Test
  void resolvedAsOfProjectsSnapshotIdEvenWhenBlobVersionIsEmpty() {
    // Keyed on the blob URI, not the version: a resolved pin whose captured version is empty is
    // still resolved and must project its snapshot id, not fall back to the timestamp.
    TablePin pin =
        asOf("t", 7).toBuilder()
            .setOriginalAsOf(Timestamp.newBuilder().setSeconds(456).build())
            .setSnapshotBlobUri("s3://t/snap-7.pb") // URI present, version absent
            .build();

    SnapshotPin projected = QueryPins.toSnapshotPin(pin);
    assertThat(projected.hasAsOf()).isFalse();
    assertThat(projected.getSnapshotId()).isEqualTo(7);
  }

  @Test
  void resolvedAsOfProjectsItsSnapshotId() {
    // Every pin resolves to a concrete snapshot at construction; an AS_OF pin keeps its timestamp
    // only as provenance, so the snapshot-selector projection always carries the snapshot id.
    Timestamp ts = Timestamp.newBuilder().setSeconds(789).build();
    TablePin pin =
        TablePin.newBuilder()
            .setTableId(tableId("t"))
            .setPinKind(PinKind.PIN_KIND_AS_OF)
            .setSnapshotId(42)
            .setSnapshotBlobUri("s3://t/snap-42.pb")
            .setOriginalAsOf(ts)
            .build();

    SnapshotPin projected = QueryPins.toSnapshotPin(pin);
    assertThat(projected.hasSnapshotId()).isTrue();
    assertThat(projected.getSnapshotId()).isEqualTo(42);
    assertThat(projected.hasAsOf()).isFalse();
  }

  @org.junit.jupiter.api.Test
  void gcRootUrisIsTheOneDefinitionForResolvingAndCommittedRoots() {
    // The resolving-window registration and the committed context's GC roots MUST share this
    // definition: omitting the pinned ROOT here once left the whole manifest chain sweepable
    // during the window between pin resolution and context commit.
    var pin =
        ai.floedb.floecat.query.rpc.TablePin.newBuilder()
            .setRootUri("/accounts/a/tables/t/root/r.pb")
            .setTableBlobUri("/accounts/a/tables/t/table/d.pb")
            .setSnapshotBlobUri("/accounts/a/tables/t/snapshots/1/snapshot/s.pb")
            .setConstraintsRefUri("/accounts/a/tables/t/constraints/1/c.pb")
            .setStatsGenerationRefUri("/accounts/a/tables/t/snapshots/1/stats/gen-1/manifest.pb")
            .build();

    org.junit.jupiter.api.Assertions.assertEquals(
        java.util.List.of(
            "/accounts/a/tables/t/root/r.pb",
            "/accounts/a/tables/t/table/d.pb",
            "/accounts/a/tables/t/snapshots/1/snapshot/s.pb",
            "/accounts/a/tables/t/constraints/1/c.pb",
            // The frozen stats generation manifest is a direct pin root: the planner reads it for
            // the query's lifetime, so it must not rely on the GC's chain walk alone.
            "/accounts/a/tables/t/snapshots/1/stats/gen-1/manifest.pb"),
        QueryPins.gcRootUris(pin));

    // Absent legs (no constraints, no root on a hand-built pin) are skipped, never empty strings.
    var bare =
        ai.floedb.floecat.query.rpc.TablePin.newBuilder()
            .setTableBlobUri("/accounts/a/tables/t/table/d.pb")
            .build();
    org.junit.jupiter.api.Assertions.assertEquals(
        java.util.List.of("/accounts/a/tables/t/table/d.pb"), QueryPins.gcRootUris(bare));
  }

  @Test
  void findTablePinRequiresMatchingResourceKind() {
    RelationPinSet pins =
        RelationPinSet.newBuilder().addPins(QueryPins.ofTable(current("a", 4))).build();
    // Identity is account + kind + id (matching pinKey): a non-table id reusing a pinned table's
    // account/id must NOT match the table pin.
    ResourceId asView =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("a")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    assertThat(QueryPins.findTablePin(pins, asView)).isEmpty();
    assertThat(QueryPins.findTablePin(pins, tableId("a")).orElseThrow().getSnapshotId())
        .isEqualTo(4);
  }
}
