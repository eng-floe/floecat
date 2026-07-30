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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;

class FileGroupTargetStatsRollupSketchTest {
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acc")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  @Test
  void fileRecordRollup_mergesScalarAndThetaOnly() {
    ScalarStats s1 =
        scalarWithSketches(
            50,
            ndvPayload("apache-datasketches-theta-v1", SketchRole.SKETCH_ROLE_NDV, thetaBytes("a")),
            scalarPayload("apache-datasketches-tdigest-v1", SketchRole.SKETCH_ROLE_QUANTILES));
    ScalarStats s2 =
        scalarWithSketches(
            70,
            ndvPayload("apache-datasketches-theta-v1", SketchRole.SKETCH_ROLE_NDV, thetaBytes("b")),
            scalarPayload("apache-datasketches-frequencies-utf8-v1", SketchRole.SKETCH_ROLE_MCV));

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, s1), fileRecord(1L, s2)));

    ScalarStats merged = onlyScalar(result);
    assertEquals(120L, merged.getRowCount());
    assertEquals(12L, merged.getNullCount());
    assertEquals(8L, merged.getAvgWidthBytes());
    assertTrue(merged.getSketchesList().isEmpty(), "Floecat must not merge scalar sketch payloads");
    assertEquals(2.0, merged.getNdv().getApprox().getEstimate(), 0.01);
    assertTrue(
        merged.getNdv().getSketchesList().stream()
            .anyMatch(s -> s.getSketchType().equals("apache-datasketches-theta-v1")));
  }

  @Test
  void fileRecordRollup_dropsNonOwnedNdvSketchesWhenMultipleSourcesContribute() {
    ScalarStats s1 =
        scalarWithSketches(
            50,
            ndvPayload("apache-datasketches-hll-v1", SketchRole.SKETCH_ROLE_NDV, new byte[] {1}),
            null);
    ScalarStats s2 =
        scalarWithSketches(
            50,
            ndvPayload("external-tuple-v1", SketchRole.SKETCH_ROLE_TUPLE_NDV, new byte[] {2}),
            null);

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, s1), fileRecord(1L, s2)));

    ScalarStats merged = onlyScalar(result);
    assertFalse(
        merged.hasNdv()
            && merged.getNdv().getSketchesList().stream()
                .anyMatch(
                    s -> s.getSketchType().contains("hll") || s.getSketchType().contains("tuple")),
        "Floecat must not synthesize aggregates for producer-owned NDV sketches");
    assertTrue(merged.hasNdv(), "scalar NDV fallback must survive non-theta multi-source rollup");
    assertEquals(50.0, merged.getNdv().getApprox().getEstimate(), 0.01);
    assertEquals("rollup-max", merged.getNdv().getApprox().getMethod());
  }

  @Test
  void fileRecordRollup_doesNotPromoteThirdOpaqueNdvAsSingleSource() {
    ScalarStats s1 =
        scalarWithOpaqueNdvOnly(
            50, ndvPayload("opaque-ndv-a-v1", SketchRole.SKETCH_ROLE_NDV, new byte[] {1}));
    ScalarStats s2 =
        scalarWithOpaqueNdvOnly(
            50, ndvPayload("opaque-ndv-b-v1", SketchRole.SKETCH_ROLE_NDV, new byte[] {2}));
    ScalarStats s3 =
        scalarWithOpaqueNdvOnly(
            50, ndvPayload("opaque-ndv-c-v1", SketchRole.SKETCH_ROLE_NDV, new byte[] {3}));

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, s1), fileRecord(1L, s2), fileRecord(1L, s3)));

    ScalarStats merged = onlyScalar(result);
    assertFalse(
        merged.hasNdv(),
        "multi-source opaque NDV payloads must not be advertised as aggregate column NDV");
  }

  @Test
  void fileRecordRollup_preservesRowsSeenWhenMergingThetaNdv() {
    ScalarStats s1 =
        scalarWithSketches(
            50,
            ndvPayloadWithRows(
                "apache-datasketches-theta-v1",
                SketchRole.SKETCH_ROLE_NDV,
                thetaBytes("a"),
                50,
                100),
            null);
    ScalarStats s2 =
        scalarWithSketches(
            70,
            ndvPayloadWithRows(
                "apache-datasketches-theta-v1",
                SketchRole.SKETCH_ROLE_NDV,
                thetaBytes("b"),
                70,
                100),
            null);

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, s1), fileRecord(1L, s2)));

    ScalarStats merged = onlyScalar(result);
    assertTrue(merged.hasNdv());
    assertTrue(merged.getNdv().hasApprox());
    assertEquals(120L, merged.getNdv().getApprox().getRowsSeen());
    assertEquals(200L, merged.getNdv().getApprox().getRowsTotal());
  }

  @Test
  void fileRecordRollup_soleSourceKeepsTupleSketchThroughTheThetaRebuild() {
    SketchPayload theta =
        ndvPayload("apache-datasketches-theta-v1", SketchRole.SKETCH_ROLE_NDV, thetaBytes("a"));
    SketchPayload tuple =
        ndvPayload("floedb-tuple-v2", SketchRole.SKETCH_ROLE_TUPLE_NDV, new byte[] {7, 8, 9});
    ScalarStats sole =
        ScalarStats.newBuilder()
            .setDisplayName("col")
            .setLogicalType("LONG")
            .setRowCount(120)
            .setNdv(
                Ndv.newBuilder()
                    .setApprox(NdvApprox.newBuilder().setEstimate(50).setMethod("test"))
                    .addSketches(theta)
                    .addSketches(tuple))
            .build();

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, sole)));

    // The theta rebuild reconstructs the NDV envelope from merged theta; a sole source's
    // producer-owned tuple sketch must ride along verbatim instead of being silently dropped —
    // this is the join-key sketch the planner consumes for fanout/skew estimation.
    ScalarStats merged = onlyScalar(result);
    assertTrue(
        merged.getNdv().getSketchesList().stream()
            .anyMatch(s -> s.getSketchType().equals("apache-datasketches-theta-v1")));
    assertTrue(merged.getNdv().getSketchesList().stream().anyMatch(s -> s.equals(tuple)));
  }

  @Test
  void fileRecordRollup_propagatesScalarSketchesFromASoleSourceUntouched() {
    SketchPayload tdigest =
        scalarPayload("apache-datasketches-tdigest-v1", SketchRole.SKETCH_ROLE_QUANTILES);
    SketchPayload mcv =
        scalarPayload("apache-datasketches-frequencies-utf8-v1", SketchRole.SKETCH_ROLE_MCV);
    ScalarStats sole =
        scalarWithSketches(
                120,
                ndvPayload(
                    "apache-datasketches-theta-v1", SketchRole.SKETCH_ROLE_NDV, thetaBytes("a")),
                tdigest)
            .toBuilder()
            .addSketches(mcv)
            .build();

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(fileRecord(1L, sole)));

    // One source contributed the entire column, so its producer-owned payloads ARE the column's
    // distribution: propagated byte-for-byte, never re-encoded. This is the payload the planner
    // consumes for range/MCV selectivity — dropping it was the missing_quantile_sketch downgrade.
    ScalarStats merged = onlyScalar(result);
    assertEquals(List.of(tdigest, mcv), merged.getSketchesList());
  }

  @Test
  void partialMerge_propagatesScalarSketchesFromASolePartialUntouched() {
    SketchPayload tdigest =
        scalarPayload("apache-datasketches-tdigest-v1", SketchRole.SKETCH_ROLE_QUANTILES);
    TargetStatsRecord solePartial =
        TargetStatsRecords.columnRecord(
            TABLE, 1L, 1L, scalarWithSketches(120, null, tdigest), null);

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            TABLE, 1L, Set.of(FloecatConnector.StatsTargetKind.COLUMN), List.of(solePartial));

    // The full-rescan final stage merges per-group partials; a single group must not lose the
    // sketches its file rollup carried.
    ScalarStats merged = onlyScalar(result);
    assertEquals(List.of(tdigest), merged.getSketchesList());
  }

  @Test
  void partialMerge_dropsScalarSketchesAcrossMultiplePartials() {
    SketchPayload tdigest =
        scalarPayload("apache-datasketches-tdigest-v1", SketchRole.SKETCH_ROLE_QUANTILES);
    TargetStatsRecord p1 =
        TargetStatsRecords.columnRecord(TABLE, 1L, 1L, scalarWithSketches(50, null, tdigest), null);
    TargetStatsRecord p2 =
        TargetStatsRecords.columnRecord(TABLE, 1L, 1L, scalarWithSketches(70, null, null), null);

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            TABLE, 1L, Set.of(FloecatConnector.StatsTargetKind.COLUMN), List.of(p1, p2));

    // Two groups contributed the column: one group's sketch describes only its rows and must not
    // be advertised as the column's distribution.
    ScalarStats merged = onlyScalar(result);
    assertTrue(merged.getSketchesList().isEmpty());
  }

  @Test
  void partialMerge_unionsHllAndTupleAcrossGroups() {
    ScalarStats first =
        scalarWithNdvSketches(
            50,
            ndvPayload("apache-datasketches-hll-v1", SketchRole.SKETCH_ROLE_NDV, hllBytes("a", "b"))
                .toBuilder()
                .putParams("lg_k", "12")
                .build(),
            ndvPayload("floedb-tuple-v2", SketchRole.SKETCH_ROLE_TUPLE_NDV, tupleBytes(1L))
                .toBuilder()
                .putParams("nominal_entries", "4096")
                .putParams("version", "2")
                .build());
    ScalarStats second =
        scalarWithNdvSketches(
            70,
            ndvPayload("apache-datasketches-hll-v1", SketchRole.SKETCH_ROLE_NDV, hllBytes("c", "d"))
                .toBuilder()
                .putParams("lg_k", "12")
                .build(),
            ndvPayload("floedb-tuple-v2", SketchRole.SKETCH_ROLE_TUPLE_NDV, tupleBytes(2L))
                .toBuilder()
                .putParams("nominal_entries", "4096")
                .putParams("version", "2")
                .build());

    List<TargetStatsRecord> result =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            TABLE,
            1L,
            Set.of(FloecatConnector.StatsTargetKind.COLUMN),
            List.of(
                TargetStatsRecords.columnRecord(TABLE, 1L, 1L, first, null),
                TargetStatsRecords.columnRecord(TABLE, 1L, 1L, second, null)));

    ScalarStats merged = onlyScalar(result);
    assertEquals("apache-datasketches-hll", merged.getNdv().getApprox().getMethod());
    assertEquals(4.0, merged.getNdv().getApprox().getEstimate(), 0.1);
    SketchPayload mergedHll =
        merged.getNdv().getSketchesList().stream()
            .filter(sketch -> sketch.getSketchType().equals("apache-datasketches-hll-v1"))
            .findFirst()
            .orElseThrow();
    assertEquals(4.0, HllSketch.heapify(mergedHll.getData().toByteArray()).getEstimate(), 0.1);
    assertEquals("12", mergedHll.getParamsOrThrow("lg_k"));
    SketchPayload mergedTuple =
        merged.getNdv().getSketchesList().stream()
            .filter(sketch -> sketch.getSketchType().equals("floedb-tuple-v2"))
            .findFirst()
            .orElseThrow();
    ByteBuffer tuple =
        ByteBuffer.wrap(mergedTuple.getData().toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
    assertEquals(2L, tuple.getLong(16));
    assertEquals("4096", mergedTuple.getParamsOrThrow("nominal_entries"));
    assertEquals("2", mergedTuple.getParamsOrThrow("version"));
  }

  @Test
  void partialMerge_usesTupleEstimateWhenNoThetaOrHllExists() {
    ScalarStats first =
        scalarWithNdvSketches(
            50,
            ndvPayload("floedb-tuple-v2", SketchRole.SKETCH_ROLE_TUPLE_NDV, tupleBytes(1L))
                .toBuilder()
                .putParams("nominal_entries", "4096")
                .build());
    ScalarStats second =
        scalarWithNdvSketches(
            70,
            ndvPayload("floedb-tuple-v2", SketchRole.SKETCH_ROLE_TUPLE_NDV, tupleBytes(2L))
                .toBuilder()
                .putParams("nominal_entries", "4096")
                .build());

    ScalarStats merged =
        onlyScalar(
            FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
                TABLE,
                1L,
                Set.of(FloecatConnector.StatsTargetKind.COLUMN),
                List.of(
                    TargetStatsRecords.columnRecord(TABLE, 1L, 1L, first, null),
                    TargetStatsRecords.columnRecord(TABLE, 1L, 1L, second, null))));

    assertEquals("floedb-tuple", merged.getNdv().getApprox().getMethod());
    assertEquals(2.0, merged.getNdv().getApprox().getEstimate(), 0.01);
  }

  @Test
  void partialMerge_coverageMatchesDirectFileRollup() {
    Set<FloecatConnector.StatsTargetKind> requested =
        Set.of(FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.COLUMN);
    TargetStatsRecord first = fileRecord(1L, scalarWithSketches(50, null, null));
    TargetStatsRecord second = fileRecord(1L, scalarWithSketches(70, null, null));

    List<TargetStatsRecord> partials =
        java.util.stream.Stream.concat(
                FileGroupTargetStatsRollup.partialAggregatesFromFileRecords(
                    TABLE, 1L, requested, List.of(first))
                    .stream(),
                FileGroupTargetStatsRollup.partialAggregatesFromFileRecords(
                    TABLE, 1L, requested, List.of(second))
                    .stream())
            .toList();
    List<TargetStatsRecord> merged =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(TABLE, 1L, requested, partials);
    List<TargetStatsRecord> direct =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE, 1L, requested, List.of(first, second));

    assertEquals(
        direct.stream().map(TargetStatsRecord::getMetadata).toList(),
        merged.stream().map(TargetStatsRecord::getMetadata).toList());
    StatsCoverage coverage = merged.getFirst().getMetadata().getCoverage();
    assertEquals(120L, coverage.getRowsScanned());
    assertEquals(2L, coverage.getFilesScanned());
    assertEquals(4L, coverage.getRowGroupsSampled());
    assertEquals(960L, coverage.getBytesScanned());
  }

  @Test
  void complete_preservesFinalColumnRecordWithOpaqueSketchPayloads() {
    ScalarStats finalColumnStats =
        scalarWithSketches(
            100,
            ndvPayload("external-tuple-v1", SketchRole.SKETCH_ROLE_TUPLE_NDV, new byte[] {1, 2, 3}),
            scalarPayload("apache-datasketches-tdigest-v1", SketchRole.SKETCH_ROLE_QUANTILES));
    TargetStatsRecord finalColumn =
        TargetStatsRecords.columnRecord(TABLE, 1L, 1L, finalColumnStats, null);

    List<TargetStatsRecord> result =
        new FileGroupTargetStatsRollup()
            .complete(
                TABLE,
                1L,
                Set.of(FloecatConnector.StatsTargetKind.COLUMN),
                List.of(finalColumn, fileRecord(1L, scalarWithSketches(100, null, null))));

    ScalarStats preserved = onlyScalar(result);
    assertTrue(
        preserved.getNdv().getSketchesList().stream()
            .anyMatch(s -> s.getSketchType().equals("external-tuple-v1")));
    assertTrue(
        preserved.getSketchesList().stream()
            .anyMatch(s -> s.getSketchType().equals("apache-datasketches-tdigest-v1")));
  }

  private static TargetStatsRecord fileRecord(long columnId, ScalarStats scalar) {
    return TargetStatsRecord.newBuilder()
        .setTableId(TABLE)
        .setSnapshotId(1L)
        .setMetadata(
            StatsMetadata.newBuilder()
                .setCoverage(
                    StatsCoverage.newBuilder()
                        .setRowsScanned(scalar.getRowCount())
                        .setFilesScanned(1L)
                        .setRowGroupsSampled(2L)
                        .setBytesScanned(scalar.getRowCount() * 8L)))
        .setFile(
            FileTargetStats.newBuilder()
                .setTableId(TABLE)
                .setSnapshotId(1L)
                .setRowCount(scalar.getRowCount())
                .addColumns(
                    FileColumnStats.newBuilder().setColumnId(columnId).setScalar(scalar).build()))
        .build();
  }

  private static ScalarStats scalarWithSketches(
      long rowCount, SketchPayload ndvSketch, SketchPayload scalarSketch) {
    ScalarStats.Builder builder =
        ScalarStats.newBuilder()
            .setDisplayName("col")
            .setLogicalType("LONG")
            .setRowCount(rowCount)
            .setNullCount(rowCount / 10)
            .setAvgWidthBytes(8)
            .setMin("1")
            .setMax("100");
    if (ndvSketch != null) {
      NdvApprox.Builder approx = NdvApprox.newBuilder().setEstimate(50).setMethod("test");
      if (ndvSketch.getParamsMap().containsKey("rows_seen")) {
        approx.setRowsSeen(Long.parseLong(ndvSketch.getParamsOrThrow("rows_seen")));
      }
      if (ndvSketch.getParamsMap().containsKey("rows_total")) {
        approx.setRowsTotal(Long.parseLong(ndvSketch.getParamsOrThrow("rows_total")));
      }
      builder.setNdv(Ndv.newBuilder().setApprox(approx).addSketches(ndvSketch));
    }
    if (scalarSketch != null) {
      builder.addSketches(scalarSketch);
    }
    return builder.build();
  }

  private static ScalarStats scalarWithOpaqueNdvOnly(long rowCount, SketchPayload ndvSketch) {
    return ScalarStats.newBuilder()
        .setDisplayName("col")
        .setLogicalType("LONG")
        .setRowCount(rowCount)
        .setNdv(Ndv.newBuilder().addSketches(ndvSketch))
        .build();
  }

  private static ScalarStats scalarWithNdvSketches(long rowCount, SketchPayload... ndvSketches) {
    return ScalarStats.newBuilder()
        .setDisplayName("col")
        .setLogicalType("LONG")
        .setRowCount(rowCount)
        .setNdv(Ndv.newBuilder().addAllSketches(List.of(ndvSketches)))
        .build();
  }

  private static SketchPayload ndvPayload(String type, SketchRole role, byte[] data) {
    return SketchPayload.newBuilder()
        .setRole(role)
        .setSketchType(type)
        .setData(ByteString.copyFrom(data))
        .build();
  }

  private static SketchPayload ndvPayloadWithRows(
      String type, SketchRole role, byte[] data, long rowsSeen, long rowsTotal) {
    return ndvPayload(type, role, data).toBuilder()
        .putParams("rows_seen", Long.toString(rowsSeen))
        .putParams("rows_total", Long.toString(rowsTotal))
        .build();
  }

  private static SketchPayload scalarPayload(String type, SketchRole role) {
    return SketchPayload.newBuilder()
        .setRole(role)
        .setSketchType(type)
        .setData(ByteString.copyFrom(new byte[] {1, 2, 3}))
        .build();
  }

  private static byte[] thetaBytes(String... values) {
    UpdateSketch sketch = UpdateSketch.builder().build();
    for (String value : values) {
      sketch.update(value);
    }
    return sketch.compact().toByteArray();
  }

  private static byte[] hllBytes(String... values) {
    HllSketch sketch = new HllSketch(12, TgtHllType.HLL_8);
    for (String value : values) {
      sketch.update(value);
    }
    return sketch.toCompactByteArray();
  }

  private static byte[] tupleBytes(long... hashes) {
    ByteBuffer tuple = ByteBuffer.allocate(24 + hashes.length * 24).order(ByteOrder.LITTLE_ENDIAN);
    tuple.putLong(Long.MAX_VALUE);
    tuple.putLong(hashes.length);
    tuple.putLong(hashes.length);
    for (long hash : hashes) {
      tuple.putLong(hash);
      tuple.putLong(1L);
      tuple.putLong(8L);
    }
    return tuple.array();
  }

  private static ScalarStats onlyScalar(List<TargetStatsRecord> records) {
    List<ScalarStats> scalars =
        records.stream()
            .filter(TargetStatsRecord::hasScalar)
            .map(TargetStatsRecord::getScalar)
            .toList();
    assertEquals(1, scalars.size());
    return scalars.get(0);
  }
}
