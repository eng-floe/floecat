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

package ai.floedb.floecat.stats.identity;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import com.google.protobuf.ByteString;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TargetStatsRecordsTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder().setAccountId("acct").setId("table").build();

  @Test
  void contentHashImageCanonicalizesSketchParamsOrderInScalarSketches() {
    TargetStatsRecord first =
        columnRecordWithScalarSketch(params("encoding", "raw", "compression", "none"));
    TargetStatsRecord second =
        columnRecordWithScalarSketch(params("compression", "none", "encoding", "raw"));

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageCanonicalizesSketchParamsOrderInNdvSketches() {
    TargetStatsRecord first = columnRecordWithNdvSketch(params("version", "1", "lg_k", "12"));
    TargetStatsRecord second = columnRecordWithNdvSketch(params("lg_k", "12", "version", "1"));

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageIgnoresSketchCapturedAt() {
    TargetStatsRecord first = columnRecordWithScalarSketch(params("encoding", "raw"), 1_000L);
    TargetStatsRecord second = columnRecordWithScalarSketch(params("encoding", "raw"), 2_000L);

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageIgnoresFileColumnSketchCapturedAt() {
    // File-column scalars carry sketches with no upstream stamp (file-group rollups stamp
    // captured_at_ms without one). The content hash must still ignore that wall-clock value.
    TargetStatsRecord first = fileRecordWithColumnSketch(params("encoding", "raw"), 1_000L);
    TargetStatsRecord second = fileRecordWithColumnSketch(params("encoding", "raw"), 2_000L);

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageCanonicalizesFileColumnSketchParamsOrder() {
    TargetStatsRecord first =
        fileRecordWithColumnSketch(params("encoding", "raw", "compression", "none"), 123L);
    TargetStatsRecord second =
        fileRecordWithColumnSketch(params("compression", "none", "encoding", "raw"), 123L);

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageIgnoresScalarDisplayName() {
    TargetStatsRecord first =
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            55L,
            7L,
            ScalarStats.newBuilder().setDisplayName("orders").setRowCount(10).build(),
            null);
    TargetStatsRecord second =
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            55L,
            7L,
            ScalarStats.newBuilder().setDisplayName("o_orders").setRowCount(10).build(),
            null);

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageCanonicalizesScalarPropertiesOrder() {
    TargetStatsRecord first = columnRecordWithScalarProperties(params("a", "1", "b", "2"));
    TargetStatsRecord second = columnRecordWithScalarProperties(params("b", "2", "a", "1"));

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  @Test
  void contentHashImageCanonicalizesNdvApproxPropertiesOrder() {
    TargetStatsRecord first = columnRecordWithNdvApproxProperties(params("x", "1", "y", "2"));
    TargetStatsRecord second = columnRecordWithNdvApproxProperties(params("y", "2", "x", "1"));

    assertThat(TargetStatsRecords.contentHashImage(first).toByteArray())
        .isEqualTo(TargetStatsRecords.contentHashImage(second).toByteArray());
  }

  private static TargetStatsRecord columnRecordWithScalarProperties(
      Map<String, String> properties) {
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setRowCount(10)
            .putAllProperties(properties)
            .build();
    return TargetStatsRecords.columnRecord(TABLE_ID, 55L, 7L, stats, null);
  }

  private static TargetStatsRecord columnRecordWithNdvApproxProperties(
      Map<String, String> properties) {
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setRowCount(10)
            .setNdv(
                Ndv.newBuilder()
                    .setApprox(NdvApprox.newBuilder().setEstimate(5).putAllProperties(properties)))
            .build();
    return TargetStatsRecords.columnRecord(TABLE_ID, 55L, 7L, stats, null);
  }

  private static TargetStatsRecord fileRecordWithColumnSketch(
      Map<String, String> params, long capturedAtMs) {
    ScalarStats columnScalar =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setRowCount(10)
            .addSketches(sketch(params, capturedAtMs))
            .build();
    FileTargetStats file =
        FileTargetStats.newBuilder()
            .setFilePath("s3://bucket/data/file-0.parquet")
            .addColumns(FileColumnStats.newBuilder().setColumnId(7L).setScalar(columnScalar))
            .build();
    return TargetStatsRecords.fileRecord(TABLE_ID, 55L, file);
  }

  private static TargetStatsRecord columnRecordWithScalarSketch(Map<String, String> params) {
    return columnRecordWithScalarSketch(params, 123L);
  }

  private static TargetStatsRecord columnRecordWithScalarSketch(
      Map<String, String> params, long capturedAtMs) {
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setRowCount(10)
            .addSketches(sketch(params, capturedAtMs))
            .build();
    return TargetStatsRecords.columnRecord(TABLE_ID, 55L, 7L, stats, null);
  }

  private static TargetStatsRecord columnRecordWithNdvSketch(Map<String, String> params) {
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setRowCount(10)
            .setNdv(Ndv.newBuilder().addSketches(sketch(params, 123L)))
            .build();
    return TargetStatsRecords.columnRecord(TABLE_ID, 55L, 7L, stats, null);
  }

  private static SketchPayload sketch(Map<String, String> params, long capturedAtMs) {
    return SketchPayload.newBuilder()
        .setRole(SketchRole.SKETCH_ROLE_NDV)
        .setSketchType("apache-datasketches-theta-v1")
        .setData(ByteString.copyFromUtf8("sketch"))
        .setCapturedAtMs(capturedAtMs)
        .putAllParams(params)
        .build();
  }

  private static Map<String, String> params(
      String firstKey, String firstValue, String secondKey, String secondValue) {
    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    params.put(firstKey, firstValue);
    params.put(secondKey, secondValue);
    return params;
  }

  private static Map<String, String> params(String key, String value) {
    LinkedHashMap<String, String> params = new LinkedHashMap<>();
    params.put(key, value);
    return params;
  }
}
