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

package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TargetStatsRecordValidatorTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final long SNAPSHOT_ID = 42L;

  @Test
  void normalizesTableTargetAndIdentity() {
    TargetStatsRecord input =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
            .setTable(
                TableValueStats.newBuilder()
                    .setRowCount(1L)
                    .setDataFileCount(2L)
                    .setTotalSizeBytes(3L))
            .build();

    TargetStatsRecord normalized =
        TargetStatsRecordValidator.validateForPut(input, TABLE_ID, SNAPSHOT_ID, "corr");

    assertEquals(TABLE_ID, normalized.getTableId());
    assertEquals(SNAPSHOT_ID, normalized.getSnapshotId());
    assertEquals(StatsTarget.TargetCase.TABLE, normalized.getTarget().getTargetCase());
  }

  @Test
  void rejectsMismatchedTargetPayloadKinds() {
    TargetStatsRecord input =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder().setColumn(ColumnStatsTarget.newBuilder().setColumnId(5L)))
            .setTable(TableValueStats.newBuilder().setRowCount(1L))
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> TargetStatsRecordValidator.validateForPut(input, TABLE_ID, SNAPSHOT_ID, "corr"));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    Map<String, String> params = errorParams(ex);
    assertEquals("value", params.get("field"));
    assertEquals("COLUMN target requires scalar payload", params.get("reason"));
  }

  @Test
  void rejectsFilePathMismatchBetweenTargetAndPayload() {
    TargetStatsRecord input =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath("s3://bucket/a.parquet")))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(TABLE_ID)
                    .setSnapshotId(SNAPSHOT_ID)
                    .setFilePath("s3://bucket/b.parquet")
                    .setFileFormat("PARQUET")
                    .setRowCount(1L)
                    .setSizeBytes(10L))
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> TargetStatsRecordValidator.validateForPut(input, TABLE_ID, SNAPSHOT_ID, "corr"));
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    Map<String, String> params = errorParams(ex);
    assertEquals("file.file_path", params.get("field"));
    assertEquals("file payload path does not match file target path", params.get("reason"));
    assertEquals("s3://bucket/a.parquet", params.get("expected"));
    assertEquals("s3://bucket/b.parquet", params.get("actual"));
  }

  @Test
  void normalizesExpressionTargetIdentity() {
    TargetStatsRecord input =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setTarget(
                StatsTarget.newBuilder()
                    .setExpression(
                        EngineExpressionStatsTarget.newBuilder()
                            .setEngineKind(" DuckDB ")
                            .setEngineExpressionKey(ByteString.copyFromUtf8("sum(id)"))))
            .setScalar(ScalarStats.newBuilder().setLogicalType("BIGINT").setValueCount(1L))
            .build();

    TargetStatsRecord normalized =
        TargetStatsRecordValidator.validateForPut(input, TABLE_ID, SNAPSHOT_ID, "corr");

    assertEquals(StatsTarget.TargetCase.EXPRESSION, normalized.getTarget().getTargetCase());
    assertEquals("duckdb", normalized.getTarget().getExpression().getEngineKind());
  }

  private static Map<String, String> errorParams(StatusRuntimeException ex) {
    com.google.rpc.Status status = StatusProto.fromThrowable(ex);
    if (status == null) {
      throw new AssertionError("missing rpc status details");
    }
    for (Any detail : status.getDetailsList()) {
      if (detail.is(ai.floedb.floecat.common.rpc.Error.class)) {
        try {
          return detail.unpack(ai.floedb.floecat.common.rpc.Error.class).getParamsMap();
        } catch (Exception e) {
          throw new AssertionError("failed to unpack error details", e);
        }
      }
    }
    throw new AssertionError("missing error detail payload");
  }
}
