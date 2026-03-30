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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsRepositoryTargetStorageTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder().setAccountId("a").setId("t").setKind(ResourceKind.RK_TABLE).build();

  @Test
  void writesAndReadsTableColumnExpressionAndFileTargets() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 101L;

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(5).build(), null));

    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            7L,
            ScalarStats.newBuilder()
                .setDisplayName("c7")
                .setLogicalType("BIGINT")
                .setValueCount(5L)
                .build(),
            null));

    EngineExpressionStatsTarget expressionTarget =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("DuckDB")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:payload:user.id"))
            .build();
    ScalarStats expressionStats =
        ScalarStats.newBuilder()
            .setLogicalType("BIGINT")
            .setValueCount(100L)
            .setNullCount(5L)
            .build();
    repository.putTargetStats(
        TargetStatsRecords.expressionRecord(
            TABLE_ID, snapshotId, expressionTarget, expressionStats));

    FileTargetStats fileStats =
        FileTargetStats.newBuilder().setFilePath("s3://bucket/path/file-1.parquet").build();
    repository.putTargetStats(TargetStatsRecords.fileRecord(TABLE_ID, snapshotId, fileStats));

    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .isPresent()
        .get()
        .extracting(r -> r.getValueCase().name())
        .isEqualTo("TABLE");
    assertThat(
            repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.columnTarget(7L)))
        .isPresent()
        .get()
        .extracting(r -> r.getValueCase().name())
        .isEqualTo("SCALAR");
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.expressionTarget(expressionTarget)))
        .isPresent()
        .get()
        .extracting(r -> r.getValueCase().name())
        .isEqualTo("SCALAR");
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.expressionTarget(expressionTarget)))
        .isPresent()
        .get()
        .extracting(TargetStatsRecord::getScalar)
        .isEqualTo(expressionStats);
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(fileStats.getFilePath())))
        .isPresent()
        .get()
        .extracting(r -> r.getValueCase().name())
        .isEqualTo("FILE");
  }

  @Test
  void listAndCountReturnAllTargetRecordsForSnapshot() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 303L;

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(12).build(), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            1L,
            ScalarStats.newBuilder().setDisplayName("c1").setLogicalType("BIGINT").build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.expressionRecord(
            TABLE_ID,
            snapshotId,
            EngineExpressionStatsTarget.newBuilder()
                .setEngineKind("duckdb")
                .setEngineExpressionKey(ByteString.copyFromUtf8("expr:1"))
                .build(),
            ScalarStats.newBuilder().setLogicalType("VARCHAR").setValueCount(1L).build()));
    repository.putTargetStats(
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath("s3://bucket/path/file.parquet").build()));

    var page = repository.listTargetStats(TABLE_ID, snapshotId, Optional.empty(), 10, "");
    List<TargetStatsRecord> listed = page.records();
    assertThat(listed).hasSize(4);
    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.empty())).isEqualTo(4);
  }

  @Test
  void listAndCountCanFilterByTargetKind() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 808L;

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(12).build(), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            1L,
            ScalarStats.newBuilder().setDisplayName("c1").setLogicalType("BIGINT").build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath("s3://bucket/path/file.parquet").build()));

    var page =
        repository.listTargetStats(TABLE_ID, snapshotId, Optional.of(StatsTargetType.FILE), 10, "");

    assertThat(page.records()).hasSize(1);
    assertThat(page.records().get(0).getTarget().hasFile()).isTrue();
    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.of(StatsTargetType.FILE)))
        .isEqualTo(1);
  }

  @Test
  void deleteAllStatsForSnapshotRemovesTargetAndFileStats() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 404L;
    EngineExpressionStatsTarget expressionTarget =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("duckdb")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:payload:user.id"))
            .build();
    String filePath = "s3://bucket/path/file-1.parquet";

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(3).build(), null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            10L,
            ScalarStats.newBuilder().setDisplayName("c10").setLogicalType("BIGINT").build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.expressionRecord(
            TABLE_ID,
            snapshotId,
            expressionTarget,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setValueCount(5L).build()));
    repository.putTargetStats(
        TargetStatsRecords.fileRecord(
            TABLE_ID, snapshotId, FileTargetStats.newBuilder().setFilePath(filePath).build()));

    assertThat(repository.deleteAllStatsForSnapshot(TABLE_ID, snapshotId)).isTrue();
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .isEmpty();
    assertThat(
            repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.columnTarget(10L)))
        .isEmpty();
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.expressionTarget(expressionTarget)))
        .isEmpty();
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(filePath)))
        .isEmpty();
  }

  @Test
  void rejectsIncompatibleTargetAndValueCombinations() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 505L;

    TargetStatsRecord tableTargetWithScalar =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.tableTarget())
            .setScalar(ScalarStats.newBuilder().setLogicalType("BIGINT").setValueCount(1L).build())
            .build();

    assertThatThrownBy(() -> repository.putTargetStats(tableTargetWithScalar))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("incompatible target/value");

    TargetStatsRecord columnTargetWithTable =
        TargetStatsRecord.newBuilder()
            .setTableId(TABLE_ID)
            .setSnapshotId(snapshotId)
            .setTarget(StatsTargetIdentity.columnTarget(7L))
            .setTable(TableValueStats.newBuilder().setRowCount(1L).build())
            .build();

    assertThatThrownBy(() -> repository.putTargetStats(columnTargetWithTable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("incompatible target/value");
  }

  @Test
  void metadataIsStoredAtRecordLevelForScalarAndTableTargets() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long scalarSnapshotId = 606L;
    long tableSnapshotId = 707L;
    StatsMetadata metadata =
        StatsMetadata.newBuilder()
            .setProducer(StatsProducer.SPROD_ENGINE_COMPUTED)
            .setCaptureMode(StatsCaptureMode.SCM_SYNC)
            .setCompleteness(StatsCompleteness.SC_PARTIAL)
            .setConfidenceLevel(0.7d)
            .build();

    TargetStatsRecord scalarRecord =
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            scalarSnapshotId,
            9L,
            ScalarStats.newBuilder()
                .setDisplayName("c9")
                .setLogicalType("BIGINT")
                .setValueCount(100L)
                .build(),
            metadata);
    repository.putTargetStats(scalarRecord);

    TargetStatsRecord tableRecord =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            tableSnapshotId,
            TableValueStats.newBuilder().setRowCount(11L).build(),
            metadata);
    repository.putTargetStats(tableRecord);

    assertThat(
            repository
                .getTargetStats(TABLE_ID, scalarSnapshotId, StatsTargetIdentity.columnTarget(9L))
                .orElseThrow()
                .getMetadata())
        .isEqualTo(metadata);
    assertThat(
            repository
                .getTargetStats(TABLE_ID, tableSnapshotId, StatsTargetIdentity.tableTarget())
                .orElseThrow()
                .getMetadata())
        .isEqualTo(metadata);
  }
}
