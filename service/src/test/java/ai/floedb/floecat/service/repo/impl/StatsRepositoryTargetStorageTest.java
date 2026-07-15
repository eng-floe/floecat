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
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
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
                .setRowCount(5L)
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
            .setRowCount(100L)
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
            ScalarStats.newBuilder().setLogicalType("VARCHAR").setRowCount(1L).build()));
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
  void putTargetStatsBatchWritesMultipleTargets() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 707L;
    EngineExpressionStatsTarget expressionTarget =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("duckdb")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr:batched"))
            .build();
    String filePath = "s3://bucket/path/file-batch.parquet";

    repository.putTargetStatsBatch(
        TABLE_ID,
        snapshotId,
        List.of(
            TargetStatsRecords.tableRecord(
                TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(12).build(), null),
            TargetStatsRecords.columnRecord(
                TABLE_ID,
                snapshotId,
                1L,
                ScalarStats.newBuilder().setDisplayName("c1").setLogicalType("BIGINT").build(),
                null),
            TargetStatsRecords.expressionRecord(
                TABLE_ID,
                snapshotId,
                expressionTarget,
                ScalarStats.newBuilder().setLogicalType("VARCHAR").setRowCount(2L).build()),
            TargetStatsRecords.fileRecord(
                TABLE_ID, snapshotId, FileTargetStats.newBuilder().setFilePath(filePath).build())));

    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.empty())).isEqualTo(4);
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .isPresent();
    assertThat(
            repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.columnTarget(1L)))
        .isPresent();
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.expressionTarget(expressionTarget)))
        .isPresent();
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(filePath)))
        .isPresent();
  }

  @Test
  void putTargetStatsBatchFillsMissingPointersWhenSomeAlreadyExist() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 708L;
    TargetStatsRecord tableRecord =
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(12).build(), null);
    TargetStatsRecord columnRecord =
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            1L,
            ScalarStats.newBuilder().setDisplayName("c1").setLogicalType("BIGINT").build(),
            null);

    repository.putTargetStats(tableRecord);

    repository.putTargetStatsBatch(TABLE_ID, snapshotId, List.of(tableRecord, columnRecord));

    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.empty())).isEqualTo(2);
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .isPresent();
    assertThat(
            repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.columnTarget(1L)))
        .isPresent();
  }

  @Test
  void draftGenerationIsInvisibleUntilPublished() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 709L;
    String filePath = "s3://bucket/path/file-overwrite.parquet";
    TargetStatsRecord original =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath(filePath).setRowCount(12L).build());
    TargetStatsRecord replacement =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath(filePath).setRowCount(34L).build());

    repository.putTargetStatsBatch(TABLE_ID, snapshotId, List.of(original));
    String originalGeneration =
        repository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();
    repository.replaceTargetStatsInGeneration(
        TABLE_ID,
        snapshotId,
        "draft-job-1",
        List.of(StatsTargetIdentity.fileTarget(filePath)),
        List.of(replacement));

    Optional<TargetStatsRecord> stored =
        repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(filePath));
    assertThat(stored).isPresent();
    assertThat(stored.get().getFile().getRowCount()).isEqualTo(12L);
    assertThat(repository.activeStatsGeneration(TABLE_ID, snapshotId)).contains(originalGeneration);

    repository.publishStatsGeneration(TABLE_ID, snapshotId, "draft-job-1", List.of());

    stored =
        repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(filePath));
    assertThat(stored).isPresent();
    assertThat(stored.get().getFile().getRowCount()).isEqualTo(34L);
    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.empty())).isEqualTo(1);
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
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(5L).build()));
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
            .setScalar(ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(1L).build())
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
                .setRowCount(100L)
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

  @Test
  void identicalPutTargetStatsRetryIsIdempotent() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 8080L;

    TargetStatsRecord tableRecord =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder().setRowCount(11L).setDataFileCount(2L).build(),
            null);

    repository.putTargetStats(tableRecord);
    repository.putTargetStats(tableRecord);

    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .contains(tableRecord);
  }

  @Test
  void conflictingPutTargetStatsForSameTargetIsRejected() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9090L;

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(11L).build(), null));

    assertThatThrownBy(
            () ->
                repository.putTargetStats(
                    TargetStatsRecords.tableRecord(
                        TABLE_ID,
                        snapshotId,
                        TableValueStats.newBuilder().setRowCount(12L).build(),
                        null)))
        .isInstanceOf(BaseResourceRepository.NameConflictException.class)
        .hasMessageContaining("pointer bound to different blob");
  }

  @Test
  void resubmitDifferingOnlyInOperationalTimestampsIsIdempotent() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9091L;

    TargetStatsRecord first =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(11L)
                .setUpstream(
                    UpstreamStamp.newBuilder().setCommitRef("snap").setFetchedAt(ts(1_000L)))
                .build(),
            metadataAt(ts(1_000L), ts(1_000L)));
    TargetStatsRecord second =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(11L)
                .setUpstream(
                    UpstreamStamp.newBuilder().setCommitRef("snap").setFetchedAt(ts(2_000L)))
                .build(),
            metadataAt(ts(2_000L), ts(2_000L)));

    repository.putTargetStats(first);
    // Identical payload, only operational timestamps differ: both records hash to the same content
    // blob, so the stable target pointer re-binds to that same blob (last write wins on content)
    // instead of failing with a "pointer bound to different blob" conflict. Regression for the
    // MC_CONFLICT resubmit bug.
    repository.putTargetStats(second);

    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .contains(second);
  }

  @Test
  void resubmitFileTargetDifferingOnlyInColumnUpstreamIsIdempotent() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9092L;

    TargetStatsRecord first =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            fileStatsWithColumnFetchedAt(ts(1_000L)),
            metadataAt(ts(1_000L), ts(1_000L)));
    TargetStatsRecord second =
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            fileStatsWithColumnFetchedAt(ts(2_000L)),
            metadataAt(ts(2_000L), ts(2_000L)));

    repository.putTargetStats(first);
    // Mirrors the ingest case: per-column scalar upstream fetched_at is wall-clock and differs on
    // every capture; it must not change the content blob address, so the resubmit re-binds the same
    // pointer instead of conflicting (last write wins on content).
    repository.putTargetStats(second);

    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget("s3://bucket/f.parquet")))
        .contains(second);
  }

  private static FileTargetStats fileStatsWithColumnFetchedAt(Timestamp fetchedAt) {
    return FileTargetStats.newBuilder()
        .setFilePath("s3://bucket/f.parquet")
        .setRowCount(7L)
        .addColumns(
            FileColumnStats.newBuilder()
                .setColumnId(1L)
                .setScalar(
                    ScalarStats.newBuilder()
                        .setLogicalType("BIGINT")
                        .setRowCount(7L)
                        .setUpstream(
                            UpstreamStamp.newBuilder()
                                .setCommitRef("snap")
                                .setFetchedAt(fetchedAt))))
        .build();
  }

  private static StatsMetadata metadataAt(Timestamp capturedAt, Timestamp refreshedAt) {
    return StatsMetadata.newBuilder()
        .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
        .setCompleteness(StatsCompleteness.SC_COMPLETE)
        .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
        .setCapturedAt(capturedAt)
        .setRefreshedAt(refreshedAt)
        .build();
  }

  private static Timestamp ts(long seconds) {
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  @Test
  void putTargetStatsIfAbsentPreservesExistingConflictingRecord() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9190L;

    TargetStatsRecord existing =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder().setRowCount(11L).setDataFileCount(2L).build(),
            null);
    TargetStatsRecord zeroMarker =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder().setRowCount(0L).setDataFileCount(0L).build(),
            null);

    repository.putTargetStats(existing);

    assertThat(repository.putTargetStatsIfAbsent(zeroMarker)).isFalse();
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .contains(existing);
  }

  @Test
  void putTargetStatsIfAbsentDoesNotOverwriteWhenOnlyOperationalTimestampsDiffer() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9191L;

    TargetStatsRecord existing =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(11L)
                .setUpstream(
                    UpstreamStamp.newBuilder().setCommitRef("snap").setFetchedAt(ts(1_000L)))
                .build(),
            metadataAt(ts(1_000L), ts(1_000L)));
    // Same payload, only operational timestamps differ -> same content blob address as `existing`.
    TargetStatsRecord resubmit =
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(11L)
                .setUpstream(
                    UpstreamStamp.newBuilder().setCommitRef("snap").setFetchedAt(ts(2_000L)))
                .build(),
            metadataAt(ts(2_000L), ts(2_000L)));

    repository.putTargetStats(existing);

    // The target is already bound, so ifAbsent must report false AND leave the stored record's
    // bytes (including its real operational timestamps) untouched -- it must not overwrite the
    // shared blob before the pointer CAS miss.
    assertThat(repository.putTargetStatsIfAbsent(resubmit)).isFalse();
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .contains(existing);
  }

  @Test
  void replaceAllStatsForSnapshotSwapsAndRemovesSnapshotStats() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9192L;
    String oldFilePath = "s3://bucket/path/old.parquet";

    repository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder().setRowCount(11L).setDataFileCount(1L).build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.fileRecord(
            TABLE_ID,
            snapshotId,
            FileTargetStats.newBuilder().setFilePath(oldFilePath).setRowCount(11L).build(),
            null));

    repository.replaceAllStatsForSnapshot(
        TABLE_ID,
        snapshotId,
        List.of(
            TargetStatsRecords.tableRecord(
                TABLE_ID,
                snapshotId,
                TableValueStats.newBuilder().setRowCount(27L).setDataFileCount(0L).build(),
                null)));

    assertThat(repository.countTargetStats(TABLE_ID, snapshotId, Optional.empty())).isEqualTo(1);
    assertThat(repository.getTargetStats(TABLE_ID, snapshotId, StatsTargetIdentity.tableTarget()))
        .isPresent()
        .get()
        .extracting(record -> record.getTable().getRowCount())
        .isEqualTo(27L);
    assertThat(
            repository.getTargetStats(
                TABLE_ID, snapshotId, StatsTargetIdentity.fileTarget(oldFilePath)))
        .isEmpty();
  }

  @Test
  void replaceAllRetainsTheSupersededGenerationForPinnedReaders() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    StatsRepository statsRepository = new StatsRepository(pointerStore, blobStore);
    long snapshotId = 4242L;
    statsRepository.replaceAllStatsForSnapshot(
        TABLE_ID,
        snapshotId,
        java.util.List.of(
            TargetStatsRecords.tableRecord(
                TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(1L).build(), null)));
    String pinnedManifestUri =
        statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();

    statsRepository.replaceAllStatsForSnapshot(
        TABLE_ID,
        snapshotId,
        java.util.List.of(
            TargetStatsRecords.tableRecord(
                TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(2L).build(), null)));

    // The pointer moved to the new generation...
    String liveManifestUri =
        statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();
    assertThat(liveManifestUri).isNotEqualTo(pinnedManifestUri);
    // ...but the superseded generation's manifest is retained: a query that froze it keeps
    // reading its immutable keyspace to completion (GC collects it once unreferenced).
    assertThat(blobStore.get(pinnedManifestUri)).isNotNull();
  }

  @Test
  void gcReclaimsSupersededUnprotectedGenerationsOnly() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    StatsRepository statsRepository = new StatsRepository(pointerStore, blobStore);
    long snapshotId = 777L;
    var record =
        TargetStatsRecords.tableRecord(
            TABLE_ID, snapshotId, TableValueStats.newBuilder().setRowCount(1L).build(), null);

    // gen-1 published, then superseded by gen-2, then gen-3 (live active).
    statsRepository.replaceAllStatsForSnapshot(TABLE_ID, snapshotId, java.util.List.of(record));
    String gen1 = statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();
    statsRepository.replaceAllStatsForSnapshot(TABLE_ID, snapshotId, java.util.List.of(record));
    String gen2 = statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();
    statsRepository.replaceAllStatsForSnapshot(TABLE_ID, snapshotId, java.util.List.of(record));
    String gen3 = statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();

    // gen-1 is protected (a retained root or a frozen scan references it); gen-2 is not.
    // minAge 0: the age guard is exercised separately; here the reclaim rules are under test.
    int reclaimed =
        statsRepository.deleteUnreferencedGenerations(
            TABLE_ID, gen1::equals, System.currentTimeMillis(), 0L);

    assertThat(reclaimed).isEqualTo(1);
    assertThat(blobStore.get(gen1)).isNotNull();
    assertThat(blobStore.get(gen2)).isNull();
    assertThat(blobStore.get(gen3)).as("live active survives regardless of roots").isNotNull();
    // The protected and live generations' records still serve; the reclaimed one is gone.
    assertThat(
            statsRepository
                .listTargetStatsInGeneration(
                    TABLE_ID, snapshotId, gen1, java.util.Optional.empty(), 10, "")
                .records())
        .hasSize(1);
    assertThat(
            statsRepository
                .listTargetStats(TABLE_ID, snapshotId, java.util.Optional.empty(), 10, "")
                .records())
        .hasSize(1);
  }

  @Test
  void reclaimFindsCandidatesForTableIdsThatNeedEncoding() {
    // The scan prefix is built with Keys.snapshotRootPrefix (percent-encoded); a hand-built
    // unencoded prefix silently scanned nothing for ids with reserved characters and the table's
    // superseded generations leaked forever.
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    StatsRepository statsRepository = new StatsRepository(pointerStore, blobStore);
    var encodedTable =
        ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
            .setAccountId(TABLE_ID.getAccountId())
            .setId("tbl with spaces/and/slashes")
            .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
            .build();
    long snapshotId = 3L;
    var record =
        ai.floedb.floecat.stats.identity.TargetStatsRecords.tableRecord(
            encodedTable,
            snapshotId,
            ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder().setRowCount(1L).build(),
            null);
    statsRepository.replaceAllStatsForSnapshot(encodedTable, snapshotId, java.util.List.of(record));
    String gen1 = statsRepository.activeStatsGeneration(encodedTable, snapshotId).orElseThrow();
    statsRepository.replaceAllStatsForSnapshot(encodedTable, snapshotId, java.util.List.of(record));

    int reclaimed =
        statsRepository.deleteUnreferencedGenerations(
            encodedTable, uri -> false, System.currentTimeMillis(), 0L);

    assertThat(reclaimed)
        .as("the encoded prefix must surface the superseded generation")
        .isEqualTo(1);
    assertThat(blobStore.get(gen1)).isNull();
  }

  @Test
  void negativeAgeGenerationSurvivesReclaimEvenAtMinAgeZero() {
    // A generation whose manifest was published AFTER the pass began (lastModified later than the
    // frozen pass-start nowMs) must be fenced even at min-age 0 — GC must not reclaim an in-flight
    // publish mid-sweep. Simulate by passing a pass-start an hour before the (just-written) blobs.
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    StatsRepository statsRepository = new StatsRepository(pointerStore, blobStore);
    long snapshotId = 3L;
    var record =
        ai.floedb.floecat.stats.identity.TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder().setRowCount(1L).build(),
            null);
    statsRepository.replaceAllStatsForSnapshot(TABLE_ID, snapshotId, java.util.List.of(record));
    String gen1 = statsRepository.activeStatsGeneration(TABLE_ID, snapshotId).orElseThrow();
    statsRepository.replaceAllStatsForSnapshot(TABLE_ID, snapshotId, java.util.List.of(record));

    long passStartBeforeTheWrites = System.currentTimeMillis() - 3_600_000L;
    int reclaimed =
        statsRepository.deleteUnreferencedGenerations(
            TABLE_ID, uri -> false, passStartBeforeTheWrites, 0L);

    assertThat(reclaimed).as("negative-age generation is fenced even at min-age 0").isZero();
    assertThat(blobStore.get(gen1)).as("the mid-sweep-published generation survives").isNotNull();
  }

  @Test
  void puttingStatsDoesNotAdvanceTablePointerVersion() {
    InMemoryPointerStore pointerStore = new InMemoryPointerStore();
    InMemoryBlobStore blobStore = new InMemoryBlobStore();
    TableRepository tableRepository = new TableRepository(pointerStore, blobStore);
    StatsRepository statsRepository = new StatsRepository(pointerStore, blobStore);
    long snapshotId = 9191L;
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("a")
            .setId("c")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("a")
            .setId("n")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    tableRepository.create(
        Table.newBuilder()
            .setResourceId(TABLE_ID)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .build());
    long before = tableRepository.metaFor(TABLE_ID).getPointerVersion();

    statsRepository.putTargetStats(
        TargetStatsRecords.tableRecord(
            TABLE_ID,
            snapshotId,
            TableValueStats.newBuilder().setRowCount(42L).setDataFileCount(3L).build(),
            null));

    long after = tableRepository.metaFor(TABLE_ID).getPointerVersion();

    assertThat(after).isEqualTo(before);
    assertThat(
            statsRepository.metaForTargetStats(
                TABLE_ID,
                snapshotId,
                StatsTargetIdentity.tableTarget(),
                com.google.protobuf.Timestamp.getDefaultInstance()))
        .extracting(ai.floedb.floecat.common.rpc.MutationMeta::getPointerVersion)
        .isEqualTo(1L);
  }

  // ── getTargetStatsBatch ────────────────────────────────────────────────────

  @Test
  void batchReturnsAllRequestedTargetsInOneCall() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9001L;

    // Write 5 column stats.
    for (long colId = 1; colId <= 5; colId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE_ID,
              snapshotId,
              colId,
              ScalarStats.newBuilder()
                  .setDisplayName("col" + colId)
                  .setLogicalType("BIGINT")
                  .setRowCount(colId * 10)
                  .build(),
              null));
    }

    var targets =
        List.of(
            StatsTargetIdentity.columnTarget(1L),
            StatsTargetIdentity.columnTarget(3L),
            StatsTargetIdentity.columnTarget(5L));
    var batch = repository.getTargetStatsBatch(TABLE_ID, snapshotId, targets);

    assertThat(batch).hasSize(3);
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(1L))))
        .isPresent()
        .get()
        .extracting(r -> r.getScalar().getDisplayName())
        .isEqualTo("col1");
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(3L))))
        .isPresent()
        .get()
        .extracting(r -> r.getScalar().getRowCount())
        .isEqualTo(30L);
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(5L))))
        .isPresent();
  }

  @Test
  void batchReturnsEmptyForUnknownSnapshot() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var targets = List.of(StatsTargetIdentity.columnTarget(1L));
    var batch = repository.getTargetStatsBatch(TABLE_ID, 99999L, targets);
    assertThat(batch).hasSize(1);
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(1L))))
        .isEmpty();
  }

  @Test
  void batchReturnsMissForUnwrittenTarget() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    long snapshotId = 9002L;
    // Write col 1 only.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(1L).build(),
            null));

    var batch =
        repository.getTargetStatsBatch(
            TABLE_ID,
            snapshotId,
            List.of(
                StatsTargetIdentity.columnTarget(1L), // written
                StatsTargetIdentity.columnTarget(2L))); // not written

    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(1L))))
        .isPresent();
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(2L))))
        .isEmpty();
  }

  @Test
  void batchHandlesEmptyTargetList() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    assertThat(repository.getTargetStatsBatch(TABLE_ID, 1L, List.of())).isEmpty();
    assertThat(repository.getTargetStatsBatch(TABLE_ID, 1L, null)).isEmpty();
  }

  // ── getStaleTargetStatsBatch / latest-snapshot index ─────────────────────

  @Test
  void staleIndexAdvancesToNewerSnapshot() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    // Write stats for snapshot 100, then snapshot 200.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            100L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(10L).build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            200L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(20L).build(),
            null));

    // Asking for snapshot 300 (no exact stats) should return the latest — snapshot 200.
    var target = StatsTargetIdentity.columnTarget(1L);
    Optional<TargetStatsRecord> stale = repository.getStaleTargetStats(TABLE_ID, 300L, target);
    assertThat(stale).isPresent();
    assertThat(stale.get().getScalar().getRowCount()).isEqualTo(20L);
  }

  @Test
  void staleBatchReturnsLatestSnapshotForAllTargets() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    // Write 3 columns for snapshot 50, only col1+col2 for snapshot 60.
    for (long colId = 1; colId <= 3; colId++) {
      repository.putTargetStats(
          TargetStatsRecords.columnRecord(
              TABLE_ID,
              50L,
              colId,
              ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(50L).build(),
              null));
    }
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            60L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(60L).build(),
            null));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            60L,
            2L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(61L).build(),
            null));

    // Request stale for snapshot 999 (no exact stats). Index points to snapshot 60.
    var targets =
        List.of(
            StatsTargetIdentity.columnTarget(1L),
            StatsTargetIdentity.columnTarget(2L),
            StatsTargetIdentity.columnTarget(3L));
    var batch = repository.getStaleTargetStatsBatch(TABLE_ID, 999L, targets);

    // col1 and col2 are in snapshot 60 (latest); col3 is absent from snapshot 60 but
    // present in snapshot 50 — fallback scan must surface it.
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(1L))))
        .isPresent()
        .get()
        .extracting(r -> r.getScalar().getRowCount())
        .isEqualTo(60L);
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(2L))))
        .isPresent()
        .get()
        .extracting(r -> r.getScalar().getRowCount())
        .isEqualTo(61L);
    assertThat(batch.get(StatsTargetIdentity.storageId(StatsTargetIdentity.columnTarget(3L))))
        .isPresent()
        .get()
        .extracting(r -> r.getScalar().getRowCount())
        .isEqualTo(50L);
  }

  @Test
  void staleBatchMatchesSinglePathWhenPerTargetPointerIsNewer() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    // Initial capture at snapshot 100 advances both the table-level and per-target pointers.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            100L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(100L).build(),
            null));

    // Replace advances the per-target pointer to 200 but NOT the table-level pointer. A
    // table-level-first batch would then serve the older snapshot-100 stats while the single
    // stale lookup (per-target-first) serves snapshot 200.
    repository.replaceAllStatsForSnapshot(
        TABLE_ID,
        200L,
        List.of(
            TargetStatsRecords.columnRecord(
                TABLE_ID,
                200L,
                1L,
                ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(200L).build(),
                null)));

    var target = StatsTargetIdentity.columnTarget(1L);
    Optional<TargetStatsRecord> single = repository.getStaleTargetStats(TABLE_ID, 999L, target);
    var batch = repository.getStaleTargetStatsBatch(TABLE_ID, 999L, List.of(target));

    assertThat(single).isPresent();
    assertThat(single.get().getScalar().getRowCount()).isEqualTo(200L);
    assertThat(batch.get(StatsTargetIdentity.storageId(target)))
        .as("batch must resolve the same (newest) snapshot as the single stale lookup")
        .isEqualTo(single);
  }

  @Test
  void staleBatchUsesPerTargetIndexWrittenByBatchBeforeScanFallback() {
    RepoTestPointerStores.CountingPrefixScanPointerStore pointerStore =
        new RepoTestPointerStores.CountingPrefixScanPointerStore(new InMemoryPointerStore());
    StatsRepository repository = new StatsRepository(pointerStore, new InMemoryBlobStore());

    repository.putTargetStatsBatch(
        TABLE_ID,
        100L,
        List.of(
            TargetStatsRecords.columnRecord(
                TABLE_ID,
                100L,
                1L,
                ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(100L).build(),
                null),
            TargetStatsRecords.columnRecord(
                TABLE_ID,
                100L,
                2L,
                ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(101L).build(),
                null)));
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            200L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(200L).build(),
            null));

    var target = StatsTargetIdentity.columnTarget(2L);
    Optional<TargetStatsRecord> stale = repository.getStaleTargetStats(TABLE_ID, 999L, target);

    assertThat(stale).isPresent();
    assertThat(stale.get().getSnapshotId()).isEqualTo(100L);
    assertThat(stale.get().getScalar().getRowCount()).isEqualTo(101L);
    assertThat(pointerStore.listPointersByPrefixCalls())
        .as("batch writes must populate the per-target stale index instead of relying on scan")
        .isEqualTo(0);
  }

  @Test
  void staleIndexDoesNotReturnNewerSnapshot() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());

    // Write stats for snapshot 500 only.
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            500L,
            1L,
            ScalarStats.newBuilder().setLogicalType("BIGINT").setRowCount(500L).build(),
            null));

    // Request stale for snapshot 100 — the index points to 500, which is NEWER than 100.
    // Must return empty, not the snapshot-500 data.
    var target = StatsTargetIdentity.columnTarget(1L);
    assertThat(repository.getStaleTargetStats(TABLE_ID, 100L, target)).isEmpty();
    var batch = repository.getStaleTargetStatsBatch(TABLE_ID, 100L, List.of(target));
    assertThat(batch.get(StatsTargetIdentity.storageId(target))).isEmpty();
  }

  @Test
  void staleBatchReturnsEmptyWhenNoStatsExist() {
    StatsRepository repository =
        new StatsRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    var target = StatsTargetIdentity.columnTarget(1L);
    assertThat(repository.getStaleTargetStatsBatch(TABLE_ID, 999L, List.of(target)))
        .containsEntry(StatsTargetIdentity.storageId(target), Optional.empty());
  }

  @Test
  void aMissingFrozenGenerationManifestSurfacesTheDescriptiveError() {
    // S3BlobStore THROWS StorageNotFoundException on a miss (never returns null), so the
    // descriptive broken-retention error was dead code in production. A throwing store must still
    // reach it.
    var throwingBlobs =
        new InMemoryBlobStore() {
          @Override
          public byte[] get(String uri) {
            throw new ai.floedb.floecat.storage.errors.StorageNotFoundException(
                "no such blob: " + uri);
          }
        };
    var repo = new StatsRepository(new InMemoryPointerStore(), throwingBlobs);

    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            ai.floedb.floecat.service.repo.util.BaseResourceRepository.NotFoundException.class,
            () ->
                repo.listTargetStatsInGeneration(
                    TABLE_ID,
                    5L,
                    "s3://t/stats/5/manifest/gen.pb",
                    java.util.Optional.empty(),
                    10,
                    ""));
    org.junit.jupiter.api.Assertions.assertTrue(
        ex.getMessage().contains("frozen stats generation manifest missing"),
        "the descriptive retention-invariant error must survive an S3-style throwing miss");
  }

  @Test
  void batchReadFaultsKeepTheirOriginalExceptionType() {
    var pointers = new FailingTargetPointerStore();
    StatsRepository repository = new StatsRepository(pointers, new InMemoryBlobStore());
    long snapshotId = 101L;
    repository.putTargetStats(
        TargetStatsRecords.columnRecord(
            TABLE_ID,
            snapshotId,
            7L,
            ScalarStats.newBuilder()
                .setDisplayName("c7")
                .setLogicalType("BIGINT")
                .setRowCount(5L)
                .build(),
            null));
    pointers.failTargetReads = true;

    // The parallel batch path must rethrow the storage fault with its ORIGINAL type — the
    // instanceof-keyed gRPC error mapping never unwraps a CompletionException, so a wrapped
    // retryable fault would collapse into a generic INTERNAL on the batch path only.
    assertThatThrownBy(
            () ->
                repository.getTargetStatsBatch(
                    TABLE_ID, snapshotId, List.of(StatsTargetIdentity.columnTarget(7L))))
        .isInstanceOf(StorageAbortRetryableException.class);
  }

  /** Pointer store that fails reads of per-target stats pointers once armed (writes unaffected). */
  private static final class FailingTargetPointerStore extends InMemoryPointerStore {
    volatile boolean failTargetReads;

    @Override
    public Optional<Pointer> get(String key) {
      if (failTargetReads && Keys.generationFromTargetPointerKey(key) != null) {
        throw new StorageAbortRetryableException("injected fault: " + key);
      }
      return super.get(key);
    }
  }
}
