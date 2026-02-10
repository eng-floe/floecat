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

package ai.floedb.floecat.service.execution.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.DataFile;
import ai.floedb.floecat.query.rpc.DataFileBatch;
import ai.floedb.floecat.query.rpc.DeleteFile;
import ai.floedb.floecat.query.rpc.DeleteFileBatch;
import ai.floedb.floecat.query.rpc.DeleteRef;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.service.common.ScanPruningUtils;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.query.impl.ScanSession;
import ai.floedb.floecat.service.query.impl.ScanSession.DeleteFileMetadata;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiEmitter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
/**
 * Builds and streams ScanFile batches by pulling stats + deletes directly from the repositories.
 */
public class ScanBundleService {

  private static final int FILE_STATS_PAGE_SIZE = 1000;

  private final TableRepository tables;
  private final SnapshotRepository snapshots;
  private final StatsRepository stats;

  @Inject
  public ScanBundleService(
      TableRepository tables, SnapshotRepository snapshots, StatsRepository stats) {
    this.tables = tables;
    this.snapshots = snapshots;
    this.stats = stats;
  }

  /** Loads table & snapshot metadata and builds the initial TableInfo needed for a scan handle. */
  public InitData initScan(String correlationId, ResourceId tableId, long snapshotId) {
    Table table =
        tables
            .getById(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(correlationId, TABLE, Map.of("id", tableId.getId())));

    Snapshot snapshot =
        snapshots
            .getById(tableId, snapshotId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId,
                        SNAPSHOT,
                        Map.of(
                            "table_id",
                            tableId.getId(),
                            "snapshot_id",
                            Long.toString(snapshotId))));

    TableInfo info = buildTableInfo(table, snapshot);
    return new InitData(tableId, snapshotId, info);
  }

  /** Streams delete batches, caching metadata for potential retries before completion. */
  public Multi<DeleteFileBatch> streamDeleteFiles(ScanSession session, String correlationId) {
    CompletableFuture<List<List<DeleteFileMetadata>>> existing = session.deleteBatchesFuture();
    if (existing != null) {
      List<DeleteFileBatch> cached = batchesFromMetadata(existing.join());
      if (!session.deletesComplete()) {
        session.markDeletesComplete();
      }
      return Multi.createFrom().iterable(cached);
    }
    CompletableFuture<List<List<DeleteFileMetadata>>> completion = new CompletableFuture<>();
    if (!session.initDeleteBatchesFuture(completion)) {
      CompletableFuture<List<List<DeleteFileMetadata>>> active = session.deleteBatchesFuture();
      List<DeleteFileBatch> cached = batchesFromMetadata(active.join());
      if (!session.deletesComplete()) {
        session.markDeletesComplete();
      }
      return Multi.createFrom().iterable(cached);
    }
    completion.whenComplete(
        (ignored, failure) -> {
          if (failure == null) {
            session.markDeletesComplete();
          }
        });
    return Multi.createFrom()
        .emitter(
            emitter -> {
              List<List<DeleteFileMetadata>> batches = new ArrayList<>();
              try {
                emitDeleteBatches(session, emitter, batches);
                completion.complete(List.copyOf(batches));
                emitter.complete();
              } catch (RuntimeException e) {
                completion.completeExceptionally(e);
                emitter.fail(e);
              }
            });
  }

  /** Streams data files after delete streaming has completed. */
  public Multi<DataFileBatch> streamDataFiles(ScanSession session, String correlationId) {
    if (!session.deletesComplete()) {
      throw GrpcErrors.preconditionFailed(
          correlationId, SCAN_DELETES_NOT_COMPLETE, Map.of("handle", session.handleId()));
    }
    return Multi.createFrom()
        .emitter(
            emitter -> {
              try {
                emitDataBatches(session, emitter);
                emitter.complete();
              } catch (RuntimeException e) {
                emitter.fail(e);
              }
            });
  }

  /** Pulls delete file stats pages, emits delete batches, and records metadata for replay. */
  private void emitDeleteBatches(
      ScanSession session,
      MultiEmitter<? super DeleteFileBatch> emitter,
      List<List<DeleteFileMetadata>> stored) {
    int batchItems = Math.max(1, session.targetBatchItems());
    int batchBytes = Math.max(1, session.targetBatchBytes());
    String pageToken = "";
    List<DeleteFile> batch = new ArrayList<>();
    List<DeleteFileMetadata> metaBatch = new ArrayList<>();
    long bytes = 0;
    // Byte heuristic uses on-disk size rather than actual serialized payload; prefer
    // targetBatchItems when include_column_stats means column metadata dominates the grpc frame.
    do {
      StringBuilder next = new StringBuilder();
      List<FileColumnStats> page =
          stats.listFileStats(
              session.tableId(), session.snapshotId(), FILE_STATS_PAGE_SIZE, pageToken, next);
      for (FileColumnStats fcs : page) {
        if (fcs.getFileContent() == FileContent.FC_DATA) {
          continue;
        }
        DeleteFile deleteFile = toDeleteFile(session, fcs);
        if (deleteFile != null) {
          batch.add(deleteFile);
          metaBatch.add(DeleteFileMetadata.fromDeleteFile(deleteFile));
          bytes += Math.max(0L, fcs.getSizeBytes());
          if (batch.size() >= batchItems || bytes >= batchBytes) {
            DeleteFileBatch chunk = buildDeleteBatch(batch);
            emitter.emit(chunk);
            stored.add(List.copyOf(metaBatch));
            batch = new ArrayList<>();
            metaBatch = new ArrayList<>();
            bytes = 0;
          }
        }
      }
      pageToken = next.toString();
    } while (!pageToken.isBlank());

    if (!batch.isEmpty()) {
      DeleteFileBatch chunk = buildDeleteBatch(batch);
      emitter.emit(chunk);
      stored.add(List.copyOf(metaBatch));
    }
  }

  /** Streams data file batches after deletes are ready, honoring batching hints. */
  private void emitDataBatches(ScanSession session, MultiEmitter<? super DataFileBatch> emitter) {
    int batchItems = Math.max(1, session.targetBatchItems());
    int batchBytes = Math.max(1, session.targetBatchBytes());
    String pageToken = "";
    List<DataFile> batch = new ArrayList<>();
    long bytes = 0;
    do {
      StringBuilder next = new StringBuilder();
      List<FileColumnStats> page =
          stats.listFileStats(
              session.tableId(), session.snapshotId(), FILE_STATS_PAGE_SIZE, pageToken, next);
      for (FileColumnStats fcs : page) {
        if (fcs.getFileContent() != FileContent.FC_DATA) {
          continue;
        }
        DataFile dataFile = toDataFile(session, fcs);
        if (dataFile == null) {
          continue;
        }
        batch.add(dataFile);
        bytes += Math.max(0L, fcs.getSizeBytes());
        if (batch.size() >= batchItems || bytes >= batchBytes) {
          emitter.emit(buildDataBatch(batch));
          batch = new ArrayList<>();
          bytes = 0;
        }
      }
      pageToken = next.toString();
    } while (!pageToken.isBlank());

    if (!batch.isEmpty()) {
      emitter.emit(buildDataBatch(batch));
    }
  }

  /** Convenience builder for delete batches. */
  private DeleteFileBatch buildDeleteBatch(List<DeleteFile> batch) {
    return DeleteFileBatch.newBuilder().addAllItems(batch).build();
  }

  /** Convenience builder for data batches. */
  private DataFileBatch buildDataBatch(List<DataFile> batch) {
    return DataFileBatch.newBuilder().addAllItems(batch).build();
  }

  /** Converts a FileColumnStats entry into a DeleteFile, applying pruning filters. */
  private DeleteFile toDeleteFile(ScanSession session, FileColumnStats fcs) {
    ScanFile scanFile = applySessionFilters(session, toScanFileBuilder(fcs));
    if (scanFile == null) {
      return null;
    }
    int deleteId = session.nextDeleteId();
    // Reserved for future delete->data mapping; currently we always emit all_deletes=true.
    session.deleteIdByPath().put(scanFile.getFilePath(), deleteId);
    session.recordDeleteId(deleteId);
    return DeleteFile.newBuilder().setDeleteId(deleteId).setFile(scanFile).build();
  }

  /**
   * Converts a FileColumnStats entry into a DataFile, attaching the current delete reference state.
   */
  private DataFile toDataFile(ScanSession session, FileColumnStats fcs) {
    ScanFile scanFile = applySessionFilters(session, toScanFileBuilder(fcs));
    if (scanFile == null) {
      return null;
    }
    List<Integer> recorded = session.recordedDeleteIds();
    if (recorded.isEmpty()) {
      return DataFile.newBuilder().setFile(scanFile).build();
    }
    DeleteRef.Builder deletes = DeleteRef.newBuilder().setAllDeletes(true);
    // TODO(mrouvroy): switch to DeleteIdList when we know per-file applicability
    // (equality/partition filters).
    return DataFile.newBuilder().setFile(scanFile).setDeletes(deletes).build();
  }

  /** Reconstructs delete batches from cached metadata for retries. */
  private List<DeleteFileBatch> batchesFromMetadata(List<List<DeleteFileMetadata>> metadata) {
    return metadata.stream()
        .map(
            batch ->
                DeleteFileBatch.newBuilder()
                    .addAllItems(batch.stream().map(DeleteFileMetadata::toDeleteFile).toList())
                    .build())
        .toList();
  }

  /** Builds a ScanFile from raw stats. */
  private ScanFile toScanFile(FileColumnStats fcs) {
    return toScanFileBuilder(fcs).build();
  }

  /** Builds a scan file builder from FileColumnStats for further filtering. */
  private ScanFile.Builder toScanFileBuilder(FileColumnStats fcs) {
    ScanFile.Builder builder =
        ScanFile.newBuilder()
            .setFilePath(fcs.getFilePath())
            .setFileFormat(fcs.getFileFormat())
            .setFileSizeInBytes(fcs.getSizeBytes())
            .setRecordCount(fcs.getRowCount())
            .setPartitionDataJson(fcs.getPartitionDataJson())
            .setPartitionSpecId(fcs.getPartitionSpecId())
            .addAllEqualityFieldIds(fcs.getEqualityFieldIdsList())
            .setFileContent(mapContent(fcs.getFileContent()))
            .addAllColumns(fcs.getColumnsList());
    if (fcs.hasSequenceNumber()) {
      builder.setSequenceNumber(fcs.getSequenceNumber());
    }
    return builder;
  }

  /**
   * Applies pruning, required-column filtering, and partition stripping before emitting a ScanFile.
   */
  private ScanFile applySessionFilters(ScanSession session, ScanFile.Builder builder) {
    if (session.excludePartitionDataJson()) {
      builder.clearPartitionDataJson();
    }
    boolean needsFiltering =
        (!session.requiredColumns().isEmpty() || !session.predicates().isEmpty())
            && builder.getColumnsCount() > 0;
    List<ColumnStats> filteredColumns = null;
    if (needsFiltering) {
      HashSet<String> lowerRequired = new HashSet<>();
      for (String col : session.requiredColumns()) {
        lowerRequired.add(col.toLowerCase(Locale.ROOT));
      }
      for (Predicate p : session.predicates()) {
        if (p.getColumn() != null && !p.getColumn().isBlank()) {
          lowerRequired.add(p.getColumn().toLowerCase(Locale.ROOT));
        }
      }
      long builderCount = builder.getColumnsCount();
      filteredColumns = new ArrayList<>();
      for (int i = 0; i < builderCount; i++) {
        ColumnStats stats = builder.getColumns(i);
        if (lowerRequired.contains(stats.getColumnName().toLowerCase(Locale.ROOT))) {
          filteredColumns.add(stats);
        }
      }
      builder.clearColumns();
      builder.addAllColumns(filteredColumns);
    }
    ScanFile file = builder.build();
    if (!ScanPruningUtils.matchesPredicates(file, session.predicates())) {
      return null;
    }
    if (!session.includeColumnStats()) {
      if (filteredColumns != null) {
        // The pruning step already worked on the filtered stats, so we can drop them now.
        return file.toBuilder().clearColumns().build();
      }
      return file.toBuilder().clearColumns().build();
    }
    return file;
  }

  /** Builds the TableInfo payload from catalog+snapshot state. */
  private TableInfo buildTableInfo(Table table, Snapshot snapshot) {
    TableInfo.Builder builder = TableInfo.newBuilder().setTableId(table.getResourceId());
    String schemaJson = snapshot.getSchemaJson();
    if (schemaJson == null || schemaJson.isBlank()) {
      schemaJson = table.getSchemaJson();
    }
    if (schemaJson != null && !schemaJson.isBlank()) {
      builder.setSchemaJson(schemaJson);
    }
    if (snapshot.hasPartitionSpec()) {
      builder.setPartitionSpecs(snapshot.getPartitionSpec());
    }
    if (table.getPropertiesCount() > 0) {
      builder.putAllProperties(table.getPropertiesMap());
    }
    String metadataLocation = table.getPropertiesMap().getOrDefault("metadata-location", "");
    if (!metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    return builder.build();
  }

  /** Map catalog FileContent enum into execution ScanFileContent. */
  private ScanFileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES;
      default -> ScanFileContent.SCAN_FILE_CONTENT_DATA;
    };
  }

  public record InitData(ResourceId tableId, long snapshotId, TableInfo tableInfo) {}
}
