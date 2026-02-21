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

package ai.floedb.floecat.service.query.impl;

import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.DeleteFile;
import ai.floedb.floecat.query.rpc.TableInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Tracks state for an open scan handle: metadata, batching hints, and cached delete metadata. */
public final class ScanSession {

  private final String handleId;
  private final String queryId;
  private final ResourceId tableId;
  private final long snapshotId;
  private final TableInfo tableInfo;
  private final boolean includeColumnStats;
  private final boolean excludePartitionDataJson;
  private final int targetBatchItems;
  private final int targetBatchBytes;
  private final List<String> requiredColumns;
  private final List<Predicate> predicates;
  private final AtomicInteger nextDeleteId;
  private final List<Integer> deleteIdsByIndex;
  private final Map<String, Integer> deleteIdByPath;
  private final AtomicBoolean deletesComplete;
  private final AtomicReference<CompletableFuture<List<List<DeleteFileMetadata>>>>
      deleteBatchesFuture;

  private ScanSession(Builder builder) {
    this.handleId = builder.handleId;
    this.queryId = builder.queryId;
    this.tableId = builder.tableId;
    this.snapshotId = builder.snapshotId;
    this.tableInfo = builder.tableInfo;
    this.includeColumnStats = builder.includeColumnStats;
    this.excludePartitionDataJson = builder.excludePartitionDataJson;
    this.targetBatchItems = builder.targetBatchItems;
    this.targetBatchBytes = builder.targetBatchBytes;
    this.requiredColumns = builder.requiredColumns;
    this.predicates = builder.predicates;
    this.nextDeleteId = new AtomicInteger(0);
    this.deleteIdsByIndex = Collections.synchronizedList(new ArrayList<>());
    this.deleteIdByPath = new ConcurrentHashMap<>();
    this.deletesComplete = new AtomicBoolean(false);
    this.deleteBatchesFuture = new AtomicReference<>();
  }

  public String handleId() {
    return handleId;
  }

  public String queryId() {
    return queryId;
  }

  public ResourceId tableId() {
    return tableId;
  }

  public long snapshotId() {
    return snapshotId;
  }

  public TableInfo tableInfo() {
    return tableInfo;
  }

  public boolean includeColumnStats() {
    return includeColumnStats;
  }

  public boolean excludePartitionDataJson() {
    return excludePartitionDataJson;
  }

  public int targetBatchItems() {
    return targetBatchItems;
  }

  public int targetBatchBytes() {
    return targetBatchBytes;
  }

  public int nextDeleteId() {
    return nextDeleteId.getAndIncrement();
  }

  public int recordDeleteId(int deleteId) {
    synchronized (deleteIdsByIndex) {
      deleteIdsByIndex.add(deleteId);
      return deleteIdsByIndex.size() - 1;
    }
  }

  public List<Integer> recordedDeleteIds() {
    synchronized (deleteIdsByIndex) {
      return new ArrayList<>(deleteIdsByIndex);
    }
  }

  public Map<String, Integer> deleteIdByPath() {
    return deleteIdByPath;
  }

  public boolean deletesComplete() {
    return deletesComplete.get();
  }

  public void markDeletesComplete() {
    deletesComplete.set(true);
  }

  public CompletableFuture<List<List<DeleteFileMetadata>>> deleteBatchesFuture() {
    return deleteBatchesFuture.get();
  }

  public boolean initDeleteBatchesFuture(CompletableFuture<List<List<DeleteFileMetadata>>> future) {
    return this.deleteBatchesFuture.compareAndSet(null, future);
  }

  public void cacheDeleteBatches(List<List<DeleteFileMetadata>> batches) {
    CompletableFuture<List<List<DeleteFileMetadata>>> future = this.deleteBatchesFuture.get();
    if (future != null && !future.isDone()) {
      future.complete(List.copyOf(batches));
    }
  }

  public static final class DeleteFileMetadata {
    private final int deleteId;
    private final String filePath;
    private final String fileFormat;
    private final long fileSizeInBytes;
    private final long recordCount;
    private final ScanFileContent fileContent;
    private final String partitionDataJson;
    private final int partitionSpecId;
    private final List<Integer> equalityFieldIds;
    private final OptionalLong sequenceNumber;

    private DeleteFileMetadata(
        int deleteId,
        String filePath,
        String fileFormat,
        long fileSizeInBytes,
        long recordCount,
        ScanFileContent fileContent,
        String partitionDataJson,
        int partitionSpecId,
        List<Integer> equalityFieldIds,
        OptionalLong sequenceNumber) {
      this.deleteId = deleteId;
      this.filePath = filePath;
      this.fileFormat = fileFormat;
      this.fileSizeInBytes = fileSizeInBytes;
      this.recordCount = recordCount;
      this.fileContent = fileContent;
      this.partitionDataJson = partitionDataJson;
      this.partitionSpecId = partitionSpecId;
      this.equalityFieldIds = equalityFieldIds;
      this.sequenceNumber = sequenceNumber;
    }

    public static DeleteFileMetadata fromDeleteFile(DeleteFile file) {
      ScanFile scanFile = file.getFile();
      return new DeleteFileMetadata(
          file.getDeleteId(),
          scanFile.getFilePath(),
          scanFile.getFileFormat(),
          scanFile.getFileSizeInBytes(),
          scanFile.getRecordCount(),
          scanFile.getFileContent(),
          scanFile.getPartitionDataJson(),
          scanFile.getPartitionSpecId(),
          List.copyOf(scanFile.getEqualityFieldIdsList()),
          scanFile.hasSequenceNumber()
              ? OptionalLong.of(scanFile.getSequenceNumber())
              : OptionalLong.empty());
    }

    public DeleteFile toDeleteFile() {
      ScanFile.Builder builder =
          ScanFile.newBuilder()
              .setFilePath(filePath)
              .setFileFormat(fileFormat)
              .setFileSizeInBytes(fileSizeInBytes)
              .setRecordCount(recordCount)
              .setFileContent(fileContent)
              .setPartitionDataJson(partitionDataJson)
              .setPartitionSpecId(partitionSpecId)
              .addAllEqualityFieldIds(equalityFieldIds);
      sequenceNumber.ifPresent(builder::setSequenceNumber);
      return DeleteFile.newBuilder().setDeleteId(deleteId).setFile(builder.build()).build();
    }
  }

  public List<String> requiredColumns() {
    return requiredColumns;
  }

  public List<Predicate> predicates() {
    return predicates;
  }

  /** Builds a ScanSession before handing the handle to the store. */
  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private String handleId;
    private String queryId;
    private ResourceId tableId;
    private long snapshotId;
    private TableInfo tableInfo;
    private boolean includeColumnStats;
    private boolean excludePartitionDataJson;
    private int targetBatchItems;
    private int targetBatchBytes;

    private List<String> requiredColumns = List.of();
    private List<Predicate> predicates = List.of();

    private Builder() {}

    public Builder handleId(String handleId) {
      this.handleId = handleId;
      return this;
    }

    public Builder queryId(String queryId) {
      this.queryId = queryId;
      return this;
    }

    public Builder tableId(ResourceId tableId) {
      this.tableId = tableId;
      return this;
    }

    public Builder snapshotId(long snapshotId) {
      this.snapshotId = snapshotId;
      return this;
    }

    public Builder tableInfo(TableInfo tableInfo) {
      this.tableInfo = tableInfo;
      return this;
    }

    public Builder includeColumnStats(boolean includeColumnStats) {
      this.includeColumnStats = includeColumnStats;
      return this;
    }

    public Builder excludePartitionDataJson(boolean excludePartitionDataJson) {
      this.excludePartitionDataJson = excludePartitionDataJson;
      return this;
    }

    public Builder targetBatchItems(int targetBatchItems) {
      this.targetBatchItems = targetBatchItems;
      return this;
    }

    public Builder targetBatchBytes(int targetBatchBytes) {
      this.targetBatchBytes = targetBatchBytes;
      return this;
    }

    public Builder requiredColumns(List<String> requiredColumns) {
      this.requiredColumns = requiredColumns == null ? List.of() : List.copyOf(requiredColumns);
      return this;
    }

    public Builder predicates(List<Predicate> predicates) {
      this.predicates = predicates == null ? List.of() : List.copyOf(predicates);
      return this;
    }

    public ScanSession build() {
      return new ScanSession(this);
    }
  }
}
