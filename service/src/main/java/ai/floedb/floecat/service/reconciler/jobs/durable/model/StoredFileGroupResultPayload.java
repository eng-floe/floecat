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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import java.util.List;

public class StoredFileGroupResultPayload {
  public String fileStatsBlobUri = "";
  public int fileStatsRecordCount;
  public List<String> filePaths = List.of();
  public List<ReconcileFileResult> fileResults = List.of();
  public List<byte[]> partialAggregateRecordPayloads = List.of();

  public static StoredFileGroupResultPayload of(ReconcileFileGroupTask task) {
    ReconcileFileGroupTask effective = task == null ? ReconcileFileGroupTask.empty() : task;
    StoredFileGroupResultPayload payload = new StoredFileGroupResultPayload();
    payload.fileStatsBlobUri =
        effective.fileStatsBlobUri() == null ? "" : effective.fileStatsBlobUri();
    payload.fileStatsRecordCount = effective.fileStatsRecordCount();
    payload.filePaths = effective.filePaths() == null ? List.of() : effective.filePaths();
    payload.fileResults = effective.fileResults() == null ? List.of() : effective.fileResults();
    payload.partialAggregateRecordPayloads =
        effective.partialAggregateRecords() == null
            ? List.of()
            : effective.partialAggregateRecords().stream()
                .map(TargetStatsRecord::toByteArray)
                .toList();
    return payload;
  }

  public String fileStatsBlobUri() {
    return fileStatsBlobUri == null ? "" : fileStatsBlobUri;
  }

  public int fileStatsRecordCount() {
    return Math.max(0, fileStatsRecordCount);
  }

  public List<String> filePaths() {
    return filePaths == null ? List.of() : filePaths;
  }

  public List<ReconcileFileResult> fileResults() {
    return fileResults == null ? List.of() : fileResults;
  }

  public List<TargetStatsRecord> partialAggregateRecords() {
    return partialAggregateRecordPayloads == null
        ? List.of()
        : partialAggregateRecordPayloads.stream()
            .map(StoredFileGroupResultPayload::parseTargetStatsRecord)
            .toList();
  }

  private static TargetStatsRecord parseTargetStatsRecord(byte[] payload) {
    try {
      return TargetStatsRecord.parseFrom(payload);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to decode partial aggregate record payload", e);
    }
  }
}
