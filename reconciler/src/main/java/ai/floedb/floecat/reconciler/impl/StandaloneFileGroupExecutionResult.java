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

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.List;

public record StandaloneFileGroupExecutionResult(
    String resultId,
    List<TargetStatsRecord> statsRecords,
    FileStatsBlobManifest fileStatsBlobManifest,
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts,
    List<PreUploadedIndexArtifact> preUploadedIndexArtifacts) {
  public StandaloneFileGroupExecutionResult {
    resultId = resultId == null ? "" : resultId.trim();
    statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
    fileStatsBlobManifest =
        fileStatsBlobManifest == null ? FileStatsBlobManifest.empty() : fileStatsBlobManifest;
    stagedIndexArtifacts =
        stagedIndexArtifacts == null ? List.of() : List.copyOf(stagedIndexArtifacts);
    preUploadedIndexArtifacts =
        preUploadedIndexArtifacts == null ? List.of() : List.copyOf(preUploadedIndexArtifacts);
  }

  public StandaloneFileGroupExecutionResult(
      String resultId,
      List<TargetStatsRecord> statsRecords,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    this(resultId, statsRecords, FileStatsBlobManifest.empty(), stagedIndexArtifacts, List.of());
  }

  public static StandaloneFileGroupExecutionResult empty(String resultId) {
    return new StandaloneFileGroupExecutionResult(
        resultId, List.of(), FileStatsBlobManifest.empty(), List.of(), List.of());
  }

  public record FileStatsBlobManifest(String blobUri, int recordCount) {
    public FileStatsBlobManifest {
      blobUri = blobUri == null ? "" : blobUri.trim();
      recordCount = Math.max(0, recordCount);
    }

    public static FileStatsBlobManifest empty() {
      return new FileStatsBlobManifest("", 0);
    }

    public boolean isEmpty() {
      return blobUri.isBlank() || recordCount <= 0;
    }
  }

  public record PreUploadedIndexArtifact(
      IndexArtifactRecord record, String contentType, String uploadedArtifactUri) {
    public PreUploadedIndexArtifact {
      record = record == null ? IndexArtifactRecord.getDefaultInstance() : record;
      contentType = contentType == null ? "" : contentType;
      uploadedArtifactUri = uploadedArtifactUri == null ? "" : uploadedArtifactUri.trim();
    }
  }
}
