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

package ai.floedb.floecat.reconciler.jobs;

/** Compact durable metadata for one immutable, worker-published file-group result object. */
public record ReconcileFileGroupResultDescriptor(
    int formatVersion,
    String accountId,
    String connectorId,
    String parentJobId,
    String fileGroupJobId,
    String planId,
    String groupId,
    String tableId,
    long snapshotId,
    String leaseEpoch,
    String resultId,
    String payloadUri,
    long payloadBytes,
    String payloadSha256,
    int plannedFileCount,
    int succeededFileCount,
    int failedFileCount,
    int skippedFileCount,
    int partialAggregateRecordCount,
    int indexArtifactCount,
    String statsObjectPrefix,
    int fileStatsRecordCount,
    String artifactReferencesSha256,
    IndexGenerationPredecessor indexPredecessor,
    long createdAtMs) {

  public record IndexGenerationPredecessor(
      String generationId,
      long activePointerVersion,
      String captureManifestUri,
      long captureManifestPointerVersion) {
    public IndexGenerationPredecessor {
      generationId = generationId == null ? "" : generationId;
      captureManifestUri = captureManifestUri == null ? "" : captureManifestUri;
    }
  }

  public static ReconcileFileGroupResultDescriptor empty() {
    return new ReconcileFileGroupResultDescriptor(
        0, "", "", "", "", "", "", "", 0L, "", "", "", 0L, "", 0, 0, 0, 0, 0, 0, "", 0, "", null,
        0L);
  }

  public boolean isEmpty() {
    return formatVersion <= 0 || payloadUri == null || payloadUri.isBlank();
  }
}
