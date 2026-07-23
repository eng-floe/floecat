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
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import java.util.List;

interface RemoteSnapshotFinalizeWorkerClient {
  record FileStatsRecordLocation(
      TargetStatsRecord record,
      String payloadUri,
      int recordIndex,
      long byteOffset,
      int byteLength,
      byte[] recordSha256) {}

  StandaloneSnapshotFinalizeExecutionPayload getSnapshotFinalizeInput(RemoteLeasedJob lease);

  List<ReconcileFileGroupResultDescriptor> listSnapshotFileGroupResults(RemoteLeasedJob lease);

  boolean submitSnapshotFinalizeSuccess(
      RemoteLeasedJob lease,
      String resultId,
      String statsPayloadUri,
      String captureManifestUri,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<FileStatsRecordLocation> fileStats,
      List<TargetStatsRecord> finalStats,
      List<IndexArtifactRecord> indexArtifacts);

  boolean submitSnapshotFinalizeFailure(RemoteLeasedJob lease, String resultId, String message);
}
