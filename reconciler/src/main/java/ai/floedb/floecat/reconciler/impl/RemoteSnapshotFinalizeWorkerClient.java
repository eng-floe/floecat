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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import java.util.List;

interface RemoteSnapshotFinalizeWorkerClient {
  StandaloneSnapshotFinalizeExecutionPayload getSnapshotFinalizeInput(RemoteLeasedJob lease);

  List<ReconcileFileGroupResultDescriptor> listSnapshotFileGroupResults(RemoteLeasedJob lease);

  PreparedSnapshotFinalizeSuccess prepareSnapshotFinalizeSuccess(
      RemoteLeasedJob lease,
      String resultId,
      String statsObjectPrefix,
      String captureManifestUri,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<StatsObjectDescriptor> fileStats,
      List<TargetStatsRecord> finalStats,
      List<StatsObjectDescriptor> indexArtifacts,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor indexPredecessor);

  boolean submitSnapshotFinalizeSuccess(
      RemoteLeasedJob lease, PreparedSnapshotFinalizeSuccess prepared);

  boolean submitSnapshotFinalizeFailure(RemoteLeasedJob lease, String resultId, String message);

  record PreparedSnapshotFinalizeSuccess(
      String resultId, SnapshotCaptureManifestDescriptor manifestDescriptor) {}
}
