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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.util.List;

/** Shared manifest construction used by local and remote Java snapshot finalizers. */
public final class SnapshotFinalizeManifestWriter {
  private SnapshotFinalizeManifestWriter() {}

  public static RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess prepare(
      BlobStore blobStore,
      ReconcileJobStore.LeasedJob lease,
      String resultId,
      String statsObjectPrefix,
      String durableCaptureManifestPrefix,
      String reusableArtifactIndexObjectPrefix,
      String statsGenerationManifestUri,
      String indexGenerationCaptureManifestPrefix,
      int sourceFileCount,
      List<ReconcileFileGroupResultDescriptor> fileGroups,
      List<StatsObjectDescriptor> fileStats,
      List<TargetStatsRecord> finalStats,
      List<StatsObjectDescriptor> indexArtifacts,
      List<ReusableArtifactBundleReference> reusableArtifactBundles,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors,
      ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor indexPredecessor,
      SnapshotPlanBlobStore.AppendOnlyBase appendOnlyBase) {
    return GrpcRemoteReconcileExecutorClient.prepareSnapshotFinalizeSuccess(
        blobStore,
        lease,
        resultId,
        statsObjectPrefix,
        durableCaptureManifestPrefix,
        reusableArtifactIndexObjectPrefix,
        statsGenerationManifestUri,
        indexGenerationCaptureManifestPrefix,
        sourceFileCount,
        fileGroups,
        fileStats,
        finalStats,
        indexArtifacts,
        reusableArtifactBundles,
        realizedStatsSelectors,
        realizedIndexSelectors,
        indexPredecessor,
        appendOnlyBase);
  }
}
