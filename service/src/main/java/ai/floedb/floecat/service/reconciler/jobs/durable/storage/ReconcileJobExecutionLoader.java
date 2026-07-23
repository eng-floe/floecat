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

package ai.floedb.floecat.service.reconciler.jobs.durable.storage;

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobDefinition;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ReconcileJobExecutionLoader {
  private ReconcilePayloadStore payloadStore;

  public void bind(ReconcilePayloadStore payloadStore) {
    this.payloadStore = payloadStore;
  }

  public StoredJobDefinition requireDefinition(StoredReconcileJob state) {
    return payloadStore.requireDefinition(state);
  }

  public ReconcileSnapshotTask compactSnapshotTask(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileSnapshotTask.empty();
    }
    return ReconcileSnapshotTask.of(
        state.snapshotTaskTableId,
        state.snapshotTaskSnapshotId,
        state.snapshotTaskSourceNamespace,
        state.snapshotTaskSourceTable,
        java.util.List.of(),
        state.snapshotTaskFileGroupPlanRecorded,
        ReconcileSnapshotTask.CompletionMode.fromString(state.snapshotTaskCompletionMode),
        state.snapshotPlanBlobUri == null ? "" : state.snapshotPlanBlobUri,
        Math.max(0, state.snapshotTaskFileGroupCount),
        (int) Math.max(0L, state.snapshotTaskSourceFileCount),
        state.snapshotTaskDirectStatsBlobUri == null ? "" : state.snapshotTaskDirectStatsBlobUri,
        (int) Math.max(0L, state.snapshotTaskDirectStatsRecordCount));
  }

  public ReconcileFileGroupTask compactFileGroupTask(StoredReconcileJob state) {
    if (state == null) {
      return ReconcileFileGroupTask.empty();
    }
    return ReconcileFileGroupTask.of(
        state.fileGroupPlanId,
        state.fileGroupGroupId,
        state.fileGroupTableId,
        state.fileGroupSnapshotId,
        state.fileGroupFileCount,
        "",
        0,
        java.util.List.of(),
        java.util.List.of());
  }
}
