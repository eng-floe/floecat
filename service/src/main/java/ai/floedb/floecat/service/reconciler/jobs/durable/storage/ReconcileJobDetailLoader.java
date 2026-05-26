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
public class ReconcileJobDetailLoader {
  private ReconcilePayloadStore payloadStore;

  public void bind(ReconcilePayloadStore payloadStore) {
    this.payloadStore = payloadStore;
  }

  public StoredJobDefinition requireDefinition(StoredReconcileJob state) {
    return payloadStore.requireDefinition(state);
  }

  public ReconcileSnapshotTask snapshotTask(StoredReconcileJob state) {
    return payloadStore.snapshotTaskForDetail(state);
  }

  public ReconcileFileGroupTask fileGroupTask(StoredReconcileJob state) {
    return payloadStore.fileGroupTaskForDetail(state);
  }
}
