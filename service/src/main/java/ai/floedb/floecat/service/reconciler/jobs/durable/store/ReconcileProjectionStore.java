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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import ai.floedb.floecat.service.reconciler.jobs.durable.storage.ReconcilePayloadStore;
import java.util.List;

public interface ReconcileProjectionStore {
  void bind(
      ReconcileProjectionBackend projectionBackend, ReconcilePayloadStore payloadStore, int casMax);

  // Projection rows are intentionally a separate domain from canonical job-index state. They may
  // be updated by projection workflows, but they are not part of the no-drift canonical-index
  // invariant and are not repaired on reads.
  List<StoredJobContribution> loadDirectContributions(String accountId, String parentJobId);

  boolean upsertContribution(StoredJobContribution contribution);

  java.util.Optional<String> loadFileGroupResultReference(String accountId, String jobId);

  boolean upsertFileGroupResultReference(String accountId, String jobId, String blobUri);
}
