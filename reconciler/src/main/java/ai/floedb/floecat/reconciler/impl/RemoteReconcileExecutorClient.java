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

import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.Optional;

interface RemoteReconcileExecutorClient {
  Optional<RemoteLeasedJob> lease(ReconcileExecutor executor);

  void start(RemoteLeasedJob lease, String executorId);

  LeaseHeartbeat renew(RemoteLeasedJob lease);

  LeaseHeartbeat reportProgress(
      RemoteLeasedJob lease,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message);

  CompletionResult complete(
      RemoteLeasedJob lease,
      RemoteLeasedJob.CompletionState state,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message);

  boolean cancellationRequested(RemoteLeasedJob lease);

  record LeaseHeartbeat(boolean leaseValid, boolean cancellationRequested) {}

  record CompletionResult(boolean accepted) {}
}
