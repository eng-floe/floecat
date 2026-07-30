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

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

/** Canonicalizes request-shaped reconcile scope into its resolved work identity. */
public final class ReconcileScopeCanonicalizer {
  private ReconcileScopeCanonicalizer() {}

  public static ReconcileScope resolvedWorkScope(
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileSnapshotTask snapshotTask,
      boolean captureRequestsAreExactWork) {
    ReconcileScope effective = scope == null ? ReconcileScope.empty() : scope;
    ReconcileSnapshotTask task =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;

    List<ReconcileScope.ScopedCaptureRequest> captureRequests =
        effective.destinationCaptureRequests().stream()
            .filter(request -> request != null)
            .filter(
                request ->
                    !hasResolvedSnapshot(jobKind, task)
                        || (task.tableId().equals(request.tableId())
                            && task.snapshotId() == request.snapshotId()))
            .sorted(
                Comparator.comparing(ReconcileScope.ScopedCaptureRequest::tableId)
                    .thenComparingLong(ReconcileScope.ScopedCaptureRequest::snapshotId)
                    .thenComparing(ReconcileScope.ScopedCaptureRequest::targetSpec)
                    .thenComparing(request -> String.join("\u0000", request.columnSelectors())))
            .toList();

    String resolvedTableId =
        resolvedTableId(effective, jobKind, task, captureRequests, captureRequestsAreExactWork);
    List<String> namespaces =
        resolvedTableId == null
            ? effective.destinationNamespaceIds().stream().sorted().toList()
            : List.of();
    ReconcileSnapshotSelection selection =
        resolvedSnapshotSelection(
            effective.snapshotSelection(),
            jobKind,
            task,
            captureRequests,
            captureRequestsAreExactWork);

    return ReconcileScope.of(
        namespaces,
        resolvedTableId,
        resolvedTableId == null ? effective.destinationViewId() : null,
        captureRequests,
        effective.capturePolicy(),
        selection);
  }

  private static String resolvedTableId(
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileSnapshotTask snapshotTask,
      List<ReconcileScope.ScopedCaptureRequest> captureRequests,
      boolean captureRequestsAreExactWork) {
    if (hasResolvedSnapshot(jobKind, snapshotTask)) {
      return snapshotTask.tableId();
    }
    if (scope.destinationTableId() != null) {
      return scope.destinationTableId();
    }
    if (!captureRequestsAreExactWork) {
      return null;
    }
    LinkedHashSet<String> requestTableIds = new LinkedHashSet<>();
    for (ReconcileScope.ScopedCaptureRequest request : captureRequests) {
      if (request != null && !request.tableId().isBlank()) {
        requestTableIds.add(request.tableId());
      }
    }
    return requestTableIds.size() == 1 ? requestTableIds.getFirst() : null;
  }

  private static ReconcileSnapshotSelection resolvedSnapshotSelection(
      ReconcileSnapshotSelection selection,
      ReconcileJobKind jobKind,
      ReconcileSnapshotTask snapshotTask,
      List<ReconcileScope.ScopedCaptureRequest> captureRequests,
      boolean captureRequestsAreExactWork) {
    if (hasResolvedSnapshot(jobKind, snapshotTask)) {
      return ReconcileSnapshotSelection.explicit(List.of(snapshotTask.snapshotId()));
    }
    if (!captureRequestsAreExactWork) {
      return selection == null ? ReconcileSnapshotSelection.unspecified() : selection;
    }
    LinkedHashSet<Long> resolvedSnapshotIds = new LinkedHashSet<>();
    for (ReconcileScope.ScopedCaptureRequest request : captureRequests) {
      if (request != null && request.snapshotId() >= 0L) {
        resolvedSnapshotIds.add(request.snapshotId());
      }
    }
    if (!resolvedSnapshotIds.isEmpty()) {
      return ReconcileSnapshotSelection.explicit(resolvedSnapshotIds.stream().sorted().toList());
    }
    return selection == null ? ReconcileSnapshotSelection.unspecified() : selection;
  }

  private static boolean hasResolvedSnapshot(
      ReconcileJobKind jobKind, ReconcileSnapshotTask snapshotTask) {
    return (jobKind == ReconcileJobKind.PLAN_SNAPSHOT
            || jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE)
        && snapshotTask != null
        && !snapshotTask.tableId().isBlank()
        && snapshotTask.snapshotId() >= 0L;
  }
}
