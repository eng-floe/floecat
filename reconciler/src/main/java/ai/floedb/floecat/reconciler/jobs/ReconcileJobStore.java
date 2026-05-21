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

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface ReconcileJobStore {
  default String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope) {
    return enqueuePlan(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "");
  }

  String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId);

  default BulkEnqueueResult bulkEnqueue(List<BulkEnqueueSpec> specs) {
    if (specs == null || specs.isEmpty()) {
      return new BulkEnqueueResult(List.of());
    }
    List<BulkEnqueueItemResult> items = new java.util.ArrayList<>(specs.size());
    for (int index = 0; index < specs.size(); index++) {
      BulkEnqueueSpec spec = specs.get(index);
      try {
        String jobId =
            enqueue(
                spec.accountId,
                spec.connectorId,
                spec.fullRescan,
                spec.captureMode,
                spec.scope,
                spec.jobKind,
                spec.tableTask,
                spec.viewTask,
                spec.snapshotTask,
                spec.fileGroupTask,
                spec.executionPolicy,
                spec.parentJobId,
                spec.pinnedExecutorId);
        items.add(new BulkEnqueueItemResult(index, jobId, true, ""));
      } catch (IllegalArgumentException e) {
        items.add(
            new BulkEnqueueItemResult(
                index, "", false, e.getMessage() == null ? "" : e.getMessage(), true));
      } catch (RuntimeException e) {
        items.add(
            new BulkEnqueueItemResult(
                index,
                "",
                false,
                e.getMessage() == null ? e.getClass().getName() : e.getMessage()));
      }
    }
    return new BulkEnqueueResult(items);
  }

  default String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        jobKind,
        tableTask,
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        jobKind,
        tableTask,
        viewTask,
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileExecutionPolicy executionPolicy,
      String pinnedExecutorId) {
    return enqueuePlan(
        accountId, connectorId, fullRescan, captureMode, scope, executionPolicy, pinnedExecutorId);
  }

  default String enqueuePlan(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileExecutionPolicy executionPolicy,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.PLAN_CONNECTOR,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        "",
        pinnedExecutorId);
  }

  default String enqueueTablePlan(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileTableTask tableTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.PLAN_TABLE,
        tableTask,
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueueViewPlan(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileViewTask viewTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.PLAN_VIEW,
        ReconcileTableTask.empty(),
        viewTask,
        ReconcileSnapshotTask.empty(),
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueueSnapshotPlan(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileSnapshotTask snapshotTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.PLAN_SNAPSHOT,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueueFileGroupExecution(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.EXEC_FILE_GROUP,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        ReconcileSnapshotTask.empty(),
        fileGroupTask,
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  default String enqueueSnapshotFinalization(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileSnapshotTask snapshotTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    ReconcileSnapshotTask effectiveSnapshotTask =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    if (!effectiveSnapshotTask.fileGroupPlanRecorded()) {
      throw new IllegalArgumentException(
          "FINALIZE_SNAPSHOT_CAPTURE requires explicit snapshot coverage metadata");
    }
    return enqueue(
        accountId,
        connectorId,
        fullRescan,
        captureMode,
        scope,
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        effectiveSnapshotTask,
        ReconcileFileGroupTask.empty(),
        executionPolicy,
        parentJobId,
        pinnedExecutorId);
  }

  Optional<ReconcileJob> get(String accountId, String jobId);

  default Optional<ReconcileJob> get(String jobId) {
    return get(null, jobId);
  }

  default Optional<ReconcileJob> getLeaseView(String jobId) {
    return get(jobId);
  }

  ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states);

  default ReconcileJobPage childJobsPage(
      String accountId, String parentJobId, int pageSize, String pageToken) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return new ReconcileJobPage(List.of(), "");
    }
    List<ReconcileJob> out = new java.util.ArrayList<>();
    String nextToken = pageToken == null ? "" : pageToken;
    int limit = Math.max(1, pageSize);
    do {
      ReconcileJobPage page = list(accountId, Math.max(200, limit), nextToken, "", Set.of());
      if (page == null || page.jobs == null || page.jobs.isEmpty()) {
        return new ReconcileJobPage(out, "");
      }
      for (ReconcileJob candidate : page.jobs) {
        if (parentJobId.equals(candidate.parentJobId)) {
          out.add(candidate);
          if (out.size() >= limit) {
            return new ReconcileJobPage(out, page.nextPageToken);
          }
        }
      }
      nextToken = page.nextPageToken;
    } while (nextToken != null && !nextToken.isBlank());
    return new ReconcileJobPage(out, "");
  }

  QueueStats queueStats();

  default Optional<LeasedJob> leaseNext() {
    return leaseNext(LeaseRequest.all());
  }

  Optional<LeasedJob> leaseNext(LeaseRequest request);

  boolean renewLease(String jobId, String leaseEpoch);

  default ProgressUpdate reportProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    boolean leaseValid = renewLease(jobId, leaseEpoch);
    if (leaseValid) {
      markProgress(
          jobId,
          leaseEpoch,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          message);
    }
    return new ProgressUpdate(leaseValid, isCancellationRequested(jobId));
  }

  default ProgressUpdate reportProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    return reportProgress(
        jobId,
        leaseEpoch,
        tablesScanned,
        tablesChanged,
        0L,
        0L,
        errors,
        snapshotsProcessed,
        statsProcessed,
        message);
  }

  void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId);

  void markProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message);

  default void markProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    markProgress(
        jobId,
        leaseEpoch,
        tablesScanned,
        tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessed,
        statsProcessed,
        message);
  }

  void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed);

  default void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    markSucceeded(
        jobId,
        leaseEpoch,
        finishedAtMs,
        tablesScanned,
        tablesChanged,
        0,
        0,
        snapshotsProcessed,
        statsProcessed);
  }

  void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  void markWaiting(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  void markFailedTerminal(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  default void markFailedTerminal(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    markFailedTerminal(
        jobId,
        leaseEpoch,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  default void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    markFailed(
        jobId,
        leaseEpoch,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  default void markWaiting(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    markWaiting(
        jobId,
        leaseEpoch,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  Optional<ReconcileJob> cancel(String accountId, String jobId, String reason);

  boolean isCancellationRequested(String jobId);

  void persistSnapshotPlan(String jobId, ReconcileSnapshotTask snapshotTask);

  void persistFileGroupResult(String jobId, ReconcileFileGroupTask fileGroupTask);

  void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  default void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    markCancelled(
        jobId,
        leaseEpoch,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  boolean applyLeaseOutcome(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  default boolean completeLease(
      String jobId,
      String leaseEpoch,
      CompletionKind completionKind,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    return applyLeaseOutcome(
        jobId,
        leaseEpoch,
        completionKind,
        finishedAtMs,
        message,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed);
  }

  final class BulkEnqueueSpec {
    public final String accountId;
    public final String connectorId;
    public final boolean fullRescan;
    public final CaptureMode captureMode;
    public final ReconcileScope scope;
    public final ReconcileJobKind jobKind;
    public final ReconcileTableTask tableTask;
    public final ReconcileViewTask viewTask;
    public final ReconcileSnapshotTask snapshotTask;
    public final ReconcileFileGroupTask fileGroupTask;
    public final ReconcileExecutionPolicy executionPolicy;
    public final String parentJobId;
    public final String pinnedExecutorId;

    public BulkEnqueueSpec(
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        ReconcileExecutionPolicy executionPolicy,
        String parentJobId,
        String pinnedExecutorId) {
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.fullRescan = fullRescan;
      this.captureMode = java.util.Objects.requireNonNull(captureMode, "captureMode");
      this.scope = scope == null ? ReconcileScope.empty() : scope;
      this.jobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
      this.tableTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
      this.viewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      this.snapshotTask = snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
      this.fileGroupTask = fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
      this.executionPolicy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      this.parentJobId = parentJobId == null ? "" : parentJobId;
      this.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId;
    }

    public static BulkEnqueueSpec of(
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        ReconcileExecutionPolicy executionPolicy,
        String parentJobId,
        String pinnedExecutorId) {
      return new BulkEnqueueSpec(
          accountId,
          connectorId,
          fullRescan,
          captureMode,
          scope,
          jobKind,
          tableTask,
          viewTask,
          snapshotTask,
          fileGroupTask,
          executionPolicy,
          parentJobId,
          pinnedExecutorId);
    }
  }

  final class BulkEnqueueItemResult {
    public final int index;
    public final String jobId;
    public final boolean created;
    public final String error;
    public final boolean invalidRequest;

    public BulkEnqueueItemResult(int index, String jobId, boolean created, String error) {
      this(index, jobId, created, error, false);
    }

    public BulkEnqueueItemResult(
        int index, String jobId, boolean created, String error, boolean invalidRequest) {
      this.index = Math.max(0, index);
      this.jobId = jobId == null ? "" : jobId;
      this.created = created;
      this.error = error == null ? "" : error;
      this.invalidRequest = invalidRequest;
    }

    public boolean succeeded() {
      return error.isBlank() && !jobId.isBlank();
    }
  }

  final class BulkEnqueueResult {
    public final List<BulkEnqueueItemResult> items;

    public BulkEnqueueResult(List<BulkEnqueueItemResult> items) {
      this.items = items == null ? List.of() : List.copyOf(items);
    }

    public boolean hasFailures() {
      return items.stream().anyMatch(item -> item == null || !item.succeeded());
    }

    public void requireAllSucceeded(String operation) {
      if (!hasFailures()) {
        return;
      }
      String reason =
          items.stream()
              .filter(item -> item == null || !item.succeeded())
              .map(
                  item ->
                      item == null
                          ? "unknown"
                          : "#" + item.index + "=" + (item.error == null ? "" : item.error))
              .reduce((left, right) -> left + ", " + right)
              .orElse("unknown bulk enqueue failure");
      throw new IllegalStateException(operation + " failed: " + reason);
    }

    public String singleJobId() {
      if (items.size() != 1) {
        throw new IllegalStateException("Expected exactly one bulk enqueue result");
      }
      BulkEnqueueItemResult item = items.get(0);
      if (!item.succeeded()) {
        if (item.invalidRequest) {
          throw new IllegalArgumentException(item.error);
        }
        throw new IllegalStateException(item.error);
      }
      return item.jobId;
    }
  }

  final class ReconcileJob {
    public final String jobId;
    public final String accountId;
    public final String connectorId;
    public final String state;
    public final String message;
    public final long startedAtMs;
    public final long finishedAtMs;
    public final long tablesScanned;
    public final long tablesChanged;
    public final long viewsScanned;
    public final long viewsChanged;
    public final long errors;
    public final boolean fullRescan;
    public final CaptureMode captureMode;
    public final long snapshotsProcessed;
    public final long statsProcessed;
    public final long indexesProcessed;
    public final boolean aggregateSummaryPresent;
    public final ReconcileScope scope;
    public final ReconcileExecutionPolicy executionPolicy;
    public final String pinnedExecutorId;
    public final String executorId;
    public final ReconcileJobKind jobKind;
    public final ReconcileTableTask tableTask;
    public final ReconcileViewTask viewTask;
    public final ReconcileSnapshotTask snapshotTask;
    public final ReconcileFileGroupTask fileGroupTask;
    public final long plannedFileGroups;
    public final long plannedFiles;
    public final long completedFileGroups;
    public final long failedFileGroups;
    public final long completedFiles;
    public final long failedFiles;
    public final String parentJobId;

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String executorId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          0,
          0,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          scope,
          executionPolicy,
          "",
          executorId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          0L,
          false,
          scope,
          executionPolicy,
          pinnedExecutorId,
          executorId,
          ReconcileJobKind.PLAN_CONNECTOR,
          ReconcileTableTask.empty(),
          ReconcileViewTask.empty(),
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          "");
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        long indexesProcessed,
        boolean aggregateSummaryPresent,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          0,
          0,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          indexesProcessed,
          aggregateSummaryPresent,
          scope,
          executionPolicy,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          ReconcileViewTask.empty(),
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          scope,
          executionPolicy,
          "",
          executorId,
          jobKind,
          tableTask,
          ReconcileViewTask.empty(),
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        long indexesProcessed,
        boolean aggregateSummaryPresent,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          indexesProcessed,
          aggregateSummaryPresent,
          scope,
          executionPolicy,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          viewTask,
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        long plannedFileGroups,
        long plannedFiles,
        long completedFileGroups,
        long failedFileGroups,
        long completedFiles,
        long failedFiles,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          0L,
          false,
          scope,
          executionPolicy,
          "",
          executorId,
          jobKind,
          tableTask,
          viewTask,
          snapshotTask,
          fileGroupTask,
          plannedFileGroups,
          plannedFiles,
          completedFileGroups,
          failedFileGroups,
          completedFiles,
          failedFiles,
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          0L,
          false,
          scope,
          executionPolicy,
          "",
          executorId,
          jobKind,
          tableTask,
          viewTask,
          snapshotTask,
          fileGroupTask,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          scope,
          executionPolicy,
          "",
          executorId,
          jobKind,
          tableTask,
          viewTask,
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        long indexesProcessed,
        boolean aggregateSummaryPresent,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          indexesProcessed,
          aggregateSummaryPresent,
          scope,
          executionPolicy,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          viewTask,
          snapshotTask,
          fileGroupTask,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          parentJobId);
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        long indexesProcessed,
        boolean aggregateSummaryPresent,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        long plannedFileGroups,
        long plannedFiles,
        long completedFileGroups,
        long failedFileGroups,
        long completedFiles,
        long failedFiles,
        String parentJobId) {
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.state = state;
      this.message = message;
      this.startedAtMs = startedAtMs;
      this.finishedAtMs = finishedAtMs;
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.viewsScanned = viewsScanned;
      this.viewsChanged = viewsChanged;
      this.errors = errors;
      this.fullRescan = fullRescan;
      this.captureMode = java.util.Objects.requireNonNull(captureMode, "captureMode");
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.indexesProcessed = indexesProcessed;
      this.aggregateSummaryPresent = aggregateSummaryPresent;
      this.scope = scope == null ? ReconcileScope.empty() : scope;
      this.executionPolicy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      this.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId;
      this.executorId = executorId == null ? "" : executorId;
      this.jobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
      this.tableTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
      this.viewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      this.snapshotTask = snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
      this.fileGroupTask = fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
      this.plannedFileGroups = plannedFileGroups;
      this.plannedFiles = plannedFiles;
      this.completedFileGroups = completedFileGroups;
      this.failedFileGroups = failedFileGroups;
      this.completedFiles = completedFiles;
      this.failedFiles = failedFiles;
      this.parentJobId = parentJobId == null ? "" : parentJobId;
    }

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        boolean fullRescan,
        CaptureMode captureMode,
        long snapshotsProcessed,
        long statsProcessed,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          state,
          message,
          startedAtMs,
          finishedAtMs,
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          fullRescan,
          captureMode,
          snapshotsProcessed,
          statsProcessed,
          0L,
          false,
          scope,
          executionPolicy,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          viewTask,
          snapshotTask,
          fileGroupTask,
          0L,
          0L,
          0L,
          0L,
          0L,
          0L,
          parentJobId);
    }
  }

  final class ProgressUpdate {
    public final boolean leaseValid;
    public final boolean cancellationRequested;

    public ProgressUpdate(boolean leaseValid, boolean cancellationRequested) {
      this.leaseValid = leaseValid;
      this.cancellationRequested = cancellationRequested;
    }
  }

  enum CompletionKind {
    SUCCEEDED,
    FAILED_RETRYABLE,
    FAILED_WAITING,
    FAILED_TERMINAL,
    CANCELLED
  }

  final class LeasedJob {
    public final String jobId;
    public final String accountId;
    public final String connectorId;
    public final boolean fullRescan;
    public final CaptureMode captureMode;
    public final ReconcileScope scope;
    public final ReconcileExecutionPolicy executionPolicy;
    public final String leaseEpoch;
    public final String pinnedExecutorId;
    public final String executorId;
    public final ReconcileJobKind jobKind;
    public final ReconcileTableTask tableTask;
    public final ReconcileViewTask viewTask;
    public final ReconcileSnapshotTask snapshotTask;
    public final ReconcileFileGroupTask fileGroupTask;
    public final String parentJobId;

    public LeasedJob(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String leaseEpoch,
        String pinnedExecutorId,
        String executorId) {
      this(
          jobId,
          accountId,
          connectorId,
          fullRescan,
          captureMode,
          scope,
          executionPolicy,
          leaseEpoch,
          pinnedExecutorId,
          executorId,
          ReconcileJobKind.PLAN_CONNECTOR,
          ReconcileTableTask.empty(),
          ReconcileViewTask.empty(),
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          "");
    }

    public LeasedJob(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String leaseEpoch,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          fullRescan,
          captureMode,
          scope,
          executionPolicy,
          leaseEpoch,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          ReconcileViewTask.empty(),
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          parentJobId);
    }

    public LeasedJob(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String leaseEpoch,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        String parentJobId) {
      this(
          jobId,
          accountId,
          connectorId,
          fullRescan,
          captureMode,
          scope,
          executionPolicy,
          leaseEpoch,
          pinnedExecutorId,
          executorId,
          jobKind,
          tableTask,
          viewTask,
          ReconcileSnapshotTask.empty(),
          ReconcileFileGroupTask.empty(),
          parentJobId);
    }

    public LeasedJob(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        CaptureMode captureMode,
        ReconcileScope scope,
        ReconcileExecutionPolicy executionPolicy,
        String leaseEpoch,
        String pinnedExecutorId,
        String executorId,
        ReconcileJobKind jobKind,
        ReconcileTableTask tableTask,
        ReconcileViewTask viewTask,
        ReconcileSnapshotTask snapshotTask,
        ReconcileFileGroupTask fileGroupTask,
        String parentJobId) {
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.fullRescan = fullRescan;
      this.captureMode = java.util.Objects.requireNonNull(captureMode, "captureMode");
      this.scope = scope == null ? ReconcileScope.empty() : scope;
      this.executionPolicy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      this.leaseEpoch = leaseEpoch == null ? "" : leaseEpoch;
      this.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId;
      this.executorId = executorId == null ? "" : executorId;
      this.jobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
      this.tableTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
      this.viewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
      this.snapshotTask = snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
      this.fileGroupTask = fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
      this.parentJobId = parentJobId == null ? "" : parentJobId;
    }
  }

  final class ReconcileJobPage {
    public final List<ReconcileJob> jobs;
    public final String nextPageToken;

    public ReconcileJobPage(List<ReconcileJob> jobs, String nextPageToken) {
      this.jobs = jobs == null ? List.of() : List.copyOf(jobs);
      this.nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
  }

  final class QueueStats {
    public final long queued;
    public final long running;
    public final long cancelling;
    public final long oldestQueuedCreatedAtMs;

    public QueueStats(long queued, long running, long cancelling, long oldestQueuedCreatedAtMs) {
      this.queued = Math.max(0L, queued);
      this.running = Math.max(0L, running);
      this.cancelling = Math.max(0L, cancelling);
      this.oldestQueuedCreatedAtMs = Math.max(0L, oldestQueuedCreatedAtMs);
    }
  }

  final class LeaseRequest {
    private static final String ANY_LANE = "*";
    public final Set<ReconcileExecutionClass> executionClasses;
    public final Set<String> lanes;
    public final Set<String> executorIds;
    public final Set<ReconcileJobKind> jobKinds;

    public LeaseRequest(
        Set<ReconcileExecutionClass> executionClasses,
        Set<String> lanes,
        Set<String> executorIds,
        Set<ReconcileJobKind> jobKinds) {
      this.executionClasses =
          executionClasses == null
              ? Set.of()
              : EnumSet.copyOf(
                  executionClasses.isEmpty()
                      ? EnumSet.noneOf(ReconcileExecutionClass.class)
                      : executionClasses);
      this.lanes =
          lanes == null
              ? Set.of()
              : lanes.stream()
                  .map(lane -> lane == null ? "" : lane.trim())
                  .collect(java.util.stream.Collectors.toUnmodifiableSet());
      this.executorIds =
          executorIds == null
              ? Set.of()
              : executorIds.stream()
                  .map(executorId -> executorId == null ? "" : executorId.trim())
                  .filter(executorId -> !executorId.isEmpty())
                  .collect(java.util.stream.Collectors.toUnmodifiableSet());
      this.jobKinds =
          jobKinds == null
              ? Set.of()
              : EnumSet.copyOf(
                  jobKinds.isEmpty() ? EnumSet.noneOf(ReconcileJobKind.class) : jobKinds);
    }

    public static LeaseRequest all() {
      return new LeaseRequest(Set.of(), Set.of(), Set.of(), Set.of());
    }

    public static LeaseRequest of(
        Set<ReconcileExecutionClass> executionClasses, Set<String> lanes) {
      return new LeaseRequest(executionClasses, lanes, Set.of(), Set.of());
    }

    public static LeaseRequest of(
        Set<ReconcileExecutionClass> executionClasses, Set<String> lanes, Set<String> executorIds) {
      return new LeaseRequest(executionClasses, lanes, executorIds, Set.of());
    }

    public static LeaseRequest of(
        Set<ReconcileExecutionClass> executionClasses,
        Set<String> lanes,
        Set<String> executorIds,
        Set<ReconcileJobKind> jobKinds) {
      return new LeaseRequest(executionClasses, lanes, executorIds, jobKinds);
    }

    public boolean matches(
        ReconcileExecutionPolicy policy, String pinnedExecutorId, ReconcileJobKind jobKind) {
      ReconcileExecutionPolicy effective =
          policy == null ? ReconcileExecutionPolicy.defaults() : policy;
      ReconcileJobKind effectiveJobKind =
          jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
      boolean classMatches =
          executionClasses.isEmpty() || executionClasses.contains(effective.executionClass());
      boolean laneMatches =
          lanes.isEmpty() || lanes.contains(ANY_LANE) || lanes.contains(effective.lane());
      boolean kindMatches = jobKinds.isEmpty() || jobKinds.contains(effectiveJobKind);
      String effectivePinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId.trim();
      boolean executorMatches =
          effectivePinnedExecutorId.isEmpty() || executorIds.contains(effectivePinnedExecutorId);
      return classMatches && laneMatches && kindMatches && executorMatches;
    }

    public static String anyLaneToken() {
      return ANY_LANE;
    }
  }
}
