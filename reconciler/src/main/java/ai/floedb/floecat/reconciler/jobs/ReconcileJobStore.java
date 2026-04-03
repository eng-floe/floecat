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
    return enqueue(
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
      ReconcileExecutionPolicy executionPolicy,
      String pinnedExecutorId);

  Optional<ReconcileJob> get(String accountId, String jobId);

  default Optional<ReconcileJob> get(String jobId) {
    return get(null, jobId);
  }

  ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states);

  QueueStats queueStats();

  default Optional<LeasedJob> leaseNext() {
    return leaseNext(LeaseRequest.all());
  }

  Optional<LeasedJob> leaseNext(LeaseRequest request);

  boolean renewLease(String jobId, String leaseEpoch);

  void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId);

  void markProgress(
      String jobId,
      String leaseEpoch,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message);

  void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long scanned,
      long changed,
      long snapshotsProcessed,
      long statsProcessed);

  void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

  Optional<ReconcileJob> cancel(String accountId, String jobId, String reason);

  boolean isCancellationRequested(String jobId);

  void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessed,
      long statsProcessed);

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
    public final long errors;
    public final boolean fullRescan;
    public final CaptureMode captureMode;
    public final long snapshotsProcessed;
    public final long statsProcessed;
    public final ReconcileScope scope;
    public final ReconcileExecutionPolicy executionPolicy;
    public final String executorId;

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
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.state = state;
      this.message = message;
      this.startedAtMs = startedAtMs;
      this.finishedAtMs = finishedAtMs;
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.errors = errors;
      this.fullRescan = fullRescan;
      this.captureMode = captureMode == null ? CaptureMode.METADATA_AND_STATS : captureMode;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.scope = scope == null ? ReconcileScope.empty() : scope;
      this.executionPolicy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      this.executorId = executorId == null ? "" : executorId;
    }
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
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.fullRescan = fullRescan;
      this.captureMode = captureMode == null ? CaptureMode.METADATA_AND_STATS : captureMode;
      this.scope = scope == null ? ReconcileScope.empty() : scope;
      this.executionPolicy =
          executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
      this.leaseEpoch = leaseEpoch == null ? "" : leaseEpoch;
      this.pinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId;
      this.executorId = executorId == null ? "" : executorId;
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
    public final Set<ReconcileExecutionClass> executionClasses;
    public final Set<String> lanes;
    public final Set<String> executorIds;

    public LeaseRequest(
        Set<ReconcileExecutionClass> executionClasses, Set<String> lanes, Set<String> executorIds) {
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
    }

    public static LeaseRequest all() {
      return new LeaseRequest(Set.of(), Set.of(), Set.of());
    }

    public static LeaseRequest of(
        Set<ReconcileExecutionClass> executionClasses, Set<String> lanes) {
      return new LeaseRequest(executionClasses, lanes, Set.of());
    }

    public static LeaseRequest of(
        Set<ReconcileExecutionClass> executionClasses, Set<String> lanes, Set<String> executorIds) {
      return new LeaseRequest(executionClasses, lanes, executorIds);
    }

    public boolean matches(ReconcileExecutionPolicy policy, String pinnedExecutorId) {
      ReconcileExecutionPolicy effective =
          policy == null ? ReconcileExecutionPolicy.defaults() : policy;
      boolean classMatches =
          executionClasses.isEmpty() || executionClasses.contains(effective.executionClass());
      boolean laneMatches = lanes.isEmpty() || lanes.contains(effective.lane());
      String effectivePinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId.trim();
      boolean executorMatches =
          effectivePinnedExecutorId.isEmpty()
              || executorIds.isEmpty()
              || executorIds.contains(effectivePinnedExecutorId);
      return classMatches && laneMatches && executorMatches;
    }
  }
}
