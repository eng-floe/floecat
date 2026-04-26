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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotPlanningReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(SnapshotPlanningReconcileExecutor.class);

  private final ReconcilerBackend backend;
  private final ReconcileJobStore jobs;
  private final ReconcileExecutorRegistry executorRegistry;
  private final boolean enabled;
  private final int maxFilesPerGroup;

  @Inject
  public SnapshotPlanningReconcileExecutor(
      ReconcilerBackend backend,
      ReconcileJobStore jobs,
      ReconcileExecutorRegistry executorRegistry,
      @ConfigProperty(
              name = "floecat.reconciler.snapshot-plan.max-files-per-group",
              defaultValue = "128")
          int maxFilesPerGroup,
      @ConfigProperty(
              name = "floecat.reconciler.executor.snapshot-planner.enabled",
              defaultValue = "true")
          boolean enabled) {
    this.backend = backend;
    this.jobs = jobs;
    this.executorRegistry = executorRegistry;
    this.maxFilesPerGroup = Math.max(1, maxFilesPerGroup);
    this.enabled = enabled;
  }

  @Override
  public String id() {
    return "snapshot_planner_reconciler";
  }

  @Override
  public boolean enabled() {
    return enabled;
  }

  @Override
  public int priority() {
    return 20;
  }

  @Override
  public Set<ReconcileJobKind> supportedJobKinds() {
    return EnumSet.of(ReconcileJobKind.PLAN_SNAPSHOT);
  }

  @Override
  public Set<String> supportedLanes() {
    return Set.of();
  }

  @Override
  public boolean supportsLane(String lane) {
    return true;
  }

  @Override
  public boolean supports(ReconcileJobStore.LeasedJob lease) {
    return lease != null && lease.jobKind == ReconcileJobKind.PLAN_SNAPSHOT;
  }

  @Override
  public ExecutionResult execute(ExecutionContext context) {
    var lease = context.lease();
    if (lease == null || lease.jobKind != ReconcileJobKind.PLAN_SNAPSHOT) {
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    ReconcileSnapshotTask task =
        lease.snapshotTask == null ? ReconcileSnapshotTask.empty() : lease.snapshotTask;
    LOG.infof(
        "execute PLAN_SNAPSHOT jobId=%s connectorId=%s tableId=%s snapshotId=%d source=%s.%s fileGroups=%d",
        lease.jobId,
        lease.connectorId,
        task.tableId(),
        task.snapshotId(),
        task.sourceNamespace(),
        task.sourceTable(),
        task.fileGroups().size());
    if (task.isEmpty()
        || task.tableId().isBlank()
        || task.sourceNamespace().isBlank()
        || task.sourceTable().isBlank()
        || task.snapshotId() < 0) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "snapshot task is required for PLAN_SNAPSHOT jobs",
          new IllegalArgumentException("snapshot task is required"));
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }

    Snapshot snapshot;
    try {
      snapshot = fetchSnapshot(lease, task).orElse(null);
    } catch (Exception e) {
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "Failed to fetch snapshot " + task.snapshotId(), e);
    }
    if (snapshot == null) {
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          "Snapshot " + task.snapshotId() + " was not found for table " + task.tableId(),
          new IllegalStateException("snapshot not found"));
    }
    List<ReconcileFileGroupTask> fileGroupTasks =
        !task.fileGroups().isEmpty() ? task.fileGroups() : buildFileGroupTasks(lease, task);
    LOG.infof(
        "planned PLAN_SNAPSHOT jobId=%s tableId=%s snapshotId=%d fileGroups=%d",
        lease.jobId, task.tableId(), task.snapshotId(), fileGroupTasks.size());
    if (task.fileGroups().isEmpty()) {
      jobs.persistSnapshotPlan(lease.jobId, plannedSnapshotTask(task, fileGroupTasks));
    }
    ensureFileGroupExecutorAvailable(fileGroupTasks);
    enqueueFileGroupExecution(lease, fileGroupTasks, context);
    context
        .progressListener()
        .onProgress(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "Planned snapshot "
                + task.snapshotId()
                + " for "
                + task.sourceNamespace()
                + "."
                + task.sourceTable()
                + " into "
                + fileGroupTasks.size()
                + " file group(s)");
    return ExecutionResult.success(
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        "Snapshot plan recorded for "
            + task.sourceNamespace()
            + "."
            + task.sourceTable()
            + " with "
            + fileGroupTasks.size()
            + " file group(s)");
  }

  private Optional<Snapshot> fetchSnapshot(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask task) {
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(lease.connectorId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(task.tableId())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();
    ReconcileContext reconcileContext =
        new ReconcileContext(
            "reconciler-job-" + lease.jobId, principal, id(), Instant.now(), Optional.empty());
    backend.lookupConnector(reconcileContext, connectorId);
    return backend.fetchSnapshot(reconcileContext, tableId, task.snapshotId());
  }

  private List<ReconcileFileGroupTask> buildFileGroupTasks(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask task) {
    Optional<FloecatConnector.SnapshotFilePlan> planned = fetchSnapshotFilePlan(lease, task);
    if (planned.isPresent()) {
      List<String> parquetFilePaths =
          java.util.stream.Stream.concat(
                  planned.get().dataFiles().stream(), planned.get().deleteFiles().stream())
              .filter(file -> file != null && isParquetFile(file))
              .map(FloecatConnector.SnapshotFileEntry::filePath)
              .filter(path -> path != null && !path.isBlank())
              .distinct()
              .toList();
      if (!parquetFilePaths.isEmpty()) {
        return partitionFilePaths(lease.jobId, task, parquetFilePaths);
      }
      return List.of();
    }
    return List.of();
  }

  private static boolean isParquetFile(FloecatConnector.SnapshotFileEntry file) {
    String format = file.fileFormat() == null ? "" : file.fileFormat().trim();
    if ("PARQUET".equalsIgnoreCase(format)) {
      return true;
    }
    String path = file.filePath() == null ? "" : file.filePath().toLowerCase(java.util.Locale.ROOT);
    return path.endsWith(".parquet") || path.endsWith(".parq");
  }

  private Optional<FloecatConnector.SnapshotFilePlan> fetchSnapshotFilePlan(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask task) {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(task.tableId())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    Optional<FloecatConnector.SnapshotFilePlan> planned =
        backend.fetchSnapshotFilePlan(reconcileContext(lease), tableId, task.snapshotId());
    LOG.infof(
        "fetchSnapshotFilePlan jobId=%s tableId=%s snapshotId=%d present=%s dataFiles=%d deleteFiles=%d",
        lease.jobId,
        task.tableId(),
        task.snapshotId(),
        planned.isPresent(),
        planned.map(plan -> plan.dataFiles().size()).orElse(0),
        planned.map(plan -> plan.deleteFiles().size()).orElse(0));
    return planned;
  }

  private List<ReconcileFileGroupTask> partitionFilePaths(
      String planId, ReconcileSnapshotTask task, List<String> filePaths) {
    java.util.ArrayList<ReconcileFileGroupTask> groups = new java.util.ArrayList<>();
    for (int offset = 0; offset < filePaths.size(); offset += maxFilesPerGroup) {
      int end = Math.min(filePaths.size(), offset + maxFilesPerGroup);
      String groupId = "snapshot-" + task.snapshotId() + "-group-" + groups.size();
      groups.add(
          ReconcileFileGroupTask.of(
              planId, groupId, task.tableId(), task.snapshotId(), filePaths.subList(offset, end)));
    }
    return List.copyOf(groups);
  }

  private static ReconcileSnapshotTask plannedSnapshotTask(
      ReconcileSnapshotTask task, List<ReconcileFileGroupTask> fileGroupTasks) {
    if (task == null) {
      return ReconcileSnapshotTask.empty();
    }
    return ReconcileSnapshotTask.of(
        task.tableId(),
        task.snapshotId(),
        task.sourceNamespace(),
        task.sourceTable(),
        fileGroupTasks);
  }

  private void ensureFileGroupExecutorAvailable(List<ReconcileFileGroupTask> fileGroupTasks) {
    if (fileGroupTasks == null || fileGroupTasks.isEmpty()) {
      return;
    }
    if (executorRegistry != null
        && executorRegistry.hasExecutorForJobKind(ReconcileJobKind.EXEC_FILE_GROUP)) {
      return;
    }
    throw new IllegalStateException(
        "No enabled reconcile executor is available for EXEC_FILE_GROUP jobs");
  }

  private void enqueueFileGroupExecution(
      ReconcileJobStore.LeasedJob lease,
      List<ReconcileFileGroupTask> fileGroupTasks,
      ExecutionContext context) {
    if (fileGroupTasks == null || fileGroupTasks.isEmpty() || context.shouldStop().getAsBoolean()) {
      return;
    }
    ReconcileScope scope = lease.scope == null ? ReconcileScope.empty() : lease.scope;
    ReconcileExecutionPolicy executionPolicy =
        lease.executionPolicy == null ? ReconcileExecutionPolicy.defaults() : lease.executionPolicy;
    for (ReconcileFileGroupTask fileGroupTask : fileGroupTasks) {
      if (context.shouldStop().getAsBoolean()) {
        break;
      }
      ReconcileScope fileGroupScope = effectiveFileGroupScope(scope, fileGroupTask);
      jobs.enqueueFileGroupExecution(
          lease.accountId,
          lease.connectorId,
          lease.fullRescan,
          lease.captureMode,
          fileGroupScope,
          fileGroupTask.asReference(),
          executionPolicy,
          lease.jobId,
          lease.pinnedExecutorId);
    }
  }

  private ReconcileContext reconcileContext(ReconcileJobStore.LeasedJob lease) {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();
    return new ReconcileContext(
        "reconciler-job-" + lease.jobId, principal, id(), Instant.now(), Optional.empty());
  }

  private static ReconcileScope effectiveFileGroupScope(
      ReconcileScope baseScope, ReconcileFileGroupTask fileGroupTask) {
    if (baseScope == null || !baseScope.hasCaptureRequestFilter() || fileGroupTask == null) {
      return baseScope == null ? ReconcileScope.empty() : baseScope;
    }
    List<ReconcileScope.ScopedCaptureRequest> snapshotRequests =
        baseScope.destinationCaptureRequests().stream()
            .filter(request -> request != null)
            .filter(request -> fileGroupTask.tableId().equals(request.tableId()))
            .filter(request -> fileGroupTask.snapshotId() == request.snapshotId())
            .toList();
    if (snapshotRequests.isEmpty()) {
      return baseScope;
    }
    ReconcileCapturePolicy capturePolicy =
        mergeCapturePolicy(baseScope.capturePolicy(), snapshotRequests);
    return ReconcileScope.of(
        baseScope.destinationNamespaceIds(),
        baseScope.destinationTableId(),
        baseScope.destinationViewId(),
        snapshotRequests,
        capturePolicy);
  }

  private static ReconcileCapturePolicy mergeCapturePolicy(
      ReconcileCapturePolicy basePolicy,
      List<ReconcileScope.ScopedCaptureRequest> snapshotRequests) {
    LinkedHashMap<String, ReconcileCapturePolicy.Column> columns = new LinkedHashMap<>();
    LinkedHashSet<ReconcileCapturePolicy.Output> outputs = new LinkedHashSet<>();
    if (basePolicy != null) {
      basePolicy.columns().forEach(column -> columns.put(column.selector(), column));
      outputs.addAll(basePolicy.outputs());
    }
    for (ReconcileScope.ScopedCaptureRequest request : snapshotRequests) {
      if (request == null) {
        continue;
      }
      StatsTarget target =
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Invalid scoped capture target spec for table="
                              + request.tableId()
                              + " snapshot="
                              + request.snapshotId()
                              + " spec="
                              + request.targetSpec()));
      switch (target.getTargetCase()) {
        case TABLE -> {}
        case COLUMN -> {
          String selector = "#" + target.getColumn().getColumnId();
          selectorPolicy(basePolicy, outputs, selector)
              .ifPresent(column -> columns.putIfAbsent(selector, column));
        }
        case FILE, EXPRESSION, TARGET_NOT_SET -> {
          // FILE is intentionally not an execution scope, and EXPRESSION remains a recognized
          // placeholder target until unified capture grows explicit expression support.
        }
      }
      for (String selector : request.columnSelectors()) {
        if (selector == null || selector.isBlank()) {
          continue;
        }
        selectorPolicy(basePolicy, outputs, selector)
            .ifPresent(column -> columns.putIfAbsent(selector, column));
      }
    }
    return ReconcileCapturePolicy.of(new ArrayList<>(columns.values()), Set.copyOf(outputs));
  }

  private static Optional<ReconcileCapturePolicy.Column> selectorPolicy(
      ReconcileCapturePolicy basePolicy,
      Set<ReconcileCapturePolicy.Output> outputs,
      String selector) {
    String normalized = selector == null ? "" : selector.trim();
    if (normalized.isBlank()) {
      return Optional.empty();
    }
    if (basePolicy != null) {
      for (ReconcileCapturePolicy.Column existing : basePolicy.columns()) {
        if (existing.selector().equals(normalized)) {
          return Optional.of(existing);
        }
      }
    }
    boolean captureStats = outputs.contains(ReconcileCapturePolicy.Output.COLUMN_STATS);
    boolean captureIndex = outputs.contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX);
    if (!captureStats && !captureIndex) {
      return Optional.empty();
    }
    return Optional.of(new ReconcileCapturePolicy.Column(normalized, captureStats, captureIndex));
  }
}
