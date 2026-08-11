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
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan.DeltaDeletionVector;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteSnapshotPlanningReconcileExecutor implements ReconcileExecutor {
  private static final Logger LOG = Logger.getLogger(RemoteSnapshotPlanningReconcileExecutor.class);
  private static final long ESTIMATED_FILE_OVERHEAD_BYTES = 4L * 1024L * 1024L;

  private final ReconcilerBackend backend;
  private final RemotePlannerWorkerClient workerClient;
  private final ReconcileWorkerAuthProvider reconcileWorkerAuthProvider;
  private final boolean enabled;
  private final boolean workerAuthRequired;
  private final int maxFilesPerGroup;
  @Inject BlobStore blobStore;

  @Inject
  public RemoteSnapshotPlanningReconcileExecutor(
      ReconcilerBackend backend,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      @ConfigProperty(
              name = "floecat.reconciler.snapshot-plan.max-files-per-group",
              defaultValue = "128")
          int maxFilesPerGroup,
      @ConfigProperty(
              name = "floecat.reconciler.executor.remote-snapshot-planner.enabled",
              defaultValue = "false")
          boolean enabled,
      @ConfigProperty(name = "floecat.reconciler.worker.auth.required", defaultValue = "true")
          boolean workerAuthRequired) {
    this.backend = backend;
    this.workerClient = workerClient;
    this.reconcileWorkerAuthProvider = reconcileWorkerAuthProvider;
    this.maxFilesPerGroup = Math.max(1, maxFilesPerGroup);
    this.enabled = enabled;
    this.workerAuthRequired = workerAuthRequired;
  }

  RemoteSnapshotPlanningReconcileExecutor(
      ReconcilerBackend backend,
      RemotePlannerWorkerClient workerClient,
      ReconcileWorkerAuthProvider reconcileWorkerAuthProvider,
      int maxFilesPerGroup,
      boolean enabled) {
    this(backend, workerClient, reconcileWorkerAuthProvider, maxFilesPerGroup, enabled, true);
  }

  @Override
  public String id() {
    return "remote_snapshot_planner_worker";
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
      return ExecutionResult.terminalFailure(
          0, 0, 0, 0, 1, 0, 0, "Unsupported reconcile job kind", new IllegalArgumentException());
    }
    if (context.shouldStop().getAsBoolean()) {
      return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled");
    }

    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    StandalonePlanSnapshotPayload payload = workerClient.getPlanSnapshotInput(remoteLease);
    ReconcileSnapshotTask task =
        payload.snapshotTask() == null ? ReconcileSnapshotTask.empty() : payload.snapshotTask();

    LOG.infof(
        "execute PLAN_SNAPSHOT jobId=%s connectorId=%s tableId=%s snapshotId=%d source=%s.%s"
            + " fileGroups=%d",
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
      return ExecutionResult.terminalFailure(
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

    try {
      PlannedSnapshotCapture plannedCapture = planSnapshotCapture(lease, payload, task);
      List<ReconcileFileGroupTask> fileGroupTasks = plannedCapture.fileGroupTasks();
      LOG.infof(
          "planned PLAN_SNAPSHOT jobId=%s tableId=%s snapshotId=%d completionMode=%s fileGroups=%d",
          lease.jobId,
          task.tableId(),
          task.snapshotId(),
          plannedCapture.snapshotTask().completionMode(),
          fileGroupTasks.size());
      List<PlannedFileGroupJob> fileGroupJobs =
          fileGroupTasks.stream()
              .map(
                  group ->
                      new PlannedFileGroupJob(
                          effectiveFileGroupScope(payload.scope(), group), group))
              .toList();
      context.beforeHandledCompletion().run();
      if (!workerClient.submitPlanSnapshotSuccess(
          remoteLease,
          plannedCapture.snapshotTask(),
          fileGroupJobs,
          plannedCapture.directStats())) {
        throw plannerSubmissionRejected();
      }
      return ExecutionResult.successHandled(
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
    } catch (RuntimeException e) {
      RuntimeException classified =
          e instanceof ReconcileFailureException
              ? e
              : (RuntimeException) ReconcileFailureClassifier.normalize(e);
      if (classified instanceof RemoteLeasePreconditionFailedException) {
        LOG.infof(
            "Snapshot planning result submission ignored because reconcile lease is no longer valid jobId=%s tableId=%s snapshotId=%d",
            lease.jobId, task.tableId(), task.snapshotId());
        context.beforeHandledCompletion().run();
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Lease no longer valid");
      }
      if (retryClassOf(classified) == ExecutionResult.RetryClass.STATE_UNCERTAIN) {
        throw classified;
      }
      String failureDetail = failureDetail(classified);
      LOG.errorf(
          classified,
          "Snapshot planning failed jobId=%s tableId=%s snapshotId=%d",
          lease.jobId,
          task.tableId(),
          task.snapshotId());
      try {
        workerClient.submitPlanSnapshotFailure(
            remoteLease,
            failureKindOf(classified),
            retryDispositionOf(classified),
            retryClassOf(classified),
            failureDetail);
      } catch (RemoteLeasePreconditionFailedException leaseRejected) {
        LOG.infof(
            "Snapshot planning failure submission ignored because reconcile lease is no longer valid jobId=%s tableId=%s snapshotId=%d",
            lease.jobId, task.tableId(), task.snapshotId());
        context.beforeHandledCompletion().run();
        return ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Lease no longer valid");
      }
      if (isObsoleteFailureKind(failureKindOf(classified))) {
        return ExecutionResult.obsolete(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            failureKindOf(classified),
            "Snapshot planning failed: " + classified.getMessage(),
            classified);
      }
      if (retryDispositionOf(classified) == ExecutionResult.RetryDisposition.TERMINAL) {
        return ExecutionResult.terminalFailure(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            failureKindOf(classified),
            "Snapshot planning failed: " + classified.getMessage(),
            classified);
      }
      return ExecutionResult.failure(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          failureKindOf(classified),
          retryDispositionOf(classified),
          retryClassOf(classified),
          "Snapshot planning failed: " + classified.getMessage(),
          classified);
    }
  }

  private static ExecutionResult.FailureKind failureKindOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.failureKind()
        : ExecutionResult.FailureKind.INTERNAL;
  }

  private static ExecutionResult.RetryDisposition retryDispositionOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.retryDisposition()
        : ExecutionResult.RetryDisposition.RETRYABLE;
  }

  private static ExecutionResult.RetryClass retryClassOf(Throwable error) {
    return error instanceof ReconcileFailureException failure
        ? failure.retryClass()
        : ExecutionResult.RetryClass.TRANSIENT_ERROR;
  }

  private static boolean isObsoleteFailureKind(ExecutionResult.FailureKind failureKind) {
    return failureKind == ExecutionResult.FailureKind.CONNECTOR_MISSING
        || failureKind == ExecutionResult.FailureKind.TABLE_MISSING
        || failureKind == ExecutionResult.FailureKind.VIEW_MISSING;
  }

  private static ReconcileFailureException plannerSubmissionRejected() {
    return new ReconcileFailureException(
        ExecutionResult.FailureKind.INTERNAL,
        ExecutionResult.RetryDisposition.RETRYABLE,
        ExecutionResult.RetryClass.STATE_UNCERTAIN,
        "standalone planner result submission was rejected",
        new IllegalStateException("planner result submission rejected"));
  }

  private static String failureDetail(Throwable error) {
    if (error == null) {
      return "unknown error";
    }
    var seen = new HashSet<Throwable>();
    var parts = new ArrayList<String>();
    Throwable current = error;
    while (current != null && seen.add(current)) {
      parts.add(renderThrowable(current));
      current = current.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private static String renderThrowable(Throwable error) {
    if (error instanceof StatusRuntimeException statusError) {
      var status = statusError.getStatus();
      String description = status.getDescription();
      if (description == null || description.isBlank()) {
        description = statusError.getMessage();
      }
      if (description == null || description.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + description;
    }
    String type = error.getClass().getSimpleName();
    String message = error.getMessage();
    if (message == null || message.isBlank()) {
      return type;
    }
    return type + ": " + message;
  }

  private List<ReconcileFileGroupTask> buildFileGroupTasks(
      ReconcileJobStore.LeasedJob lease, ReconcileSnapshotTask task) {
    Optional<FloecatConnector.SnapshotFilePlan> planned = fetchSnapshotFilePlan(lease, task);
    if (planned.isPresent()) {
      LinkedHashMap<String, FloecatConnector.SnapshotFileEntry> parquetFiles =
          new LinkedHashMap<>();
      planned.get().dataFiles().stream()
          .filter(file -> file != null && isParquetFile(file))
          .filter(file -> file.filePath() != null && !file.filePath().isBlank())
          .forEach(file -> parquetFiles.putIfAbsent(file.filePath(), file));
      if (!parquetFiles.isEmpty()) {
        return partitionFiles(
            lease.jobId,
            task,
            List.copyOf(parquetFiles.values()),
            planned.get().executionSchemaJson());
      }
      return List.of();
    }
    return List.of();
  }

  private PlannedSnapshotCapture planSnapshotCapture(
      ReconcileJobStore.LeasedJob lease,
      StandalonePlanSnapshotPayload payload,
      ReconcileSnapshotTask task) {
    if (payload.captureMode() == ReconcilerService.CaptureMode.METADATA_ONLY
        || (!task.sourceRevision().isBlank() && task.requestedCoverage().isEmpty())) {
      return PlannedSnapshotCapture.fileGroups(
          ReconcileSnapshotTask.of(
              task.tableId(),
              task.snapshotId(),
              task.sourceNamespace(),
              task.sourceTable(),
              List.of(),
              true,
              ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
              "",
              0,
              0,
              "",
              0,
              task.sourceRevision(),
              task.metadataFingerprint(),
              task.requestedCoverage(),
              task.indexPredecessor()),
          List.of());
    }
    Optional<PlannedSnapshotCapture> directSnapshotTask =
        tryDirectStatsCapture(lease, payload, task);
    if (directSnapshotTask.isPresent()) {
      return directSnapshotTask.get();
    }
    List<ReconcileFileGroupTask> fileGroupTasks =
        enrichFileGroupTasks(lease, payload, task, buildFileGroupTasks(lease, task));
    return PlannedSnapshotCapture.fileGroups(
        ReconcileSnapshotTask.of(
            task.tableId(),
            task.snapshotId(),
            task.sourceNamespace(),
            task.sourceTable(),
            fileGroupTasks,
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "",
            fileGroupTasks.size(),
            plannedSourceFileCount(fileGroupTasks),
            "",
            0,
            task.sourceRevision(),
            task.metadataFingerprint(),
            task.requestedCoverage(),
            task.indexPredecessor()),
        fileGroupTasks);
  }

  private List<ReconcileFileGroupTask> enrichFileGroupTasks(
      ReconcileJobStore.LeasedJob lease,
      StandalonePlanSnapshotPayload payload,
      ReconcileSnapshotTask task,
      List<ReconcileFileGroupTask> groups) {
    if (groups.isEmpty()) {
      return groups;
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(task.tableId())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ReconcileContext context = reconcileContext(lease);
    ReconcileScope scope = effectiveSnapshotScope(payload.scope(), task);
    ReconcileCapturePolicy capturePolicy =
        scope == null ? ReconcileCapturePolicy.empty() : scope.capturePolicy();
    List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> historicalBundles =
        new ArrayList<>();
    if (!lease.fullRescan) {
      Optional<HistoricalArtifacts> manifestArtifacts =
          loadLatestReconciledReuseManifest(context, tableId, task.snapshotId(), lease.connectorId);
      manifestArtifacts.ifPresent(artifacts -> historicalBundles.addAll(artifacts.bundles()));
    }
    List<ReconcileFileGroupTask> enriched =
        groups.stream()
            .map(
                group ->
                    group.withFileExecutionPlans(
                        RemoteFileArtifactReusePlanner.enrichFromBundles(
                            group.executionSchemaJson(),
                            group.fileExecutionPlans(),
                            capturePolicy,
                            lease.fullRescan,
                            historicalBundles)))
            .toList();
    return historicalBundles.isEmpty()
        ? enriched
        : regroupByReuseBundleAffinity(enriched, maxFilesPerGroup);
  }

  static List<ReconcileFileGroupTask> regroupByReuseBundleAffinity(
      List<ReconcileFileGroupTask> groups, int maxFilesPerGroup) {
    if (groups == null || groups.isEmpty()) {
      return List.of();
    }
    ReconcileFileGroupTask template = groups.get(0);
    List<ReconcileFileExecutionPlan> plans =
        groups.stream().flatMap(group -> group.fileExecutionPlans().stream()).toList();
    if (plans.isEmpty()) {
      return List.copyOf(groups);
    }
    int effectiveMaxFiles = Math.max(1, maxFilesPerGroup);
    Map<String, List<ReconcileFileExecutionPlan>> plansByBundle = new java.util.TreeMap<>();
    List<ReconcileFileExecutionPlan> unbound = new ArrayList<>();
    for (ReconcileFileExecutionPlan plan : plans) {
      String affinity = primaryReuseBundleUri(plan);
      if (affinity.isBlank()) {
        unbound.add(plan);
      } else {
        plansByBundle.computeIfAbsent(affinity, ignored -> new ArrayList<>()).add(plan);
      }
    }
    if (plansByBundle.isEmpty()) {
      return List.copyOf(groups);
    }

    List<ExecutionPlanBucket> buckets = new ArrayList<>();
    for (List<ReconcileFileExecutionPlan> bundlePlans : plansByBundle.values()) {
      int bundleGroupCount = (bundlePlans.size() + effectiveMaxFiles - 1) / effectiveMaxFiles;
      List<ExecutionPlanBucket> bundleBuckets = new ArrayList<>(bundleGroupCount);
      PriorityQueue<ExecutionPlanBucket> available = executionPlanBucketQueue();
      for (int index = 0; index < bundleGroupCount; index++) {
        ExecutionPlanBucket bucket = new ExecutionPlanBucket(buckets.size() + index);
        bundleBuckets.add(bucket);
        available.add(bucket);
      }
      for (ReconcileFileExecutionPlan plan : plansByEstimatedWork(bundlePlans)) {
        ExecutionPlanBucket bucket = available.remove();
        bucket.add(plan);
        if (bucket.fileCount() < effectiveMaxFiles) {
          available.add(bucket);
        }
      }
      buckets.addAll(bundleBuckets);
    }

    int minimumGroupCount = (plans.size() + effectiveMaxFiles - 1) / effectiveMaxFiles;
    while (buckets.size() < minimumGroupCount) {
      buckets.add(new ExecutionPlanBucket(buckets.size()));
    }
    PriorityQueue<ExecutionPlanBucket> available = executionPlanBucketQueue();
    buckets.stream()
        .filter(bucket -> bucket.fileCount() < effectiveMaxFiles)
        .forEach(available::add);
    for (ReconcileFileExecutionPlan plan : plansByEstimatedWork(unbound)) {
      ExecutionPlanBucket bucket = available.remove();
      bucket.add(plan);
      if (bucket.fileCount() < effectiveMaxFiles) {
        available.add(bucket);
      }
    }

    List<ReconcileFileGroupTask> regrouped = new ArrayList<>(buckets.size());
    for (ExecutionPlanBucket bucket : buckets) {
      if (bucket.fileCount() == 0) {
        continue;
      }
      List<ReconcileFileExecutionPlan> bucketPlans = bucket.immutablePlans();
      String groupId = "snapshot-" + template.snapshotId() + "-group-" + regrouped.size();
      List<String> filePaths =
          bucketPlans.stream().map(ReconcileFileExecutionPlan::filePath).toList();
      regrouped.add(
          ReconcileFileGroupTask.of(
              template.planId(),
              groupId,
              template.tableId(),
              template.snapshotId(),
              filePaths.size(),
              "",
              0,
              filePaths,
              List.of(),
              List.of(),
              template.executionSchemaJson(),
              bucketPlans));
    }
    return List.copyOf(regrouped);
  }

  private static String primaryReuseBundleUri(ReconcileFileExecutionPlan plan) {
    return plan.reusableArtifactBundleSelections().stream()
        .filter(
            selection ->
                selection.statsFilePaths().contains(plan.filePath())
                    || selection.indexFilePaths().contains(plan.filePath()))
        .map(ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection::payloadUri)
        .filter(uri -> uri != null && !uri.isBlank())
        .sorted()
        .findFirst()
        .orElseGet(
            () ->
                plan.reusableArtifactBundleSelections().stream()
                    .map(
                        ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection
                            ::payloadUri)
                    .filter(uri -> uri != null && !uri.isBlank())
                    .sorted()
                    .findFirst()
                    .orElse(""));
  }

  private static List<ReconcileFileExecutionPlan> plansByEstimatedWork(
      List<ReconcileFileExecutionPlan> plans) {
    return plans.stream()
        .sorted(
            Comparator.comparingLong(
                    RemoteSnapshotPlanningReconcileExecutor::estimatedExecutionPlanWork)
                .reversed()
                .thenComparing(ReconcileFileExecutionPlan::filePath)
                .thenComparing(ReconcileFileExecutionPlan::fileFormat))
        .toList();
  }

  private static PriorityQueue<ExecutionPlanBucket> executionPlanBucketQueue() {
    return new PriorityQueue<>(
        Comparator.comparingLong(ExecutionPlanBucket::estimatedWork)
            .thenComparingInt(ExecutionPlanBucket::fileCount)
            .thenComparingInt(ExecutionPlanBucket::index));
  }

  private static long estimatedExecutionPlanWork(ReconcileFileExecutionPlan plan) {
    long estimatedWork = saturatedAdd(plan.fileSizeInBytes(), ESTIMATED_FILE_OVERHEAD_BYTES);
    if (plan.deletionVector() != null) {
      estimatedWork = saturatedAdd(estimatedWork, plan.deletionVector().sizeInBytes());
    }
    for (ReconcileFileExecutionPlan.IcebergDeleteFile deleteFile : plan.icebergDeleteFiles()) {
      estimatedWork = saturatedAdd(estimatedWork, deleteFile.fileSizeInBytes());
    }
    return estimatedWork;
  }

  private static final class ExecutionPlanBucket {
    private final int index;
    private final List<ReconcileFileExecutionPlan> plans = new ArrayList<>();
    private long estimatedWork;

    private ExecutionPlanBucket(int index) {
      this.index = index;
    }

    private void add(ReconcileFileExecutionPlan plan) {
      plans.add(plan);
      estimatedWork = saturatedAdd(estimatedWork, estimatedExecutionPlanWork(plan));
    }

    private int index() {
      return index;
    }

    private int fileCount() {
      return plans.size();
    }

    private long estimatedWork() {
      return estimatedWork;
    }

    private List<ReconcileFileExecutionPlan> immutablePlans() {
      return plans.stream()
          .sorted(Comparator.comparing(ReconcileFileExecutionPlan::filePath))
          .toList();
    }
  }

  private Optional<HistoricalArtifacts> loadReuseManifest(
      ResourceId tableId, Snapshot snapshot, String expectedConnectorId) {
    if (blobStore == null) {
      throw invalidReuseManifest("snapshot reuse manifest storage is unavailable", null);
    }
    long snapshotId = snapshot.getSnapshotId();
    if (!snapshot.hasReuseManifestRef()) {
      throw invalidReuseManifest(
          "selected snapshot reuse manifest reference is missing: " + snapshotId, null);
    }
    var manifestRef = snapshot.getReuseManifestRef();
    String uri = manifestRef.getUri().trim();
    if (uri.isBlank()) {
      throw invalidReuseManifest("snapshot reuse manifest URI is missing: " + snapshotId, null);
    }
    byte[] bytes;
    try {
      bytes = blobStore.get(uri);
    } catch (StorageNotFoundException e) {
      throw unavailableReuseManifest("snapshot reuse manifest is unavailable: " + uri, e);
    }
    if (bytes == null) {
      throw unavailableReuseManifest("snapshot reuse manifest is unavailable: " + uri, null);
    }
    if (manifestRef.getPayloadBytes() <= 0L
        || manifestRef.getPayloadBytes() != bytes.length
        || manifestRef.getPayloadSha256().size() != 32
        || manifestRef.getStatsGenerationManifestUri().isBlank()
        || !MessageDigest.isEqual(manifestRef.getPayloadSha256().toByteArray(), sha256(bytes))) {
      throw invalidReuseManifest("snapshot reuse manifest metadata mismatch: " + uri, null);
    }
    try {
      SnapshotCaptureManifest manifest = SnapshotCaptureManifest.parseFrom(bytes);
      validateReuseManifestIdentity(tableId, snapshotId, expectedConnectorId, manifest, uri);
      if (!manifest.getReusableArtifactBundlesComplete()) {
        throw invalidReuseManifest("snapshot reuse manifest is incomplete: " + uri, null);
      }
      ReusableArtifactManifest.validate(manifest);
      return Optional.of(new HistoricalArtifacts(manifest.getReusableArtifactBundlesList()));
    } catch (com.google.protobuf.InvalidProtocolBufferException | IllegalArgumentException e) {
      throw invalidReuseManifest("invalid snapshot reuse manifest: " + uri, e);
    }
  }

  private static ReconcileFailureException invalidReuseManifest(String message, Throwable cause) {
    return new ReconcileFailureException(
        ExecutionResult.FailureKind.INTERNAL,
        ExecutionResult.RetryDisposition.TERMINAL,
        ExecutionResult.RetryClass.NONE,
        message,
        cause);
  }

  private static ReconcileFailureException unavailableReuseManifest(
      String message, Throwable cause) {
    return new ReconcileFailureException(
        ExecutionResult.FailureKind.INTERNAL,
        ExecutionResult.RetryDisposition.RETRYABLE,
        ExecutionResult.RetryClass.TRANSIENT_ERROR,
        message,
        cause);
  }

  static void validateReuseManifestIdentity(
      ResourceId tableId,
      long snapshotId,
      String expectedConnectorId,
      SnapshotCaptureManifest manifest,
      String uri) {
    if (manifest.getFormatVersion() != 1
        || !tableId.getAccountId().equals(manifest.getAccountId())
        || !java.util.Objects.equals(expectedConnectorId, manifest.getConnectorId())
        || !tableId.getId().equals(manifest.getTableId())
        || snapshotId != manifest.getSnapshotId()) {
      throw invalidReuseManifest("snapshot reuse manifest identity mismatch: " + uri, null);
    }
  }

  private Optional<HistoricalArtifacts> loadLatestReconciledReuseManifest(
      ReconcileContext context,
      ResourceId tableId,
      long targetSnapshotId,
      String expectedConnectorId) {
    Snapshot basis =
        backend.latestReconciledSnapshotForReuse(context, tableId, targetSnapshotId).orElse(null);
    if (basis == null) {
      LOG.infof(
          "No reconciled snapshot reuse basis found tableId=%s targetSnapshotId=%d",
          tableId.getId(), targetSnapshotId);
      return Optional.empty();
    }
    LOG.infof(
        "Selected reconciled snapshot reuse basis tableId=%s targetSnapshotId=%d basisSnapshotId=%d",
        tableId.getId(), targetSnapshotId, basis.getSnapshotId());
    return loadReuseManifest(tableId, basis, expectedConnectorId);
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private record HistoricalArtifacts(
      List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> bundles) {}

  private Optional<PlannedSnapshotCapture> tryDirectStatsCapture(
      ReconcileJobStore.LeasedJob lease,
      StandalonePlanSnapshotPayload payload,
      ReconcileSnapshotTask task) {
    SnapshotDirectStatsRequest request = directStatsRequest(payload, task);
    if (!request.eligible()) {
      return Optional.empty();
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setId(task.tableId())
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ReconcileContext reconcileContext = reconcileContext(lease);
    Optional<FloecatConnector.DirectSnapshotStatsCapture> directStats =
        backend.captureSnapshotTargetStatsDirect(
            reconcileContext,
            tableId,
            task.snapshotId(),
            request.includeColumns(),
            request.includeTargetKinds(),
            request.columnSelectorPolicy());
    if (directStats.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        PlannedSnapshotCapture.direct(
            ReconcileSnapshotTask.of(
                task.tableId(),
                task.snapshotId(),
                task.sourceNamespace(),
                task.sourceTable(),
                List.of(),
                true,
                ReconcileSnapshotTask.CompletionMode.DIRECT_STATS,
                "",
                0,
                directStats.get().sourceFileCount(),
                "",
                directStats.get().records().size(),
                task.sourceRevision(),
                task.metadataFingerprint(),
                ReconcileSnapshotContentState.materializedCoverage(
                    task.requestedCoverage(),
                    directStats.get().realizedStatsSelectors(),
                    List.of(),
                    directStats.get().sourceFileCount()),
                task.indexPredecessor()),
            directStats.get().records()));
  }

  private SnapshotDirectStatsRequest directStatsRequest(
      StandalonePlanSnapshotPayload payload, ReconcileSnapshotTask task) {
    ReconcileScope scope = effectiveSnapshotScope(payload.scope(), task);
    ReconcileCapturePolicy capturePolicy =
        scope == null ? ReconcileCapturePolicy.empty() : scope.capturePolicy();
    if (!isDirectStatsEligible(payload.captureMode(), capturePolicy)) {
      return SnapshotDirectStatsRequest.ineligible();
    }
    return new SnapshotDirectStatsRequest(
        true,
        capturePolicy.selectorsForStats(),
        FileGroupExecutionSupport.requestedStatsTargetKinds(capturePolicy),
        FileGroupExecutionSupport.columnSelectorPolicy(capturePolicy));
  }

  private static boolean isDirectStatsEligible(
      ReconcilerService.CaptureMode captureMode, ReconcileCapturePolicy capturePolicy) {
    if (captureMode == ReconcilerService.CaptureMode.METADATA_ONLY || capturePolicy == null) {
      return false;
    }
    if (capturePolicy.outputs().isEmpty()
        || capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)) {
      return false;
    }
    for (ReconcileCapturePolicy.Output output : capturePolicy.outputs()) {
      switch (output) {
        case TABLE_STATS, FILE_STATS, COLUMN_STATS -> {}
        default -> {
          return false;
        }
      }
    }
    return capturePolicy.requestsStats() && !capturePolicy.requestsIndexes();
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
        "fetchSnapshotFilePlan jobId=%s tableId=%s snapshotId=%d present=%s dataFiles=%d"
            + " deleteFiles=%d",
        lease.jobId,
        task.tableId(),
        task.snapshotId(),
        planned.isPresent(),
        planned.map(plan -> plan.dataFiles().size()).orElse(0),
        planned.map(plan -> plan.deleteFiles().size()).orElse(0));
    if (planned.isEmpty()) {
      throw new ReconcileFailureException(
          ExecutionResult.FailureKind.INTERNAL,
          ExecutionResult.RetryDisposition.TERMINAL,
          "Snapshot id does not exist: tableId="
              + task.tableId()
              + " snapshotId="
              + task.snapshotId(),
          null);
    }
    return planned;
  }

  private List<ReconcileFileGroupTask> partitionFiles(
      String planId,
      ReconcileSnapshotTask task,
      List<FloecatConnector.SnapshotFileEntry> files,
      String executionSchemaJson) {
    java.util.ArrayList<ReconcileFileGroupTask> groups = new java.util.ArrayList<>();
    for (List<FloecatConnector.SnapshotFileEntry> groupFiles :
        partitionByEstimatedWork(files, maxFilesPerGroup)) {
      String groupId = "snapshot-" + task.snapshotId() + "-group-" + groups.size();
      List<String> filePaths =
          groupFiles.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList();
      List<ReconcileFileExecutionPlan> executionPlans =
          groupFiles.stream().map(RemoteSnapshotPlanningReconcileExecutor::executionPlan).toList();
      groups.add(
          ReconcileFileGroupTask.of(
              planId,
              groupId,
              task.tableId(),
              task.snapshotId(),
              filePaths.size(),
              "",
              0,
              filePaths,
              List.of(),
              List.of(),
              executionSchemaJson,
              executionPlans));
    }
    return List.copyOf(groups);
  }

  static List<List<FloecatConnector.SnapshotFileEntry>> partitionByEstimatedWork(
      List<FloecatConnector.SnapshotFileEntry> files, int maxFilesPerGroup) {
    if (files == null || files.isEmpty()) {
      return List.of();
    }

    int effectiveMaxFiles = Math.max(1, maxFilesPerGroup);
    int groupCount = (files.size() + effectiveMaxFiles - 1) / effectiveMaxFiles;
    List<FileGroupBucket> buckets = new ArrayList<>(groupCount);
    PriorityQueue<FileGroupBucket> available =
        new PriorityQueue<>(
            Comparator.comparingLong(FileGroupBucket::estimatedWork)
                .thenComparingInt(FileGroupBucket::fileCount)
                .thenComparingInt(FileGroupBucket::index));
    for (int index = 0; index < groupCount; index++) {
      FileGroupBucket bucket = new FileGroupBucket(index);
      buckets.add(bucket);
      available.add(bucket);
    }

    List<FloecatConnector.SnapshotFileEntry> weightedFiles =
        files.stream()
            .sorted(
                Comparator.comparingLong(RemoteSnapshotPlanningReconcileExecutor::estimatedFileWork)
                    .reversed()
                    .thenComparing(FloecatConnector.SnapshotFileEntry::filePath)
                    .thenComparing(FloecatConnector.SnapshotFileEntry::fileFormat))
            .toList();
    for (FloecatConnector.SnapshotFileEntry file : weightedFiles) {
      FileGroupBucket bucket = available.remove();
      bucket.add(file);
      if (bucket.fileCount() < effectiveMaxFiles) {
        available.add(bucket);
      }
    }

    return buckets.stream().map(FileGroupBucket::immutableFiles).toList();
  }

  private static long estimatedFileWork(FloecatConnector.SnapshotFileEntry file) {
    long estimatedWork = saturatedAdd(file.fileSizeInBytes(), ESTIMATED_FILE_OVERHEAD_BYTES);
    if (file.deletionVector() != null) {
      estimatedWork = saturatedAdd(estimatedWork, file.deletionVector().sizeInBytes());
    }
    for (FloecatConnector.SnapshotIcebergDeleteFile deleteFile : file.icebergDeleteFiles()) {
      estimatedWork = saturatedAdd(estimatedWork, deleteFile.fileSizeInBytes());
    }
    return estimatedWork;
  }

  private static long saturatedAdd(long left, long right) {
    long nonNegativeRight = Math.max(0L, right);
    return left > Long.MAX_VALUE - nonNegativeRight ? Long.MAX_VALUE : left + nonNegativeRight;
  }

  private static final class FileGroupBucket {
    private final int index;
    private final List<FloecatConnector.SnapshotFileEntry> files = new ArrayList<>();
    private long estimatedWork;

    private FileGroupBucket(int index) {
      this.index = index;
    }

    private void add(FloecatConnector.SnapshotFileEntry file) {
      files.add(file);
      estimatedWork = saturatedAdd(estimatedWork, estimatedFileWork(file));
    }

    private int index() {
      return index;
    }

    private int fileCount() {
      return files.size();
    }

    private long estimatedWork() {
      return estimatedWork;
    }

    private List<FloecatConnector.SnapshotFileEntry> immutableFiles() {
      return List.copyOf(files);
    }
  }

  private static ReconcileFileExecutionPlan executionPlan(FloecatConnector.SnapshotFileEntry file) {
    FloecatConnector.SnapshotDeletionVector dv = file.deletionVector();
    DeltaDeletionVector plannedDv =
        dv == null
            ? null
            : new DeltaDeletionVector(
                dv.storageType(),
                dv.pathOrInlineDv(),
                dv.offset(),
                dv.sizeInBytes(),
                dv.cardinality());
    return ReconcileFileExecutionPlan.of(
        file.filePath(),
        file.fileSizeInBytes(),
        file.partitionDataJson(),
        plannedDv,
        file.fileFormat(),
        file.partitionSpecId(),
        file.icebergDeleteFiles().stream()
            .map(RemoteSnapshotPlanningReconcileExecutor::icebergDeleteFile)
            .toList(),
        file.contentIdentity());
  }

  private static ReconcileFileExecutionPlan.IcebergDeleteFile icebergDeleteFile(
      FloecatConnector.SnapshotIcebergDeleteFile deleteFile) {
    ReconcileFileExecutionPlan.IcebergDeleteContent content =
        switch (deleteFile.fileContent()) {
          case FC_POSITION_DELETES -> ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION;
          case FC_EQUALITY_DELETES -> ReconcileFileExecutionPlan.IcebergDeleteContent.EQUALITY;
          default -> ReconcileFileExecutionPlan.IcebergDeleteContent.UNSPECIFIED;
        };
    return new ReconcileFileExecutionPlan.IcebergDeleteFile(
        deleteFile.filePath(),
        deleteFile.fileSizeInBytes(),
        content,
        deleteFile.partitionSpecId(),
        deleteFile.equalityFieldIds(),
        deleteFile.contentIdentity());
  }

  private ReconcileContext reconcileContext(ReconcileJobStore.LeasedJob lease) {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setAccountId(lease.accountId)
            .setSubject("reconciler.scheduler")
            .setCorrelationId("reconciler-job-" + lease.jobId)
            .build();
    return new ReconcileContext(
        "reconciler-job-" + lease.jobId,
        principal,
        id(),
        Instant.now(),
        Optional.ofNullable(workerAuthorizationHeader(lease.accountId)),
        Optional.of(lease.jobId),
        Optional.of(lease.leaseEpoch));
  }

  private String workerAuthorizationHeader(String accountId) {
    if (!workerAuthRequired) {
      return null;
    }
    return reconcileWorkerAuthProvider.authorizationHeader(accountId).orElse(null);
  }

  public static ReconcileScope effectiveFileGroupScope(
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

  private static ReconcileScope effectiveSnapshotScope(
      ReconcileScope baseScope, ReconcileSnapshotTask snapshotTask) {
    if (baseScope == null
        || !baseScope.hasCaptureRequestFilter()
        || snapshotTask == null
        || snapshotTask.isEmpty()) {
      return baseScope == null ? ReconcileScope.empty() : baseScope;
    }
    List<ReconcileScope.ScopedCaptureRequest> snapshotRequests =
        baseScope.destinationCaptureRequests().stream()
            .filter(request -> request != null)
            .filter(request -> snapshotTask.tableId().equals(request.tableId()))
            .filter(request -> snapshotTask.snapshotId() == request.snapshotId())
            .toList();
    if (snapshotRequests.isEmpty()) {
      return baseScope;
    }
    return ReconcileScope.of(
        baseScope.destinationNamespaceIds(),
        baseScope.destinationTableId(),
        baseScope.destinationViewId(),
        snapshotRequests,
        mergeCapturePolicy(baseScope.capturePolicy(), snapshotRequests),
        baseScope.snapshotSelection());
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
        case FILE, EXPRESSION, TARGET_NOT_SET -> {}
      }
      for (String selector : request.columnSelectors()) {
        if (selector == null || selector.isBlank()) {
          continue;
        }
        selectorPolicy(basePolicy, outputs, selector)
            .ifPresent(column -> columns.putIfAbsent(selector, column));
      }
    }
    return ReconcileCapturePolicy.of(
        new ArrayList<>(columns.values()),
        Set.copyOf(outputs),
        basePolicy == null
            ? ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
            : basePolicy.defaultColumnScope(),
        basePolicy == null
            ? ReconcileCapturePolicy.DEFAULT_MAX_COLUMNS
            : basePolicy.maxDefaultColumns());
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

  private record PlannedSnapshotCapture(
      ReconcileSnapshotTask snapshotTask,
      List<ReconcileFileGroupTask> fileGroupTasks,
      List<TargetStatsRecord> directStats) {
    private static PlannedSnapshotCapture direct(ReconcileSnapshotTask snapshotTask) {
      return new PlannedSnapshotCapture(snapshotTask, List.of(), List.of());
    }

    private static PlannedSnapshotCapture direct(
        ReconcileSnapshotTask snapshotTask, List<TargetStatsRecord> directStats) {
      return new PlannedSnapshotCapture(snapshotTask, List.of(), directStats);
    }

    private static PlannedSnapshotCapture fileGroups(
        ReconcileSnapshotTask snapshotTask, List<ReconcileFileGroupTask> fileGroupTasks) {
      return new PlannedSnapshotCapture(
          snapshotTask,
          fileGroupTasks == null ? List.of() : List.copyOf(fileGroupTasks),
          List.of());
    }
  }

  private record SnapshotDirectStatsRequest(
      boolean eligible,
      Set<String> includeColumns,
      Set<FloecatConnector.StatsTargetKind> includeTargetKinds,
      FloecatConnector.ColumnSelectorPolicy columnSelectorPolicy) {
    private static SnapshotDirectStatsRequest ineligible() {
      return new SnapshotDirectStatsRequest(
          false, Set.of(), Set.of(), FloecatConnector.ColumnSelectorPolicy.defaults());
    }
  }

  private static int plannedSourceFileCount(List<ReconcileFileGroupTask> fileGroupTasks) {
    if (fileGroupTasks == null || fileGroupTasks.isEmpty()) {
      return 0;
    }
    return fileGroupTasks.stream()
        .map(ReconcileFileGroupTask::filePaths)
        .filter(paths -> paths != null)
        .mapToInt(List::size)
        .sum();
  }
}
