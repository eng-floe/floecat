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

package ai.floedb.floecat.service.reconciler.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CONNECTOR;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.FileArtifactReuse;
import ai.floedb.floecat.reconciler.impl.FileGroupExecutionSupport;
import ai.floedb.floecat.reconciler.impl.ReconcileLeaseGrpcStatus;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.StandaloneFileGroupExecutionPayload;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.rpc.CommitLeasedFileGroupResultRequest;
import ai.floedb.floecat.reconciler.rpc.CommitLeasedFileGroupResultResponse;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
public class LeasedFileGroupExecutionService extends BaseServiceImpl {
  @Inject ReconcileJobStore jobs;
  @Inject TableRepository tableRepo;
  @Inject ConnectorRepository connectorRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject CredentialResolver credentialResolver;
  @Inject StatsStore statsStore;
  @Inject IndexArtifactRepository indexArtifactRepository;
  @Inject IdempotencyRepository idempotencyStore;
  LegacyFileIdentityResolverFactory legacyFileIdentityResolverFactory =
      LeasedFileGroupExecutionService::openLegacyFileIdentityResolver;

  public StandaloneFileGroupExecutionPayload resolve(
      PrincipalContext principalContext, String jobId, String leaseEpoch) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    Table table =
        tableRepo
            .getById(tableId)
            .orElseThrow(
                () -> GrpcErrors.notFound(corr, TABLE, Map.of("table_id", tableId.getId())));
    if (!table.hasUpstream() || !table.getUpstream().hasConnectorId()) {
      throw Status.FAILED_PRECONDITION
          .withDescription("table upstream connector metadata is required for file-group execution")
          .asRuntimeException();
    }
    ResourceId connectorId = table.getUpstream().getConnectorId();
    Connector connector =
        connectorRepo
            .getById(connectorId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr, CONNECTOR, Map.of("connector_id", connectorId.getId())));
    Connector resolvedConnector = resolvedConnectorPayload(connector, table);
    String sourceNamespace = String.join(".", table.getUpstream().getNamespacePathList());
    String sourceTable = table.getUpstream().getTableDisplayName();
    ReconcileCapturePolicy capturePolicy = FileGroupExecutionSupport.effectiveCapturePolicy(lease);
    IndexArtifactRepository.GenerationInput capturedIndexInput =
        capturePolicy.requestsIndexes()
            ? pinnedIndexInput(lease, tableId, plannedTask)
            : new IndexArtifactRepository.GenerationInput(
                new IndexArtifactRepository.GenerationPredecessor("", 0L, "", 0L), List.of());
    IndexArtifactRepository.GenerationInput indexInput =
        lease.fullRescan
            ? new IndexArtifactRepository.GenerationInput(
                capturedIndexInput.predecessor(), List.of())
            : capturedIndexInput;
    LegacyFileIdentityResolver legacyIdentityResolver =
        lease.fullRescan
            ? LegacyFileIdentityResolver.NONE
            : legacyFileIdentityResolverFactory.open(
                resolvedConnector,
                sourceNamespace,
                sourceTable,
                tableId,
                Set.copyOf(plannedTask.filePaths()));
    List<ReconcileFileExecutionPlan> executionPlans;
    try {
      executionPlans =
          enrichExecutionPlans(
              tableId,
              plannedTask.snapshotId(),
              plannedTask.executionSchemaJson(),
              plannedTask.fileExecutionPlans(),
              capturePolicy,
              lease.fullRescan,
              legacyIdentityResolver);
    } finally {
      legacyIdentityResolver.close();
    }
    return new StandaloneFileGroupExecutionPayload(
        lease.jobId,
        lease.leaseEpoch,
        lease.parentJobId,
        resolvedConnector,
        sourceNamespace,
        sourceTable,
        resolvePayloadStorageLocation(table),
        tableId,
        plannedTask.snapshotId(),
        plannedTask.planId(),
        plannedTask.groupId(),
        Keys.reconcileFileGroupResultPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch),
        Keys.reconcileFileGroupStatsObjectPrefix(
            lease.accountId,
            plannedTask.tableId(),
            plannedTask.snapshotId(),
            lease.parentJobId,
            lease.jobId,
            lease.leaseEpoch),
        plannedTask.filePaths(),
        plannedTask.executionSchemaJson(),
        executionPlans,
        capturePolicy,
        new StandaloneFileGroupExecutionPayload.IndexGenerationPredecessor(
            indexInput.predecessor().generationId(),
            indexInput.predecessor().activePointerVersion(),
            indexInput.predecessor().captureManifestUri(),
            indexInput.predecessor().captureManifestPointerVersion()),
        indexInput.artifacts());
  }

  private List<ReconcileFileExecutionPlan> enrichExecutionPlans(
      ResourceId tableId,
      long snapshotId,
      String executionSchemaJson,
      List<ReconcileFileExecutionPlan> plans,
      ReconcileCapturePolicy capturePolicy,
      boolean fullRescan,
      LegacyFileIdentityResolver legacyIdentityResolver) {
    String statsSignature = FileArtifactReuse.statsCaptureSignature(capturePolicy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(capturePolicy);
    boolean requestsStats =
        !FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy).isEmpty();
    boolean requestsIndexes = capturePolicy.requestsIndexes();
    List<ReconcileFileExecutionPlan> enriched = new ArrayList<>();
    for (ReconcileFileExecutionPlan plan :
        plans == null ? List.<ReconcileFileExecutionPlan>of() : plans) {
      String sourceFingerprint = FileArtifactReuse.sourceFingerprint(plan, executionSchemaJson);
      String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
      Map<String, String> auxiliaryFingerprints =
          FileArtifactReuse.auxiliaryStatsFingerprints(plan);
      TargetStatsRecord reusableStats = TargetStatsRecord.getDefaultInstance();
      List<TargetStatsRecord> reusableAuxiliaryStats = List.of();
      IndexArtifactRecord reusableIndex = IndexArtifactRecord.getDefaultInstance();
      boolean hasContentIdentity = !plan.contentIdentity().isBlank();
      TargetStatsRecord priorStats = null;
      boolean migratedLegacyStats = false;
      if (!fullRescan && hasContentIdentity && snapshotId >= 0L && requestsStats) {
        priorStats =
            statsStore
                .getReusableTargetStats(
                    tableId,
                    StatsTargetIdentity.fileTarget(plan.filePath()),
                    sourceFingerprint,
                    statsSignature)
                .orElse(null);
        if (priorStats == null && legacyStatsPolicyCompatible(capturePolicy)) {
          priorStats =
              statsStore
                  .findHistoricalTargetStats(
                      tableId,
                      StatsTargetIdentity.fileTarget(plan.filePath()),
                      candidate ->
                          legacyStatsCompatible(
                              candidate, plan, capturePolicy, legacyIdentityResolver))
                  .orElse(null);
          migratedLegacyStats = priorStats != null;
        }
        if (FileArtifactReuse.compatibleStats(
                priorStats, plan.filePath(), sourceFingerprint, statsSignature)
            || migratedLegacyStats) {
          reusableStats =
              FileArtifactReuse.bindStatsToSnapshot(
                  priorStats, tableId, snapshotId, sourceFingerprint, statsSignature);
          if (migratedLegacyStats) {
            reusableStats =
                FileArtifactReuse.stampStats(
                    reusableStats,
                    sourceFingerprint,
                    statsSignature,
                    priorStats.getFile().getColumnsList().stream()
                        .map(column -> "#" + column.getColumnId())
                        .toList());
          }
        }
        ArrayList<TargetStatsRecord> auxiliary = new ArrayList<>();
        for (Map.Entry<String, String> entry : auxiliaryFingerprints.entrySet()) {
          TargetStatsRecord priorAuxiliary =
              statsStore
                  .getReusableTargetStats(
                      tableId,
                      StatsTargetIdentity.fileTarget(entry.getKey()),
                      entry.getValue(),
                      statsSignature)
                  .orElse(null);
          auxiliary.add(
              FileArtifactReuse.compatibleStats(
                      priorAuxiliary, entry.getKey(), entry.getValue(), statsSignature)
                  ? FileArtifactReuse.bindStatsToSnapshot(
                      priorAuxiliary, tableId, snapshotId, entry.getValue(), statsSignature)
                  : FileArtifactReuse.auxiliaryStatsRecord(
                      plan, entry.getKey(), tableId, snapshotId, entry.getValue(), statsSignature));
        }
        reusableAuxiliaryStats = List.copyOf(auxiliary);
      }
      if (!fullRescan
          && hasContentIdentity
          && snapshotId >= 0L
          && requestsIndexes
          && priorStats == null) {
        priorStats =
            statsStore
                .findHistoricalTargetStats(
                    tableId,
                    StatsTargetIdentity.fileTarget(plan.filePath()),
                    candidate ->
                        legacySourceIdentityCompatible(candidate, plan, legacyIdentityResolver))
                .orElse(null);
      }
      if (!fullRescan && hasContentIdentity && snapshotId >= 0L && requestsIndexes) {
        IndexTarget target =
            IndexTarget.newBuilder()
                .setFile(IndexFileTarget.newBuilder().setFilePath(plan.filePath()))
                .build();
        IndexArtifactRecord prior =
            indexArtifactRepository
                .getReusableIndexArtifact(tableId, target, indexSourceFingerprint, indexSignature)
                .orElse(null);
        if (prior == null && priorStats != null && legacyIndexPolicyCompatible(capturePolicy)) {
          prior =
              indexArtifactRepository
                  .getIndexArtifact(tableId, priorStats.getSnapshotId(), target)
                  .filter(candidate -> legacyIndexCompatible(candidate, plan, capturePolicy))
                  .orElse(null);
        }
        if (FileArtifactReuse.compatibleIndex(
                prior, plan.filePath(), indexSourceFingerprint, indexSignature)
            || (priorStats != null && legacyIndexCompatible(prior, plan, capturePolicy))) {
          reusableIndex =
              FileArtifactReuse.bindIndexToSnapshot(
                  prior, tableId, snapshotId, indexSourceFingerprint, indexSignature);
        }
      }
      enriched.add(
          plan.withReuse(
              sourceFingerprint,
              indexSourceFingerprint,
              statsSignature,
              indexSignature,
              auxiliaryFingerprints,
              reusableStats,
              reusableAuxiliaryStats,
              reusableIndex));
    }
    return List.copyOf(enriched);
  }

  private static boolean legacyStatsPolicyCompatible(ReconcileCapturePolicy policy) {
    ReconcileCapturePolicy effective = policy == null ? ReconcileCapturePolicy.empty() : policy;
    if (!effective.properties().isEmpty()) {
      return false;
    }
    if (!effective.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)) {
      return true;
    }
    Set<String> selectors = effective.selectorsForStats();
    return !selectors.isEmpty()
        && selectors.stream().allMatch(selector -> selector.startsWith("#"));
  }

  private static boolean legacyStatsCompatible(
      TargetStatsRecord record,
      ReconcileFileExecutionPlan plan,
      ReconcileCapturePolicy policy,
      LegacyFileIdentityResolver legacyIdentityResolver) {
    if (!legacySourceIdentityCompatible(record, plan, legacyIdentityResolver)) {
      return false;
    }
    Set<String> selectors = policy.selectorsForStats();
    if (selectors.isEmpty()) {
      return true;
    }
    Set<String> present =
        record.getFile().getColumnsList().stream()
            .map(column -> "#" + column.getColumnId())
            .collect(java.util.stream.Collectors.toSet());
    return present.containsAll(selectors);
  }

  private static boolean legacySourceIdentityCompatible(
      TargetStatsRecord record,
      ReconcileFileExecutionPlan plan,
      LegacyFileIdentityResolver legacyIdentityResolver) {
    if (FileArtifactReuse.legacyCompatibleIcebergStats(record, plan)) {
      return true;
    }
    if (record == null || !plan.contentIdentity().startsWith("delta-add-v1:")) {
      return false;
    }
    String historicalIdentity =
        legacyIdentityResolver.contentIdentity(record.getSnapshotId(), plan.filePath()).orElse("");
    return FileArtifactReuse.legacyCompatibleDeltaStats(record, plan, historicalIdentity);
  }

  private static boolean legacyIndexPolicyCompatible(ReconcileCapturePolicy policy) {
    ReconcileCapturePolicy effective = policy == null ? ReconcileCapturePolicy.empty() : policy;
    return effective.properties().isEmpty() && !effective.selectorsForIndex().isEmpty();
  }

  private static boolean legacyIndexCompatible(
      IndexArtifactRecord record, ReconcileFileExecutionPlan plan, ReconcileCapturePolicy policy) {
    if (record == null
        || !record.hasTarget()
        || !record.getTarget().hasFile()
        || !plan.filePath().equals(record.getTarget().getFile().getFilePath())
        || record.getState() != ai.floedb.floecat.catalog.rpc.IndexArtifactState.IAS_READY
        || record.getArtifactUri().isBlank()) {
      return false;
    }
    Set<String> indexed =
        java.util.Arrays.stream(record.getPropertiesOrDefault("indexed_columns", "").split(","))
            .map(String::trim)
            .filter(selector -> !selector.isBlank())
            .collect(java.util.stream.Collectors.toSet());
    return indexed.containsAll(policy.selectorsForIndex());
  }

  @FunctionalInterface
  interface LegacyFileIdentityResolverFactory {
    LegacyFileIdentityResolver open(
        Connector connector,
        String namespace,
        String table,
        ResourceId tableId,
        Set<String> filePaths);
  }

  interface LegacyFileIdentityResolver {
    LegacyFileIdentityResolver NONE =
        new LegacyFileIdentityResolver() {
          @Override
          public java.util.Optional<String> contentIdentity(long snapshotId, String filePath) {
            return java.util.Optional.empty();
          }

          @Override
          public void close() {}
        };

    java.util.Optional<String> contentIdentity(long snapshotId, String filePath);

    void close();
  }

  private static LegacyFileIdentityResolver openLegacyFileIdentityResolver(
      Connector connector,
      String namespace,
      String table,
      ResourceId tableId,
      Set<String> filePaths) {
    if (connector == null || connector.getKind() != ConnectorKind.CK_DELTA) {
      return LegacyFileIdentityResolver.NONE;
    }
    try {
      FloecatConnector source = ConnectorFactory.create(ConnectorConfigMapper.fromProto(connector));
      Map<Long, Map<String, String>> bySnapshot = new HashMap<>();
      return new LegacyFileIdentityResolver() {
        @Override
        public java.util.Optional<String> contentIdentity(long snapshotId, String filePath) {
          Map<String, String> identities =
              bySnapshot.computeIfAbsent(
                  snapshotId,
                  id -> {
                    try {
                      return source.snapshotFileContentIdentities(
                          namespace, table, tableId, id, filePaths);
                    } catch (RuntimeException ignored) {
                      return Map.of();
                    }
                  });
          return java.util.Optional.ofNullable(identities.get(filePath))
              .filter(identity -> !identity.isBlank());
        }

        @Override
        public void close() {
          try {
            source.close();
          } catch (Exception ignored) {
          }
        }
      };
    } catch (RuntimeException ignored) {
      return LegacyFileIdentityResolver.NONE;
    }
  }

  private IndexArtifactRepository.GenerationInput pinnedIndexInput(
      ReconcileJobStore.LeasedJob lease, ResourceId tableId, ReconcileFileGroupTask plannedTask) {
    ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor pinned =
        pinnedIndexPredecessor(lease);
    IndexArtifactRepository.GenerationPredecessor predecessor =
        new IndexArtifactRepository.GenerationPredecessor(
            pinned.generationId(),
            pinned.activePointerVersion(),
            pinned.captureManifestUri(),
            pinned.captureManifestPointerVersion());
    return lease.fullRescan
        ? new IndexArtifactRepository.GenerationInput(predecessor, List.of())
        : indexArtifactRepository.loadGenerationInput(
            tableId, plannedTask.snapshotId(), predecessor, plannedTask.filePaths());
  }

  private ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor pinnedIndexPredecessor(
      ReconcileJobStore.LeasedJob lease) {
    ReconcileJobStore.ReconcileJob parent =
        jobs.get(lease.accountId, lease.parentJobId)
            .orElseThrow(
                () ->
                    Status.FAILED_PRECONDITION
                        .withDescription("snapshot plan parent is required for index capture")
                        .asRuntimeException());
    ReconcileSnapshotTask snapshotTask =
        parent.snapshotTask == null ? ReconcileSnapshotTask.empty() : parent.snapshotTask;
    ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor pinned =
        snapshotTask.indexPredecessor();
    if (pinned == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("snapshot index generation predecessor was not pinned before fan-out")
          .asRuntimeException();
    }
    return pinned;
  }

  private Connector withTableStorageLocation(Connector connector, Table table) {
    if (connector == null
        || table == null
        || connector.getKind() != ConnectorKind.CK_DELTA
        || !table.getPropertiesMap().containsKey("storage_location")) {
      return connector;
    }
    String storageLocation = table.getPropertiesMap().get("storage_location");
    if (storageLocation == null || storageLocation.isBlank()) {
      return connector;
    }
    return connector.toBuilder().putProperties("storage_location", storageLocation).build();
  }

  private Connector resolvedConnectorPayload(Connector connector, Table table) {
    ConnectorConfig resolved = resolveCredentials(connector);
    Connector payload =
        connector.toBuilder()
            .putAllProperties(resolved.options())
            .setAuth(toAuthConfig(resolved.auth()))
            .build();
    return withTableStorageLocation(payload, table);
  }

  private String resolvePayloadStorageLocation(Table table) {
    if (table == null) {
      return "";
    }
    String location = firstNonBlank(table.getPropertiesMap().get("storage_location"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("location"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("delta.table-root"));
    if (location != null) {
      return location;
    }
    location = firstNonBlank(table.getPropertiesMap().get("external.location"));
    if (location != null) {
      return location;
    }
    location = deriveTableRootLocation(table.getPropertiesMap().get("source_metadata_location"));
    if (!location.isBlank()) {
      return location;
    }
    if (snapshotRepo != null) {
      location =
          snapshotRepo
              .latestRegisteredSnapshot(table.getResourceId())
              .map(SnapshotRepository::metadataLocation)
              .map(LeasedFileGroupExecutionService::deriveTableRootLocation)
              .orElse("");
      if (!location.isBlank()) {
        return location;
      }
    }
    return firstNonBlank(table.hasUpstream() ? table.getUpstream().getUri() : null, "");
  }

  private static String deriveTableRootLocation(String location) {
    String normalized = firstNonBlank(location);
    if (normalized == null) {
      return "";
    }
    int metadataSegment = normalized.indexOf("/metadata/");
    if (metadataSegment > 0) {
      return normalized.substring(0, metadataSegment);
    }
    int deltaLogSegment = normalized.indexOf("/_delta_log/");
    if (deltaLogSegment > 0) {
      return normalized.substring(0, deltaLogSegment);
    }
    if (normalized.endsWith("/metadata")) {
      return normalized.substring(0, normalized.length() - "/metadata".length());
    }
    if (normalized.endsWith("/_delta_log")) {
      return normalized.substring(0, normalized.length() - "/_delta_log".length());
    }
    return normalized;
  }

  private static String firstNonBlank(String value) {
    return firstNonBlank(value, null);
  }

  private static String firstNonBlank(String value, String defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? defaultValue : trimmed;
  }

  public boolean persistSuccess(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor,
      List<StatsObjectDescriptor> fileStats,
      List<StatsObjectDescriptor> indexArtifacts) {
    String corr = principalContext.getCorrelationId();
    String requiredResultId = requireResultId(resultId);
    ReconcileJobStore.ReconcileJob existing = jobs.getLeaseView(jobId).orElse(null);
    if (existing != null
        && ("JS_SUCCEEDED".equals(existing.state) || "JS_CANCELLED".equals(existing.state))) {
      boolean replayed =
          jobs.completeFileGroupSuccess(
              jobId, leaseEpoch, descriptor, System.currentTimeMillis(), "Executed file group");
      requireAcceptedLeaseOutcome(replayed, jobId);
      StagedArtifactReferences staged =
          prepareArtifactReferences(
              existing.accountId,
              existing.parentJobId,
              existing.jobId,
              leaseEpoch,
              existing.fileGroupTask,
              descriptor,
              fileStats,
              indexArtifacts,
              publishesFileStats(existing.scope.capturePolicy()));
      stageArtifactReferences(staged);
      return true;
    }
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ReconcileFileGroupResultDescriptor validated =
        validateResultDescriptor(lease, plannedTask, requiredResultId, descriptor);
    StagedArtifactReferences staged =
        prepareArtifactReferences(
            lease.accountId,
            lease.parentJobId,
            lease.jobId,
            lease.leaseEpoch,
            plannedTask,
            validated,
            fileStats,
            indexArtifacts,
            publishesFileStats(lease.scope.capturePolicy()));
    boolean accepted =
        jobs.completeFileGroupSuccess(
            lease.jobId,
            lease.leaseEpoch,
            validated,
            System.currentTimeMillis(),
            "Executed file group " + plannedTask.groupId());
    requireAcceptedLeaseOutcome(accepted, lease.jobId);
    stageArtifactReferences(staged);
    return true;
  }

  private ReconcileFileGroupResultDescriptor validateResultDescriptor(
      ReconcileJobStore.LeasedJob lease,
      ReconcileFileGroupTask plannedTask,
      String resultId,
      ReconcileFileGroupResultDescriptor descriptor) {
    if (descriptor == null || descriptor.isEmpty() || descriptor.formatVersion() != 1) {
      throw new IllegalArgumentException("file-group result descriptor format_version must be 1");
    }
    String expectedUri =
        Keys.reconcileFileGroupResultPayloadUri(
            lease.accountId, lease.parentJobId, lease.jobId, lease.leaseEpoch);
    String expectedStatsUri =
        Keys.reconcileFileGroupStatsObjectPrefix(
            lease.accountId,
            plannedTask.tableId(),
            plannedTask.snapshotId(),
            lease.parentJobId,
            lease.jobId,
            lease.leaseEpoch);
    if (!lease.accountId.equals(descriptor.accountId())
        || !lease.connectorId.equals(descriptor.connectorId())
        || !lease.parentJobId.equals(descriptor.parentJobId())
        || !lease.jobId.equals(descriptor.fileGroupJobId())
        || !plannedTask.planId().equals(descriptor.planId())
        || !plannedTask.groupId().equals(descriptor.groupId())
        || !plannedTask.tableId().equals(descriptor.tableId())
        || plannedTask.snapshotId() != descriptor.snapshotId()
        || !lease.leaseEpoch.equals(descriptor.leaseEpoch())
        || !resultId.equals(descriptor.resultId())) {
      throw new IllegalArgumentException("file-group result descriptor identity mismatch");
    }
    if (!expectedUri.equals(descriptor.payloadUri())) {
      throw new IllegalArgumentException(
          "file-group result descriptor payload_uri is outside the leased result location");
    }
    if (!expectedStatsUri.equals(descriptor.statsObjectPrefix())) {
      throw new IllegalArgumentException(
          "file-group result descriptor stats_object_prefix is outside the leased stats location");
    }
    if (descriptor.payloadBytes() <= 0L
        || descriptor.payloadSha256() == null
        || descriptor.payloadSha256().isBlank()
        || descriptor.artifactReferencesSha256() == null
        || descriptor.artifactReferencesSha256().length() != 64
        || descriptor.fileStatsRecordCount() < 0) {
      throw new IllegalArgumentException(
          "file-group result descriptor payload size, hashes, and stats count are required");
    }
    int plannedCount = plannedTask.filePaths().size();
    if (descriptor.plannedFileCount() != plannedCount
        || descriptor.succeededFileCount() != plannedCount
        || descriptor.failedFileCount() != 0
        || descriptor.skippedFileCount() != 0) {
      throw new IllegalArgumentException(
          "file-group result descriptor outcome counts do not match successful plan");
    }
    if (lease.scope != null && lease.scope.capturePolicy().requestsIndexes()) {
      if (descriptor.indexPredecessor() == null) {
        throw new IllegalArgumentException("index file-group result is missing its predecessor");
      }
      if (!pinnedIndexPredecessor(lease).equals(descriptor.indexPredecessor())) {
        throw new IllegalArgumentException(
            "index file-group result predecessor does not match the pinned snapshot predecessor");
      }
    }
    return descriptor;
  }

  private record StagedArtifactReferences(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String fileGroupJobId,
      String leaseEpoch,
      String artifactReferencesSha256,
      String indexArtifactObjectPrefix,
      List<StatsStore.PrewrittenStatsObject> objects,
      List<StatsStore.PrewrittenTargetStatsReference> statsReferences,
      List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences) {}

  private StagedArtifactReferences prepareArtifactReferences(
      String accountId,
      String parentJobId,
      String fileGroupJobId,
      String leaseEpoch,
      ReconcileFileGroupTask plannedTask,
      ReconcileFileGroupResultDescriptor descriptor,
      List<StatsObjectDescriptor> fileStats,
      List<StatsObjectDescriptor> indexArtifacts,
      boolean publishFileStats) {
    List<StatsObjectDescriptor> requiredFileStats = fileStats == null ? List.of() : fileStats;
    List<StatsObjectDescriptor> requiredIndexArtifacts =
        indexArtifacts == null ? List.of() : indexArtifacts;
    if (descriptor.fileStatsRecordCount() != requiredFileStats.size()
        || descriptor.indexArtifactCount() != requiredIndexArtifacts.size()) {
      throw new IllegalArgumentException("file-group pointer counts do not match descriptor");
    }
    String artifactReferencesSha256 =
        ArtifactReferenceDigest.sha256(requiredFileStats, requiredIndexArtifacts);
    if (!artifactReferencesSha256.equals(descriptor.artifactReferencesSha256())) {
      throw new IllegalArgumentException(
          "file-group pointer mappings do not match the durable result descriptor");
    }
    List<StatsStore.PrewrittenStatsObject> objects =
        new ArrayList<>(requiredFileStats.size() + requiredIndexArtifacts.size());
    List<StatsStore.PrewrittenTargetStatsReference> statsReferences =
        new ArrayList<>(requiredFileStats.size());
    List<IndexArtifactRepository.PrewrittenIndexArtifactReference> indexReferences =
        new ArrayList<>(requiredIndexArtifacts.size());
    String indexArtifactObjectPrefix = descriptor.statsObjectPrefix() + "index-artifacts/";
    List<StatsObjectDescriptor> descriptors = new ArrayList<>(requiredFileStats);
    descriptors.addAll(requiredIndexArtifacts);
    HashSet<String> payloadUris = new HashSet<>();
    for (StatsObjectDescriptor object : descriptors) {
      if (object.getTargetStorageId().isBlank()
          || object.getPayloadUri().isBlank()
          || !payloadUris.add(object.getPayloadUri())
          || !object.getPayloadUri().startsWith(descriptor.statsObjectPrefix())
          || object.getPayloadBytes() <= 0L
          || object.getPayloadSha256().size() != 32) {
        throw new IllegalArgumentException("invalid target stats object descriptor");
      }
      objects.add(
          new StatsStore.PrewrittenStatsObject(
              object.getPayloadUri(),
              object.getPayloadBytes(),
              object.getPayloadSha256().toByteArray()));
    }
    HashSet<String> statsTargets = new HashSet<>();
    HashSet<String> plannedStatsTargets = new HashSet<>();
    HashSet<String> plannedIndexTargets = new HashSet<>();
    for (String filePath : plannedTask.filePaths()) {
      plannedStatsTargets.add(
          StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(filePath)));
      plannedIndexTargets.add("file:" + filePath);
    }
    for (ReconcileFileExecutionPlan executionPlan : plannedTask.fileExecutionPlans()) {
      ReconcileFileExecutionPlan.DeltaDeletionVector deletionVector =
          executionPlan.deletionVector();
      if (deletionVector != null && deletionVector.onDisk()) {
        plannedStatsTargets.add(
            StatsTargetIdentity.storageId(
                StatsTargetIdentity.fileTarget(deletionVector.pathOrInlineDv())));
      }
      for (ReconcileFileExecutionPlan.IcebergDeleteFile deleteFile :
          executionPlan.icebergDeleteFiles()) {
        if (deleteFile != null && !deleteFile.filePath().isBlank()) {
          plannedStatsTargets.add(
              StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(deleteFile.filePath())));
        }
      }
    }
    for (StatsObjectDescriptor object : requiredFileStats) {
      if (!plannedStatsTargets.contains(object.getTargetStorageId())) {
        throw new IllegalArgumentException(
            "file stats target is outside the leased file group: " + object.getTargetStorageId());
      }
      if (!statsTargets.add(object.getTargetStorageId())) {
        throw new IllegalArgumentException(
            "duplicate file stats target: " + object.getTargetStorageId());
      }
      if (publishFileStats) {
        statsReferences.add(
            new StatsStore.PrewrittenTargetStatsReference(
                object.getTargetStorageId(),
                object.getPayloadUri(),
                object.getPayloadBytes(),
                object.getPayloadSha256().toByteArray()));
      }
    }
    HashSet<String> indexTargets = new HashSet<>();
    for (StatsObjectDescriptor object : requiredIndexArtifacts) {
      if (!plannedIndexTargets.contains(object.getTargetStorageId())) {
        throw new IllegalArgumentException(
            "index artifact target is outside the leased file group: "
                + object.getTargetStorageId());
      }
      if (!object.getPayloadUri().startsWith(indexArtifactObjectPrefix)) {
        throw new IllegalArgumentException("invalid prewritten index artifact object prefix");
      }
      if (!indexTargets.add(object.getTargetStorageId())) {
        throw new IllegalArgumentException(
            "duplicate index artifact target: " + object.getTargetStorageId());
      }
      indexReferences.add(
          new IndexArtifactRepository.PrewrittenIndexArtifactReference(
              object.getTargetStorageId(),
              object.getPayloadUri(),
              object.getPayloadBytes(),
              object.getPayloadSha256().toByteArray()));
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    return new StagedArtifactReferences(
        tableId,
        plannedTask.snapshotId(),
        "full-rescan-" + parentJobId,
        fileGroupJobId,
        leaseEpoch,
        artifactReferencesSha256,
        indexArtifactObjectPrefix,
        List.copyOf(objects),
        List.copyOf(statsReferences),
        List.copyOf(indexReferences));
  }

  private static boolean publishesFileStats(ReconcileCapturePolicy policy) {
    return policy == null
        || policy.outputs().isEmpty()
        || policy.outputs().contains(ReconcileCapturePolicy.Output.FILE_STATS);
  }

  private void stageArtifactReferences(StagedArtifactReferences staged) {
    if (statsStore.isPreparedFileGroup(
        staged.tableId(),
        staged.snapshotId(),
        staged.generationId(),
        staged.fileGroupJobId(),
        staged.leaseEpoch(),
        staged.artifactReferencesSha256())) {
      return;
    }
    statsStore.protectPrewrittenStatsObjectsInGeneration(
        staged.tableId(),
        staged.snapshotId(),
        staged.generationId(),
        staged.fileGroupJobId() + ":" + staged.leaseEpoch(),
        staged.objects());
    statsStore.registerPrewrittenStatsReferencesInGeneration(
        staged.tableId(), staged.snapshotId(), staged.generationId(), staged.statsReferences());
    indexArtifactRepository.registerPrewrittenIndexArtifactReferencesInGeneration(
        staged.tableId(),
        staged.snapshotId(),
        staged.generationId(),
        staged.indexArtifactObjectPrefix(),
        staged.indexReferences());
    statsStore.markPreparedFileGroup(
        staged.tableId(),
        staged.snapshotId(),
        staged.generationId(),
        staged.fileGroupJobId(),
        staged.leaseEpoch(),
        staged.artifactReferencesSha256());
  }

  private static void requireAcceptedLeaseOutcome(boolean accepted, String jobId) {
    if (!accepted) {
      throw ReconcileLeaseGrpcStatus.leasePreconditionFailed(
          "reconcile lease is no longer valid for job " + jobId);
    }
  }

  public boolean persistFailure(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      String message) {
    String corr = principalContext.getCorrelationId();
    ReconcileJobStore.LeasedJob lease = requireLeasedFileGroupJob(corr, jobId, leaseEpoch);
    ReconcileFileGroupTask plannedTask = resolvePlannedTask(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(lease.accountId)
            .setKind(ResourceKind.RK_TABLE)
            .setId(plannedTask.tableId())
            .build();
    String requiredResultId = requireResultId(resultId);
    String effectiveMessage = message == null ? "" : message;
    byte[] requestBytes = failurePayload(requiredResultId, effectiveMessage).toByteArray();
    return runIdempotentCreate(
            () ->
                MutationOps.createProto(
                    principalContext.getAccountId(),
                    "CommitLeasedFileGroupResult",
                    resultIdempotencyKey(jobId, requiredResultId),
                    () -> requestBytes,
                    () ->
                        new IdempotencyGuard.CreateResult<>(
                            CommitLeasedFileGroupResultResponse.newBuilder()
                                .setAccepted(true)
                                .build(),
                            tableId),
                    ignored -> MutationMeta.getDefaultInstance(),
                    idempotencyStore,
                    nowTs(),
                    idempotencyTtlSeconds(),
                    principalContext::getCorrelationId,
                    CommitLeasedFileGroupResultResponse::parseFrom))
        .body
        .getAccepted();
  }

  private static AuthConfig toAuthConfig(ConnectorConfig.Auth resolved) {
    return AuthConfig.newBuilder()
        .setScheme(resolved.scheme() == null ? "" : resolved.scheme())
        .putAllProperties(resolved.props())
        .putAllHeaderHints(resolved.headerHints())
        .build();
  }

  private ConnectorConfig resolveCredentials(Connector connector) {
    ConnectorConfig base = ConnectorConfigMapper.fromProto(connector);
    AuthConfig auth = connector == null ? AuthConfig.getDefaultInstance() : connector.getAuth();
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (connector == null
        || !connector.hasResourceId()
        || auth.getScheme().isBlank()
        || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connector.getResourceId().getAccountId(), connector.getResourceId().getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private static CommitLeasedFileGroupResultRequest.Failure failurePayload(
      String resultId, String message) {
    return CommitLeasedFileGroupResultRequest.Failure.newBuilder()
        .setResultId(resultId)
        .setMessage(message == null ? "" : message)
        .build();
  }

  private ReconcileJobStore.LeasedJob requireLeasedFileGroupJob(
      String corr, String jobId, String leaseEpoch) {
    boolean renewed = jobs.renewLease(jobId, leaseEpoch);
    if (!renewed) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile lease is no longer valid")
          .asRuntimeException();
    }
    ReconcileJobStore.ReconcileJob job =
        jobs.getLeaseView(jobId)
            .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("job_id", jobId)));
    if (job.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
      throw Status.FAILED_PRECONDITION
          .withDescription("reconcile job is not an EXEC_FILE_GROUP job")
          .asRuntimeException();
    }
    if (!isActiveLeasedState(job.state)) {
      throw Status.FAILED_PRECONDITION
          .withDescription(
              "reconcile job is no longer active for lease "
                  + jobId
                  + " state="
                  + (job.state == null ? "" : job.state))
          .asRuntimeException();
    }
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode == null
            ? ReconcilerService.CaptureMode.METADATA_AND_CAPTURE
            : job.captureMode,
        job.scope,
        job.executionPolicy,
        leaseEpoch,
        "",
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
  }

  private static String resultIdempotencyKey(String jobId, String resultId) {
    return (jobId == null ? "" : jobId.trim()) + ":" + resultId;
  }

  private static String requireResultId(String resultId) {
    if (resultId == null || resultId.isBlank()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("result_id is required for file-group result submission")
          .asRuntimeException();
    }
    return resultId.trim();
  }

  private ReconcileFileGroupTask resolvePlannedTask(ReconcileJobStore.LeasedJob lease) {
    ReconcileFileGroupTask task =
        lease == null || lease.fileGroupTask == null
            ? ReconcileFileGroupTask.empty()
            : lease.fileGroupTask;
    if (jobs == null
        || lease == null
        || lease.parentJobId == null
        || lease.parentJobId.isBlank()
        || lease.accountId == null
        || lease.accountId.isBlank()) {
      throw unresolvedPlannedTask();
    }
    return jobs.get(lease.accountId, lease.parentJobId)
        .map(parent -> parent.snapshotTask)
        .filter(snapshotTask -> snapshotTask != null && !snapshotTask.isEmpty())
        .flatMap(snapshotTask -> resolveFromParentSnapshotTask(snapshotTask, task))
        .orElseThrow(this::unresolvedPlannedTask);
  }

  private static java.util.Optional<ReconcileFileGroupTask> resolveFromParentSnapshotTask(
      ReconcileSnapshotTask snapshotTask, ReconcileFileGroupTask task) {
    if (snapshotTask == null || snapshotTask.isEmpty() || task == null || task.isEmpty()) {
      return java.util.Optional.empty();
    }
    return snapshotTask.fileGroups().stream()
        .filter(group -> group != null && !group.isEmpty())
        .filter(group -> group.groupId().equals(task.groupId()))
        .filter(group -> group.planId().equals(task.planId()))
        .findFirst();
  }

  private StatusRuntimeException unresolvedPlannedTask() {
    return Status.FAILED_PRECONDITION
        .withDescription("planned file group could not be resolved from parent snapshot plan")
        .asRuntimeException();
  }

  private static boolean isActiveLeasedState(String state) {
    return "JS_RUNNING".equals(state) || "JS_CANCELLING".equals(state);
  }
}
