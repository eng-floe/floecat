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

import static ai.floedb.floecat.reconciler.util.NameParts.split;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationTableMetadata;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationViewMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ApplicationScoped
public class ReconcilerService {
  public static final String CONNECTOR_MODE_PROPERTY = "floecat.connector.mode";
  public static final String CONNECTOR_MODE_CAPTURE_ONLY = "capture-only";

  public enum CaptureMode {
    METADATA_ONLY,
    METADATA_AND_CAPTURE,
    CAPTURE_ONLY
  }

  @Inject ReconcilerBackend backend;
  @Inject CredentialResolver credentialResolver;
  @Inject ServerSideStorageConfigResolver serverSideStorageConfigResolver;

  /** Opens a connector from a resolved configuration. */
  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config);
  }

  // Package-private; replaced in tests to avoid going through ConnectorFactory's ServiceLoader.
  ConnectorOpener connectorOpener = ConnectorFactory::create;

  public List<ReconcileTableTask> planTableTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      String bearerToken) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    validateScopeCombinations(scope, null, false)
        .ifPresent(
            reason -> {
              throw new IllegalArgumentException(reason);
            });
    if (scope.hasViewFilter()) {
      throw new IllegalArgumentException(
          "table planning cannot be combined with destination view id scope");
    }
    validateScopedCaptureRequests(scope)
        .ifPresent(
            reason -> {
              throw new IllegalArgumentException(reason);
            });
    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));

    ActiveConnector active = activeConnectorForResult(ctx, connectorId);
    SourceSelector source = active.source();
    DestinationTarget dest = active.destination();

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      if (scope.hasTableFilter()) {
        return List.of(
            planStrictTableTask(ctx, connectorId, scope.destinationTableId(), connector));
      }
      if (!source.hasNamespace() || source.getNamespace().getSegmentsList().isEmpty()) {
        throw new IllegalArgumentException("connector.source.namespace is required");
      }

      ResourceId destCatalogId = dest.getCatalogId();
      String sourceNsFq = fq(source.getNamespace().getSegmentsList());
      String destNsFq;
      String tableDisplayHint;
      Optional<ResourceId> destNamespaceId;
      destNsFq =
          dest.hasNamespaceId()
              ? resolveNamespaceFq(ctx, dest.getNamespaceId())
              : (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
                  ? fq(dest.getNamespace().getSegmentsList())
                  : sourceNsFq;
      tableDisplayHint =
          dest.getTableDisplayName() == null || dest.getTableDisplayName().isBlank()
              ? null
              : dest.getTableDisplayName();
      destNamespaceId = lookupDestinationNamespaceId(ctx, destCatalogId, dest, destNsFq);

      if (dest.hasTableId() && (source.getTable() == null || source.getTable().isBlank())) {
        throw new IllegalArgumentException(
            "Pinned destination table id requires connector.source.table; "
                + "namespace discovery connectors must not set destination.tableId");
      }

      List<FloecatConnector.PlannedTableTask> planned =
          connector
              .planTableTasks(
                  new FloecatConnector.TablePlanningRequest(
                      sourceNsFq,
                      source.getTable(),
                      destNsFq,
                      tableDisplayHint,
                      destinationNamespacePlanningPaths(destNsFq),
                      null))
              .stream()
              .filter(task -> matchesPlannedNamespaceScope(destNamespaceId.orElse(null), scope))
              .toList();
      if (dest.hasTableId()) {
        return List.of(pinnedDestinationTableTask(dest.getTableId(), planned));
      }
      return planned.stream()
          .map(
              task ->
                  ReconcileTableTask.discovery(
                      task.sourceNamespaceFq(),
                      task.sourceTable(),
                      destNamespaceId.map(ResourceId::getId).orElse(""),
                      lookupDestinationTableIdByName(
                              ctx,
                              destCatalogId,
                              destNamespaceId.orElse(null),
                              destNsFq,
                              task.destinationTableDisplayName())
                          .map(ResourceId::getId)
                          .orElse(null),
                      task.destinationTableDisplayName()))
          .toList();
    } catch (RuntimeException e) {
      throw classifyConnectorPlanningFailure(e);
    } catch (Exception e) {
      throw classifyConnectorPlanningFailure(
          new RuntimeException(
              "Failed to plan reconcile tasks for connector " + connectorId.getId(), e));
    }
  }

  /** Plans either strict destination-view-id work or namespace discovery view tasks. */
  public List<ReconcileViewTask> planViewTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      String bearerToken) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    validateScopeCombinations(scope, null, false)
        .ifPresent(
            reason -> {
              throw new IllegalArgumentException(reason);
            });
    if (scope.hasTableFilter()) {
      throw new IllegalArgumentException(
          "view planning cannot be combined with destination table id scope");
    }
    validateScopedCaptureRequests(scope)
        .ifPresent(
            reason -> {
              throw new IllegalArgumentException(reason);
            });
    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));

    ActiveConnector active = activeConnectorForResult(ctx, connectorId);

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      if (scope.hasViewFilter()) {
        return List.of(planStrictViewTask(ctx, connectorId, scope.destinationViewId(), connector));
      }
      SourceSelector source = active.source();
      DestinationTarget dest = active.destination();
      if (!source.hasNamespace() || source.getNamespace().getSegmentsList().isEmpty()) {
        throw new IllegalArgumentException("connector.source.namespace is required");
      }
      ResourceId destCatalogId = dest.getCatalogId();
      String sourceNsFq = fq(source.getNamespace().getSegmentsList());
      String destNsFq =
          dest.hasNamespaceId()
              ? resolveNamespaceFq(ctx, dest.getNamespaceId())
              : (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
                  ? fq(dest.getNamespace().getSegmentsList())
                  : sourceNsFq;
      Optional<ResourceId> destNamespaceId =
          lookupDestinationNamespaceId(ctx, destCatalogId, dest, destNsFq);
      if (!matchesPlannedNamespaceScope(destNamespaceId.orElse(null), scope)) {
        return List.of();
      }
      return connector
          .planViewTasks(
              new FloecatConnector.ViewPlanningRequest(
                  sourceNsFq, destNsFq, destinationNamespacePlanningPaths(destNsFq)))
          .stream()
          .map(
              task ->
                  ReconcileViewTask.discovery(
                      task.sourceNamespaceFq(),
                      task.sourceView(),
                      destNamespaceId.map(ResourceId::getId).orElse(""),
                      lookupDestinationViewIdByName(
                              ctx,
                              destCatalogId,
                              destNamespaceId.orElse(null),
                              destNsFq,
                              task.destinationViewDisplayName())
                          .map(ResourceId::getId)
                          .orElse(null),
                      task.destinationViewDisplayName()))
          .toList();
    } catch (RuntimeException e) {
      throw classifyConnectorPlanningFailure(e);
    } catch (Exception e) {
      throw classifyConnectorPlanningFailure(
          new RuntimeException(
              "Failed to plan reconcile view tasks for connector " + connectorId.getId(), e));
    }
  }

  public List<ReconcileSnapshotTask> planSnapshotTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      ReconcileTableTask tableTask,
      CaptureMode captureMode,
      String bearerToken) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    ReconcileTableTask effectiveTask = tableTask == null ? ReconcileTableTask.empty() : tableTask;
    if (effectiveTask.isEmpty()
        || effectiveTask.sourceNamespace().isBlank()
        || effectiveTask.sourceTable().isBlank()
        || effectiveTask.destinationTableId() == null
        || effectiveTask.destinationTableId().isBlank()) {
      return List.of();
    }

    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    ActiveConnector active = activeConnectorForResult(ctx, connectorId);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId(effectiveTask.destinationTableId())
            .build();
    Set<Long> targetSnapshotIds =
        scope.destinationCaptureRequests().stream()
            .filter(request -> request != null)
            .filter(request -> tableId.getId().equals(request.tableId()))
            .map(ReconcileScope.ScopedCaptureRequest::snapshotId)
            .filter(snapshotId -> snapshotId >= 0)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    Set<Long> knownSnapshotIds = fullRescan ? Set.of() : backend.existingSnapshotIds(ctx, tableId);
    Set<String> defaultColumnSelectors = normalizeSelectors(active.source().getColumnsList());
    ReconcileCapturePolicy capturePolicy = effectiveCapturePolicy(scope, captureMode);
    // Capture completeness is evaluated in planSnapshotTasks(), which is responsible for
    // enqueuing follow-up snapshot work. Direct table execution here is metadata-only.
    Set<Long> enumerationKnownSnapshotIds =
        enumerationKnownSnapshotIds(
            ctx,
            tableId,
            fullRescan,
            knownSnapshotIds,
            capturePolicy,
            scope.destinationCaptureRequests().stream()
                .filter(request -> request != null && tableId.getId().equals(request.tableId()))
                .collect(
                    Collectors.groupingBy(
                        ReconcileScope.ScopedCaptureRequest::snapshotId,
                        LinkedHashMap::new,
                        Collectors.toList())),
            defaultColumnSelectors);

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      List<FloecatConnector.SnapshotBundle> bundles =
          connector.enumerateSnapshots(
              effectiveTask.sourceNamespace(),
              effectiveTask.sourceTable(),
              tableId,
              fullRescan
                  ? FloecatConnector.SnapshotEnumerationOptions.full(true, targetSnapshotIds)
                  : FloecatConnector.SnapshotEnumerationOptions.incremental(
                      enumerationKnownSnapshotIds, targetSnapshotIds));
      if (bundles == null || bundles.isEmpty()) {
        return List.of();
      }
      return bundles.stream()
          .filter(bundle -> bundle != null && bundle.snapshotId() >= 0)
          .map(
              bundle ->
                  ReconcileSnapshotTask.of(
                      effectiveTask.destinationTableId(),
                      bundle.snapshotId(),
                      effectiveTask.sourceNamespace(),
                      effectiveTask.sourceTable()))
          .distinct()
          .toList();
    } catch (RuntimeException e) {
      throw classifyConnectorPlanningFailure(e);
    } catch (Exception e) {
      throw classifyConnectorPlanningFailure(
          new RuntimeException(
              "Failed to plan snapshot tasks for connector " + connectorId.getId(), e));
    }
  }

  static Optional<String> validateScopeCombinations(
      ReconcileScope scope, CaptureMode captureMode, boolean viewsOnly) {
    if (scope == null) {
      return Optional.empty();
    }
    if (scope.hasTableFilter() && scope.hasViewFilter()) {
      return Optional.of("destinationTableId cannot be combined with destinationViewId");
    }
    if (scope.hasTableFilter() && scope.hasNamespaceFilter()) {
      return Optional.of("destinationTableId cannot be combined with destinationNamespaceIds");
    }
    if (scope.hasViewFilter() && scope.hasNamespaceFilter()) {
      return Optional.of("destinationViewId cannot be combined with destinationNamespaceIds");
    }
    if (viewsOnly && scope.hasTableFilter()) {
      return Optional.of("views-only reconcile cannot be combined with destination table id scope");
    }
    if (!includesMetadata(captureMode) && scope.hasViewFilter()) {
      return Optional.of("capture-only reconcile is not valid for view reconcile");
    }
    return Optional.empty();
  }

  static Set<Long> knownSnapshotIdsForEnumeration(
      boolean fullRescan,
      boolean includeStats,
      Set<Long> knownSnapshotIds,
      Predicate<Long> statsAlreadyCaptured) {
    if (fullRescan || knownSnapshotIds == null || knownSnapshotIds.isEmpty()) {
      return Set.of();
    }
    if (!includeStats) {
      return Set.copyOf(knownSnapshotIds);
    }
    if (statsAlreadyCaptured == null) {
      return Set.of();
    }
    Set<Long> fullyCaptured = new LinkedHashSet<>();
    for (Long snapshotId : knownSnapshotIds) {
      if (snapshotId == null || snapshotId < 0) {
        continue;
      }
      if (statsAlreadyCaptured.test(snapshotId)) {
        fullyCaptured.add(snapshotId);
      }
    }
    if (fullyCaptured.isEmpty()) {
      return Set.of();
    }
    return Set.copyOf(fullyCaptured);
  }

  static ReconcileCapturePolicy effectiveCapturePolicy(
      ReconcileScope scope, CaptureMode captureMode) {
    if (scope != null && scope.hasCapturePolicy()) {
      return scope.capturePolicy();
    }
    if (captureMode == CaptureMode.METADATA_ONLY) {
      return ReconcileCapturePolicy.empty();
    }
    throw new IllegalArgumentException("capture policy is required for capture reconcile modes");
  }

  static boolean includesMetadata(CaptureMode captureMode) {
    return captureMode != CaptureMode.CAPTURE_ONLY;
  }

  static boolean isCaptureOnlyConnector(Connector connector) {
    if (connector == null) {
      return false;
    }
    String mode = connector.getPropertiesMap().get(CONNECTOR_MODE_PROPERTY);
    return CONNECTOR_MODE_CAPTURE_ONLY.equalsIgnoreCase(mode);
  }

  static boolean allowsTableMetadataMutation(Connector connector, CaptureMode captureMode) {
    return includesMetadata(captureMode) && !isCaptureOnlyConnector(connector);
  }

  Set<Long> enumerationKnownSnapshotIds(
      ReconcileContext ctx,
      ResourceId tableId,
      boolean fullRescan,
      Set<Long> knownSnapshotIds,
      ReconcileCapturePolicy capturePolicy,
      Map<Long, List<ReconcileScope.ScopedCaptureRequest>> scopedCaptureRequestsBySnapshot,
      Set<String> defaultColumnSelectors) {
    if (capturePolicy == null || capturePolicy.outputs().isEmpty()) {
      return knownSnapshotIdsForEnumeration(fullRescan, false, knownSnapshotIds, null);
    }
    return knownSnapshotIdsForEnumeration(
        fullRescan,
        capturePolicy.requestsStats() || capturePolicy.requestsIndexes(),
        knownSnapshotIds,
        snapshotId -> {
          List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests =
              scopedCaptureRequestsBySnapshot.getOrDefault(snapshotId, List.of());
          boolean statsComplete =
              !capturePolicy.requestsStats()
                  || isStatsCaptureCompleteForScope(
                      ctx,
                      tableId,
                      snapshotId,
                      capturePolicy,
                      scopedCaptureRequests,
                      defaultColumnSelectors);
          boolean indexesComplete =
              !capturePolicy.requestsIndexes()
                  || isIndexCaptureCompleteForScope(
                      ctx, tableId, snapshotId, capturePolicy, scopedCaptureRequests);
          return statsComplete && indexesComplete;
        });
  }

  private boolean isStatsCaptureCompleteForScope(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      ReconcileCapturePolicy capturePolicy,
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests,
      Set<String> defaultColumnSelectors) {
    boolean emptySnapshotMarkerPresent =
        backend.hasZeroDataFileTableStats(ctx, tableId, snapshotId);
    boolean requiresTableStats =
        capturePolicy != null
            && capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.TABLE_STATS);
    boolean requiresFileStats =
        capturePolicy != null
            && capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.FILE_STATS);
    boolean requiresColumnStats =
        capturePolicy != null
            && capturePolicy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS);
    if (scopedCaptureRequests != null && !scopedCaptureRequests.isEmpty()) {
      Set<String> scopedSelectors =
          requiresColumnStats ? selectorsForScopedTableRequests(scopedCaptureRequests) : Set.of();
      if (matchesDefaultTableWideScopedCapture(scopedCaptureRequests, defaultColumnSelectors)) {
        if (requiresTableStats
            && !backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, snapshotId, StatsTargetKind.STK_TABLE)) {
          return false;
        }
        if (requiresFileStats
            && !backend.statsAlreadyCapturedForTargetKind(
                ctx, tableId, snapshotId, StatsTargetKind.STK_FILE)
            && !emptySnapshotMarkerPresent) {
          return false;
        }
        return !requiresColumnStats
            || scopedSelectors.isEmpty()
            || emptySnapshotMarkerPresent
            || backend.statsCapturedForColumnSelectors(ctx, tableId, snapshotId, scopedSelectors);
      }
      Set<StatsTarget> scopedTargets = decodeTargetSpecsFromRequests(scopedCaptureRequests);
      if (!scopedTargets.isEmpty()
          && !backend.statsCapturedForTargets(ctx, tableId, snapshotId, scopedTargets)) {
        return false;
      }
      if (requiresFileStats
          && !backend.statsAlreadyCapturedForTargetKind(
              ctx, tableId, snapshotId, StatsTargetKind.STK_FILE)
          && !emptySnapshotMarkerPresent) {
        return false;
      }
      return !requiresColumnStats
          || scopedSelectors.isEmpty()
          || emptySnapshotMarkerPresent
          || backend.statsCapturedForColumnSelectors(ctx, tableId, snapshotId, scopedSelectors);
    }
    if (requiresTableStats
        && !backend.statsAlreadyCapturedForTargetKind(
            ctx, tableId, snapshotId, StatsTargetKind.STK_TABLE)) {
      return false;
    }
    if (requiresFileStats
        && !backend.statsAlreadyCapturedForTargetKind(
            ctx, tableId, snapshotId, StatsTargetKind.STK_FILE)
        && !emptySnapshotMarkerPresent) {
      return false;
    }
    return !requiresColumnStats
        || defaultColumnSelectors == null
        || defaultColumnSelectors.isEmpty()
        || emptySnapshotMarkerPresent
        || backend.statsCapturedForColumnSelectors(
            ctx, tableId, snapshotId, defaultColumnSelectors);
  }

  private boolean isIndexCaptureCompleteForScope(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      ReconcileCapturePolicy capturePolicy,
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests) {
    Set<String> requestedSelectors =
        capturePolicy == null ? Set.of() : capturePolicy.selectorsForIndex();
    if (!supportsIndexCompletenessCheck(scopedCaptureRequests)) {
      return false;
    }
    List<String> parquetFilePaths =
        backend
            .fetchSnapshotFilePlan(ctx, tableId, snapshotId)
            .map(
                plan ->
                    java.util.stream.Stream.concat(
                            plan.dataFiles().stream(), plan.deleteFiles().stream())
                        .filter(file -> file != null && isParquetSnapshotFile(file))
                        .map(FloecatConnector.SnapshotFileEntry::filePath)
                        .filter(path -> path != null && !path.isBlank())
                        .distinct()
                        .toList())
            .orElse(null);
    if (parquetFilePaths == null) {
      return false;
    }
    return backend.indexArtifactsCapturedForFilePaths(
        ctx, tableId, snapshotId, parquetFilePaths, requestedSelectors);
  }

  private boolean supportsIndexCompletenessCheck(
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests) {
    if (scopedCaptureRequests == null || scopedCaptureRequests.isEmpty()) {
      return true;
    }
    for (ReconcileScope.ScopedCaptureRequest request : scopedCaptureRequests) {
      Optional<StatsTarget> decoded =
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec());
      if (decoded.isEmpty() || decoded.get().getTargetCase() != StatsTarget.TargetCase.TABLE) {
        return false;
      }
    }
    return true;
  }

  private static boolean isParquetSnapshotFile(FloecatConnector.SnapshotFileEntry file) {
    String format = file.fileFormat() == null ? "" : file.fileFormat().trim();
    if ("PARQUET".equalsIgnoreCase(format)) {
      return true;
    }
    String path = file.filePath() == null ? "" : file.filePath().toLowerCase(java.util.Locale.ROOT);
    return path.endsWith(".parquet") || path.endsWith(".parq");
  }

  private Set<StatsTarget> decodeTargetSpecsFromRequests(
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests) {
    if (scopedCaptureRequests == null || scopedCaptureRequests.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<StatsTarget> decodedTargets = new LinkedHashSet<>();
    for (ReconcileScope.ScopedCaptureRequest request : scopedCaptureRequests) {
      decodedTargets.add(
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
              .orElseThrow(
                  () ->
                      terminalValidation(
                          "Invalid scoped capture target spec for table="
                              + request.tableId()
                              + " snapshot="
                              + request.snapshotId()
                              + " spec="
                              + request.targetSpec())));
    }
    return decodedTargets;
  }

  private static boolean matchesDefaultTableWideScopedCapture(
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests,
      Set<String> defaultColumnSelectors) {
    if (scopedCaptureRequests == null || scopedCaptureRequests.isEmpty()) {
      return false;
    }
    for (ReconcileScope.ScopedCaptureRequest request : scopedCaptureRequests) {
      Optional<StatsTarget> decodedTarget =
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec());
      if (decodedTarget.isEmpty()
          || decodedTarget.get().getTargetCase() != StatsTarget.TargetCase.TABLE) {
        return false;
      }
    }
    return selectorsForScopedTableRequests(scopedCaptureRequests)
        .equals(defaultColumnSelectors == null ? Set.of() : defaultColumnSelectors);
  }

  String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
    return backend.resolveNamespaceFq(ctx, namespaceId);
  }

  private ReconcileTableTask planStrictTableTask(
      ReconcileContext ctx,
      ResourceId connectorId,
      String destinationTableId,
      FloecatConnector connector) {
    ResourceId tableId =
        destinationResourceId(connectorId, ResourceKind.RK_TABLE, destinationTableId);
    DestinationTableMetadata metadata = requiredTableMetadata(ctx, tableId);
    SourceBinding sourceBinding = sourceBinding(metadata);
    if (!sourceBinding.hasSourceIdentity()) {
      throw terminalValidation(
          "Destination table id " + destinationTableId + " is missing persisted source identity");
    }
    validateSourceConnector(metadata.sourceConnectorId(), connectorId, "destination table id");
    try {
      FloecatConnector.TableDescriptor ignored =
          connector.describe(sourceBinding.namespace(), sourceBinding.name());
      if (ignored == null) {
        throw new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            "Table not found: " + sourceBinding.namespace() + "." + sourceBinding.name(),
            null);
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      if (isMissingObjectFailure(e)) {
        throw new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            "Table not found: " + sourceBinding.namespace() + "." + sourceBinding.name(),
            e);
      }
      throw e;
    }
    return ReconcileTableTask.of(
        sourceBinding.namespace(),
        sourceBinding.name(),
        destinationTableId,
        displayNameOrSourceName(metadata.displayName(), sourceBinding.name()));
  }

  private ReconcileViewTask planStrictViewTask(
      ReconcileContext ctx,
      ResourceId connectorId,
      String destinationViewId,
      FloecatConnector connector) {
    ResourceId viewId = destinationResourceId(connectorId, ResourceKind.RK_VIEW, destinationViewId);
    DestinationViewMetadata metadata = requiredViewMetadata(ctx, viewId);
    SourceBinding sourceBinding = sourceBinding(metadata);
    if (!sourceBinding.hasSourceIdentity()) {
      throw terminalValidation(
          "Destination view id " + destinationViewId + " is missing persisted source identity");
    }
    validateSourceConnector(metadata.sourceConnectorId(), connectorId, "destination view id");
    try {
      connector
          .describeView(sourceBinding.namespace(), sourceBinding.name())
          .orElseThrow(
              () ->
                  new ReconcileFailureException(
                      ReconcileExecutor.ExecutionResult.FailureKind.VIEW_MISSING,
                      ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                      "View not found: " + sourceBinding.namespace() + "." + sourceBinding.name(),
                      null));
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (RuntimeException e) {
      if (isMissingObjectFailure(e)) {
        throw new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.VIEW_MISSING,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            "View not found: " + sourceBinding.namespace() + "." + sourceBinding.name(),
            e);
      }
      throw e;
    }
    return ReconcileViewTask.of(
        sourceBinding.namespace(),
        sourceBinding.name(),
        metadata.namespaceId().getId(),
        viewId.getId());
  }

  private DestinationTableMetadata requiredTableMetadata(ReconcileContext ctx, ResourceId tableId) {
    DestinationTableMetadata metadata =
        backend
            .lookupDestinationTableMetadata(ctx, tableId)
            .orElseThrow(
                () ->
                    terminalValidation("Destination table id does not exist: " + tableId.getId()));
    if (metadata.namespaceId() == null || metadata.namespaceId().getId().isBlank()) {
      throw terminalValidation(
          "Destination table namespace cannot be resolved from id: " + tableId.getId());
    }
    if (metadata.catalogId() == null || metadata.catalogId().getId().isBlank()) {
      throw terminalValidation(
          "Destination table catalog cannot be resolved from id: " + tableId.getId());
    }
    return metadata;
  }

  private DestinationViewMetadata requiredViewMetadata(ReconcileContext ctx, ResourceId viewId) {
    DestinationViewMetadata metadata =
        backend
            .lookupDestinationViewMetadata(ctx, viewId)
            .orElseThrow(
                () -> terminalValidation("Destination view id does not exist: " + viewId.getId()));
    if (metadata.namespaceId() == null || metadata.namespaceId().getId().isBlank()) {
      throw terminalValidation(
          "Destination view namespace cannot be resolved from id: " + viewId.getId());
    }
    if (metadata.catalogId() == null || metadata.catalogId().getId().isBlank()) {
      throw terminalValidation(
          "Destination view catalog cannot be resolved from id: " + viewId.getId());
    }
    return metadata;
  }

  private static SourceBinding sourceBinding(DestinationTableMetadata metadata) {
    return new SourceBinding(metadata.sourceNamespace(), metadata.sourceName());
  }

  private static SourceBinding sourceBinding(DestinationViewMetadata metadata) {
    return new SourceBinding(metadata.sourceNamespace(), metadata.sourceName());
  }

  private static void validateSourceConnector(
      ResourceId metadataConnectorId, ResourceId requestedConnectorId, String resourceLabel) {
    if (metadataConnectorId == null || metadataConnectorId.getId().isBlank()) {
      throw terminalValidation("Persisted source connector for " + resourceLabel + " is missing");
    }
    if (requestedConnectorId == null || requestedConnectorId.getId().isBlank()) {
      throw terminalValidation("Requested connector for " + resourceLabel + " is missing");
    }
    if (!metadataConnectorId.getId().equals(requestedConnectorId.getId())) {
      throw terminalValidation(
          "Persisted source connector for "
              + resourceLabel
              + " does not match requested connector");
    }
    if (!metadataConnectorId.getAccountId().isBlank()
        && !requestedConnectorId.getAccountId().isBlank()
        && !metadataConnectorId.getAccountId().equals(requestedConnectorId.getAccountId())) {
      throw terminalValidation(
          "Persisted source connector for "
              + resourceLabel
              + " does not match requested connector");
    }
    if (metadataConnectorId.getKind() != ResourceKind.RK_UNSPECIFIED
        && requestedConnectorId.getKind() != ResourceKind.RK_UNSPECIFIED
        && metadataConnectorId.getKind() != requestedConnectorId.getKind()) {
      throw terminalValidation(
          "Persisted source connector for "
              + resourceLabel
              + " does not match requested connector");
    }
  }

  private static ReconcileFailureException terminalValidation(String message) {
    return terminalValidation(message, null);
  }

  private static ReconcileFailureException terminalValidation(String message, Throwable cause) {
    return new ReconcileFailureException(
        ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        message,
        cause);
  }

  private static RuntimeException classifyConnectorPlanningFailure(RuntimeException error) {
    Exception normalized = ReconcileFailureClassifier.normalize(error);
    return normalized instanceof RuntimeException runtime ? runtime : error;
  }

  static ResourceId destinationResourceId(
      ResourceId connectorId, ResourceKind kind, String destinationId) {
    return ResourceId.newBuilder()
        .setAccountId(connectorId.getAccountId())
        .setKind(kind)
        .setId(destinationId)
        .build();
  }

  private static String displayNameOrSourceName(String displayName, String sourceName) {
    return displayName == null || displayName.isBlank() ? sourceName : displayName;
  }

  static Map<String, String> sourceIdentityProperties(
      ResourceId connectorId, String sourceNamespace, String sourceName) {
    LinkedHashMap<String, String> properties = new LinkedHashMap<>();
    if (sourceNamespace != null && !sourceNamespace.isBlank()) {
      properties.put(ReconcilerBackend.SOURCE_NAMESPACE_PROPERTY, sourceNamespace);
    }
    if (sourceName != null && !sourceName.isBlank()) {
      properties.put(ReconcilerBackend.SOURCE_NAME_PROPERTY, sourceName);
    }
    if (connectorId != null && !connectorId.getId().isBlank()) {
      properties.put(ReconcilerBackend.SOURCE_CONNECTOR_ID_PROPERTY, connectorId.getId());
    }
    return Map.copyOf(properties);
  }

  Optional<ResourceId> lookupDestinationNamespaceId(
      ReconcileContext ctx,
      ResourceId destCatalogId,
      DestinationTarget destination,
      String destNamespaceFq) {
    if (destination != null && destination.hasNamespaceId()) {
      return Optional.of(destination.getNamespaceId());
    }
    if (destCatalogId == null
        || destCatalogId.getId().isBlank()
        || destNamespaceFq == null
        || destNamespaceFq.isBlank()) {
      return Optional.empty();
    }
    String catalogName = backend.lookupCatalogName(ctx, destCatalogId);
    return backend.lookupNamespace(ctx, namespaceNameRef(catalogName, destNamespaceFq));
  }

  private NameRef namespaceNameRef(String catalogName, String namespaceFq) {
    var parts = split(namespaceFq);
    return NameRef.newBuilder()
        .setCatalog(catalogName)
        .addAllPath(parts.parents)
        .setName(parts.leaf)
        .build();
  }

  Optional<ResourceId> lookupDestinationTableIdByName(
      ReconcileContext ctx,
      ResourceId destCatalogId,
      ResourceId destNamespaceId,
      String destNamespaceFq,
      String displayName) {
    if (destCatalogId == null
        || destCatalogId.getId().isBlank()
        || destNamespaceId == null
        || destNamespaceId.getId().isBlank()
        || destNamespaceFq == null
        || destNamespaceFq.isBlank()
        || displayName == null
        || displayName.isBlank()) {
      return Optional.empty();
    }
    try {
      String catalogName = backend.lookupCatalogName(ctx, destCatalogId);
      return backend.lookupTable(ctx, objectNameRef(catalogName, destNamespaceFq, displayName));
    } catch (UnsupportedOperationException ignored) {
      return Optional.empty();
    }
  }

  Optional<ResourceId> lookupDestinationViewIdByName(
      ReconcileContext ctx,
      ResourceId destCatalogId,
      ResourceId destNamespaceId,
      String destNamespaceFq,
      String displayName) {
    if (destCatalogId == null
        || destCatalogId.getId().isBlank()
        || destNamespaceId == null
        || destNamespaceId.getId().isBlank()
        || destNamespaceFq == null
        || destNamespaceFq.isBlank()
        || displayName == null
        || displayName.isBlank()) {
      return Optional.empty();
    }
    try {
      String catalogName = backend.lookupCatalogName(ctx, destCatalogId);
      return backend.lookupView(ctx, objectNameRef(catalogName, destNamespaceFq, displayName));
    } catch (UnsupportedOperationException ignored) {
      return Optional.empty();
    }
  }

  private NameRef objectNameRef(String catalogName, String namespaceFq, String displayName) {
    return NameRef.newBuilder()
        .setCatalog(catalogName)
        .addAllPath(namespacePathSegments(namespaceFq))
        .setName(displayName)
        .build();
  }

  private boolean matchesPlannedNamespaceScope(ResourceId destNamespaceId, ReconcileScope scope) {
    if (scope == null || !scope.hasNamespaceFilter()) {
      return true;
    }
    // Planning is read-only and may run before discovery execution creates the destination
    // namespace. When the configured namespace cannot be resolved yet, allow planning to proceed
    // and let execution enforce the concrete namespace id after resolution.
    if (destNamespaceId == null) {
      return true;
    }
    return scope.matchesNamespaceId(destNamespaceId.getId());
  }

  private ReconcileTableTask pinnedDestinationTableTask(
      ResourceId destinationTableId, List<FloecatConnector.PlannedTableTask> planned) {
    int plannedCount = planned == null ? 0 : planned.size();
    if (destinationTableId == null || destinationTableId.getId().isBlank()) {
      throw new IllegalArgumentException("Pinned destination table id is blank");
    }
    if (plannedCount != 1) {
      throw new IllegalArgumentException(
          "Pinned destination table id requires exactly one planned source table, but found "
              + plannedCount);
    }
    FloecatConnector.PlannedTableTask task = planned.getFirst();
    return ReconcileTableTask.of(
        task.sourceNamespaceFq(),
        task.sourceTable(),
        destinationTableId.getId(),
        task.destinationTableDisplayName());
  }

  private static boolean isMissingObjectFailure(Throwable t) {
    if (t == null) {
      return false;
    }
    String className = t.getClass().getName();
    if (className.endsWith("NoSuchTableException")
        || className.endsWith("NoSuchViewException")
        || className.endsWith("NoSuchObjectException")
        || className.endsWith("NotFoundException")) {
      return true;
    }
    String message = t.getMessage();
    if (message == null || message.isBlank()) {
      return false;
    }
    String normalized = message.toLowerCase();
    return normalized.contains("http 404")
        || normalized.contains("status 404")
        || normalized.contains("not found")
        || normalized.contains("does not exist");
  }

  private static String fq(List<String> segments) {
    return String.join(".", segments);
  }

  private static List<List<String>> destinationNamespacePlanningPaths(String namespaceFq) {
    if (namespaceFq == null || namespaceFq.isBlank()) {
      return List.of();
    }
    return List.of(namespacePathSegments(namespaceFq));
  }

  static List<String> namespacePathSegments(String namespaceFq) {
    var parts = split(namespaceFq);
    if (parts.leaf == null || parts.leaf.isBlank()) {
      return parts.parents;
    }
    ArrayList<String> segments = new ArrayList<>(parts.parents.size() + 1);
    segments.addAll(parts.parents);
    segments.add(parts.leaf);
    return List.copyOf(segments);
  }

  private ActiveConnector activeConnector(
      ReconcileContext ctx, Connector connector, ResourceId connectorId) {
    if (connector.getState() != ConnectorState.CS_ACTIVE) {
      throw new IllegalStateException("Connector not ACTIVE: " + connectorId.getId());
    }
    ConnectorConfig config = ConnectorConfigMapper.fromProto(connector);
    ConnectorConfig resolved =
        resolveServerSideStorage(
            ctx, connector, resolveCredentials(config, connector.getAuth(), connectorId));
    return new ActiveConnector(
        connector,
        connector.hasSource() ? connector.getSource() : SourceSelector.getDefaultInstance(),
        connector.hasDestination()
            ? connector.getDestination()
            : DestinationTarget.getDefaultInstance(),
        config,
        resolved);
  }

  ActiveConnector activeConnectorForResult(ReconcileContext ctx, ResourceId connectorId) {
    Connector connector;
    try {
      connector = backend.lookupConnector(ctx, connectorId);
    } catch (RuntimeException e) {
      throw new ReconcileFailureException(
          ExecutionResult.FailureKind.CONNECTOR_MISSING,
          "getConnector failed: " + connectorId.getId(),
          e);
    }
    return activeConnector(ctx, connector, connectorId);
  }

  ReconcileContext buildContext(PrincipalContext principal, Optional<String> bearerToken) {
    String correlationId = principal.getCorrelationId();
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    String source = principal.getSubject();
    if (source == null || source.isBlank()) {
      source = "reconciler-service";
    }
    return new ReconcileContext(correlationId, principal, source, Instant.now(), bearerToken);
  }

  static Set<String> normalizeSelectors(List<String> in) {
    if (in == null || in.isEmpty()) {
      return Set.of();
    }

    var out = new LinkedHashSet<String>();
    for (var s : in) {
      if (s == null) {
        continue;
      }

      var t = s.trim();
      if (t.isEmpty()) {
        continue;
      }

      out.add(t.startsWith("#") ? "#" + t.substring(1).trim() : t);
    }
    return out;
  }

  static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  static Map<String, Map<Long, List<ReconcileScope.ScopedCaptureRequest>>>
      indexScopedCaptureRequestsByTableScope(
          List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests) {
    if (scopedCaptureRequests == null || scopedCaptureRequests.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<Long, List<ReconcileScope.ScopedCaptureRequest>>> out = new LinkedHashMap<>();
    for (ReconcileScope.ScopedCaptureRequest request : scopedCaptureRequests) {
      out.computeIfAbsent(request.tableId(), ignored -> new LinkedHashMap<>())
          .computeIfAbsent(request.snapshotId(), ignored -> new ArrayList<>())
          .add(request);
    }
    return out;
  }

  private Optional<String> validateScopedCaptureRequests(ReconcileScope scope) {
    if (scope == null) {
      return Optional.empty();
    }
    if (!scope.hasCaptureRequestFilter()) {
      return Optional.empty();
    }
    if (scope.capturePolicy() == null || scope.capturePolicy().outputs().isEmpty()) {
      return Optional.of(
          "Scoped capture requests require explicit capture_policy.outputs; no output inference is"
              + " performed");
    }
    for (ReconcileScope.ScopedCaptureRequest request : scope.destinationCaptureRequests()) {
      if (request.snapshotId() < 0L) {
        return Optional.of(
            "Scoped capture request has invalid snapshot id for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId());
      }
      if (request.tableId().isBlank()) {
        return Optional.of(
            "Scoped capture request is missing table id for snapshot=" + request.snapshotId());
      }
      if (request.targetSpec().isBlank()) {
        return Optional.of(
            "Scoped capture request is missing target spec for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId());
      }
      if (ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
          .isEmpty()) {
        return Optional.of(
            "Scoped capture request has invalid target spec for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId()
                + " spec="
                + request.targetSpec());
      }
      StatsTarget decodedTarget =
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
              .orElseThrow();
      if (decodedTarget.getTargetCase() == StatsTarget.TargetCase.FILE) {
        return Optional.of(
            "Scoped capture request file targets are not supported; reconcile execution is"
                + " file-group scoped for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId()
                + " spec="
                + request.targetSpec());
      }
      if (decodedTarget.getTargetCase() == StatsTarget.TargetCase.EXPRESSION) {
        return Optional.of(
            "Scoped capture request expression targets are recognized but not yet implemented in"
                + " unified capture; reconcile execution remains file-group scoped for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId()
                + " spec="
                + request.targetSpec());
      }
    }
    return Optional.empty();
  }

  private static Set<String> selectorsForScopedTableRequests(
      List<ReconcileScope.ScopedCaptureRequest> scopedCaptureRequests) {
    if (scopedCaptureRequests == null || scopedCaptureRequests.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> selectors = new LinkedHashSet<>();
    for (ReconcileScope.ScopedCaptureRequest request : scopedCaptureRequests) {
      Optional<StatsTarget> decodedTarget =
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec());
      if (decodedTarget.isEmpty()
          || decodedTarget.get().getTargetCase() != StatsTarget.TargetCase.TABLE) {
        continue;
      }
      selectors.addAll(normalizeSelectors(request.columnSelectors()));
    }
    return selectors;
  }

  private ConnectorConfig resolveCredentials(
      ConnectorConfig base,
      ai.floedb.floecat.connector.rpc.AuthConfig auth,
      ResourceId connectorId) {
    if (auth == null) {
      return base;
    }
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    var credential = credentialResolver.resolve(connectorId.getAccountId(), connectorId.getId());
    return credential
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private ConnectorConfig resolveServerSideStorage(
      ReconcileContext ctx, Connector connector, ConnectorConfig config) {
    if (serverSideStorageConfigResolver == null) {
      return config;
    }
    return serverSideStorageConfigResolver.resolve(Optional.of(ctx), connector, config);
  }

  record ActiveConnector(
      Connector connector,
      SourceSelector source,
      DestinationTarget destination,
      ConnectorConfig config,
      ConnectorConfig resolvedConfig) {}

  private record SourceBinding(String namespace, String name) {
    private SourceBinding {
      namespace = namespace == null ? "" : namespace.trim();
      name = name == null ? "" : name.trim();
    }

    private boolean hasSourceIdentity() {
      return !namespace.isBlank() && !name.isBlank();
    }
  }
}
