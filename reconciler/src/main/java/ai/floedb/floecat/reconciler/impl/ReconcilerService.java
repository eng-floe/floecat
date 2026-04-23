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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationTableMetadata;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.DestinationViewMetadata;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcilerService {
  private static final Logger LOG = Logger.getLogger(ReconcilerService.class);
  private static final BooleanSupplier NO_CANCEL = () -> false;
  private static final ProgressListener NO_PROGRESS = (ts, tc, vs, vc, e, sp, stp, m) -> {};

  public enum CaptureMode {
    METADATA_ONLY,
    METADATA_AND_STATS,
    STATS_ONLY
  }

  @FunctionalInterface
  public interface ProgressListener {
    void onProgress(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message);
  }

  @Inject ReconcilerBackend backend;
  @Inject LogicalSchemaMapper schemaMapper;
  @Inject CredentialResolver credentialResolver;
  @Inject Instance<StatsCaptureControlPlane> statsCaptureControlPlane;
  @Inject Instance<ReconcileJobStore> reconcileJobStore;

  /** Opens a connector from a resolved configuration. */
  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config);
  }

  // Package-private; replaced in tests to avoid going through ConnectorFactory's ServiceLoader.
  ConnectorOpener connectorOpener = ConnectorFactory::create;

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn) {
    return reconcileWithDefaults(
        principal, connectorId, fullRescan, scopeIn, CaptureMode.METADATA_AND_STATS, null);
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode) {
    return reconcileWithDefaults(principal, connectorId, fullRescan, scopeIn, captureMode, null);
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode,
      String bearerToken) {
    return reconcileWithDefaults(
        principal, connectorId, fullRescan, scopeIn, captureMode, bearerToken);
  }

  private Result reconcileWithDefaults(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode,
      String bearerToken) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidScope = validateScopeCombinations(scope, captureMode, false);
    if (invalidScope.isPresent()) {
      return new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()));
    }
    if (scope.hasViewFilter()) {
      List<ReconcileViewTask> viewTasks;
      try {
        viewTasks = planViewTasks(principal, connectorId, scope, bearerToken);
      } catch (RuntimeException e) {
        return planningFailure(e);
      }
      if (viewTasks.isEmpty()) {
        return new Result(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException("No views matched scope: " + scope.destinationViewId()));
      }
      return reconcileView(
          principal, connectorId, scope, viewTasks.getFirst(), bearerToken, NO_CANCEL, NO_PROGRESS);
    }
    if (scope.hasTableFilter()) {
      List<ReconcileTableTask> tableTasks;
      try {
        tableTasks = planTableTasks(principal, connectorId, scope, bearerToken);
      } catch (RuntimeException e) {
        return planningFailure(e);
      }
      if (tableTasks.isEmpty()) {
        return new Result(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException("No tables matched scope: " + scope.destinationTableId()));
      }
      if (tableTasks.size() > 1) {
        return new Result(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException(
                "Multiple tables matched scope: " + scope.destinationTableId()));
      }
      return reconcileSingleTableTask(
          principal,
          connectorId,
          fullRescan,
          scope,
          tableTasks.getFirst(),
          captureMode,
          bearerToken,
          NO_CANCEL,
          NO_PROGRESS);
    }
    List<ReconcileTableTask> tableTasks;
    try {
      tableTasks = planTableTasks(principal, connectorId, scope, bearerToken);
    } catch (RuntimeException e) {
      return planningFailure(e);
    }
    Result tableResult =
        executePlannedTableTasks(
            principal,
            connectorId,
            fullRescan,
            scope,
            tableTasks,
            captureMode,
            bearerToken,
            NO_CANCEL,
            NO_PROGRESS);
    if (tableResult.cancelled()) {
      return tableResult;
    }
    if (captureMode == CaptureMode.STATS_ONLY) {
      return tableResult;
    }
    List<ReconcileViewTask> viewTasks;
    try {
      viewTasks = planViewTasks(principal, connectorId, scope, bearerToken);
    } catch (RuntimeException e) {
      return combineResults(List.of(tableResult, planningFailure(e)));
    }
    Result viewResult =
        executePlannedViewTasks(
            principal, connectorId, scope, viewTasks, bearerToken, NO_CANCEL, NO_PROGRESS);
    return combineResults(List.of(tableResult, viewResult));
  }

  public Result reconcileViewsOnly(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidScope =
        validateScopeCombinations(scope, CaptureMode.METADATA_ONLY, true);
    if (invalidScope.isPresent()) {
      return new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()));
    }
    return planAndExecuteViewTasks(
        principal, connectorId, scope, bearerToken, cancelRequested, progress);
  }

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
    validateScopedStatsRequests(scope)
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
      ResourceId destNamespaceId;
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
      destNamespaceId =
          lookupDestinationNamespaceId(ctx, destCatalogId, dest, destNsFq)
              .orElseGet(() -> ensureNamespace(ctx, destCatalogId, destNsFq));

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
              .filter(task -> matchesPlannedNamespaceScope(destNamespaceId, scope))
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
                      destNamespaceId.getId(),
                      lookupDestinationTableIdByName(
                              ctx,
                              destCatalogId,
                              destNamespaceId,
                              destNsFq,
                              task.destinationTableDisplayName())
                          .map(ResourceId::getId)
                          .orElse(null),
                      task.destinationTableDisplayName()))
          .toList();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to plan reconcile tasks for connector " + connectorId.getId(), e);
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
    validateScopedStatsRequests(scope)
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
      ResourceId destNamespaceId =
          lookupDestinationNamespaceId(ctx, destCatalogId, dest, destNsFq)
              .orElseGet(() -> ensureNamespace(ctx, destCatalogId, destNsFq));
      if (!matchesPlannedNamespaceScope(destNamespaceId, scope)) {
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
                      destNamespaceId.getId(),
                      lookupDestinationViewIdByName(
                              ctx,
                              destCatalogId,
                              destNamespaceId,
                              destNsFq,
                              task.destinationViewDisplayName())
                          .map(ResourceId::getId)
                          .orElse(null),
                      task.destinationViewDisplayName()))
          .toList();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to plan reconcile view tasks for connector " + connectorId.getId(), e);
    }
  }

  public Result reconcileView(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      ReconcileViewTask viewTask,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidScope =
        validateScopeCombinations(scope, CaptureMode.METADATA_ONLY, true);
    if (invalidScope.isPresent()) {
      return new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()));
    }
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    if (effectiveViewTask.isEmpty()) {
      if (scope.hasViewFilter()) {
        return new Result(
            0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException("view task is required"));
      }
      return reconcileViewsOnly(
          principal, connectorId, scope, bearerToken, cancelRequested, progress);
    }
    if (effectiveViewTask.discoveryMode()) {
      return reconcileDiscoveryViewTask(
          principal, connectorId, scope, effectiveViewTask, bearerToken, cancelRequested, progress);
    }
    if (effectiveViewTask.destinationViewId().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException("destinationViewId is required for single-view reconcile"));
    }
    if (effectiveViewTask.sourceNamespace().isBlank() || effectiveViewTask.sourceView().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "sourceNamespace and sourceView are required for single-view reconcile"));
    }
    if (scope.hasViewFilter()
        && !scope.destinationViewId().equals(effectiveViewTask.destinationViewId())) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Connector destination view id "
                  + effectiveViewTask.destinationViewId()
                  + " does not match requested scope"));
    }

    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    final BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    final ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destinationViewId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_VIEW)
            .setId(effectiveViewTask.destinationViewId())
            .build();
    DestinationViewMetadata destinationViewMetadata;
    try {
      destinationViewMetadata =
          backend
              .lookupDestinationViewMetadata(ctx, destinationViewId)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Destination view id does not exist: "
                              + effectiveViewTask.destinationViewId()));
    } catch (Exception e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }
    if (destinationViewMetadata.namespaceId() == null
        || destinationViewMetadata.namespaceId().getId().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Destination view namespace cannot be resolved from id: "
                  + effectiveViewTask.destinationViewId()));
    }
    if (destinationViewMetadata.catalogId() == null
        || destinationViewMetadata.catalogId().getId().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Destination view catalog cannot be resolved from id: "
                  + effectiveViewTask.destinationViewId()));
    }

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      ensureNotCancelled(cancelCheck);
      FloecatConnector.ViewDescriptor view =
          connector
              .describeView(effectiveViewTask.sourceNamespace(), effectiveViewTask.sourceView())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "View not found: "
                              + effectiveViewTask.sourceNamespace()
                              + "."
                              + effectiveViewTask.sourceView()));

      ResourceId destNamespaceId = destinationViewMetadata.namespaceId();
      if (!scope.matchesNamespaceId(destNamespaceId.getId())) {
        return new Result(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException(
                "Connector destination namespace id "
                    + destNamespaceId.getId()
                    + " does not match requested scope"));
      }

      ensureNotCancelled(cancelCheck);
      String destinationViewDisplayName = destinationViewMetadata.displayName();
      List<SchemaColumn> outputColumns = viewOutputColumns(connector, view);
      if (view.sqlDefinitions().isEmpty() || outputColumns.isEmpty()) {
        return new Result(0, 0, 0, 0, 0, 0, 0, null);
      }

      progressOut.onProgress(
          0, 0, 1, 0, 0, 0, 0, "Processing view " + view.namespaceFq() + "." + view.name());
      ViewSpec viewSpec =
          ViewSpec.newBuilder()
              .setCatalogId(destinationViewMetadata.catalogId())
              .setNamespaceId(destNamespaceId)
              .setDisplayName(destinationViewDisplayName)
              .addAllSqlDefinitions(toCatalogSqlDefinitions(view))
              .addAllCreationSearchPath(view.searchPath() != null ? view.searchPath() : List.of())
              .addAllOutputColumns(outputColumns)
              .putAllProperties(
                  sourceIdentityProperties(connectorId, view.namespaceFq(), view.name()))
              .build();
      boolean viewChanged = backend.updateViewById(ctx, destinationViewId, viewSpec);
      long changed = viewChanged ? 1L : 0L;
      progressOut.onProgress(
          0, 0, 1, changed, 0, 0, 0, "Finished view " + view.namespaceFq() + "." + view.name());
      return new Result(0, 0, 1, changed, 0, 0, 0, null);
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new Result(0, 0, 0, 0, 0, 0, 0, e);
      }
      return new Result(0, 0, 1, 0, 1, 0, 0, e);
    }
  }

  private Result reconcileDiscoveryViewTask(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      ReconcileViewTask viewTask,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    if (viewTask.sourceNamespace().isBlank() || viewTask.sourceView().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "sourceNamespace and sourceView are required for discovery view reconcile"));
    }
    if (viewTask.destinationNamespaceId().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "destinationNamespaceId is required for discovery view reconcile"));
    }
    if (scope.hasTableFilter() || scope.hasViewFilter()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Discovery view execution cannot be combined with table or view id scope"));
    }

    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    final BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    final ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destNamespaceId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(viewTask.destinationNamespaceId())
            .build();
    if (!scope.matchesNamespaceId(destNamespaceId.getId())) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Connector destination namespace id "
                  + destNamespaceId.getId()
                  + " does not match requested scope"));
    }
    ResourceId destCatalogId = active.destination().getCatalogId();
    String destNsFq = resolveNamespaceFq(ctx, destNamespaceId);
    String displayName =
        viewTask.destinationViewDisplayName().isBlank()
            ? viewTask.sourceView()
            : viewTask.destinationViewDisplayName();

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      ensureNotCancelled(cancelCheck);
      FloecatConnector.ViewDescriptor view =
          connector
              .describeView(viewTask.sourceNamespace(), viewTask.sourceView())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "View not found: "
                              + viewTask.sourceNamespace()
                              + "."
                              + viewTask.sourceView()));
      List<SchemaColumn> outputColumns = viewOutputColumns(connector, view);
      if (view.sqlDefinitions().isEmpty() || outputColumns.isEmpty()) {
        return new Result(0, 0, 0, 0, 0, 0, 0, null);
      }
      progressOut.onProgress(
          0, 0, 1, 0, 0, 0, 0, "Processing view " + view.namespaceFq() + "." + view.name());
      ViewSpec viewSpec =
          ViewSpec.newBuilder()
              .setCatalogId(destCatalogId)
              .setNamespaceId(destNamespaceId)
              .setDisplayName(displayName)
              .addAllSqlDefinitions(toCatalogSqlDefinitions(view))
              .addAllCreationSearchPath(view.searchPath() != null ? view.searchPath() : List.of())
              .addAllOutputColumns(outputColumns)
              .putAllProperties(
                  sourceIdentityProperties(connectorId, view.namespaceFq(), view.name()))
              .build();
      Optional<ResourceId> existingViewId =
          !blank(viewTask.destinationViewId())
              ? Optional.of(
                  destinationResourceId(
                      connectorId, ResourceKind.RK_VIEW, viewTask.destinationViewId()))
              : lookupDestinationViewIdByName(
                  ctx, destCatalogId, destNamespaceId, destNsFq, displayName);
      long changed;
      if (existingViewId.isPresent()) {
        changed = backend.updateViewById(ctx, existingViewId.get(), viewSpec) ? 1L : 0L;
      } else {
        String idempotencyKey =
            "namespace-id:" + destNamespaceId.getId() + "|view-name:" + displayName;
        ReconcilerBackend.ViewMutationResult viewResult =
            backend.ensureView(ctx, viewSpec, idempotencyKey);
        changed = viewResult.changed() ? 1L : 0L;
      }
      progressOut.onProgress(
          0, 0, 1, changed, 0, 0, 0, "Finished view " + view.namespaceFq() + "." + view.name());
      return new Result(0, 0, 1, changed, 0, 0, 0, null);
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new Result(0, 0, 0, 0, 0, 0, 0, e);
      }
      return new Result(0, 0, 1, 0, 1, 0, 0, e);
    }
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      ReconcileTableTask tableTask,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidScope = validateScopeCombinations(scope, captureMode, false);
    if (invalidScope.isPresent()) {
      return new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()));
    }
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    if (!effectiveTableTask.isEmpty()) {
      if (effectiveTableTask.discoveryMode()) {
        return reconcileDiscoveryTableTask(
            principal,
            connectorId,
            fullRescan,
            scope,
            effectiveTableTask,
            captureMode,
            bearerToken,
            cancelRequested,
            progress);
      }
      return reconcileSingleTableTask(
          principal,
          connectorId,
          fullRescan,
          scope,
          effectiveTableTask,
          captureMode,
          bearerToken,
          cancelRequested,
          progress);
    }
    if (scope.hasTableFilter()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Concrete table task is required for destination table id scoped reconcile"));
    }
    return planAndExecuteTableTasks(
        principal,
        connectorId,
        fullRescan,
        scope,
        captureMode,
        bearerToken,
        cancelRequested,
        progress);
  }

  private Result planAndExecuteTableTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scope,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    try {
      return executePlannedTableTasks(
          principal,
          connectorId,
          fullRescan,
          scope,
          planTableTasks(principal, connectorId, scope, bearerToken),
          captureMode,
          bearerToken,
          cancelRequested,
          progress);
    } catch (RuntimeException e) {
      return planningFailure(e);
    }
  }

  private Result planAndExecuteViewTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scope,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    try {
      return executePlannedViewTasks(
          principal,
          connectorId,
          scope,
          planViewTasks(principal, connectorId, scope, bearerToken),
          bearerToken,
          cancelRequested,
          progress);
    } catch (RuntimeException e) {
      return planningFailure(e);
    }
  }

  private static Result planningFailure(RuntimeException e) {
    return new Result(0, 0, 0, 0, 1, 0, 0, e);
  }

  private static Optional<String> validateScopeCombinations(
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
    if (captureMode == CaptureMode.STATS_ONLY && scope.hasViewFilter()) {
      return Optional.of("STATS_ONLY is not valid for view reconcile");
    }
    return Optional.empty();
  }

  private static Result tableSuccessResult(TableExecutionOutcome outcome) {
    List<String> matchedTableIds =
        outcome.destinationTableId() == null || outcome.destinationTableId().getId().isBlank()
            ? List.of()
            : List.of(outcome.destinationTableId().getId());
    return new Result(
        outcome.tablesScanned(),
        outcome.tablesChanged(),
        0,
        0,
        0,
        outcome.snapshotsProcessed(),
        outcome.statsProcessed(),
        null,
        outcome.degradedReason().map(List::of).orElseGet(List::of),
        matchedTableIds);
  }

  private Result executePlannedTableTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      List<ReconcileTableTask> tableTasks,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ResultAccumulator aggregate = new ResultAccumulator();
    LinkedHashSet<String> unmatchedScopedRequestTables =
        new LinkedHashSet<>(
            indexScopedStatsRequestsByTableScope(scope.destinationStatsRequests()).keySet());
    for (ReconcileTableTask task :
        tableTasks == null ? List.<ReconcileTableTask>of() : tableTasks) {
      if (cancelCheck.getAsBoolean()) {
        return aggregate.cancelled();
      }
      Result result =
          task.discoveryMode()
              ? reconcileDiscoveryTableTask(
                  principal,
                  connectorId,
                  fullRescan,
                  scope,
                  task,
                  captureMode,
                  bearerToken,
                  cancelCheck,
                  progress)
              : reconcileSingleTableTask(
                  principal,
                  connectorId,
                  fullRescan,
                  scope,
                  task,
                  captureMode,
                  bearerToken,
                  cancelCheck,
                  progress);
      aggregate.add(result);
      if (result.cancelled()) {
        return aggregate.cancelled();
      }
      if (result.tablesScanned > 0) {
        result.matchedTableIds.forEach(unmatchedScopedRequestTables::remove);
        if (!blank(task.destinationTableId())) {
          unmatchedScopedRequestTables.remove(task.destinationTableId());
        }
      }
    }
    if (captureMode == CaptureMode.STATS_ONLY && !unmatchedScopedRequestTables.isEmpty()) {
      aggregate.addError(
          "No tables matched scoped stats requests: "
              + unmatchedScopedRequestTables.stream().sorted().collect(Collectors.joining(", ")));
    }
    return aggregate.toResult();
  }

  private Result executePlannedViewTasks(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      List<ReconcileViewTask> viewTasks,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ResultAccumulator aggregate = new ResultAccumulator();
    for (ReconcileViewTask task : viewTasks == null ? List.<ReconcileViewTask>of() : viewTasks) {
      if (cancelCheck.getAsBoolean()) {
        return aggregate.cancelled();
      }
      Result result =
          reconcileView(principal, connectorId, scope, task, bearerToken, cancelCheck, progress);
      aggregate.add(result);
      if (result.cancelled()) {
        return aggregate.cancelled();
      }
    }
    return aggregate.toResult();
  }

  private static Result combineResults(List<Result> results) {
    ResultAccumulator aggregate = new ResultAccumulator();
    if (results != null) {
      for (Result result : results) {
        aggregate.add(result);
        if (result != null && result.cancelled()) {
          return aggregate.cancelled();
        }
      }
    }
    return aggregate.toResult();
  }

  private Result reconcileSingleTableTask(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      ReconcileTableTask tableTask,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidStrictTableExecution = validateStrictTableExecution(scope, tableTask);
    if (invalidStrictTableExecution.isPresent()) {
      return new Result(
          0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidStrictTableExecution.get()));
    }

    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    final BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    final ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destinationTableId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_TABLE)
            .setId(tableTask.destinationTableId())
            .build();
    DestinationTableMetadata tableMetadata;
    try {
      tableMetadata =
          backend
              .lookupDestinationTableMetadata(ctx, destinationTableId)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Destination table id does not exist: "
                              + tableTask.destinationTableId()));
    } catch (Exception e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }
    if (tableMetadata.namespaceId() == null || tableMetadata.namespaceId().getId().isBlank()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Destination table namespace cannot be resolved from id: "
                  + tableTask.destinationTableId()));
    }

    final SourceSelector source = active.source();
    TableExecutionProgress tableProgress = new TableExecutionProgress();
    ProgressListener trackingProgress =
        (tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed,
            message) -> {
          tableProgress.observe(tablesScanned, tablesChanged, snapshotsProcessed, statsProcessed);
          progressOut.onProgress(
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed,
              message);
        };
    ResourceId destNamespaceId = tableMetadata.namespaceId();
    String destNsFq = resolveNamespaceFq(ctx, destNamespaceId);
    String destTableDisplay =
        tableMetadata.displayName() == null || tableMetadata.displayName().isBlank()
            ? tableTask.destinationTableDisplayName()
            : tableMetadata.displayName();

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      TableExecutionOutcome outcome =
          executeResolvedTable(
              ctx,
              connectorId,
              connector,
              active.resolvedConfig().kind(),
              fullRescan,
              captureMode,
              scope,
              new ResolvedTable(
                  tableTask.sourceNamespace(),
                  tableTask.sourceTable(),
                  destNamespaceId,
                  destNsFq,
                  destTableDisplay,
                  ignoredDisplay -> Optional.of(destinationTableId),
                  (upstream, ignoredDisplay, ignoredCandidate) -> {
                    boolean tableMetadataChanged = false;
                    if (captureMode != CaptureMode.STATS_ONLY) {
                      FloecatConnector.TableDescriptor effective =
                          overrideDisplay(upstream, destNsFq, destTableDisplay);
                      tableMetadataChanged =
                          updateTableById(
                              ctx,
                              destinationTableId,
                              tableMetadata.catalogId(),
                              destNamespaceId,
                              effective,
                              connector.format(),
                              active.connector().getResourceId(),
                              active.config().uri(),
                              tableTask.sourceNamespace(),
                              tableTask.sourceTable());
                    }
                    return DestinationTableResolution.resolved(
                        destinationTableId, tableMetadataChanged);
                  }),
              normalizeSelectors(source.getColumnsList()),
              cancelCheck,
              trackingProgress,
              tableProgress,
              0,
              0,
              0,
              0,
              0);
      if (captureMode == CaptureMode.STATS_ONLY
          && !scope.destinationStatsRequests().isEmpty()
          && outcome.tablesScanned() == 0) {
        return new Result(
            0,
            0,
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException(
                "No tables matched scoped stats requests: "
                    + indexScopedStatsRequestsByTableScope(scope.destinationStatsRequests())
                        .keySet()
                        .stream()
                        .sorted()
                        .collect(Collectors.joining(", "))));
      }
      return tableSuccessResult(outcome);
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new Result(
            tableProgress.tablesScanned,
            tableProgress.tablesChanged,
            0,
            0,
            0,
            tableProgress.snapshotsProcessed,
            tableProgress.statsProcessed,
            e,
            tableProgress.degradedReason.map(List::of).orElseGet(List::of));
      }
      return new Result(
          tableProgress.tablesScanned,
          tableProgress.tablesChanged,
          0,
          0,
          1,
          tableProgress.snapshotsProcessed,
          tableProgress.statsProcessed,
          e,
          tableProgress.degradedReason.map(List::of).orElseGet(List::of));
    }
  }

  private Result reconcileDiscoveryTableTask(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      ReconcileTableTask tableTask,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidDiscoveryTableExecution =
        validateDiscoveryTableExecution(scope, tableTask);
    if (invalidDiscoveryTableExecution.isPresent()) {
      return new Result(
          0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidDiscoveryTableExecution.get()));
    }

    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    final BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    final ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destNamespaceId =
        ResourceId.newBuilder()
            .setAccountId(connectorId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(tableTask.destinationNamespaceId())
            .build();
    if (!scope.matchesNamespaceId(destNamespaceId.getId())) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException(
              "Connector destination namespace id "
                  + destNamespaceId.getId()
                  + " does not match requested scope"));
    }

    String destNsFq = resolveNamespaceFq(ctx, destNamespaceId);
    String displayName =
        tableTask.destinationTableDisplayName().isBlank()
            ? tableTask.sourceTable()
            : tableTask.destinationTableDisplayName();
    TableExecutionProgress tableProgress = new TableExecutionProgress();
    ProgressListener trackingProgress =
        (tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed,
            message) -> {
          tableProgress.observe(tablesScanned, tablesChanged, snapshotsProcessed, statsProcessed);
          progressOut.onProgress(
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed,
              message);
        };

    try (FloecatConnector connector = connectorOpener.open(active.resolvedConfig())) {
      ResourceId destCatalogId = active.destination().getCatalogId();
      Set<String> defaultColumnSelectors = normalizeSelectors(active.source().getColumnsList());
      TableExecutionOutcome outcome =
          executeResolvedTable(
              ctx,
              connectorId,
              connector,
              active.resolvedConfig().kind(),
              fullRescan,
              captureMode,
              scope,
              new ResolvedTable(
                  tableTask.sourceNamespace(),
                  tableTask.sourceTable(),
                  destNamespaceId,
                  destNsFq,
                  displayName,
                  destTableDisplay ->
                      !blank(tableTask.destinationTableId())
                          ? Optional.of(
                              destinationResourceId(
                                  connectorId,
                                  ResourceKind.RK_TABLE,
                                  tableTask.destinationTableId()))
                          : lookupDestinationTableIdByName(
                              ctx, destCatalogId, destNamespaceId, destNsFq, destTableDisplay),
                  (upstream, destTableDisplay, existingTableId) -> {
                    FloecatConnector.TableDescriptor effective =
                        overrideDisplay(upstream, destNsFq, destTableDisplay);
                    if (captureMode == CaptureMode.STATS_ONLY) {
                      return DestinationTableResolution.of(existingTableId, false);
                    }
                    if (existingTableId.isPresent()) {
                      boolean tableMetadataChanged =
                          updateTableById(
                              ctx,
                              existingTableId.get(),
                              destCatalogId,
                              destNamespaceId,
                              effective,
                              connector.format(),
                              active.connector().getResourceId(),
                              active.config().uri(),
                              tableTask.sourceNamespace(),
                              tableTask.sourceTable());
                      return DestinationTableResolution.resolved(
                          existingTableId.get(), tableMetadataChanged);
                    }
                    return DestinationTableResolution.resolved(
                        ensureTable(
                            ctx,
                            destCatalogId,
                            destNamespaceId,
                            effective,
                            connector.format(),
                            active.connector().getResourceId(),
                            active.config().uri(),
                            tableTask.sourceNamespace(),
                            tableTask.sourceTable()),
                        true);
                  }),
              defaultColumnSelectors,
              cancelCheck,
              trackingProgress,
              tableProgress,
              0,
              0,
              0,
              0,
              0);
      return tableSuccessResult(outcome);
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new Result(
            tableProgress.tablesScanned,
            tableProgress.tablesChanged,
            0,
            0,
            0,
            tableProgress.snapshotsProcessed,
            tableProgress.statsProcessed,
            e,
            tableProgress.degradedReason.map(List::of).orElseGet(List::of));
      }
      return new Result(
          tableProgress.tablesScanned,
          tableProgress.tablesChanged,
          0,
          0,
          1,
          tableProgress.snapshotsProcessed,
          tableProgress.statsProcessed,
          e,
          tableProgress.degradedReason.map(List::of).orElseGet(List::of));
    }
  }

  private TableExecutionOutcome executeResolvedTable(
      ReconcileContext ctx,
      ResourceId connectorId,
      FloecatConnector connector,
      ConnectorConfig.Kind connectorKind,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ResolvedTable table,
      Set<String> defaultColumnSelectors,
      BooleanSupplier cancelRequested,
      ProgressListener progress,
      TableExecutionProgress progressState,
      long tablesScannedBase,
      long tablesChangedBase,
      long errors,
      long snapshotsProcessedBase,
      long statsProcessedBase) {
    ensureNotCancelled(cancelRequested);
    FloecatConnector.TableDescriptor upstream =
        connector.describe(table.sourceNamespace(), table.sourceTable());
    String destTableDisplay =
        table.destinationTableDisplayName() == null || table.destinationTableDisplayName().isBlank()
            ? upstream.tableName()
            : table.destinationTableDisplayName();
    Optional<ResourceId> candidateTableId = table.destinationTableLookup().lookup(destTableDisplay);
    if (!scope.matchesNamespaceId(table.destinationNamespaceId().getId())) {
      return TableExecutionOutcome.skipped(false);
    }
    if (candidateTableId.isPresent()
        && !scope.acceptsTable(
            table.destinationNamespaceId().getId(), candidateTableId.get().getId())) {
      return TableExecutionOutcome.skipped(false);
    }
    if (captureMode == CaptureMode.STATS_ONLY && candidateTableId.isEmpty()) {
      LOG.debugf(
          "Skipping stats-only reconcile for %s.%s because destination table was not found",
          table.sourceNamespace(), table.sourceTable());
      return TableExecutionOutcome.skipped(false);
    }

    DestinationTableResolution destinationTable =
        table.destinationTableMutation().apply(upstream, destTableDisplay, candidateTableId);
    if (destinationTable.tableId().isEmpty()) {
      LOG.debugf(
          "Skipping stats-only reconcile for %s.%s because destination table was not found",
          table.sourceNamespace(), table.sourceTable());
      return TableExecutionOutcome.skipped(false);
    }
    ResourceId tableId = destinationTable.tableId().get();
    if (!scope.acceptsTable(table.destinationNamespaceId().getId(), tableId.getId())) {
      return TableExecutionOutcome.skipped(false);
    }

    Map<String, Map<Long, List<ReconcileScope.ScopedStatsRequest>>> scopedStatsRequestsByTable =
        indexScopedStatsRequestsByTableScope(scope.destinationStatsRequests());
    Map<Long, List<ReconcileScope.ScopedStatsRequest>> tableScopedStatsRequestsBySnapshot =
        scopedStatsRequestsByTable.getOrDefault(tableId.getId(), Map.of());
    boolean hasScopedStatsRequestFilter = !scopedStatsRequestsByTable.isEmpty();
    boolean tableHasScopedStatsRequests = !tableScopedStatsRequestsBySnapshot.isEmpty();
    if (captureMode == CaptureMode.STATS_ONLY
        && hasScopedStatsRequestFilter
        && !tableHasScopedStatsRequests) {
      return TableExecutionOutcome.skipped(true);
    }

    long tablesScanned = 1L;
    if (progressState != null) {
      progressState.observe(
          tablesScannedBase + tablesScanned,
          tablesChangedBase,
          snapshotsProcessedBase,
          statsProcessedBase);
    }
    progress.onProgress(
        tablesScannedBase + tablesScanned,
        tablesChangedBase,
        0,
        0,
        errors,
        snapshotsProcessedBase,
        statsProcessedBase,
        "Processing table " + table.sourceNamespace() + "." + table.sourceTable() + " (metadata)");

    boolean includeCoreMetadata =
        captureMode == CaptureMode.METADATA_ONLY || captureMode == CaptureMode.METADATA_AND_STATS;
    boolean includeStats = captureMode == CaptureMode.STATS_ONLY;
    Set<Long> targetSnapshotIds =
        !tableScopedStatsRequestsBySnapshot.isEmpty()
            ? tableScopedStatsRequestsBySnapshot.keySet()
            : Set.of();
    Set<Long> knownSnapshotIds = fullRescan ? Set.of() : backend.existingSnapshotIds(ctx, tableId);
    Set<Long> enumerationKnownSnapshotIds =
        knownSnapshotIdsForEnumeration(
            fullRescan,
            includeStats,
            knownSnapshotIds,
            snapshotId ->
                isStatsCaptureCompleteForScope(
                    ctx,
                    tableId,
                    snapshotId,
                    tableScopedStatsRequestsBySnapshot.getOrDefault(snapshotId, List.of()),
                    defaultColumnSelectors));

    long tablesChanged = 0L;
    long snapshotsProcessed;
    long statsProcessed;
    Optional<String> degradedReason = Optional.empty();
    if (captureMode == CaptureMode.STATS_ONLY) {
      IngestCounts ingestCounts =
          captureStatsOnlyViaControlPlane(
              ctx,
              tableId,
              connector,
              table.sourceNamespace(),
              table.sourceTable(),
              fullRescan,
              knownSnapshotIds,
              enumerationKnownSnapshotIds,
              targetSnapshotIds,
              tableScopedStatsRequestsBySnapshot,
              defaultColumnSelectors,
              cancelRequested,
              progress,
              tablesScannedBase + tablesScanned,
              tablesChangedBase,
              errors,
              snapshotsProcessedBase,
              statsProcessedBase,
              connectorKind);
      snapshotsProcessed = ingestCounts.snapshotsProcessed;
      statsProcessed = ingestCounts.statsProcessed;
      if (progressState != null) {
        progressState.observe(
            tablesScannedBase + tablesScanned,
            tablesChangedBase,
            snapshotsProcessedBase + snapshotsProcessed,
            statsProcessedBase + statsProcessed);
      }
    } else {
      MetadataPassOutcome outcome =
          processMetadataPass(
              ctx,
              connectorId,
              tableId,
              connector,
              table.sourceNamespace(),
              table.sourceTable(),
              table.destinationNamespaceFq(),
              destTableDisplay,
              fullRescan,
              includeCoreMetadata,
              includeStats,
              tableScopedStatsRequestsBySnapshot,
              hasScopedStatsRequestFilter,
              defaultColumnSelectors,
              knownSnapshotIds,
              enumerationKnownSnapshotIds,
              targetSnapshotIds,
              cancelRequested,
              progress,
              tablesScannedBase + tablesScanned,
              tablesChangedBase,
              errors,
              snapshotsProcessedBase,
              statsProcessedBase,
              captureMode);
      snapshotsProcessed = outcome.ingestCounts().snapshotsProcessed;
      statsProcessed = outcome.ingestCounts().statsProcessed;
      tablesChanged = destinationTable.tableMetadataChanged() || outcome.tableChanged() ? 1L : 0L;
      degradedReason = outcome.degradedReason();
      if (progressState != null) {
        progressState.observe(
            tablesScannedBase + tablesScanned,
            tablesChangedBase + tablesChanged,
            snapshotsProcessedBase + snapshotsProcessed,
            statsProcessedBase + statsProcessed);
        progressState.degradedReason = degradedReason;
      }
    }

    progress.onProgress(
        tablesScannedBase + tablesScanned,
        tablesChangedBase + tablesChanged,
        0,
        0,
        errors,
        snapshotsProcessedBase + snapshotsProcessed,
        statsProcessedBase + statsProcessed,
        "Finished table " + table.sourceNamespace() + "." + table.sourceTable());
    return new TableExecutionOutcome(
        true,
        tableId,
        tablesScanned,
        tablesChanged,
        snapshotsProcessed,
        statsProcessed,
        degradedReason);
  }

  private static List<ViewSqlDefinition> toCatalogSqlDefinitions(
      FloecatConnector.ViewDescriptor view) {
    return view.sqlDefinitions().stream()
        .map(
            def ->
                ViewSqlDefinition.newBuilder().setSql(def.sql()).setDialect(def.dialect()).build())
        .toList();
  }

  private ResourceId ensureNamespace(
      ReconcileContext ctx, ResourceId catalogId, String namespaceFq) {
    var parts = split(namespaceFq);
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    NameRef nameRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(parts.parents)
            .setName(parts.leaf)
            .build();
    NameRef normalized = NameRefNormalizer.normalize(nameRef);
    return backend.ensureNamespace(ctx, catalogId, normalized);
  }

  private ResourceId ensureTable(
      ReconcileContext ctx,
      ResourceId catalogId,
      ResourceId destNamespaceId,
      FloecatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    TableSpecDescriptor descriptor =
        tableSpecDescriptor(
            landingView, format, connectorRid, connectorUri, sourceNsFq, sourceTable);
    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(namespacePathSegments(landingView.namespaceFq()))
            .setName(landingView.tableName())
            .build();
    return backend.ensureTable(
        ctx, destNamespaceId, NameRefNormalizer.normalize(tableRef), descriptor);
  }

  private TableSpecDescriptor tableSpecDescriptor(
      FloecatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    return new TableSpecDescriptor(
        landingView.namespaceFq(),
        landingView.tableName(),
        landingView.schemaJson(),
        landingView.properties(),
        landingView.partitionKeys(),
        landingView.columnIdAlgorithm(),
        format,
        connectorRid,
        connectorUri,
        sourceNsFq,
        sourceTable);
  }

  private boolean updateTableById(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId catalogId,
      ResourceId destNamespaceId,
      FloecatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(namespacePathSegments(landingView.namespaceFq()))
            .setName(landingView.tableName())
            .build();
    return backend.updateTableById(
        ctx,
        tableId,
        destNamespaceId,
        NameRefNormalizer.normalize(tableRef),
        tableSpecDescriptor(
            landingView, format, connectorRid, connectorUri, sourceNsFq, sourceTable));
  }

  private boolean ensureSnapshot(
      ReconcileContext ctx, ResourceId tableId, FloecatConnector.SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return false;
    }
    Snapshot existing =
        backend.fetchSnapshot(ctx, tableId, snapshotBundle.snapshotId()).orElse(null);
    Optional<Snapshot> snapshot = buildSnapshot(ctx, tableId, snapshotBundle, existing);
    snapshot.ifPresent(candidate -> backend.ingestSnapshot(ctx, tableId, candidate));
    return snapshot.isPresent();
  }

  Optional<Snapshot> buildSnapshot(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector.SnapshotBundle bundle,
      Snapshot existing) {
    long parentSnapshotId = bundle.parentId();
    if (parentSnapshotId <= 0 && existing != null) {
      parentSnapshotId = existing.getParentSnapshotId();
    }

    Timestamp upstreamTimestamp;
    if (bundle.upstreamCreatedAtMs() > 0) {
      upstreamTimestamp = Timestamps.fromMillis(bundle.upstreamCreatedAtMs());
    } else if (existing != null && existing.hasUpstreamCreatedAt()) {
      upstreamTimestamp = existing.getUpstreamCreatedAt();
    } else {
      upstreamTimestamp = Timestamps.fromMillis(ctx.now().toEpochMilli());
    }

    Snapshot.Builder builder =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(bundle.snapshotId())
            .setUpstreamCreatedAt(upstreamTimestamp);
    if (parentSnapshotId > 0) {
      builder.setParentSnapshotId(parentSnapshotId);
    }
    applyField(
        () -> bundle.schemaJson(),
        () -> existing != null ? existing.getSchemaJson() : null,
        builder::setSchemaJson,
        str -> str != null && !str.isBlank());
    applyField(
        () -> bundle.partitionSpec(),
        () ->
            (existing != null && existing.hasPartitionSpec()) ? existing.getPartitionSpec() : null,
        builder::setPartitionSpec,
        spec -> spec != null);
    applyLongField(
        () -> bundle.sequenceNumber(),
        () -> existing != null ? existing.getSequenceNumber() : 0L,
        builder::setSequenceNumber,
        value -> value > 0);
    applyField(
        () -> bundle.manifestList(),
        () -> existing != null ? existing.getManifestList() : null,
        builder::setManifestList,
        str -> str != null && !str.isBlank());
    if (bundle.summary() != null && !bundle.summary().isEmpty()) {
      var merged = new LinkedHashMap<>(bundle.summary());
      if (existing != null && !existing.getSummaryMap().isEmpty()) {
        existing.getSummaryMap().forEach(merged::putIfAbsent);
      }
      builder.putAllSummary(merged);
    }
    applyLongField(
        () -> (long) bundle.schemaId(),
        () -> existing != null ? existing.getSchemaId() : 0L,
        value -> builder.setSchemaId((int) value),
        value -> value > 0);
    Map<String, ByteString> mergedMetadata = new LinkedHashMap<>();
    if (existing != null && !existing.getFormatMetadataMap().isEmpty()) {
      mergedMetadata.putAll(existing.getFormatMetadataMap());
    }
    if (bundle.metadata() != null && !bundle.metadata().isEmpty()) {
      bundle
          .metadata()
          .forEach(
              (key, value) -> mergedMetadata.put(key, value != null ? value : ByteString.EMPTY));
    }
    if (!mergedMetadata.isEmpty()) {
      // Null metadata values from connectors are stored as empty bytes rather than removing keys.
      builder.putAllFormatMetadata(mergedMetadata);
    }
    Snapshot candidate = builder.build();
    if (existing != null && SnapshotHelpers.equalsIgnoringIngested(candidate, existing)) {
      return Optional.empty();
    }
    return Optional.of(
        candidate.toBuilder()
            .setIngestedAt(Timestamps.fromMillis(ctx.now().toEpochMilli()))
            .build());
  }

  static List<FloecatConnector.SnapshotBundle> filterBundlesForMode(
      List<FloecatConnector.SnapshotBundle> bundles,
      boolean fullRescan,
      boolean includeStats,
      Set<Long> existingSnapshotIds,
      ProgressListener progress) {
    if (bundles == null || bundles.isEmpty() || fullRescan) {
      return bundles == null ? List.of() : bundles;
    }

    List<FloecatConnector.SnapshotBundle> scoped = bundles;
    if (includeStats) {
      return scoped;
    }
    if (existingSnapshotIds == null || existingSnapshotIds.isEmpty()) {
      return scoped;
    }

    List<FloecatConnector.SnapshotBundle> filtered = new ArrayList<>(scoped.size());
    int skipped = 0;
    for (FloecatConnector.SnapshotBundle bundle : scoped) {
      if (bundle == null) {
        continue;
      }
      long snapshotId = bundle.snapshotId();
      if (snapshotId >= 0 && existingSnapshotIds.contains(snapshotId)) {
        skipped++;
        continue;
      }
      filtered.add(bundle);
    }
    if (skipped > 0) {
      progress.onProgress(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          "Incremental reconcile skipped " + skipped + " already-ingested snapshots");
    }
    return filtered;
  }

  private List<FloecatConnector.SnapshotBundle> filterBundlesForSnapshotScope(
      List<FloecatConnector.SnapshotBundle> bundles,
      Set<Long> targetSnapshotIds,
      ProgressListener progress) {
    if (bundles == null
        || bundles.isEmpty()
        || targetSnapshotIds == null
        || targetSnapshotIds.isEmpty()) {
      return bundles == null ? List.of() : bundles;
    }
    List<FloecatConnector.SnapshotBundle> filtered = new ArrayList<>(bundles.size());
    int skipped = 0;
    for (FloecatConnector.SnapshotBundle bundle : bundles) {
      if (bundle == null) {
        continue;
      }
      if (!targetSnapshotIds.contains(bundle.snapshotId())) {
        skipped++;
        continue;
      }
      filtered.add(bundle);
    }
    if (skipped > 0) {
      progress.onProgress(
          0,
          0,
          0,
          0,
          0,
          0,
          0,
          "Reconcile skipped " + skipped + " snapshots outside explicit snapshot scope");
    }
    return filtered;
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

  private static <T> void applyField(
      Supplier<T> bundleValue,
      Supplier<T> existingValue,
      Consumer<T> setter,
      Predicate<T> hasValue) {
    T value = bundleValue.get();
    if (hasValue.test(value)) {
      setter.accept(value);
      return;
    }
    T existing = existingValue.get();
    if (hasValue.test(existing)) {
      setter.accept(existing);
    }
  }

  private static void applyLongField(
      Supplier<Long> bundleValue,
      Supplier<Long> existingValue,
      LongConsumer setter,
      LongPredicate hasValue) {
    long value = bundleValue.get();
    if (hasValue.test(value)) {
      setter.accept(value);
      return;
    }
    long existing = existingValue.get();
    if (hasValue.test(existing)) {
      setter.accept(existing);
    }
  }

  private IngestCounts ingestMetadataSnapshots(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      List<FloecatConnector.SnapshotBundle> bundles,
      boolean includeCoreMetadata,
      BooleanSupplier cancelRequested,
      ProgressListener progress,
      String sourceNs,
      String sourceTable,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessedBase,
      long statsProcessedBase) {

    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;
    boolean tableChanged = false;
    var seen = new HashSet<Long>();

    for (var snapshotBundle : bundles) {
      ensureNotCancelled(cancelRequested);
      if (snapshotBundle == null) {
        continue;
      }

      long snapshotId = snapshotBundle.snapshotId();
      if (snapshotId < 0 || !seen.add(snapshotId)) {
        continue;
      }
      progress.onProgress(
          scanned,
          changed,
          0,
          0,
          errors,
          snapshotsProcessedBase + snapshotsProcessed,
          statsProcessedBase + statsProcessed,
          "Processing snapshot " + snapshotId + " for " + sourceNs + "." + sourceTable);
      snapshotsProcessed++;

      if (includeCoreMetadata) {
        boolean snapshotChanged = ensureSnapshot(ctx, tableId, snapshotBundle);
        boolean constraintsChanged =
            maybeIngestSnapshotConstraints(
                ctx, tableId, connector, sourceNs, sourceTable, snapshotBundle, snapshotId);
        tableChanged = tableChanged || snapshotChanged || constraintsChanged;
      }
    }
    return new IngestCounts(snapshotsProcessed, statsProcessed, tableChanged);
  }

  private IngestCounts captureStatsOnlyViaControlPlane(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      boolean fullRescan,
      Set<Long> knownSnapshotIds,
      Set<Long> enumerationKnownSnapshotIds,
      Set<Long> targetSnapshotIds,
      Map<Long, List<ReconcileScope.ScopedStatsRequest>> scopedStatsRequestsBySnapshot,
      Set<String> defaultColumnSelectors,
      BooleanSupplier cancelRequested,
      ProgressListener progress,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessedBase,
      long statsProcessedBase,
      ConnectorConfig.Kind connectorKind) {
    Set<Long> snapshotIds =
        discoverSnapshotIdsForStatsCapture(
            connector,
            sourceNs,
            sourceTable,
            tableId,
            fullRescan,
            enumerationKnownSnapshotIds,
            targetSnapshotIds);
    if (snapshotIds.isEmpty()) {
      return new IngestCounts(0L, 0L, false);
    }

    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;
    String connectorType = connectorTypeFor(connectorKind);
    List<StatsCaptureRequest> batchRequests = new ArrayList<>();
    for (long snapshotId : snapshotIds) {
      ensureNotCancelled(cancelRequested);
      progress.onProgress(
          scanned,
          changed,
          0,
          0,
          errors,
          snapshotsProcessedBase + snapshotsProcessed,
          statsProcessedBase + statsProcessed,
          "Processing snapshot " + snapshotId + " for " + sourceNs + "." + sourceTable);
      snapshotsProcessed++;

      if (!fullRescan
          && knownSnapshotIds != null
          && knownSnapshotIds.contains(snapshotId)
          && isStatsCaptureCompleteForScope(
              ctx,
              tableId,
              snapshotId,
              scopedStatsRequestsBySnapshot.getOrDefault(snapshotId, List.of()),
              defaultColumnSelectors)) {
        continue;
      }

      batchRequests.addAll(
          buildStatsCaptureRequestsForSnapshot(
              tableId,
              snapshotId,
              scopedStatsRequestsBySnapshot.getOrDefault(snapshotId, List.of()),
              defaultColumnSelectors,
              connectorType,
              ctx.correlationId()));
    }

    if (!batchRequests.isEmpty()) {
      StatsCaptureBatchResult batchResult =
          captureBatchViaControlPlane(StatsCaptureBatchRequest.of(batchRequests));
      batchResult
          .results()
          .forEach(
              item -> {
                if (item.outcome() == StatsTriggerOutcome.CAPTURED) {
                  LOG.debugf(
                      "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
                      item.outcome(),
                      item.request().tableId(),
                      item.request().snapshotId(),
                      item.detail());
                } else {
                  LOG.warnf(
                      "stats_trigger outcome=%s table=%s snapshot=%d reason=%s",
                      item.outcome(),
                      item.request().tableId(),
                      item.request().snapshotId(),
                      item.detail());
                }
              });
      Set<Long> capturedSnapshots =
          batchResult.results().stream()
              .filter(item -> item.outcome() == StatsTriggerOutcome.CAPTURED)
              .map(item -> item.request().snapshotId())
              .collect(Collectors.toCollection(LinkedHashSet::new));
      // statsProcessed counts successful capture attempts per snapshot, not number of persisted
      // target records.
      statsProcessed += capturedSnapshots.size();
    }
    return new IngestCounts(snapshotsProcessed, statsProcessed, false);
  }

  private List<StatsCaptureRequest> buildStatsCaptureRequestsForSnapshot(
      ResourceId tableId,
      long snapshotId,
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests,
      Set<String> defaultColumnSelectors,
      String connectorType,
      String correlationId) {
    List<StatsCaptureRequest> requests = new ArrayList<>();
    if (scopedStatsRequests != null && !scopedStatsRequests.isEmpty()) {
      LinkedHashSet<String> seen = new LinkedHashSet<>();
      for (ReconcileScope.ScopedStatsRequest scopedRequest : scopedStatsRequests) {
        Optional<ai.floedb.floecat.catalog.rpc.StatsTarget> decodedTarget =
            ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(
                scopedRequest.targetSpec());
        if (decodedTarget.isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid scoped stats target spec for table="
                  + scopedRequest.tableId()
                  + " snapshot="
                  + snapshotId
                  + " spec="
                  + scopedRequest.targetSpec());
        }
        boolean tableTarget =
            decodedTarget.get().getTargetCase()
                == ai.floedb.floecat.catalog.rpc.StatsTarget.TargetCase.TABLE;
        Set<String> selectors =
            tableTarget ? normalizeSelectors(scopedRequest.columnSelectors()) : Set.of();
        String requestKey =
            scopedRequest.targetSpec()
                + "|"
                + selectors.stream().sorted().collect(Collectors.joining(","));
        if (!seen.add(requestKey)) {
          continue;
        }
        requests.add(
            StatsCaptureRequest.builder(tableId, snapshotId, decodedTarget.get())
                .columnSelectors(selectors)
                .requestedKinds(Set.of())
                .executionMode(StatsExecutionMode.ASYNC)
                .connectorType(connectorType)
                .correlationId(correlationId)
                .build());
      }
      return requests;
    }

    requests.add(
        StatsCaptureRequest.builder(tableId, snapshotId, StatsTargetIdentity.tableTarget())
            .columnSelectors(defaultColumnSelectors == null ? Set.of() : defaultColumnSelectors)
            .requestedKinds(Set.of())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType(connectorType)
            .correlationId(correlationId)
            .build());
    return requests;
  }

  private Set<Long> discoverSnapshotIdsForStatsCapture(
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      ResourceId tableId,
      boolean fullRescan,
      Set<Long> enumerationKnownSnapshotIds,
      Set<Long> targetSnapshotIds) {
    if (targetSnapshotIds != null && !targetSnapshotIds.isEmpty()) {
      return new LinkedHashSet<>(targetSnapshotIds);
    }
    List<FloecatConnector.SnapshotBundle> discovered =
        connector.enumerateSnapshots(
            sourceNs,
            sourceTable,
            tableId,
            fullRescan
                ? FloecatConnector.SnapshotEnumerationOptions.full(true, targetSnapshotIds)
                : FloecatConnector.SnapshotEnumerationOptions.incremental(
                    enumerationKnownSnapshotIds, targetSnapshotIds));
    Set<Long> snapshotIds = new LinkedHashSet<>();
    if (discovered == null) {
      return snapshotIds;
    }
    for (FloecatConnector.SnapshotBundle bundle : discovered) {
      if (bundle == null || bundle.snapshotId() < 0) {
        continue;
      }
      snapshotIds.add(bundle.snapshotId());
    }
    return snapshotIds;
  }

  private StatsCaptureBatchResult captureBatchViaControlPlane(
      StatsCaptureBatchRequest batchRequest) {
    if (statsCaptureControlPlane == null || statsCaptureControlPlane.isUnsatisfied()) {
      return StatsCaptureBatchResult.of(
          batchRequest.requests().stream()
              .map(
                  req -> StatsCaptureBatchItemResult.uncapturable(req, "control plane unavailable"))
              .toList());
    }
    try {
      return statsCaptureControlPlane.get().triggerBatch(batchRequest);
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Stats control-plane batch capture failed batch_size=%d",
          batchRequest.requests().size());
      return StatsCaptureBatchResult.of(
          batchRequest.requests().stream()
              .map(
                  req -> StatsCaptureBatchItemResult.degraded(req, "control plane runtime failure"))
              .toList());
    }
  }

  private Optional<String> enqueueStatsOnlyCapture(
      ResourceId connectorId,
      String namespaceFq,
      String tableDisplayName,
      ResourceId tableId,
      Set<Long> snapshotIds,
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests,
      Set<String> defaultColumnSelectors) {
    if (reconcileJobStore == null || reconcileJobStore.isUnsatisfied()) {
      String reason =
          "stats_followup_unavailable connector="
              + (connectorId != null ? connectorId.getId() : "")
              + " table="
              + (namespaceFq == null ? "" : namespaceFq)
              + "."
              + (tableDisplayName == null ? "" : tableDisplayName);
      LOG.warnf(
          "Skipping follow-up STATS_ONLY enqueue: reconcile job store unavailable for connector=%s"
              + " table=%s.%s",
          connectorId != null ? connectorId.getId() : "",
          namespaceFq == null ? "" : namespaceFq,
          tableDisplayName == null ? "" : tableDisplayName);
      return Optional.of(reason);
    }
    if (connectorId == null
        || connectorId.getAccountId().isBlank()
        || connectorId.getId().isBlank()) {
      LOG.warn("Skipping follow-up STATS_ONLY enqueue: connector identity is incomplete");
      return Optional.of("stats_followup_unavailable connector_identity_incomplete");
    }
    if (namespaceFq == null
        || namespaceFq.isBlank()
        || tableDisplayName == null
        || tableDisplayName.isBlank()
        || tableId == null
        || tableId.getId().isBlank()) {
      LOG.warnf(
          "Skipping follow-up STATS_ONLY enqueue: scope identity missing for connector=%s",
          connectorId.getId());
      return Optional.of(
          "stats_followup_unavailable scope_identity_missing connector=" + connectorId.getId());
    }
    List<ReconcileScope.ScopedStatsRequest> sortedRequests =
        dedupeScopedStatsRequests(
                effectiveScopedStatsRequestsForSnapshots(
                    tableId.getId(), snapshotIds, scopedStatsRequests, defaultColumnSelectors))
            .stream()
            .sorted(
                Comparator.comparing(ReconcileScope.ScopedStatsRequest::tableId)
                    .thenComparingLong(ReconcileScope.ScopedStatsRequest::snapshotId)
                    .thenComparing(ReconcileScope.ScopedStatsRequest::targetSpec)
                    .thenComparing(
                        request ->
                            request.columnSelectors().stream()
                                .sorted()
                                .reduce((a, b) -> a + "," + b)
                                .orElse("")))
            .toList();
    ReconcileScope scope = ReconcileScope.of(List.of(), tableId.getId());
    if (!sortedRequests.isEmpty()) {
      scope = ReconcileScope.of(List.of(), tableId.getId(), sortedRequests);
    }
    String jobId =
        reconcileJobStore
            .get()
            .enqueue(
                connectorId.getAccountId(),
                connectorId.getId(),
                false,
                CaptureMode.STATS_ONLY,
                scope);
    LOG.infof(
        "Enqueued follow-up STATS_ONLY capture job=%s connector=%s table=%s.%s snapshots=%s",
        jobId,
        connectorId.getId(),
        namespaceFq,
        tableDisplayName,
        sortedRequests.stream()
            .map(ReconcileScope.ScopedStatsRequest::snapshotId)
            .distinct()
            .sorted()
            .toList());
    return Optional.empty();
  }

  private Set<Long> snapshotIdsFromBundles(List<FloecatConnector.SnapshotBundle> bundles) {
    if (bundles == null || bundles.isEmpty()) {
      return Set.of();
    }
    return bundles.stream()
        .filter(bundle -> bundle != null && bundle.snapshotId() >= 0)
        .map(FloecatConnector.SnapshotBundle::snapshotId)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private MetadataPassOutcome processMetadataPass(
      ReconcileContext ctx,
      ResourceId connectorId,
      ResourceId tableId,
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      String scopeNamespaceFq,
      String destTableDisplay,
      boolean fullRescan,
      boolean includeCoreMetadata,
      boolean includeStats,
      Map<Long, List<ReconcileScope.ScopedStatsRequest>> scopedStatsRequestsBySnapshot,
      boolean hasScopedStatsRequestFilter,
      Set<String> defaultColumnSelectors,
      Set<Long> knownSnapshotIds,
      Set<Long> enumerationKnownSnapshotIds,
      Set<Long> targetSnapshotIds,
      BooleanSupplier cancelRequested,
      ProgressListener progress,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessedBase,
      long statsProcessedBase,
      CaptureMode captureMode) {
    List<FloecatConnector.SnapshotBundle> upstreamBundles =
        connector.enumerateSnapshots(
            sourceNs,
            sourceTable,
            tableId,
            fullRescan
                ? FloecatConnector.SnapshotEnumerationOptions.full(true, targetSnapshotIds)
                : FloecatConnector.SnapshotEnumerationOptions.incremental(
                    enumerationKnownSnapshotIds, targetSnapshotIds));
    List<FloecatConnector.SnapshotBundle> bundles =
        filterBundlesForMode(
            filterBundlesForSnapshotScope(upstreamBundles, targetSnapshotIds, progress),
            fullRescan,
            includeStats,
            knownSnapshotIds,
            progress);
    IngestCounts ingestCounts =
        ingestMetadataSnapshots(
            ctx,
            tableId,
            connector,
            bundles,
            includeCoreMetadata,
            cancelRequested,
            progress,
            sourceNs,
            sourceTable,
            scanned,
            changed,
            errors,
            snapshotsProcessedBase,
            statsProcessedBase);
    Optional<String> degradedReason = Optional.empty();
    if (captureMode == CaptureMode.METADATA_AND_STATS) {
      List<ReconcileScope.ScopedStatsRequest> effectiveScopedStatsRequests =
          scopedStatsRequestsForSnapshots(
              snapshotIdsFromBundles(bundles), scopedStatsRequestsBySnapshot);
      boolean shouldEnqueueStatsFollowUp =
          !hasScopedStatsRequestFilter || !effectiveScopedStatsRequests.isEmpty();
      degradedReason =
          shouldEnqueueStatsFollowUp
              ? enqueueStatsOnlyCapture(
                  connectorId,
                  scopeNamespaceFq,
                  destTableDisplay,
                  tableId,
                  snapshotIdsFromBundles(bundles),
                  effectiveScopedStatsRequests,
                  defaultColumnSelectors)
              : Optional.empty();
    }
    return new MetadataPassOutcome(ingestCounts, ingestCounts.tableChanged, degradedReason);
  }

  private static String connectorTypeFor(ConnectorConfig.Kind kind) {
    return switch (kind) {
      case ICEBERG -> "iceberg";
      case GLUE -> "glue";
      case DELTA -> "delta";
      case UNITY -> "unity";
      default -> "";
    };
  }

  private boolean maybeIngestSnapshotConstraints(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      FloecatConnector.SnapshotBundle snapshotBundle,
      long snapshotId) {
    if (snapshotId < 0) {
      return false;
    }
    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(sourceNs, sourceTable, tableId, snapshotBundle);
    if (constraints.isEmpty()) {
      return false;
    }
    return backend.putSnapshotConstraints(ctx, tableId, snapshotId, constraints.get());
  }

  private boolean isStatsCaptureCompleteForScope(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests,
      Set<String> defaultColumnSelectors) {
    if (scopedStatsRequests != null && !scopedStatsRequests.isEmpty()) {
      Set<StatsTarget> scopedTargets = decodeTargetSpecsFromRequests(scopedStatsRequests);
      if (!scopedTargets.isEmpty()
          && !backend.statsCapturedForTargets(ctx, tableId, snapshotId, scopedTargets)) {
        return false;
      }
      Set<String> scopedSelectors = selectorsForScopedTableRequests(scopedStatsRequests);
      return scopedSelectors.isEmpty()
          || backend.statsCapturedForColumnSelectors(ctx, tableId, snapshotId, scopedSelectors);
    }
    if (!backend.statsAlreadyCapturedForTargetKind(
        ctx, tableId, snapshotId, StatsTargetKind.STK_TABLE)) {
      return false;
    }
    return defaultColumnSelectors == null
        || defaultColumnSelectors.isEmpty()
        || backend.statsCapturedForColumnSelectors(
            ctx, tableId, snapshotId, defaultColumnSelectors);
  }

  private Set<StatsTarget> decodeTargetSpecsFromRequests(
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests) {
    if (scopedStatsRequests == null || scopedStatsRequests.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<StatsTarget> decodedTargets = new LinkedHashSet<>();
    for (ReconcileScope.ScopedStatsRequest request : scopedStatsRequests) {
      decodedTargets.add(
          ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Invalid scoped stats target spec for table="
                              + request.tableId()
                              + " snapshot="
                              + request.snapshotId()
                              + " spec="
                              + request.targetSpec())));
    }
    return decodedTargets;
  }

  private static TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) {
      return TableFormat.TF_UNSPECIFIED;
    }

    String name = format.name();
    int i = name.indexOf('_');
    String stem = (i >= 0 && i + 1 < name.length()) ? name.substring(i + 1) : name;
    String target = "TF_" + stem;
    try {
      return TableFormat.valueOf(target);
    } catch (IllegalArgumentException ignored) {
      return TableFormat.TF_UNKNOWN;
    }
  }

  private FloecatConnector.TableDescriptor overrideDisplay(
      FloecatConnector.TableDescriptor upstream, String destNamespace, String destTable) {
    if (destNamespace == null && destTable == null) {
      return upstream;
    }

    return new FloecatConnector.TableDescriptor(
        destNamespace != null ? destNamespace : upstream.namespaceFq(),
        destTable != null ? destTable : upstream.tableName(),
        upstream.location(),
        upstream.schemaJson(),
        upstream.partitionKeys(),
        upstream.columnIdAlgorithm(),
        upstream.properties());
  }

  private static String rootCauseMessage(Throwable t) {
    if (t == null) {
      return "unknown error";
    }
    var seen = new HashSet<Throwable>();
    var parts = new ArrayList<String>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      seen.add(cur);
      parts.add(renderThrowable(cur));
      cur = cur.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
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
      throw new IllegalArgumentException(
          "Destination table id " + destinationTableId + " is missing persisted source identity");
    }
    validateSourceConnector(metadata.sourceConnectorId(), connectorId, "destination table id");
    connector.describe(sourceBinding.namespace(), sourceBinding.name());
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
      throw new IllegalArgumentException(
          "Destination view id " + destinationViewId + " is missing persisted source identity");
    }
    validateSourceConnector(metadata.sourceConnectorId(), connectorId, "destination view id");
    connector
        .describeView(sourceBinding.namespace(), sourceBinding.name())
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "View not found: " + sourceBinding.namespace() + "." + sourceBinding.name()));
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
                    new IllegalArgumentException(
                        "Destination table id does not exist: " + tableId.getId()));
    if (metadata.namespaceId() == null || metadata.namespaceId().getId().isBlank()) {
      throw new IllegalArgumentException(
          "Destination table namespace cannot be resolved from id: " + tableId.getId());
    }
    if (metadata.catalogId() == null || metadata.catalogId().getId().isBlank()) {
      throw new IllegalArgumentException(
          "Destination table catalog cannot be resolved from id: " + tableId.getId());
    }
    return metadata;
  }

  private DestinationViewMetadata requiredViewMetadata(ReconcileContext ctx, ResourceId viewId) {
    DestinationViewMetadata metadata =
        backend
            .lookupDestinationViewMetadata(ctx, viewId)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Destination view id does not exist: " + viewId.getId()));
    if (metadata.namespaceId() == null || metadata.namespaceId().getId().isBlank()) {
      throw new IllegalArgumentException(
          "Destination view namespace cannot be resolved from id: " + viewId.getId());
    }
    if (metadata.catalogId() == null || metadata.catalogId().getId().isBlank()) {
      throw new IllegalArgumentException(
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
      throw new IllegalArgumentException(
          "Persisted source connector for " + resourceLabel + " is missing");
    }
    if (!metadataConnectorId.equals(requestedConnectorId)) {
      throw new IllegalArgumentException(
          "Persisted source connector for "
              + resourceLabel
              + " does not match requested connector");
    }
  }

  private static ResourceId destinationResourceId(
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

  private static Map<String, String> sourceIdentityProperties(
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

  private Optional<ResourceId> lookupDestinationNamespaceId(
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

  private Optional<ResourceId> lookupDestinationTableIdByName(
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

  private Optional<ResourceId> lookupDestinationViewIdByName(
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
    return destNamespaceId != null && scope.matchesNamespaceId(destNamespaceId.getId());
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

  private static String renderThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + desc;
    }
    String m = t.getMessage();
    String cls = t.getClass().getSimpleName();
    if (m == null || m.isBlank()) {
      return cls;
    }
    return cls + ": " + m;
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

  private static List<String> namespacePathSegments(String namespaceFq) {
    var parts = split(namespaceFq);
    if (parts.leaf == null || parts.leaf.isBlank()) {
      return parts.parents;
    }
    ArrayList<String> segments = new ArrayList<>(parts.parents.size() + 1);
    segments.addAll(parts.parents);
    segments.add(parts.leaf);
    return List.copyOf(segments);
  }

  private ActiveConnector activeConnector(Connector connector, ResourceId connectorId) {
    if (connector.getState() != ConnectorState.CS_ACTIVE) {
      throw new IllegalStateException("Connector not ACTIVE: " + connectorId.getId());
    }
    ConnectorConfig config = ConnectorConfigMapper.fromProto(connector);
    return new ActiveConnector(
        connector,
        connector.hasSource() ? connector.getSource() : SourceSelector.getDefaultInstance(),
        connector.hasDestination()
            ? connector.getDestination()
            : DestinationTarget.getDefaultInstance(),
        config,
        resolveCredentials(config, connector.getAuth(), connectorId));
  }

  private ActiveConnector activeConnectorForResult(ReconcileContext ctx, ResourceId connectorId) {
    Connector connector;
    try {
      connector = backend.lookupConnector(ctx, connectorId);
    } catch (RuntimeException e) {
      throw new ReconcileFailureException(
          ExecutionResult.FailureKind.CONNECTOR_MISSING,
          "getConnector failed: " + connectorId.getId(),
          e);
    }
    return activeConnector(connector, connectorId);
  }

  private ReconcileContext buildContext(PrincipalContext principal, Optional<String> bearerToken) {
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

  private static Set<String> normalizeSelectors(List<String> in) {
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

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static Map<String, Map<Long, List<ReconcileScope.ScopedStatsRequest>>>
      indexScopedStatsRequestsByTableScope(
          List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests) {
    if (scopedStatsRequests == null || scopedStatsRequests.isEmpty()) {
      return Map.of();
    }
    Map<String, Map<Long, List<ReconcileScope.ScopedStatsRequest>>> out = new LinkedHashMap<>();
    for (ReconcileScope.ScopedStatsRequest request : scopedStatsRequests) {
      out.computeIfAbsent(request.tableId(), ignored -> new LinkedHashMap<>())
          .computeIfAbsent(request.snapshotId(), ignored -> new ArrayList<>())
          .add(request);
    }
    return out;
  }

  private static List<ReconcileScope.ScopedStatsRequest> scopedStatsRequestsForSnapshots(
      Set<Long> snapshotIds,
      Map<Long, List<ReconcileScope.ScopedStatsRequest>> scopedStatsRequestsBySnapshot) {
    if (snapshotIds == null || snapshotIds.isEmpty()) {
      return List.of();
    }
    if (scopedStatsRequestsBySnapshot != null && !scopedStatsRequestsBySnapshot.isEmpty()) {
      List<ReconcileScope.ScopedStatsRequest> requests = new ArrayList<>();
      for (Long snapshotId : snapshotIds) {
        if (snapshotId == null || snapshotId < 0L) {
          continue;
        }
        requests.addAll(scopedStatsRequestsBySnapshot.getOrDefault(snapshotId, List.of()));
      }
      return requests;
    }
    return List.of();
  }

  private static List<ReconcileScope.ScopedStatsRequest> effectiveScopedStatsRequestsForSnapshots(
      String tableId,
      Set<Long> snapshotIds,
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests,
      Set<String> defaultColumnSelectors) {
    if (scopedStatsRequests != null && !scopedStatsRequests.isEmpty()) {
      return dedupeScopedStatsRequests(scopedStatsRequests);
    }
    if (snapshotIds == null || snapshotIds.isEmpty()) {
      return List.of();
    }
    List<String> selectors =
        defaultColumnSelectors == null
            ? List.of()
            : defaultColumnSelectors.stream().sorted().toList();
    String tableTargetSpec =
        ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.encode(
            StatsTargetIdentity.tableTarget());
    return snapshotIds.stream()
        .filter(snapshotId -> snapshotId != null && snapshotId >= 0L)
        .sorted()
        .map(
            snapshotId ->
                new ReconcileScope.ScopedStatsRequest(
                    tableId, snapshotId, tableTargetSpec, selectors))
        .toList();
  }

  private static List<ReconcileScope.ScopedStatsRequest> dedupeScopedStatsRequests(
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests) {
    if (scopedStatsRequests == null || scopedStatsRequests.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, ReconcileScope.ScopedStatsRequest> deduped = new LinkedHashMap<>();
    for (ReconcileScope.ScopedStatsRequest request : scopedStatsRequests) {
      if (request == null) {
        continue;
      }
      List<String> normalizedSelectors =
          normalizeSelectors(request.columnSelectors()).stream().sorted().toList();
      ReconcileScope.ScopedStatsRequest normalizedRequest =
          new ReconcileScope.ScopedStatsRequest(
              request.tableId(), request.snapshotId(), request.targetSpec(), normalizedSelectors);
      String key =
          normalizedRequest.tableId()
              + "|"
              + normalizedRequest.snapshotId()
              + "|"
              + normalizedRequest.targetSpec()
              + "|"
              + String.join(",", normalizedSelectors);
      deduped.putIfAbsent(key, normalizedRequest);
    }
    return List.copyOf(deduped.values());
  }

  private Optional<String> validateScopedStatsRequests(ReconcileScope scope) {
    if (scope == null) {
      return Optional.empty();
    }
    if (!scope.hasStatsRequestFilter()) {
      return Optional.empty();
    }
    for (ReconcileScope.ScopedStatsRequest request : scope.destinationStatsRequests()) {
      if (request.snapshotId() < 0L) {
        return Optional.of(
            "Scoped stats request has invalid snapshot id for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId());
      }
      if (request.tableId().isBlank()) {
        return Optional.of(
            "Scoped stats request is missing table id for snapshot=" + request.snapshotId());
      }
      if (request.targetSpec().isBlank()) {
        return Optional.of(
            "Scoped stats request is missing target spec for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId());
      }
      if (ai.floedb.floecat.stats.identity.StatsTargetScopeCodec.decode(request.targetSpec())
          .isEmpty()) {
        return Optional.of(
            "Scoped stats request has invalid target spec for table="
                + request.tableId()
                + " snapshot="
                + request.snapshotId()
                + " spec="
                + request.targetSpec());
      }
    }
    return Optional.empty();
  }

  private Optional<String> validateStrictTableExecution(
      ReconcileScope scope, ReconcileTableTask task) {
    // Id-scoped table execution is an executor contract, not just a planner convention.
    // The destination identity must come from the table id and the source identity from a
    // concrete task; this path must not enumerate tables or create/derive namespaces by name.
    if (task == null || task.isEmpty()) {
      return Optional.of(
          "Concrete table task is required for destination table id scoped reconcile");
    }
    if (task.sourceNamespace().isBlank() || task.sourceTable().isBlank()) {
      return Optional.of("sourceNamespace and sourceTable are required for single-table reconcile");
    }
    if (task.destinationTableId().isBlank()) {
      return Optional.of("destinationTableId is required for single-table reconcile");
    }
    if (scope != null
        && scope.hasTableFilter()
        && !task.destinationTableId().equals(scope.destinationTableId())) {
      return Optional.of(
          "Connector destination table id "
              + task.destinationTableId()
              + " does not match requested scope");
    }
    if (scope != null && (scope.hasNamespaceFilter() || scope.hasViewFilter())) {
      return Optional.of(
          "Connector destination table id cannot be combined with namespace or view scope");
    }
    return Optional.empty();
  }

  private Optional<String> validateDiscoveryTableExecution(
      ReconcileScope scope, ReconcileTableTask task) {
    if (task == null || task.isEmpty()) {
      return Optional.of("Concrete discovery table task is required");
    }
    if (task.sourceNamespace().isBlank() || task.sourceTable().isBlank()) {
      return Optional.of(
          "sourceNamespace and sourceTable are required for discovery table reconcile");
    }
    if (task.destinationNamespaceId().isBlank()) {
      return Optional.of("destinationNamespaceId is required for discovery table reconcile");
    }
    if (scope != null && (scope.hasTableFilter() || scope.hasViewFilter())) {
      return Optional.of(
          "Discovery table execution cannot be combined with table or view id scope");
    }
    return Optional.empty();
  }

  private static Set<String> selectorsForScopedTableRequests(
      List<ReconcileScope.ScopedStatsRequest> scopedStatsRequests) {
    if (scopedStatsRequests == null || scopedStatsRequests.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> selectors = new LinkedHashSet<>();
    for (ReconcileScope.ScopedStatsRequest request : scopedStatsRequests) {
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
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth == null || auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    var credential = credentialResolver.resolve(connectorId.getAccountId(), connectorId.getId());
    return credential
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private List<SchemaColumn> viewOutputColumns(
      FloecatConnector connector, FloecatConnector.ViewDescriptor view) {
    if (view == null || view.schemaJson() == null || view.schemaJson().isBlank()) {
      return List.of();
    }
    SchemaDescriptor schema =
        schemaMapper.mapRaw(
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            toTableFormat(connector.format()),
            view.schemaJson(),
            Set.of());
    return schema.getColumnsList().stream()
        .filter(SchemaColumn::getLeaf)
        .map(
            c ->
                SchemaColumn.newBuilder()
                    .setName(c.getName())
                    .setNullable(c.getNullable())
                    .setLogicalType(c.getLogicalType())
                    .build())
        .toList();
  }

  public static final class Result {
    // statsProcessed reports successful capture attempts per snapshot. Engines may persist multiple
    // target records for a single successful capture attempt.
    public final long tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        scanned,
        changed,
        errors,
        snapshotsProcessed,
        statsProcessed;
    public final Exception error;
    public final List<String> degradedReasons;
    private final List<String> matchedTableIds;

    public Result(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error) {
      this(tablesScanned, tablesChanged, 0, 0, errors, snapshotsProcessed, statsProcessed, error);
    }

    public Result(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error) {
      this(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          error,
          List.of());
    }

    public Result(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error,
        List<String> degradedReasons) {
      this(
          tablesScanned,
          tablesChanged,
          0,
          0,
          errors,
          snapshotsProcessed,
          statsProcessed,
          error,
          degradedReasons);
    }

    public Result(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error,
        List<String> degradedReasons) {
      this(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          error,
          degradedReasons,
          List.of());
    }

    private Result(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error,
        List<String> degradedReasons,
        List<String> matchedTableIds) {
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.viewsScanned = viewsScanned;
      this.viewsChanged = viewsChanged;
      this.scanned = tablesScanned + viewsScanned;
      this.changed = tablesChanged + viewsChanged;
      this.errors = errors;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.error = error;
      this.degradedReasons =
          degradedReasons == null || degradedReasons.isEmpty()
              ? List.of()
              : List.copyOf(degradedReasons);
      this.matchedTableIds =
          matchedTableIds == null || matchedTableIds.isEmpty()
              ? List.of()
              : matchedTableIds.stream().filter(id -> id != null && !id.isBlank()).toList();
    }

    public boolean ok() {
      return error == null;
    }

    public boolean cancelled() {
      return error instanceof ReconcileCancelledException;
    }

    public boolean degraded() {
      return !degradedReasons.isEmpty();
    }

    public String message() {
      if (!ok()) {
        return rootCauseMessage(error);
      }
      return degraded() ? "DEGRADED: " + String.join("; ", degradedReasons) : "OK";
    }
  }

  private static void ensureNotCancelled(BooleanSupplier cancelRequested) {
    if (cancelRequested != null && cancelRequested.getAsBoolean()) {
      throw new ReconcileCancelledException();
    }
  }

  private static final class ReconcileCancelledException extends RuntimeException {
    private ReconcileCancelledException() {
      super("Cancelled");
    }
  }

  private static final class IngestCounts {
    final long snapshotsProcessed;
    final long statsProcessed;
    final boolean tableChanged;

    private IngestCounts(long snapshotsProcessed, long statsProcessed, boolean tableChanged) {
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.tableChanged = tableChanged;
    }
  }

  private static final class ResultAccumulator {
    private long tablesScanned;
    private long tablesChanged;
    private long viewsScanned;
    private long viewsChanged;
    private long errors;
    private long snapshotsProcessed;
    private long statsProcessed;
    private final ArrayList<String> errorSummaries = new ArrayList<>();
    private final ArrayList<String> degradedReasons = new ArrayList<>();

    private void add(Result result) {
      if (result == null) {
        return;
      }
      tablesScanned += result.tablesScanned;
      tablesChanged += result.tablesChanged;
      viewsScanned += result.viewsScanned;
      viewsChanged += result.viewsChanged;
      errors += result.errors;
      snapshotsProcessed += result.snapshotsProcessed;
      statsProcessed += result.statsProcessed;
      degradedReasons.addAll(result.degradedReasons);
      if (!result.ok()) {
        if (result.errors == 0) {
          errors++;
        }
        errorSummaries.add(rootCauseMessage(result.error));
      }
    }

    private void addError(String summary) {
      errors++;
      errorSummaries.add(summary == null || summary.isBlank() ? "unknown error" : summary);
    }

    private Result cancelled() {
      return new Result(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          new ReconcileCancelledException(),
          List.copyOf(degradedReasons));
    }

    private Result toResult() {
      if (errors == 0 && errorSummaries.isEmpty()) {
        return new Result(
            tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            0,
            snapshotsProcessed,
            statsProcessed,
            null,
            List.copyOf(degradedReasons));
      }
      var summary = new StringBuilder();
      summary.append("Partial failure (errors=").append(errors).append("):");
      for (String errorSummary : errorSummaries) {
        summary.append("\n - ").append(errorSummary);
      }
      return new Result(
          tablesScanned,
          tablesChanged,
          viewsScanned,
          viewsChanged,
          errors,
          snapshotsProcessed,
          statsProcessed,
          new RuntimeException(summary.toString()),
          List.copyOf(degradedReasons));
    }
  }

  private record ActiveConnector(
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

  @FunctionalInterface
  private interface DestinationTableLookup {
    Optional<ResourceId> lookup(String destinationTableDisplayName);
  }

  @FunctionalInterface
  private interface DestinationTableMutation {
    DestinationTableResolution apply(
        FloecatConnector.TableDescriptor upstream,
        String destinationTableDisplayName,
        Optional<ResourceId> candidateTableId);
  }

  private record DestinationTableResolution(
      Optional<ResourceId> tableId, boolean tableMetadataChanged) {
    private static DestinationTableResolution of(
        Optional<ResourceId> tableId, boolean tableMetadataChanged) {
      return new DestinationTableResolution(
          tableId == null ? Optional.empty() : tableId, tableMetadataChanged);
    }

    private static DestinationTableResolution resolved(
        ResourceId tableId, boolean tableMetadataChanged) {
      return of(Optional.of(tableId), tableMetadataChanged);
    }
  }

  private record ResolvedTable(
      String sourceNamespace,
      String sourceTable,
      ResourceId destinationNamespaceId,
      String destinationNamespaceFq,
      String destinationTableDisplayName,
      DestinationTableLookup destinationTableLookup,
      DestinationTableMutation destinationTableMutation) {}

  private record TableExecutionOutcome(
      boolean matchedScope,
      ResourceId destinationTableId,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed,
      long statsProcessed,
      Optional<String> degradedReason) {
    private static TableExecutionOutcome skipped(boolean matchedScope) {
      return new TableExecutionOutcome(matchedScope, null, 0L, 0L, 0L, 0L, Optional.empty());
    }
  }

  private static final class TableExecutionProgress {
    private long tablesScanned;
    private long tablesChanged;
    private long snapshotsProcessed;
    private long statsProcessed;
    private Optional<String> degradedReason = Optional.empty();

    private void observe(
        long tablesScanned, long tablesChanged, long snapshotsProcessed, long statsProcessed) {
      this.tablesScanned = Math.max(this.tablesScanned, tablesScanned);
      this.tablesChanged = Math.max(this.tablesChanged, tablesChanged);
      this.snapshotsProcessed = Math.max(this.snapshotsProcessed, snapshotsProcessed);
      this.statsProcessed = Math.max(this.statsProcessed, statsProcessed);
    }
  }

  private record MetadataPassOutcome(
      IngestCounts ingestCounts, boolean tableChanged, Optional<String> degradedReason) {}
}
