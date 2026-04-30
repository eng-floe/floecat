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
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.ActiveConnector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

@ApplicationScoped
class QueuedReconcileWorkerSupport {
  private static final Logger LOG = Logger.getLogger(QueuedReconcileWorkerSupport.class);
  private static final BooleanSupplier NO_CANCEL = () -> false;
  private static final ProgressListener NO_PROGRESS = (ts, tc, vs, vc, e, sp, stp, m) -> {};

  @FunctionalInterface
  interface ProgressListener {
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

  record PlannedViewMutation(
      ResourceId destinationViewId, ViewSpec viewSpec, String idempotencyKey) {}

  record PlannedViewMutationResult(ExecutionResult result, PlannedViewMutation mutation) {}

  record TableExecutionResult(ExecutionResult result, List<String> matchedTableIds) {}

  @Inject ReconcilerService reconcilerService;
  @Inject ReconcilerBackend backend;
  @Inject LogicalSchemaMapper schemaMapper;

  TableExecutionResult executePlannedTable(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      ReconcileTableTask tableTask,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ReconcileExecutor.ProgressListener progress) {
    ProgressListener progressListener = progress == null ? null : progress::onProgress;
    Result result =
        reconcilePlannedTableExecution(
            principal,
            connectorId,
            fullRescan,
            scopeIn,
            tableTask,
            captureMode,
            bearerToken,
            cancelRequested,
            progressListener);
    return new TableExecutionResult(toExecutionResult(result), result.matchedTableIds());
  }

  PlannedViewMutationResult prepareViewMutation(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scopeIn,
      ReconcileViewTask viewTask,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ReconcileExecutor.ProgressListener progress) {
    // Queue-only mutation preparation: discovery view work may create the destination namespace
    // before returning the view mutation for downstream queued execution.
    ProgressListener progressListener = progress == null ? null : progress::onProgress;
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    Optional<String> invalidScope =
        ReconcilerService.validateScopeCombinations(scope, CaptureMode.METADATA_ONLY, true);
    if (invalidScope.isPresent()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()))),
          null);
    }
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    if (effectiveViewTask.isEmpty()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException("view task is required"))),
          null);
    }
    return effectiveViewTask.discoveryMode()
        ? prepareDiscoveryViewMutation(
            principal,
            connectorId,
            scope,
            effectiveViewTask,
            bearerToken,
            cancelRequested,
            progressListener)
        : prepareStrictViewMutation(
            principal,
            connectorId,
            scope,
            effectiveViewTask,
            bearerToken,
            cancelRequested,
            progressListener);
  }

  private Result reconcilePlannedTableExecution(
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
    Optional<String> invalidScope =
        ReconcilerService.validateScopeCombinations(scope, captureMode, false);
    if (invalidScope.isPresent()) {
      return new Result(0, 0, 0, 0, 1, 0, 0, new IllegalArgumentException(invalidScope.get()));
    }
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    if (effectiveTableTask.isEmpty()) {
      return new Result(
          0,
          0,
          0,
          0,
          1,
          0,
          0,
          new IllegalArgumentException("table task is required for queued table reconcile"));
    }
    return effectiveTableTask.discoveryMode()
        ? reconcileDiscoveryTableTask(
            principal,
            connectorId,
            fullRescan,
            scope,
            effectiveTableTask,
            captureMode,
            bearerToken,
            cancelRequested,
            progress)
        : reconcileSingleTableTask(
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

  private PlannedViewMutationResult prepareStrictViewMutation(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scope,
      ReconcileViewTask viewTask,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    if (viewTask.destinationViewId().isBlank()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "destinationViewId is required for single-view reconcile"))),
          null);
    }
    if (viewTask.sourceNamespace().isBlank() || viewTask.sourceView().isBlank()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "sourceNamespace and sourceView are required for single-view reconcile"))),
          null);
    }
    if (scope.hasViewFilter() && !scope.destinationViewId().equals(viewTask.destinationViewId())) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "Connector destination view id "
                          + viewTask.destinationViewId()
                          + " does not match requested scope"))),
          null);
    }

    ReconcileContext ctx =
        reconcilerService.buildContext(principal, Optional.ofNullable(bearerToken));
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = reconcilerService.activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new PlannedViewMutationResult(
          toExecutionResult(new Result(0, 0, 0, 0, 1, 0, 0, e)), null);
    }

    ResourceId destinationViewId =
        ReconcilerService.destinationResourceId(
            connectorId, ResourceKind.RK_VIEW, viewTask.destinationViewId());
    ReconcilerBackend.DestinationViewMetadata destinationViewMetadata;
    try {
      destinationViewMetadata =
          backend
              .lookupDestinationViewMetadata(ctx, destinationViewId)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Destination view id does not exist: " + viewTask.destinationViewId()));
    } catch (Exception e) {
      return new PlannedViewMutationResult(
          toExecutionResult(new Result(0, 0, 0, 0, 1, 0, 0, e)), null);
    }
    if (destinationViewMetadata.namespaceId() == null
        || destinationViewMetadata.namespaceId().getId().isBlank()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "Destination view namespace cannot be resolved from id: "
                          + viewTask.destinationViewId()))),
          null);
    }
    if (destinationViewMetadata.catalogId() == null
        || destinationViewMetadata.catalogId().getId().isBlank()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "Destination view catalog cannot be resolved from id: "
                          + viewTask.destinationViewId()))),
          null);
    }

    try (FloecatConnector connector =
        reconcilerService.connectorOpener.open(active.resolvedConfig())) {
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

      ResourceId destNamespaceId = destinationViewMetadata.namespaceId();
      if (!scope.matchesNamespaceId(destNamespaceId.getId())) {
        return new PlannedViewMutationResult(
            toExecutionResult(
                new Result(
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
                            + " does not match requested scope"))),
            null);
      }

      ensureNotCancelled(cancelCheck);
      List<SchemaColumn> outputColumns = viewOutputColumns(connector, view);
      if (view.sqlDefinitions().isEmpty() || outputColumns.isEmpty()) {
        return new PlannedViewMutationResult(
            ExecutionResult.success(0, 0, 0, 0, 0, 0, 0, "OK"), null);
      }

      progressOut.onProgress(
          0, 0, 1, 0, 0, 0, 0, "Processing view " + view.namespaceFq() + "." + view.name());
      ViewSpec viewSpec =
          ViewSpec.newBuilder()
              .setCatalogId(destinationViewMetadata.catalogId())
              .setNamespaceId(destNamespaceId)
              .setDisplayName(destinationViewMetadata.displayName())
              .addAllSqlDefinitions(toCatalogSqlDefinitions(view))
              .addAllCreationSearchPath(view.searchPath() != null ? view.searchPath() : List.of())
              .addAllOutputColumns(outputColumns)
              .putAllProperties(
                  ReconcilerService.sourceIdentityProperties(
                      connectorId, view.namespaceFq(), view.name()))
              .build();
      return new PlannedViewMutationResult(
          ExecutionResult.success(0, 0, 1, 0, 0, 0, 0, "OK"),
          new PlannedViewMutation(destinationViewId, viewSpec, ""));
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new PlannedViewMutationResult(
            ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled"), null);
      }
      return new PlannedViewMutationResult(
          toExecutionResult(new Result(0, 0, 1, 0, 1, 0, 0, e)), null);
    }
  }

  private PlannedViewMutationResult prepareDiscoveryViewMutation(
      PrincipalContext principal,
      ResourceId connectorId,
      ReconcileScope scope,
      ReconcileViewTask viewTask,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    if (viewTask.sourceNamespace().isBlank() || viewTask.sourceView().isBlank()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "sourceNamespace and sourceView are required for discovery view reconcile"))),
          null);
    }
    if (scope.hasTableFilter() || scope.hasViewFilter()) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
                  0,
                  0,
                  0,
                  0,
                  1,
                  0,
                  0,
                  new IllegalArgumentException(
                      "Discovery view execution cannot be combined with table or view id scope"))),
          null);
    }

    ReconcileContext ctx =
        reconcilerService.buildContext(principal, Optional.ofNullable(bearerToken));
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = reconcilerService.activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new PlannedViewMutationResult(
          toExecutionResult(new Result(0, 0, 0, 0, 1, 0, 0, e)), null);
    }

    ResourceId destNamespaceId =
        resolveOrCreateDiscoveryNamespaceId(
            ctx,
            connectorId,
            active.source(),
            active.destination(),
            viewTask.destinationNamespaceId());
    if (!scope.matchesNamespaceId(destNamespaceId.getId())) {
      return new PlannedViewMutationResult(
          toExecutionResult(
              new Result(
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
                          + " does not match requested scope"))),
          null);
    }
    ResourceId destCatalogId = active.destination().getCatalogId();
    String destNsFq = reconcilerService.resolveNamespaceFq(ctx, destNamespaceId);
    String displayName =
        viewTask.destinationViewDisplayName().isBlank()
            ? viewTask.sourceView()
            : viewTask.destinationViewDisplayName();

    try (FloecatConnector connector =
        reconcilerService.connectorOpener.open(active.resolvedConfig())) {
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
        return new PlannedViewMutationResult(
            ExecutionResult.success(0, 0, 0, 0, 0, 0, 0, "OK"), null);
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
                  ReconcilerService.sourceIdentityProperties(
                      connectorId, view.namespaceFq(), view.name()))
              .build();
      Optional<ResourceId> existingViewId =
          !blank(viewTask.destinationViewId())
              ? Optional.of(
                  ReconcilerService.destinationResourceId(
                      connectorId, ResourceKind.RK_VIEW, viewTask.destinationViewId()))
              : reconcilerService.lookupDestinationViewIdByName(
                  ctx, destCatalogId, destNamespaceId, destNsFq, displayName);
      return new PlannedViewMutationResult(
          ExecutionResult.success(0, 0, 1, 0, 0, 0, 0, "OK"),
          existingViewId
              .map(viewId -> new PlannedViewMutation(viewId, viewSpec, ""))
              .orElseGet(
                  () ->
                      new PlannedViewMutation(
                          null,
                          viewSpec,
                          "namespace-id:"
                              + destNamespaceId.getId()
                              + "|view-name:"
                              + displayName)));
    } catch (Exception e) {
      if (e instanceof ReconcileCancelledException) {
        return new PlannedViewMutationResult(
            ExecutionResult.cancelled(0, 0, 0, 0, 0, 0, 0, "Cancelled"), null);
      }
      return new PlannedViewMutationResult(
          toExecutionResult(new Result(0, 0, 1, 0, 1, 0, 0, e)), null);
    }
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

    ReconcileContext ctx =
        reconcilerService.buildContext(principal, Optional.ofNullable(bearerToken));
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = reconcilerService.activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destinationTableId =
        ReconcilerService.destinationResourceId(
            connectorId, ResourceKind.RK_TABLE, tableTask.destinationTableId());
    ReconcilerBackend.DestinationTableMetadata tableMetadata;
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

    SourceSelector source = active.source();
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
    String destNsFq = reconcilerService.resolveNamespaceFq(ctx, destNamespaceId);
    String destTableDisplay =
        tableMetadata.displayName() == null || tableMetadata.displayName().isBlank()
            ? tableTask.destinationTableDisplayName()
            : tableMetadata.displayName();

    try (FloecatConnector connector =
        reconcilerService.connectorOpener.open(active.resolvedConfig())) {
      boolean includeCoreMetadata =
          ReconcilerService.includesMetadata(captureMode)
              && !ReconcilerService.isCaptureOnlyConnector(active.connector());
      TableExecutionOutcome outcome =
          executeResolvedTable(
              ctx,
              connectorId,
              connector,
              fullRescan,
              includeCoreMetadata,
              captureMode,
              scope,
              new ResolvedTable(
                  tableTask.sourceNamespace(),
                  tableTask.sourceTable(),
                  destNamespaceId,
                  destTableDisplay,
                  ignoredDisplay -> Optional.of(destinationTableId),
                  (upstream, ignoredDisplay, ignoredCandidate) -> {
                    boolean tableMetadataChanged = false;
                    if (ReconcilerService.allowsTableMetadataMutation(
                        active.connector(), captureMode)) {
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
              ReconcilerService.normalizeSelectors(source.getColumnsList()),
              cancelCheck,
              trackingProgress,
              tableProgress,
              0,
              0,
              0,
              0,
              0);
      if (!ReconcilerService.includesMetadata(captureMode)
          && !scope.destinationCaptureRequests().isEmpty()
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
                "No tables matched scoped capture requests: "
                    + ReconcilerService.indexScopedCaptureRequestsByTableScope(
                            scope.destinationCaptureRequests())
                        .keySet()
                        .stream()
                        .sorted()
                        .collect(Collectors.joining(", "))));
      }
      if (outcome.errorReason().isPresent()) {
        return tableFailureResult(outcome);
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

    ReconcileContext ctx =
        reconcilerService.buildContext(principal, Optional.ofNullable(bearerToken));
    BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final ActiveConnector active;
    try {
      active = reconcilerService.activeConnectorForResult(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(0, 0, 0, 0, 1, 0, 0, e);
    }

    ResourceId destNamespaceId =
        resolveOrCreateDiscoveryNamespaceId(
            ctx,
            connectorId,
            active.source(),
            active.destination(),
            tableTask.destinationNamespaceId());
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

    String destNsFq = reconcilerService.resolveNamespaceFq(ctx, destNamespaceId);
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

    try (FloecatConnector connector =
        reconcilerService.connectorOpener.open(active.resolvedConfig())) {
      ResourceId destCatalogId = active.destination().getCatalogId();
      Set<String> defaultColumnSelectors =
          ReconcilerService.normalizeSelectors(active.source().getColumnsList());
      boolean includeCoreMetadata =
          ReconcilerService.includesMetadata(captureMode)
              && !ReconcilerService.isCaptureOnlyConnector(active.connector());
      TableExecutionOutcome outcome =
          executeResolvedTable(
              ctx,
              connectorId,
              connector,
              fullRescan,
              includeCoreMetadata,
              captureMode,
              scope,
              new ResolvedTable(
                  tableTask.sourceNamespace(),
                  tableTask.sourceTable(),
                  destNamespaceId,
                  displayName,
                  destTableDisplay ->
                      !blank(tableTask.destinationTableId())
                          ? Optional.of(
                              ReconcilerService.destinationResourceId(
                                  connectorId,
                                  ResourceKind.RK_TABLE,
                                  tableTask.destinationTableId()))
                          : reconcilerService.lookupDestinationTableIdByName(
                              ctx, destCatalogId, destNamespaceId, destNsFq, destTableDisplay),
                  (upstream, destTableDisplay, existingTableId) -> {
                    FloecatConnector.TableDescriptor effective =
                        overrideDisplay(upstream, destNsFq, destTableDisplay);
                    if (!ReconcilerService.allowsTableMetadataMutation(
                        active.connector(), captureMode)) {
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
      if (outcome.errorReason().isPresent()) {
        return tableFailureResult(outcome);
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

  private TableExecutionOutcome executeResolvedTable(
      ReconcileContext ctx,
      ResourceId connectorId,
      FloecatConnector connector,
      boolean fullRescan,
      boolean includeCoreMetadata,
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
      return TableExecutionOutcome.skipped();
    }
    if (candidateTableId.isPresent()
        && !scope.acceptsTable(
            table.destinationNamespaceId().getId(), candidateTableId.get().getId())) {
      return TableExecutionOutcome.skipped();
    }
    if (!ReconcilerService.includesMetadata(captureMode) && candidateTableId.isEmpty()) {
      LOG.debugf(
          "Skipping capture-only reconcile for %s.%s because destination table was not found",
          table.sourceNamespace(), table.sourceTable());
      return TableExecutionOutcome.skipped();
    }

    DestinationTableResolution destinationTable =
        table.destinationTableMutation().apply(upstream, destTableDisplay, candidateTableId);
    if (destinationTable.tableId().isEmpty()) {
      LOG.debugf(
          "Skipping capture-only reconcile for %s.%s because destination table was not found",
          table.sourceNamespace(), table.sourceTable());
      return TableExecutionOutcome.skipped();
    }
    ResourceId tableId = destinationTable.tableId().get();
    if (!scope.acceptsTable(table.destinationNamespaceId().getId(), tableId.getId())) {
      return TableExecutionOutcome.skipped();
    }

    Map<String, Map<Long, List<ReconcileScope.ScopedCaptureRequest>>> scopedCaptureRequestsByTable =
        ReconcilerService.indexScopedCaptureRequestsByTableScope(
            scope.destinationCaptureRequests());
    Map<Long, List<ReconcileScope.ScopedCaptureRequest>> tableScopedCaptureRequestsBySnapshot =
        scopedCaptureRequestsByTable.getOrDefault(tableId.getId(), Map.of());
    boolean hasScopedCaptureRequestFilter = !scopedCaptureRequestsByTable.isEmpty();
    boolean tableHasScopedCaptureRequests = !tableScopedCaptureRequestsBySnapshot.isEmpty();
    if (!ReconcilerService.includesMetadata(captureMode)
        && hasScopedCaptureRequestFilter
        && !tableHasScopedCaptureRequests) {
      return TableExecutionOutcome.skipped();
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
        "Processing table "
            + table.sourceNamespace()
            + "."
            + table.sourceTable()
            + (includeCoreMetadata ? " (metadata)" : " (capture)"));

    Set<Long> targetSnapshotIds =
        !tableScopedCaptureRequestsBySnapshot.isEmpty()
            ? tableScopedCaptureRequestsBySnapshot.keySet()
            : Set.of();
    Set<Long> knownSnapshotIds = fullRescan ? Set.of() : backend.existingSnapshotIds(ctx, tableId);
    Set<Long> enumerationKnownSnapshotIds =
        reconcilerService.enumerationKnownSnapshotIds(
            ctx,
            tableId,
            fullRescan,
            knownSnapshotIds,
            ReconcileCapturePolicy.empty(),
            tableScopedCaptureRequestsBySnapshot,
            defaultColumnSelectors);

    MetadataPassOutcome outcome =
        processMetadataPass(
            ctx,
            tableId,
            connector,
            table.sourceNamespace(),
            table.sourceTable(),
            fullRescan,
            includeCoreMetadata,
            knownSnapshotIds,
            enumerationKnownSnapshotIds,
            targetSnapshotIds,
            cancelRequested,
            progress,
            tablesScannedBase + tablesScanned,
            tablesChangedBase,
            errors,
            snapshotsProcessedBase,
            statsProcessedBase);
    long snapshotsProcessed = outcome.ingestCounts().snapshotsProcessed;
    long statsProcessed = 0L;
    long tablesChanged =
        destinationTable.tableMetadataChanged() || outcome.tableChanged() ? 1L : 0L;
    if (progressState != null) {
      progressState.observe(
          tablesScannedBase + tablesScanned,
          tablesChangedBase + tablesChanged,
          snapshotsProcessedBase + snapshotsProcessed,
          statsProcessedBase + statsProcessed);
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
        tableId,
        tablesScanned,
        tablesChanged,
        snapshotsProcessed,
        statsProcessed,
        Optional.empty(),
        Optional.empty());
  }

  private ResourceId resolveOrCreateDiscoveryNamespaceId(
      ReconcileContext ctx,
      ResourceId connectorId,
      SourceSelector source,
      DestinationTarget destination,
      String destinationNamespaceId) {
    if (!blank(destinationNamespaceId)) {
      return ReconcilerService.destinationResourceId(
          connectorId, ResourceKind.RK_NAMESPACE, destinationNamespaceId);
    }
    ResourceId destCatalogId = destination.getCatalogId();
    String destNamespaceFq =
        destination.hasNamespaceId()
            ? reconcilerService.resolveNamespaceFq(ctx, destination.getNamespaceId())
            : (destination.hasNamespace()
                    && !destination.getNamespace().getSegmentsList().isEmpty())
                ? fq(destination.getNamespace().getSegmentsList())
                : fq(source.getNamespace().getSegmentsList());
    return reconcilerService
        .lookupDestinationNamespaceId(ctx, destCatalogId, destination, destNamespaceFq)
        .orElseGet(() -> ensureNamespace(ctx, destCatalogId, destNamespaceFq));
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
    return backend.ensureNamespace(ctx, catalogId, NameRefNormalizer.normalize(nameRef));
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
    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(ReconcilerService.namespacePathSegments(landingView.namespaceFq()))
            .setName(landingView.tableName())
            .build();
    return backend.ensureTable(
        ctx,
        destNamespaceId,
        NameRefNormalizer.normalize(tableRef),
        tableSpecDescriptor(
            landingView, format, connectorRid, connectorUri, sourceNsFq, sourceTable));
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
            .addAllPath(ReconcilerService.namespacePathSegments(landingView.namespaceFq()))
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

  private boolean ensureSnapshot(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector.SnapshotBundle snapshotBundle,
      Snapshot existing) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return false;
    }
    Optional<Snapshot> snapshot = buildSnapshot(ctx, tableId, snapshotBundle, existing);
    snapshot.ifPresent(candidate -> backend.ingestSnapshot(ctx, tableId, candidate));
    return snapshot.isPresent();
  }

  private IngestCounts ingestMetadataSnapshots(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      List<FloecatConnector.SnapshotBundle> bundles,
      Set<Long> knownSnapshotIds,
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
      if (includeCoreMetadata) {
        Snapshot existingSnapshot =
            knownSnapshotIds.contains(snapshotId)
                ? backend.fetchSnapshot(ctx, tableId, snapshotId).orElse(null)
                : null;
        progress.onProgress(
            scanned,
            changed,
            0,
            0,
            errors,
            snapshotsProcessedBase + snapshotsProcessed,
            statsProcessedBase,
            "Processing snapshot " + snapshotId + " for " + sourceNs + "." + sourceTable);
        boolean snapshotChanged = ensureSnapshot(ctx, tableId, snapshotBundle, existingSnapshot);
        boolean constraintsChanged =
            maybeIngestSnapshotConstraints(
                ctx, tableId, connector, sourceNs, sourceTable, snapshotBundle, snapshotId);
        if (snapshotChanged || constraintsChanged) {
          snapshotsProcessed++;
        }
        tableChanged = tableChanged || snapshotChanged || constraintsChanged;
      }
    }
    return new IngestCounts(snapshotsProcessed, tableChanged);
  }

  private MetadataPassOutcome processMetadataPass(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      boolean fullRescan,
      boolean includeCoreMetadata,
      Set<Long> knownSnapshotIds,
      Set<Long> enumerationKnownSnapshotIds,
      Set<Long> targetSnapshotIds,
      BooleanSupplier cancelRequested,
      ProgressListener progress,
      long scanned,
      long changed,
      long errors,
      long snapshotsProcessedBase,
      long statsProcessedBase) {
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
            includeCoreMetadata,
            knownSnapshotIds,
            progress);
    IngestCounts ingestCounts =
        ingestMetadataSnapshots(
            ctx,
            tableId,
            connector,
            bundles,
            knownSnapshotIds,
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
    return new MetadataPassOutcome(ingestCounts, ingestCounts.tableChanged);
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

  private static List<ViewSqlDefinition> toCatalogSqlDefinitions(
      FloecatConnector.ViewDescriptor view) {
    return view.sqlDefinitions().stream()
        .map(
            def ->
                ViewSqlDefinition.newBuilder().setSql(def.sql()).setDialect(def.dialect()).build())
        .toList();
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

  private static FloecatConnector.TableDescriptor overrideDisplay(
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

  private static Result tableFailureResult(TableExecutionOutcome outcome) {
    List<String> matchedTableIds =
        outcome.destinationTableId() == null || outcome.destinationTableId().getId().isBlank()
            ? List.of()
            : List.of(outcome.destinationTableId().getId());
    return new Result(
        outcome.tablesScanned(),
        outcome.tablesChanged(),
        0,
        0,
        1,
        outcome.snapshotsProcessed(),
        outcome.statsProcessed(),
        new RuntimeException(outcome.errorReason().orElse("unknown error")),
        outcome.degradedReason().map(List::of).orElseGet(List::of),
        matchedTableIds);
  }

  private static ExecutionResult toExecutionResult(Result result) {
    if (result == null) {
      return ExecutionResult.failure(
          0, 0, 0, 0, 1, 0, 0, "unknown reconcile result", new IllegalStateException());
    }
    if (result.cancelled()) {
      return ExecutionResult.cancelled(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message());
    }
    if (!result.ok()) {
      return ExecutionResult.failure(
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          failureKindOf(result.error),
          retryDispositionOf(result.error),
          result.message(),
          result.error);
    }
    return ExecutionResult.success(
        result.tablesScanned,
        result.tablesChanged,
        result.viewsScanned,
        result.viewsChanged,
        result.errors,
        result.snapshotsProcessed,
        result.statsProcessed,
        result.message());
  }

  private static ExecutionResult.FailureKind failureKindOf(Exception error) {
    if (error instanceof ReconcileFailureException failure) {
      return failure.failureKind();
    }
    return ExecutionResult.FailureKind.INTERNAL;
  }

  private static ExecutionResult.RetryDisposition retryDispositionOf(Exception error) {
    if (error instanceof ReconcileFailureException failure) {
      return failure.retryDisposition();
    }
    return ExecutionResult.RetryDisposition.RETRYABLE;
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
      boolean includeCoreMetadata,
      Set<Long> existingSnapshotIds,
      ProgressListener progress) {
    if (bundles == null || bundles.isEmpty() || fullRescan) {
      return bundles == null ? List.of() : bundles;
    }
    if (includeCoreMetadata || existingSnapshotIds == null || existingSnapshotIds.isEmpty()) {
      return bundles;
    }

    List<FloecatConnector.SnapshotBundle> filtered = new ArrayList<>(bundles.size());
    int skipped = 0;
    for (FloecatConnector.SnapshotBundle bundle : bundles) {
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

  private static void ensureNotCancelled(BooleanSupplier cancelRequested) {
    if (cancelRequested != null && cancelRequested.getAsBoolean()) {
      throw new ReconcileCancelledException();
    }
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
    String message = t.getMessage();
    String className = t.getClass().getSimpleName();
    if (message == null || message.isBlank()) {
      return className;
    }
    return className + ": " + message;
  }

  private static final class ReconcileCancelledException extends RuntimeException {
    private ReconcileCancelledException() {
      super("Cancelled");
    }
  }

  private static final class Result {
    public final long tablesScanned;
    public final long tablesChanged;
    public final long viewsScanned;
    public final long viewsChanged;
    public final long scanned;
    public final long changed;
    public final long errors;
    public final long snapshotsProcessed;
    public final long statsProcessed;
    public final Exception error;
    public final List<String> degradedReasons;
    private final List<String> matchedTableIds;

    private Result(
        long tablesScanned,
        long tablesChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error) {
      this(tablesScanned, tablesChanged, 0, 0, errors, snapshotsProcessed, statsProcessed, error);
    }

    private Result(
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

    private Result(
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

    private Result(
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

    private boolean ok() {
      return error == null;
    }

    private boolean cancelled() {
      return error instanceof ReconcileCancelledException;
    }

    private List<String> matchedTableIds() {
      return matchedTableIds;
    }

    private String message() {
      if (!ok()) {
        return rootCauseMessage(error);
      }
      return degradedReasons.isEmpty() ? "OK" : "DEGRADED: " + String.join("; ", degradedReasons);
    }
  }

  private static Optional<String> validateStrictTableExecution(
      ReconcileScope scope, ReconcileTableTask task) {
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

  private static Optional<String> validateDiscoveryTableExecution(
      ReconcileScope scope, ReconcileTableTask task) {
    if (task == null || task.isEmpty()) {
      return Optional.of("Concrete discovery table task is required");
    }
    if (task.sourceNamespace().isBlank() || task.sourceTable().isBlank()) {
      return Optional.of(
          "sourceNamespace and sourceTable are required for discovery table reconcile");
    }
    if (scope != null && (scope.hasTableFilter() || scope.hasViewFilter())) {
      return Optional.of(
          "Discovery table execution cannot be combined with table or view id scope");
    }
    return Optional.empty();
  }

  private static String fq(List<String> segments) {
    return String.join(".", segments);
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static final class IngestCounts {
    final long snapshotsProcessed;
    final boolean tableChanged;

    private IngestCounts(long snapshotsProcessed, boolean tableChanged) {
      this.snapshotsProcessed = snapshotsProcessed;
      this.tableChanged = tableChanged;
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
      String destinationTableDisplayName,
      DestinationTableLookup destinationTableLookup,
      DestinationTableMutation destinationTableMutation) {}

  private record TableExecutionOutcome(
      ResourceId destinationTableId,
      long tablesScanned,
      long tablesChanged,
      long snapshotsProcessed,
      long statsProcessed,
      Optional<String> degradedReason,
      Optional<String> errorReason) {
    private static TableExecutionOutcome skipped() {
      return new TableExecutionOutcome(null, 0L, 0L, 0L, 0L, Optional.empty(), Optional.empty());
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

  private record MetadataPassOutcome(IngestCounts ingestCounts, boolean tableChanged) {}
}
