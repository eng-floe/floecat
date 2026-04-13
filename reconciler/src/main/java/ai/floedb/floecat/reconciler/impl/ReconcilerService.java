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
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
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
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor.ExecutionResult;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureControlPlane;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
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
  private static final ProgressListener NO_PROGRESS = (s, c, e, sp, stp, m) -> {};

  public enum CaptureMode {
    METADATA_ONLY,
    METADATA_AND_STATS,
    STATS_ONLY
  }

  @FunctionalInterface
  public interface ProgressListener {
    void onProgress(
        long scanned,
        long changed,
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
    return reconcile(
        principal,
        connectorId,
        fullRescan,
        scopeIn,
        captureMode,
        bearerToken,
        NO_CANCEL,
        NO_PROGRESS);
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode,
      String bearerToken,
      BooleanSupplier cancelRequested,
      ProgressListener progress) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    long scanned = 0;
    long changed = 0;
    long errors = 0;
    long snapshotsProcessed = 0;
    long statsProcessed = 0;
    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    String corr = ctx.correlationId();

    final ArrayList<String> errSummaries = new ArrayList<>();
    final BooleanSupplier cancelCheck = cancelRequested == null ? NO_CANCEL : cancelRequested;
    final ProgressListener progressOut = progress == null ? NO_PROGRESS : progress;

    final Connector stored;
    try {
      stored = backend.lookupConnector(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(
          0,
          0,
          1,
          0,
          0,
          new ReconcileFailureException(
              ExecutionResult.FailureKind.CONNECTOR_MISSING,
              "getConnector failed: " + connectorId.getId(),
              e));
    }

    if (stored.getState() != ConnectorState.CS_ACTIVE) {
      return new Result(
          0, 0, 1, 0, 0, new IllegalStateException("Connector not ACTIVE: " + connectorId.getId()));
    }

    DestinationTarget.Builder destB =
        stored.hasDestination()
            ? stored.getDestination().toBuilder()
            : DestinationTarget.newBuilder();

    final SourceSelector source =
        stored.hasSource() ? stored.getSource() : SourceSelector.getDefaultInstance();
    final DestinationTarget dest =
        stored.hasDestination() ? stored.getDestination() : DestinationTarget.getDefaultInstance();

    var cfg = ConnectorConfigMapper.fromProto(stored);
    var resolved = resolveCredentials(cfg, stored.getAuth(), connectorId);

    try (FloecatConnector connector = connectorOpener.open(resolved)) {
      ensureNotCancelled(cancelCheck);
      final ResourceId destCatalogId = dest.getCatalogId();

      final String sourceNsFq;
      if (source.hasNamespace() && !source.getNamespace().getSegmentsList().isEmpty()) {
        sourceNsFq = fq(source.getNamespace().getSegmentsList());
      } else {
        return new Result(
            0, 0, 1, 0, 0, new IllegalArgumentException("connector.source.namespace is required"));
      }

      final String destNsFq;
      final ResourceId destNamespaceId;

      if (dest.hasNamespaceId()) {
        destNamespaceId = dest.getNamespaceId();
        destNsFq = resolveNamespaceFq(ctx, destNamespaceId);
      } else {
        destNsFq =
            (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
                ? fq(dest.getNamespace().getSegmentsList())
                : sourceNsFq;

        destNamespaceId = ensureNamespace(ctx, destCatalogId, destNsFq);
      }

      String scopeNamespaceFq = destNsFq != null ? destNsFq : sourceNsFq;
      if (!scope.matchesNamespace(scopeNamespaceFq)) {
        return new Result(
            0,
            0,
            1,
            0,
            0,
            new IllegalArgumentException(
                "Connector destination namespace "
                    + scopeNamespaceFq
                    + " does not match requested scope"));
      }

      if (!destB.hasNamespaceId()) {
        destB.setNamespaceId(destNamespaceId);
        destB.clearNamespace();
      }

      final List<String> tables =
          (source.getTable() != null && !source.getTable().isBlank())
              ? List.of(source.getTable())
              : connector.listTables(sourceNsFq);

      if (tables.isEmpty()) {
        // A views-only namespace is valid; let the view pass proceed.
        LOG.debugf(
            "No tables found in source namespace %s; connector may have views only.", sourceNsFq);
      }

      final boolean singleTableMode = tables.size() == 1;
      final Set<String> includeSelectors = effectiveSelectors(scope, source);

      final String tableDisplayHint =
          (dest.getTableDisplayName() != null && !dest.getTableDisplayName().isBlank())
              ? dest.getTableDisplayName()
              : null;

      boolean matchedScope = false;
      for (String srcTable : tables) {
        try {
          ensureNotCancelled(cancelCheck);
          var upstream = connector.describe(sourceNsFq, srcTable);

          final String destTableDisplay =
              (tableDisplayHint != null) ? tableDisplayHint : upstream.tableName();

          if (!scope.acceptsTable(scopeNamespaceFq, destTableDisplay)) {
            continue;
          }

          matchedScope = true;
          scanned++;
          progressOut.onProgress(
              scanned,
              changed,
              errors,
              snapshotsProcessed,
              statsProcessed,
              "Processing table " + sourceNsFq + "." + srcTable + " (metadata)");

          var effective = overrideDisplay(upstream, destNsFq, destTableDisplay);

          var destTableIdOpt =
              resolveDestinationTableId(
                  ctx,
                  captureMode,
                  destCatalogId,
                  destNamespaceId,
                  dest,
                  effective,
                  connector.format(),
                  stored.getResourceId(),
                  cfg.uri(),
                  sourceNsFq,
                  srcTable);
          if (destTableIdOpt.isEmpty()) {
            LOG.debugf(
                "Skipping stats-only reconcile for %s.%s because destination table was not found",
                sourceNsFq, srcTable);
            continue;
          }
          var destTableId = destTableIdOpt.get();

          if (singleTableMode && !destB.hasTableId()) {
            destB.setTableId(destTableId);
            destB.clearTableDisplayName();
          }

          boolean includeCoreMetadata =
              captureMode == CaptureMode.METADATA_ONLY
                  || captureMode == CaptureMode.METADATA_AND_STATS;
          boolean includeStats = captureMode == CaptureMode.STATS_ONLY;
          Set<Long> targetSnapshotIds = Set.of();
          Set<Long> knownSnapshotIds =
              fullRescan ? Set.of() : backend.existingSnapshotIds(ctx, destTableId);
          Predicate<Long> statsCompleteForSnapshot =
              snapshotCompletenessChecker(ctx, destTableId, includeSelectors);
          Set<Long> enumerationKnownSnapshotIds =
              knownSnapshotIdsForEnumeration(
                  fullRescan, includeStats, knownSnapshotIds, statsCompleteForSnapshot);
          if (captureMode == CaptureMode.STATS_ONLY) {
            IngestCounts ingestCounts =
                captureStatsOnlyViaControlPlane(
                    ctx,
                    destTableId,
                    connector,
                    sourceNsFq,
                    srcTable,
                    fullRescan,
                    knownSnapshotIds,
                    enumerationKnownSnapshotIds,
                    targetSnapshotIds,
                    statsCompleteForSnapshot,
                    includeSelectors,
                    cancelCheck,
                    progressOut,
                    scanned,
                    changed,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    resolved.kind());
            snapshotsProcessed += ingestCounts.snapshotsProcessed;
            statsProcessed += ingestCounts.statsProcessed;
            progressOut.onProgress(
                scanned,
                changed,
                errors,
                snapshotsProcessed,
                statsProcessed,
                "Finished table " + sourceNsFq + "." + srcTable);
            continue;
          }
          var upstreamBundles =
              connector.enumerateSnapshots(
                  sourceNsFq,
                  srcTable,
                  destTableId,
                  fullRescan
                      ? FloecatConnector.SnapshotEnumerationOptions.full(true, targetSnapshotIds)
                      : FloecatConnector.SnapshotEnumerationOptions.incremental(
                          enumerationKnownSnapshotIds, targetSnapshotIds));
          var bundles =
              filterBundlesForMode(
                  upstreamBundles, fullRescan, includeStats, knownSnapshotIds, progressOut);
          IngestCounts ingestCounts =
              ingestAllSnapshotsAndStatsFiltered(
                  ctx,
                  destTableId,
                  connector,
                  resolved.kind(),
                  bundles,
                  includeCoreMetadata,
                  includeStats,
                  fullRescan,
                  statsCompleteForSnapshot,
                  includeSelectors,
                  cancelCheck,
                  progressOut,
                  sourceNsFq,
                  srcTable,
                  scanned,
                  changed,
                  errors,
                  snapshotsProcessed,
                  statsProcessed);
          snapshotsProcessed += ingestCounts.snapshotsProcessed;
          statsProcessed += ingestCounts.statsProcessed;
          if (tableChanged(bundles)) {
            changed++;
          }
          if (captureMode == CaptureMode.METADATA_AND_STATS) {
            enqueueStatsOnlyCapture(
                connectorId,
                scopeNamespaceFq,
                destTableDisplay,
                includeSelectors,
                snapshotIdsFromBundles(bundles));
          }
          progressOut.onProgress(
              scanned,
              changed,
              errors,
              snapshotsProcessed,
              statsProcessed,
              "Finished table " + sourceNsFq + "." + srcTable);
        } catch (Exception e) {
          if (e instanceof ReconcileCancelledException) {
            return new Result(scanned, changed, errors, snapshotsProcessed, statsProcessed, e);
          }
          errors++;
          LOG.errorf(
              "Table sync failed: ns=%s table=%s.%s — %s",
              scopeNamespaceFq, sourceNsFq, srcTable, rootCauseMessage(e));
          errSummaries.add(
              "ns="
                  + scopeNamespaceFq
                  + " table="
                  + sourceNsFq
                  + "."
                  + srcTable
                  + " : "
                  + rootCauseMessage(e));
        }
      }

      if (!matchedScope && scope.hasTableFilter()) {
        // Record the miss as an error but do NOT return — the view pass must still run.
        // A table filter is table-scoped; views are always reconciled regardless.
        errors++;
        errSummaries.add("No tables matched scope: " + scope.destinationTableDisplayName());
      }

      // View reconciliation pass — runs after tables; scope filter is table-only so all views sync.
      // Note: existing views are not updated on re-sync (create-only idempotent operation).
      List<FloecatConnector.ViewDescriptor> viewDescriptors;
      try {
        viewDescriptors = connector.listViewDescriptors(sourceNsFq);
      } catch (Exception e) {
        viewDescriptors = List.of();
        errors++;
        errSummaries.add("listViewDescriptors(" + sourceNsFq + "): " + rootCauseMessage(e));
      }
      for (FloecatConnector.ViewDescriptor view : viewDescriptors) {
        try {
          if (view.sql() == null || view.sql().isBlank()) {
            continue;
          }

          List<SchemaColumn> outputColumns;
          if (view.schemaJson() != null && !view.schemaJson().isBlank()) {
            SchemaDescriptor schema =
                schemaMapper.mapRaw(
                    ColumnIdAlgorithm.CID_PATH_ORDINAL,
                    toTableFormat(connector.format()),
                    view.schemaJson(),
                    Set.of());
            outputColumns =
                schema.getColumnsList().stream()
                    .filter(SchemaColumn::getLeaf)
                    .map(
                        c ->
                            SchemaColumn.newBuilder()
                                .setName(c.getName())
                                .setNullable(c.getNullable())
                                .setLogicalType(c.getLogicalType())
                                .build())
                    .toList();
          } else {
            outputColumns = List.of();
          }
          if (outputColumns.isEmpty()) {
            continue;
          }

          scanned++;
          ViewSpec viewSpec =
              ViewSpec.newBuilder()
                  .setCatalogId(destCatalogId)
                  .setNamespaceId(destNamespaceId)
                  .setDisplayName(view.name())
                  .setSql(view.sql())
                  .setDialect(view.dialect() != null ? view.dialect() : "")
                  .addAllCreationSearchPath(
                      view.searchPath() != null ? view.searchPath() : List.of())
                  .addAllOutputColumns(outputColumns)
                  .build();
          String idempotencyKey = destNsFq + "." + view.name();
          ResourceId viewId = backend.ensureView(ctx, viewSpec, idempotencyKey);
          // Only count genuinely new views; ALREADY_EXISTS returns getDefaultInstance() (empty id).
          if (!viewId.getId().isEmpty()) {
            changed++;
          }
        } catch (Exception e) {
          errors++;
          errSummaries.add(
              "dest-ns="
                  + scopeNamespaceFq
                  + " source-view="
                  + sourceNsFq
                  + "."
                  + view.name()
                  + " : "
                  + rootCauseMessage(e));
        }
      }

      DestinationTarget updated = destB.build();
      if (!updated.equals(stored.getDestination())) {
        try {
          backend.updateConnectorDestination(ctx, stored.getResourceId(), updated);
        } catch (RuntimeException e) {
          errors++;
          errSummaries.add("updateConnector(destination): " + rootCauseMessage(e));
        }
      }

      if (errors == 0) {
        return new Result(scanned, changed, 0, snapshotsProcessed, statsProcessed, null);
      } else {
        var summary = new StringBuilder();
        summary.append("Partial failure (errors=").append(errors).append("):");
        for (String s : errSummaries) {
          summary.append("\n - ").append(s);
        }
        return new Result(
            scanned,
            changed,
            errors,
            snapshotsProcessed,
            statsProcessed,
            new RuntimeException(summary.toString()));
      }

    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return new Result(scanned, changed, errors, snapshotsProcessed, statsProcessed, e);
    }
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
        new TableSpecDescriptor(
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

    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(List.of(landingView.namespaceFq().split("\\.")))
            .setName(landingView.tableName())
            .build();
    return backend.ensureTable(
        ctx, destNamespaceId, NameRefNormalizer.normalize(tableRef), descriptor);
  }

  private Optional<ResourceId> resolveDestinationTableId(
      ReconcileContext ctx,
      CaptureMode captureMode,
      ResourceId catalogId,
      ResourceId destNamespaceId,
      DestinationTarget destination,
      FloecatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    if (captureMode != CaptureMode.STATS_ONLY) {
      return Optional.of(
          ensureTable(
              ctx,
              catalogId,
              destNamespaceId,
              landingView,
              format,
              connectorRid,
              connectorUri,
              sourceNsFq,
              sourceTable));
    }
    if (destination != null && destination.hasTableId()) {
      return Optional.of(destination.getTableId());
    }
    String tableDisplay = landingView == null ? null : landingView.tableName();
    String namespaceFq = landingView == null ? null : landingView.namespaceFq();
    if (tableDisplay == null
        || tableDisplay.isBlank()
        || namespaceFq == null
        || namespaceFq.isBlank()) {
      return Optional.empty();
    }
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(List.of(namespaceFq.split("\\.")))
            .setName(tableDisplay)
            .build();
    Optional<ResourceId> tableId = backend.lookupTable(ctx, NameRefNormalizer.normalize(tableRef));
    if (tableId.isPresent()) {
      return tableId;
    }
    throw new ReconcileNotReadyException(
        "Destination table "
            + catalogName
            + "."
            + namespaceFq
            + "."
            + tableDisplay
            + " is not visible yet for stats-only reconcile");
  }

  private void ensureSnapshot(
      ReconcileContext ctx, ResourceId tableId, FloecatConnector.SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return;
    }
    Snapshot existing =
        backend.fetchSnapshot(ctx, tableId, snapshotBundle.snapshotId()).orElse(null);
    buildSnapshot(ctx, tableId, snapshotBundle, existing)
        .ifPresent(snapshot -> backend.ingestSnapshot(ctx, tableId, snapshot));
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

  static boolean tableChanged(List<FloecatConnector.SnapshotBundle> bundles) {
    return bundles != null && !bundles.isEmpty();
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

  private IngestCounts ingestAllSnapshotsAndStatsFiltered(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      ConnectorConfig.Kind connectorKind,
      List<FloecatConnector.SnapshotBundle> bundles,
      boolean includeCoreMetadata,
      boolean includeStats,
      boolean fullRescan,
      Predicate<Long> statsCompleteForSnapshot,
      Set<String> includeSelectors,
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
          errors,
          snapshotsProcessedBase + snapshotsProcessed,
          statsProcessedBase + statsProcessed,
          "Processing snapshot " + snapshotId + " for " + sourceNs + "." + sourceTable);
      snapshotsProcessed++;

      if (includeCoreMetadata) {
        ensureSnapshot(ctx, tableId, snapshotBundle);
        maybeIngestSnapshotConstraints(
            ctx, tableId, connector, sourceNs, sourceTable, snapshotBundle, snapshotId);
      }

      if (!includeStats) {
        continue;
      }

      boolean statsCaptured =
          !fullRescan
              && statsCompleteForSnapshot != null
              && statsCompleteForSnapshot.test(snapshotId);
      if (statsCaptured) {
        continue;
      }

      StatsCaptureRequest request =
          StatsCaptureRequest.builder(tableId, snapshotId, StatsTargetIdentity.tableTarget())
              .columnSelectors(includeSelectors)
              .requestedKinds(Set.of())
              .executionMode(StatsExecutionMode.ASYNC)
              .connectorType(connectorTypeFor(connectorKind))
              .correlationId(ctx.correlationId())
              .build();
      Optional<StatsCaptureResult> captured = captureViaControlPlane(request);
      if (captured.isPresent()) {
        // statsProcessed counts successful capture attempts per snapshot, not number of persisted
        // target records.
        statsProcessed++;
      }
    }
    return new IngestCounts(snapshotsProcessed, statsProcessed);
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
      Predicate<Long> statsCompleteForSnapshot,
      Set<String> includeSelectors,
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
            targetSnapshotIds,
            includeSelectors);
    if (snapshotIds.isEmpty()) {
      return new IngestCounts(0L, 0L);
    }

    long snapshotsProcessed = 0L;
    long statsProcessed = 0L;
    String connectorType = connectorTypeFor(connectorKind);
    for (long snapshotId : snapshotIds) {
      ensureNotCancelled(cancelRequested);
      progress.onProgress(
          scanned,
          changed,
          errors,
          snapshotsProcessedBase + snapshotsProcessed,
          statsProcessedBase + statsProcessed,
          "Processing snapshot " + snapshotId + " for " + sourceNs + "." + sourceTable);
      snapshotsProcessed++;

      if (!fullRescan
          && knownSnapshotIds != null
          && knownSnapshotIds.contains(snapshotId)
          && statsCompleteForSnapshot != null
          && statsCompleteForSnapshot.test(snapshotId)) {
        continue;
      }

      StatsCaptureRequest request =
          StatsCaptureRequest.builder(tableId, snapshotId, StatsTargetIdentity.tableTarget())
              .columnSelectors(includeSelectors)
              .requestedKinds(Set.of())
              .executionMode(StatsExecutionMode.ASYNC)
              .connectorType(connectorType)
              .correlationId(ctx.correlationId())
              .build();
      Optional<StatsCaptureResult> captured = captureViaControlPlane(request);
      if (captured.isPresent()) {
        // statsProcessed counts successful capture attempts per snapshot, not number of persisted
        // target records.
        statsProcessed++;
      }
    }
    return new IngestCounts(snapshotsProcessed, statsProcessed);
  }

  private Set<Long> discoverSnapshotIdsForStatsCapture(
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      ResourceId tableId,
      boolean fullRescan,
      Set<Long> enumerationKnownSnapshotIds,
      Set<Long> targetSnapshotIds,
      Set<String> includeSelectors) {
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

  private Optional<StatsCaptureResult> captureViaControlPlane(StatsCaptureRequest request) {
    if (statsCaptureControlPlane == null || statsCaptureControlPlane.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      StatsTriggerResult result = statsCaptureControlPlane.get().trigger(request);
      return result.captureResult();
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Stats control-plane capture failed for table=%s snapshot=%s",
          request.tableId(),
          request.snapshotId());
      return Optional.empty();
    }
  }

  private void enqueueStatsOnlyCapture(
      ResourceId connectorId,
      String namespaceFq,
      String tableDisplayName,
      Set<String> includeSelectors,
      Set<Long> snapshotIds) {
    if (reconcileJobStore == null || reconcileJobStore.isUnsatisfied()) {
      LOG.warnf(
          "Skipping follow-up STATS_ONLY enqueue: reconcile job store unavailable for connector=%s table=%s.%s",
          connectorId != null ? connectorId.getId() : "",
          namespaceFq == null ? "" : namespaceFq,
          tableDisplayName == null ? "" : tableDisplayName);
      return;
    }
    if (connectorId == null
        || connectorId.getAccountId().isBlank()
        || connectorId.getId().isBlank()) {
      LOG.warn("Skipping follow-up STATS_ONLY enqueue: connector identity is incomplete");
      return;
    }
    if (namespaceFq == null
        || namespaceFq.isBlank()
        || tableDisplayName == null
        || tableDisplayName.isBlank()) {
      LOG.warnf(
          "Skipping follow-up STATS_ONLY enqueue: scope identity missing for connector=%s",
          connectorId.getId());
      return;
    }
    List<List<String>> namespacePaths = List.of(List.of(namespaceFq.split("\\.")));
    List<String> columns =
        includeSelectors == null ? List.of() : includeSelectors.stream().sorted().toList();
    ReconcileScope scope = ReconcileScope.of(namespacePaths, tableDisplayName, columns);
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
        jobId, connectorId.getId(), namespaceFq, tableDisplayName, snapshotIds);
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

  private static String connectorTypeFor(ConnectorConfig.Kind kind) {
    return switch (kind) {
      case ICEBERG -> "iceberg";
      case GLUE -> "glue";
      case DELTA -> "delta";
      case UNITY -> "unity";
      default -> "";
    };
  }

  private void maybeIngestSnapshotConstraints(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      String sourceNs,
      String sourceTable,
      FloecatConnector.SnapshotBundle snapshotBundle,
      long snapshotId) {
    if (snapshotId < 0) {
      return;
    }
    Optional<SnapshotConstraints> constraints =
        connector.snapshotConstraints(sourceNs, sourceTable, tableId, snapshotBundle);
    if (constraints.isEmpty()) {
      return;
    }
    backend.putSnapshotConstraints(ctx, tableId, snapshotId, constraints.get());
  }

  private boolean isStatsCaptureCompleteForScope(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> includeSelectors) {
    if (!backend.statsAlreadyCapturedForTargetKind(
        ctx, tableId, snapshotId, StatsTargetKind.STK_TABLE)) {
      return false;
    }
    if (includeSelectors != null && !includeSelectors.isEmpty()) {
      return backend.statsCapturedForColumnSelectors(ctx, tableId, snapshotId, includeSelectors);
    }
    return backend.statsAlreadyCapturedForTargetKind(
            ctx, tableId, snapshotId, StatsTargetKind.STK_COLUMN)
        || backend.statsAlreadyCapturedForTargetKind(
            ctx, tableId, snapshotId, StatsTargetKind.STK_FILE)
        || backend.statsAlreadyCapturedForTargetKind(
            ctx, tableId, snapshotId, StatsTargetKind.STK_EXPRESSION);
  }

  /**
   * Builds a memoized snapshot completeness checker so repeated lookups in one table reconcile pass
   * do not issue duplicate backend calls for the same snapshot.
   */
  private Predicate<Long> snapshotCompletenessChecker(
      ReconcileContext ctx, ResourceId tableId, Set<String> includeSelectors) {
    Map<Long, Boolean> completenessBySnapshot = new LinkedHashMap<>();
    return snapshotId ->
        completenessBySnapshot.computeIfAbsent(
            snapshotId, id -> isStatsCaptureCompleteForScope(ctx, tableId, id, includeSelectors));
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

  private String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    return "/accounts/"
        + encodePathSegment(accountId)
        + "/tables/"
        + encodePathSegment(tableId)
        + "/snapshots/by-id/"
        + String.format("%019d", snapshotId);
  }

  private String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    long inverted = Long.MAX_VALUE - Math.max(0L, upstreamCreatedAtMs);
    return "/accounts/"
        + encodePathSegment(accountId)
        + "/tables/"
        + encodePathSegment(tableId)
        + "/snapshots/by-time/"
        + String.format("%019d-%019d", inverted, snapshotId);
  }

  private String encodePathSegment(String value) {
    return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
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

  static Set<String> effectiveSelectors(ReconcileScope scope, SourceSelector source) {
    if (scope != null && scope.hasColumnFilter()) {
      return normalizeSelectors(scope.destinationTableColumns());
    }
    if (source == null) {
      return Set.of();
    }
    return normalizeSelectors(source.getColumnsList());
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

  public static final class Result {
    // statsProcessed reports successful capture attempts per snapshot. Engines may persist multiple
    // target records for a single successful capture attempt.
    public final long scanned, changed, errors, snapshotsProcessed, statsProcessed;
    public final Exception error;

    public Result(
        long scanned,
        long changed,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        Exception error) {
      this.scanned = scanned;
      this.changed = changed;
      this.errors = errors;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.error = error;
    }

    public boolean ok() {
      return error == null;
    }

    public boolean cancelled() {
      return error instanceof ReconcileCancelledException;
    }

    public String message() {
      return ok() ? "OK" : rootCauseMessage(error);
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

  private static final class ReconcileNotReadyException extends RuntimeException {
    private ReconcileNotReadyException(String message) {
      super(message);
    }
  }

  private static final class IngestCounts {
    final long snapshotsProcessed;
    final long statsProcessed;

    private IngestCounts(long snapshotsProcessed, long statsProcessed) {
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
    }
  }
}
