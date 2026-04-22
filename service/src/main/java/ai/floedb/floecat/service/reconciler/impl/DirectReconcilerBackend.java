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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CATALOG;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.CONNECTOR;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.NAMESPACE;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.spi.ColumnSelectorCoverage;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
@Typed(DirectReconcilerBackend.class)
public class DirectReconcilerBackend extends BaseServiceImpl implements ReconcilerBackend {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject ViewRepository viewRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsStore statsStore;
  @Inject SnapshotHelper snapshotHelper;
  @Inject ConnectorRepository connectorRepo;

  @Override
  public ResourceId ensureNamespace(
      ReconcileContext ctx, ResourceId catalogId, NameRef namespaceRef) {
    String corrId = ctx.correlationId();
    String accountId = ctx.principal().getAccountId();
    var catalog =
        catalogRepo
            .getById(catalogId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(corrId, CATALOG, Map.of("catalog_id", catalogId.getId())));
    var parents = normalizedSegments(namespaceRef.getPathList());
    if (!parents.isEmpty()) {
      ensureParentNamespaces(ctx, catalog, parents);
    }
    String leaf = namespaceRef.getName();
    leaf = mustNonEmpty(normalizeName(leaf), "namespace.name", corrId);
    return ensureNamespaceEntry(ctx, catalog, accountId, parents, leaf);
  }

  private void ensureParentNamespaces(ReconcileContext ctx, Catalog catalog, List<String> parents) {
    List<String> prefix = new ArrayList<>();
    for (String segment : parents) {
      ensureNamespaceEntry(ctx, catalog, ctx.principal().getAccountId(), prefix, segment);
      prefix.add(segment);
    }
  }

  private ResourceId ensureNamespaceEntry(
      ReconcileContext ctx,
      Catalog catalog,
      String accountId,
      List<String> parents,
      String displayName) {
    String corrId = ctx.correlationId();
    var catalogId = catalog.getResourceId();
    String normalized = displayName;
    var fullPath = new ArrayList<>(parents);
    fullPath.add(normalized);

    var existing = namespaceRepo.getByPath(accountId, catalogId.getId(), fullPath);
    if (existing.isPresent()) {
      return existing.get().getResourceId();
    }

    var namespace =
        Namespace.newBuilder()
            .setResourceId(randomResourceId(accountId, ResourceKind.RK_NAMESPACE))
            .setCatalogId(catalogId)
            .setDisplayName(normalized)
            .addAllParents(parents)
            .build();
    namespaceRepo.create(namespace);
    return namespace.getResourceId();
  }

  @Override
  public ResourceId ensureTable(
      ReconcileContext ctx,
      ResourceId namespaceId,
      NameRef tableRef,
      TableSpecDescriptor descriptor) {
    return ensureTableInternal(ctx, namespaceId, tableRef, descriptor);
  }

  private ResourceId ensureTableInternal(
      ReconcileContext ctx,
      ResourceId namespaceId,
      NameRef tableRef,
      TableSpecDescriptor descriptor) {
    String corrId = ctx.correlationId();
    String accountId = ctx.principal().getAccountId();

    var namespace =
        namespaceRepo
            .getById(namespaceId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corrId, NAMESPACE, Map.of("namespace_id", namespaceId.getId())));

    var catalogId = namespace.getCatalogId();
    String displayName = mustNonEmpty(descriptor.displayName(), "table.display_name", corrId);
    String normalized = normalizeName(displayName);
    Map<String, String> properties =
        descriptor.properties() != null ? descriptor.properties() : Map.of();
    List<String> partitionKeys =
        descriptor.partitionKeys() != null ? descriptor.partitionKeys() : List.of();

    var existing =
        tableRepo.getByName(accountId, catalogId.getId(), namespaceId.getId(), normalized);
    if (existing.isPresent()) {
      maybeUpdateTable(ctx, existing.get(), descriptor);
      return existing.get().getResourceId();
    }

    var tableBuilder =
        Table.newBuilder()
            .setResourceId(randomResourceId(accountId, ResourceKind.RK_TABLE))
            .setDisplayName(normalized)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setCreatedAt(nowTs())
            .setSchemaJson(mustNonEmpty(descriptor.schemaJson(), "schema_json", corrId))
            .setUpstream(buildUpstream(descriptor, partitionKeys));
    tableBuilder.putAllProperties(properties);

    var table = tableBuilder.build();
    tableRepo.create(table);
    return table.getResourceId();
  }

  private void maybeUpdateTable(
      ReconcileContext ctx, Table existing, TableSpecDescriptor descriptor) {
    // Reconciler must not advance table core OCC version for existing tables.
    // Existing table shape updates are intentionally skipped here; only creation-on-miss
    // is allowed through ensureTable().
  }

  @Override
  public ResourceId ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
    String corrId = ctx.correlationId();
    String accountId = ctx.principal().getAccountId();
    var namespace =
        namespaceRepo
            .getById(spec.getNamespaceId())
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corrId, NAMESPACE, Map.of("namespace_id", spec.getNamespaceId().getId())));
    String catalogId = namespace.getCatalogId().getId();
    String namespaceId = spec.getNamespaceId().getId();
    String normalized = normalizeName(spec.getDisplayName());

    var existing = viewRepo.getByName(accountId, catalogId, namespaceId, normalized);
    if (existing.isPresent()) {
      View current = existing.get();
      View desired =
          current.toBuilder()
              .setCatalogId(namespace.getCatalogId())
              .setNamespaceId(spec.getNamespaceId())
              .setDisplayName(normalized)
              .clearSqlDefinitions()
              .addAllSqlDefinitions(spec.getSqlDefinitionsList())
              .clearCreationSearchPath()
              .addAllCreationSearchPath(spec.getCreationSearchPathList())
              .clearOutputColumns()
              .addAllOutputColumns(spec.getOutputColumnsList())
              .clearBaseRelations()
              .addAllBaseRelations(spec.getBaseRelationsList())
              .clearProperties()
              .putAllProperties(spec.getPropertiesMap())
              .build();
      if (desired.equals(current)) {
        return ResourceId.getDefaultInstance();
      }
      long expectedPointerVersion =
          viewRepo.metaForSafe(current.getResourceId()).getPointerVersion();
      if (!viewRepo.update(desired, expectedPointerVersion)) {
        throw new IllegalStateException("Failed to update reconciled view " + normalized);
      }
      return current.getResourceId();
    }

    var view =
        View.newBuilder()
            .setResourceId(randomResourceId(accountId, ResourceKind.RK_VIEW))
            .setCatalogId(namespace.getCatalogId())
            .setNamespaceId(spec.getNamespaceId())
            .setDisplayName(normalized)
            .addAllSqlDefinitions(spec.getSqlDefinitionsList())
            .addAllCreationSearchPath(spec.getCreationSearchPathList())
            .addAllOutputColumns(spec.getOutputColumnsList())
            .addAllBaseRelations(spec.getBaseRelationsList())
            .putAllProperties(spec.getPropertiesMap())
            .build();
    viewRepo.create(view);
    return view.getResourceId();
  }

  private UpstreamRef buildUpstream(TableSpecDescriptor descriptor, List<String> partitionKeys) {
    UpstreamRef.Builder builder =
        UpstreamRef.newBuilder()
            .setConnectorId(descriptor.connectorId())
            .setUri(descriptor.connectorUri())
            .setTableDisplayName(descriptor.sourceTable())
            .setFormat(toTableFormat(descriptor.connectorFormat()))
            .setColumnIdAlgorithm(descriptor.columnIdAlgorithm());

    if (descriptor.sourceNamespace() != null && !descriptor.sourceNamespace().isBlank()) {
      for (String seg : descriptor.sourceNamespace().split("\\.")) {
        if (!seg.isBlank()) {
          builder.addNamespacePath(seg);
        }
      }
    }
    builder.addAllPartitionKeys(partitionKeys);
    return builder.build();
  }

  @Override
  public Optional<ResourceId> lookupTable(ReconcileContext ctx, NameRef tableRef) {
    if (tableRef == null) {
      return Optional.empty();
    }
    String accountId = ctx.principal().getAccountId();
    String catalogName = tableRef.getCatalog();
    if (catalogName == null || catalogName.isBlank()) {
      return Optional.empty();
    }

    catalogName = normalizeName(catalogName);

    var catalogOpt = catalogRepo.getByName(accountId, catalogName);
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    String catalogId = catalogOpt.get().getResourceId().getId();

    List<String> path = new ArrayList<>(normalizedSegments(tableRef.getPathList()));
    if (path.isEmpty()) {
      return Optional.empty();
    }

    var namespaceOpt = namespaceRepo.getByPath(accountId, catalogId, path);
    if (namespaceOpt.isEmpty()) {
      return Optional.empty();
    }
    ResourceId namespaceId = namespaceOpt.get().getResourceId();

    String tableName = tableRef.getName();
    if (tableName == null || tableName.isBlank()) {
      return Optional.empty();
    }
    String normalized = normalizeName(tableName);

    var table = tableRepo.getByName(accountId, catalogId, namespaceId.getId(), normalized);
    return table.map(Table::getResourceId);
  }

  @Override
  public SnapshotPin snapshotPinFor(
      ReconcileContext ctx,
      ResourceId tableId,
      ai.floedb.floecat.common.rpc.SnapshotRef ref,
      Optional<Timestamp> asOf) {
    return snapshotHelper.snapshotPinFor(ctx.correlationId(), tableId, ref, asOf);
  }

  @Override
  public Optional<Snapshot> fetchSnapshot(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    return snapshotRepo.getById(tableId, snapshotId);
  }

  @Override
  public Set<Long> existingSnapshotIds(ReconcileContext ctx, ResourceId tableId) {
    Set<Long> snapshotIds = new LinkedHashSet<>();
    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      List<Snapshot> batch = snapshotRepo.list(tableId, 500, token, next);
      for (Snapshot snapshot : batch) {
        if (snapshot.getSnapshotId() >= 0) {
          snapshotIds.add(snapshot.getSnapshotId());
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());
    return snapshotIds;
  }

  @Override
  public void ingestSnapshot(ReconcileContext ctx, ResourceId tableId, Snapshot snapshot) {
    if (snapshot == null) {
      return;
    }
    long snapshotId = snapshot.getSnapshotId();
    if (snapshotId < 0) {
      throw new IllegalArgumentException("snapshotId must be non-negative");
    }
    Optional<Snapshot> existing = snapshotRepo.getById(tableId, snapshotId);
    if (existing.isPresent()) {
      if (SnapshotHelpers.equalsIgnoringIngested(snapshot, existing.get())) {
        return;
      }
      long version = snapshotRepo.metaForSafe(tableId, snapshotId).getPointerVersion();
      snapshotRepo.update(snapshot, version);
      return;
    }
    snapshotRepo.create(snapshot);
  }

  @Override
  public boolean statsAlreadyCapturedForTargetKind(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, StatsTargetKind targetKind) {
    if (targetKind == null || targetKind == StatsTargetKind.STK_UNSPECIFIED) {
      return statsStore.countTargetStats(tableId, snapshotId, Optional.empty()) > 0;
    }
    if (targetKind == StatsTargetKind.STK_TABLE) {
      var tableRecord =
          statsStore
              .getTargetStats(tableId, snapshotId, StatsTargetIdentity.tableTarget())
              .orElse(null);
      if (tableRecord == null || !tableRecord.hasTable()) {
        return false;
      }
      if (tableRecord.getTable().getDataFileCount() <= 0) {
        return true;
      }
      return statsStore.countTargetStats(tableId, snapshotId, Optional.of(StatsTargetType.FILE))
          > 0;
    }
    Optional<StatsTargetType> targetType =
        switch (targetKind) {
          case STK_COLUMN -> Optional.of(StatsTargetType.COLUMN);
          case STK_EXPRESSION -> Optional.of(StatsTargetType.EXPRESSION);
          case STK_FILE -> Optional.of(StatsTargetType.FILE);
          case STK_TABLE, STK_UNSPECIFIED, UNRECOGNIZED -> Optional.empty();
        };
    return statsStore.countTargetStats(tableId, snapshotId, targetType) > 0;
  }

  @Override
  public boolean statsCapturedForColumnSelectors(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<String> selectors) {
    ColumnSelectorCoverage.SelectorCoverage required = ColumnSelectorCoverage.parse(selectors);
    if (required.isUnsatisfiable()) {
      return false;
    }
    if (required.isEmpty()) {
      return statsAlreadyCapturedForTargetKind(
          ctx, tableId, snapshotId, StatsTargetKind.STK_COLUMN);
    }

    Set<Long> presentIds = new HashSet<>();
    Set<String> presentNames = new HashSet<>();
    String pageToken = "";
    final int pageSize = 256;
    do {
      StatsStore.StatsStorePage page =
          statsStore.listTargetStats(
              tableId, snapshotId, Optional.of(StatsTargetType.COLUMN), pageSize, pageToken);
      for (TargetStatsRecord record : page.records()) {
        ColumnSelectorCoverage.recordColumnCoverage(record, presentIds, presentNames);
      }
      if (required.isSatisfiedBy(presentIds, presentNames)) {
        return true;
      }
      pageToken = page.nextPageToken();
    } while (pageToken != null && !pageToken.isBlank());

    return required.isSatisfiedBy(presentIds, presentNames);
  }

  @Override
  public boolean statsCapturedForTargets(
      ReconcileContext ctx, ResourceId tableId, long snapshotId, Set<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return true;
    }
    for (StatsTarget target : targets) {
      if (target == null || target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
        return false;
      }
      if (statsStore.getTargetStats(tableId, snapshotId, target).isEmpty()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void putTargetStats(ReconcileContext ctx, List<TargetStatsRecord> stats) {
    if (stats.isEmpty()) {
      return;
    }
    for (TargetStatsRecord record : stats) {
      if (!record.hasTableId() || record.getTableId().getId().isBlank()) {
        throw new IllegalArgumentException("record missing tableId");
      }
      statsStore.putTargetStats(record);
    }
  }

  @Override
  public Connector lookupConnector(ReconcileContext ctx, ResourceId connectorId) {
    return connectorRepo
        .getById(connectorId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    ctx.correlationId(), CONNECTOR, Map.of("connector_id", connectorId.getId())));
  }

  @Override
  public String lookupCatalogName(ReconcileContext ctx, ResourceId catalogId) {
    Catalog catalog =
        catalogRepo
            .getById(catalogId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        ctx.correlationId(), CATALOG, Map.of("catalog_id", catalogId.getId())));
    return catalog.getDisplayName();
  }

  @Override
  public String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
    Namespace ns =
        namespaceRepo
            .getById(namespaceId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        ctx.correlationId(),
                        NAMESPACE,
                        Map.of("namespace_id", namespaceId.getId())));
    var segs = new ArrayList<String>(ns.getParentsCount() + 1);
    segs.addAll(ns.getParentsList());
    segs.add(ns.getDisplayName());
    return String.join(".", segs);
  }

  @Override
  public void updateConnectorDestination(
      ReconcileContext ctx, ResourceId connectorId, DestinationTarget destination) {
    Connector existing = lookupConnector(ctx, connectorId);
    var builder = existing.toBuilder();
    builder.setDestination(destination);
    long version = connectorRepo.metaForSafe(connectorId).getPointerVersion();
    connectorRepo.update(builder.build(), version);
  }

  private TableFormat toTableFormat(ConnectorFormat format) {
    return switch (format) {
      case CF_ICEBERG -> TableFormat.TF_ICEBERG;
      case CF_DELTA -> TableFormat.TF_DELTA;
      default -> TableFormat.TF_UNKNOWN;
    };
  }

  private List<String> normalizedSegments(List<String> segments) {
    if (segments == null || segments.isEmpty()) {
      return List.of();
    }
    List<String> normalized = new ArrayList<>(segments.size());
    for (String segment : segments) {
      if (segment == null || segment.isBlank()) {
        continue;
      }
      normalized.add(normalizeName(segment));
    }
    return normalized;
  }
}
