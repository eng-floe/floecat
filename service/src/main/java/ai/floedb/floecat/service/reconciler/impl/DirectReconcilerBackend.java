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
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.query.rpc.SnapshotPin;
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
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import com.google.protobuf.Timestamp;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@IfBuildProperty(name = "floecat.reconciler.backend", stringValue = "local", enableIfMissing = true)
@ApplicationScoped
public class DirectReconcilerBackend extends BaseServiceImpl implements ReconcilerBackend {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsRepository statsRepository;
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
    String corrId = ctx.correlationId();
    ResourceId tableId = existing.getResourceId();

    Table.Builder updated = existing.toBuilder();
    boolean changed = false;

    String desiredSchema = mustNonEmpty(descriptor.schemaJson(), "schema_json", corrId);
    Map<String, String> properties =
        descriptor.properties() != null ? descriptor.properties() : Map.of();
    List<String> partitionKeys =
        descriptor.partitionKeys() != null ? descriptor.partitionKeys() : List.of();

    if (!existing.getSchemaJson().equals(desiredSchema)) {
      updated.setSchemaJson(desiredSchema);
      changed = true;
    }

    var desiredUpstream = buildUpstream(descriptor, partitionKeys);
    if (!existing.hasUpstream() || !existing.getUpstream().equals(desiredUpstream)) {
      updated.setUpstream(desiredUpstream);
      changed = true;
    }

    if (!existing.getPropertiesMap().equals(properties)) {
      updated.clearProperties().putAllProperties(properties);
      changed = true;
    }

    if (!changed) {
      return;
    }

    long version = tableRepo.metaForSafe(tableId).getPointerVersion();
    tableRepo.update(updated.build(), version);
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
  public boolean statsAlreadyCaptured(ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    return statsRepository.getTableStats(tableId, snapshotId).isPresent();
  }

  @Override
  public void putTableStats(ReconcileContext ctx, ResourceId tableId, TableStats stats) {
    statsRepository.putTableStats(tableId, stats.getSnapshotId(), stats);
  }

  @Override
  public void putColumnStats(ReconcileContext ctx, List<ColumnStats> stats) {
    if (stats.isEmpty()) {
      return;
    }
    Map<StatsGroupKey, List<ColumnStats>> grouped = new LinkedHashMap<>();
    for (ColumnStats columnStats : stats) {
      StatsGroupKey key = new StatsGroupKey(columnStats.getTableId(), columnStats.getSnapshotId());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(columnStats);
    }
    for (var entry : grouped.entrySet()) {
      statsRepository.putColumnStats(
          entry.getKey().tableId, entry.getKey().snapshotId, entry.getValue());
    }
  }

  @Override
  public void putFileColumnStats(ReconcileContext ctx, List<FileColumnStats> stats) {
    if (stats.isEmpty()) {
      return;
    }
    Map<StatsGroupKey, List<FileColumnStats>> grouped = new LinkedHashMap<>();
    for (FileColumnStats fileStats : stats) {
      StatsGroupKey key = new StatsGroupKey(fileStats.getTableId(), fileStats.getSnapshotId());
      grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(fileStats);
    }
    for (var entry : grouped.entrySet()) {
      statsRepository.putFileColumnStats(
          entry.getKey().tableId, entry.getKey().snapshotId, entry.getValue());
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

  private static final class StatsGroupKey {
    private final ResourceId tableId;
    private final long snapshotId;

    private StatsGroupKey(ResourceId tableId, long snapshotId) {
      this.tableId = tableId;
      this.snapshotId = snapshotId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      StatsGroupKey that = (StatsGroupKey) o;
      return snapshotId == that.snapshotId && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
      int result = tableId.hashCode();
      result = 31 * result + Long.hashCode(snapshotId);
      return result;
    }
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
