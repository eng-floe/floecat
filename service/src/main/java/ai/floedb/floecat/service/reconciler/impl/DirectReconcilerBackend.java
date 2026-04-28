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
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexCoverage;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
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
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.reconciler.impl.FileGroupIndexArtifactStager;
import ai.floedb.floecat.reconciler.impl.ReconcileExecutor;
import ai.floedb.floecat.reconciler.impl.ReconcileFailureException;
import ai.floedb.floecat.reconciler.spi.ColumnSelectorCoverage;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRegistry;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import ai.floedb.floecat.reconciler.spi.capture.PlannedFileGroupCaptureRequest;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotHelper;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

@ApplicationScoped
@Typed(DirectReconcilerBackend.class)
public class DirectReconcilerBackend extends BaseServiceImpl implements ReconcilerBackend {
  private static final MessageType BOOTSTRAP_INDEX_SIDECAR_SCHEMA =
      MessageTypeParser.parseMessageType(
          """
          message arrow_schema {
            required binary column_name (STRING);
            required int32 row_group (INTEGER(32,false));
            required int32 page_ordinal (INTEGER(32,false));
            required int64 first_row_index;
            required int32 row_count (INTEGER(32,false));
            required int32 live_row_count (INTEGER(32,false));
            optional int64 page_header_offset;
            required int32 page_total_compressed_size;
            optional int64 dictionary_page_header_offset;
            optional int32 dictionary_page_total_compressed_size;
            required boolean requires_dictionary_page;
            required binary parquet_physical_type (STRING);
            required binary parquet_compression (STRING);
            required int32 parquet_max_def_level (INTEGER(16,true));
            required int32 parquet_max_rep_level (INTEGER(16,true));
            optional int32 min_i32;
            optional int32 max_i32;
            optional int64 min_i64;
            optional int64 max_i64;
            optional float min_f32;
            optional float max_f32;
            optional double min_f64;
            optional double max_f64;
            optional boolean min_bool;
            optional boolean max_bool;
            optional binary min_utf8 (STRING);
            optional binary max_utf8 (STRING);
            optional int32 decimal_precision (INTEGER(8,false));
            optional int32 decimal_scale (INTEGER(8,true));
            optional int32 decimal_bits (INTEGER(16,true));
            optional fixed_len_byte_array(16) min_decimal128_unscaled (DECIMAL(38,0));
            optional fixed_len_byte_array(16) max_decimal128_unscaled (DECIMAL(38,0));
            optional fixed_len_byte_array(32) min_decimal256_unscaled (DECIMAL(76,0));
            optional fixed_len_byte_array(32) max_decimal256_unscaled (DECIMAL(76,0));
          }
          """);
  private static final String INDEX_CONTENT_TYPE = "application/x-parquet";

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject ViewRepository viewRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsStore statsStore;
  @Inject SnapshotHelper snapshotHelper;
  @Inject ConnectorRepository connectorRepo;
  @Inject IndexArtifactRepository indexArtifactRepo;
  @Inject BlobStore blobStore;
  @Inject CaptureEngineRegistry captureEngineRegistry;
  @Inject CredentialResolver credentialResolver;

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

  @Override
  public Optional<ResourceId> lookupNamespace(ReconcileContext ctx, NameRef namespaceRef) {
    if (namespaceRef == null) {
      return Optional.empty();
    }
    String accountId = ctx.principal().getAccountId();
    String catalogName = namespaceRef.getCatalog();
    if (catalogName == null || catalogName.isBlank()) {
      return Optional.empty();
    }
    var catalogOpt = catalogRepo.getByName(accountId, normalizeName(catalogName));
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    List<String> path = new ArrayList<>(normalizedSegments(namespaceRef.getPathList()));
    String leaf = namespaceRef.getName();
    if (leaf == null || leaf.isBlank()) {
      return Optional.empty();
    }
    path.add(normalizeName(leaf));
    return namespaceRepo
        .getByPath(accountId, catalogOpt.get().getResourceId().getId(), path)
        .map(Namespace::getResourceId);
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
  public boolean updateTableById(
      ReconcileContext ctx,
      ResourceId tableId,
      ResourceId namespaceId,
      NameRef tableRef,
      TableSpecDescriptor descriptor) {
    String corrId = ctx.correlationId();
    String displayName = mustNonEmpty(descriptor.displayName(), "table.display_name", corrId);
    String normalized = normalizeName(displayName);
    Map<String, String> properties =
        descriptor.properties() != null ? descriptor.properties() : Map.of();
    List<String> partitionKeys =
        descriptor.partitionKeys() != null ? descriptor.partitionKeys() : List.of();

    Table current =
        tableRepo
            .getById(tableId)
            .orElseThrow(() -> GrpcErrors.notFound(corrId, TABLE, Map.of("id", tableId.getId())));
    if (!current.getNamespaceId().equals(namespaceId)) {
      throw new IllegalArgumentException(
          "Destination table namespace mismatch for id: " + tableId.getId());
    }
    if (!current.getDisplayName().equals(normalized)) {
      throw new IllegalArgumentException(
          "Destination table display name mismatch for id: " + tableId.getId());
    }

    Table desired =
        current.toBuilder()
            .setSchemaJson(mustNonEmpty(descriptor.schemaJson(), "schema_json", corrId))
            .setUpstream(buildUpstream(descriptor, partitionKeys))
            .clearProperties()
            .putAllProperties(properties)
            .build();
    if (desired.equals(current)) {
      return false;
    }
    long expectedPointerVersion = tableRepo.metaForSafe(tableId).getPointerVersion();
    if (!tableRepo.update(desired, expectedPointerVersion)) {
      throw new IllegalStateException("Failed to update reconciled table " + tableId.getId());
    }
    return true;
  }

  @Override
  public ViewMutationResult ensureView(ReconcileContext ctx, ViewSpec spec, String idempotencyKey) {
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
        return new ViewMutationResult(current.getResourceId(), false);
      }
      long expectedPointerVersion =
          viewRepo.metaForSafe(current.getResourceId()).getPointerVersion();
      if (!viewRepo.update(desired, expectedPointerVersion)) {
        throw new IllegalStateException("Failed to update reconciled view " + normalized);
      }
      return new ViewMutationResult(current.getResourceId(), true);
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
    return new ViewMutationResult(view.getResourceId(), true);
  }

  @Override
  public boolean updateViewById(ReconcileContext ctx, ResourceId viewId, ViewSpec spec) {
    if (viewId == null || viewId.getId().isBlank()) {
      throw new IllegalArgumentException("viewId is required");
    }
    View current =
        viewRepo
            .getById(viewId)
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Destination view id does not exist: " + viewId.getId()));
    if (!current.getCatalogId().equals(spec.getCatalogId())) {
      throw new IllegalArgumentException("Destination view catalog does not match requested spec");
    }
    if (!current.getNamespaceId().equals(spec.getNamespaceId())) {
      throw new IllegalArgumentException(
          "Destination view namespace does not match requested spec");
    }
    if (!current.getDisplayName().equals(spec.getDisplayName())) {
      throw new IllegalArgumentException(
          "Destination view display name does not match requested spec");
    }
    View desired =
        current.toBuilder()
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
      return false;
    }
    long expectedPointerVersion = viewRepo.metaForSafe(current.getResourceId()).getPointerVersion();
    if (!viewRepo.update(desired, expectedPointerVersion)) {
      throw new IllegalStateException("Failed to update reconciled view " + viewId.getId());
    }
    return true;
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

  private static String sourceNamespace(Table table) {
    return table != null && table.hasUpstream()
        ? String.join(".", table.getUpstream().getNamespacePathList())
        : "";
  }

  private static String sourceName(Table table) {
    return table != null && table.hasUpstream() ? table.getUpstream().getTableDisplayName() : "";
  }

  private static ResourceId sourceConnectorId(Table table) {
    return table != null && table.hasUpstream() && table.getUpstream().hasConnectorId()
        ? table.getUpstream().getConnectorId()
        : null;
  }

  private static ResourceId sourceConnectorId(String accountId, String connectorId) {
    return connectorId == null || connectorId.isBlank()
        ? null
        : ResourceId.newBuilder()
            .setAccountId(accountId == null ? "" : accountId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId(connectorId)
            .build();
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
  public Optional<String> lookupTableDisplayName(ReconcileContext ctx, ResourceId tableId) {
    if (tableId == null || tableId.getId().isBlank()) {
      return Optional.empty();
    }
    return tableRepo.getById(tableId).map(Table::getDisplayName);
  }

  @Override
  public Optional<DestinationTableMetadata> lookupDestinationTableMetadata(
      ReconcileContext ctx, ResourceId tableId) {
    if (tableId == null || tableId.getId().isBlank()) {
      return Optional.empty();
    }
    return tableRepo
        .getById(tableId)
        .map(
            table ->
                new DestinationTableMetadata(
                    table.getCatalogId(),
                    table.getNamespaceId(),
                    table.getDisplayName(),
                    sourceNamespace(table),
                    sourceName(table),
                    sourceConnectorId(table)));
  }

  @Override
  public Optional<ResourceId> lookupView(ReconcileContext ctx, NameRef viewRef) {
    if (viewRef == null) {
      return Optional.empty();
    }
    String accountId = ctx.principal().getAccountId();
    String catalogName = viewRef.getCatalog();
    if (catalogName == null || catalogName.isBlank()) {
      return Optional.empty();
    }

    catalogName = normalizeName(catalogName);

    var catalogOpt = catalogRepo.getByName(accountId, catalogName);
    if (catalogOpt.isEmpty()) {
      return Optional.empty();
    }
    String catalogId = catalogOpt.get().getResourceId().getId();

    List<String> path = new ArrayList<>(normalizedSegments(viewRef.getPathList()));
    if (path.isEmpty()) {
      return Optional.empty();
    }

    var namespaceOpt = namespaceRepo.getByPath(accountId, catalogId, path);
    if (namespaceOpt.isEmpty()) {
      return Optional.empty();
    }
    ResourceId namespaceId = namespaceOpt.get().getResourceId();

    String viewName = viewRef.getName();
    if (viewName == null || viewName.isBlank()) {
      return Optional.empty();
    }
    String normalized = normalizeName(viewName);

    var view = viewRepo.getByName(accountId, catalogId, namespaceId.getId(), normalized);
    return view.map(View::getResourceId);
  }

  @Override
  public Optional<String> lookupViewDisplayName(ReconcileContext ctx, ResourceId viewId) {
    if (viewId == null || viewId.getId().isBlank()) {
      return Optional.empty();
    }
    return viewRepo.getById(viewId).map(View::getDisplayName);
  }

  @Override
  public Optional<DestinationViewMetadata> lookupDestinationViewMetadata(
      ReconcileContext ctx, ResourceId viewId) {
    if (viewId == null || viewId.getId().isBlank()) {
      return Optional.empty();
    }
    return viewRepo
        .getById(viewId)
        .map(
            view ->
                new DestinationViewMetadata(
                    view.getCatalogId(),
                    view.getNamespaceId(),
                    view.getDisplayName(),
                    view.getPropertiesOrDefault(SOURCE_NAMESPACE_PROPERTY, ""),
                    view.getPropertiesOrDefault(SOURCE_NAME_PROPERTY, ""),
                    sourceConnectorId(
                        view.getResourceId().getAccountId(),
                        view.getPropertiesOrDefault(SOURCE_CONNECTOR_ID_PROPERTY, ""))));
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
  public Optional<FloecatConnector.SnapshotFilePlan> fetchSnapshotFilePlan(
      ReconcileContext ctx, ResourceId tableId, long snapshotId) {
    if (snapshotId < 0) {
      return Optional.empty();
    }
    Table table = tableRepo.getById(tableId).orElse(null);
    if (table == null || !table.hasUpstream()) {
      return Optional.empty();
    }
    ResourceId connectorId = sourceConnectorId(table);
    if (connectorId == null || connectorId.getId().isBlank()) {
      return Optional.empty();
    }
    String sourceNamespace = sourceNamespace(table);
    String sourceTable = sourceName(table);
    if (sourceNamespace.isBlank() || sourceTable.isBlank()) {
      return Optional.empty();
    }

    Connector connector = lookupConnector(ctx, connectorId);
    try (var source = ConnectorFactory.create(resolveCredentials(connector))) {
      return source.planSnapshotFiles(sourceNamespace, sourceTable, tableId, snapshotId);
    } catch (RuntimeException e) {
      if (isMissingObjectFailure(e)) {
        throw new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING,
            "source object missing: " + sourceNamespace + "." + sourceTable,
            e);
      }
      throw e;
    }
  }

  @Override
  public CaptureEngineResult capturePlannedFileGroup(
      ReconcileContext ctx, PlannedFileGroupCaptureRequest request) {
    if (request == null
        || request.tableId() == null
        || request.plannedFilePaths() == null
        || request.plannedFilePaths().isEmpty()
        || request.snapshotId() < 0) {
      return CaptureEngineResult.empty();
    }
    Table table = tableRepo.getById(request.tableId()).orElse(null);
    if (table == null || !table.hasUpstream()) {
      return CaptureEngineResult.empty();
    }
    ResourceId connectorId = sourceConnectorId(table);
    if (connectorId == null || connectorId.getId().isBlank()) {
      return CaptureEngineResult.empty();
    }
    String sourceNamespace = sourceNamespace(table);
    String sourceTable = sourceName(table);
    if (sourceNamespace.isBlank() || sourceTable.isBlank()) {
      return CaptureEngineResult.empty();
    }
    if (captureEngineRegistry == null) {
      return CaptureEngineResult.empty();
    }
    Connector connector = lookupConnector(ctx, connectorId);
    CaptureEngineResult capture =
        captureEngineRegistry.capture(
            new CaptureEngineRequest(
                connector.toBuilder().setAuth(resolvedAuth(connector)).build(),
                sourceNamespace,
                sourceTable,
                request.tableId(),
                request.snapshotId(),
                request.planId(),
                request.groupId(),
                request.plannedFilePaths(),
                request.statsColumns(),
                request.indexColumns(),
                request.requestedStatsTargetKinds(),
                request.capturePageIndex()));
    if (!request.capturePageIndex() || !capture.stagedIndexArtifacts().isEmpty()) {
      return capture;
    }
    return CaptureEngineResult.of(
        capture.statsRecords(),
        List.of(),
        FileGroupIndexArtifactStager.stage(
            request.tableId(),
            request.snapshotId(),
            request.plannedFilePaths(),
            capture.statsRecords(),
            capture.pageIndexEntries()));
  }

  private static FileTargetStats synthesizeIndexOnlyFileStats(
      String filePath, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    FileTargetStats.Builder builder =
        FileTargetStats.newBuilder().setFilePath(filePath).setFileFormat("parquet");
    long rowsIndexed =
        pageIndexEntries == null
            ? 0L
            : pageIndexEntries.stream()
                .map(FloecatConnector.ParquetPageIndexEntry::liveRowCount)
                .filter(value -> value > 0)
                .mapToLong(Integer::longValue)
                .max()
                .orElse(0L);
    if (rowsIndexed > 0L) {
      builder.setRowCount(rowsIndexed);
    }
    long bytesScanned =
        pageIndexEntries == null
            ? 0L
            : pageIndexEntries.stream()
                .map(FloecatConnector.ParquetPageIndexEntry::pageTotalCompressedSize)
                .filter(value -> value > 0L)
                .mapToLong(Integer::longValue)
                .sum();
    if (bytesScanned > 0L) {
      builder.setSizeBytes(bytesScanned);
    }
    return builder.build();
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
  public boolean indexArtifactsCapturedForFilePaths(
      ReconcileContext ctx,
      ResourceId tableId,
      long snapshotId,
      List<String> filePaths,
      Set<String> selectors) {
    if (filePaths == null || filePaths.isEmpty()) {
      return true;
    }
    Set<String> normalizedSelectors = normalizeIndexSelectors(selectors);
    if (normalizedSelectors == null) {
      return false;
    }
    for (String filePath : filePaths) {
      if (filePath == null || filePath.isBlank()) {
        return false;
      }
      IndexTarget target =
          IndexTarget.newBuilder()
              .setFile(IndexFileTarget.newBuilder().setFilePath(filePath).build())
              .build();
      IndexArtifactRecord record =
          indexArtifactRepo.getIndexArtifact(tableId, snapshotId, target).orElse(null);
      if (record == null || record.getState() != IndexArtifactState.IAS_READY) {
        return false;
      }
      if (!normalizedSelectors.isEmpty()
          && !persistedIndexSelectors(record).containsAll(normalizedSelectors)) {
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
  public void putIndexArtifacts(ReconcileContext ctx, List<StagedIndexArtifact> artifacts) {
    if (artifacts == null || artifacts.isEmpty()) {
      return;
    }
    for (StagedIndexArtifact artifact : artifacts) {
      IndexArtifactRecord record =
          artifact == null ? IndexArtifactRecord.getDefaultInstance() : artifact.record();
      if (!record.hasTableId() || record.getTableId().getId().isBlank()) {
        throw new IllegalArgumentException("record missing tableId");
      }
      if (!record.hasTarget()) {
        throw new IllegalArgumentException("record missing target");
      }
      byte[] content = artifact == null ? null : artifact.content();
      if (content == null || content.length == 0) {
        throw new IllegalArgumentException("staged artifact missing content");
      }
      String contentType =
          artifact.contentType() == null || artifact.contentType().isBlank()
              ? INDEX_CONTENT_TYPE
              : artifact.contentType();
      blobStore.put(record.getArtifactUri(), content, contentType);
      String etag =
          blobStore
              .head(record.getArtifactUri())
              .map(h -> h.getEtag())
              .orElse(record.getContentEtag());
      indexArtifactRepo.putIndexArtifact(record.toBuilder().setContentEtag(etag).build());
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

  AuthConfig resolvedAuth(Connector connector) {
    ConnectorConfig.Auth resolved = resolveCredentials(connector).auth();
    return AuthConfig.newBuilder()
        .setScheme(resolved.scheme() == null ? "" : resolved.scheme())
        .putAllProperties(resolved.props())
        .putAllHeaderHints(resolved.headerHints())
        .build();
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

  private static Map<String, FileTargetStats> fileStatsByPath(List<TargetStatsRecord> stats) {
    HashMap<String, FileTargetStats> byPath = new HashMap<>();
    for (TargetStatsRecord record : stats) {
      if (!record.hasFile()) {
        continue;
      }
      FileTargetStats fileStats = record.getFile();
      if (fileStats.getFilePath() == null || fileStats.getFilePath().isBlank()) {
        continue;
      }
      byPath.put(fileStats.getFilePath(), fileStats);
    }
    return byPath;
  }

  private static Map<String, List<FloecatConnector.ParquetPageIndexEntry>> pageIndexEntriesByFile(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    HashMap<String, List<FloecatConnector.ParquetPageIndexEntry>> byPath = new HashMap<>();
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return Map.of();
    }
    for (FloecatConnector.ParquetPageIndexEntry entry : pageIndexEntries) {
      if (entry == null || entry.filePath() == null || entry.filePath().isBlank()) {
        continue;
      }
      byPath.computeIfAbsent(entry.filePath(), ignored -> new ArrayList<>()).add(entry);
    }
    return byPath;
  }

  private static IndexCoverage indexCoverage(
      FileTargetStats fileStats, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    var coverage = IndexCoverage.newBuilder();
    if (fileStats.getRowCount() > 0L) {
      coverage.setRowsIndexed(fileStats.getRowCount());
      coverage.setLiveRowsIndexed(fileStats.getRowCount());
    }
    if (fileStats.getSizeBytes() > 0L) {
      coverage.setBytesScanned(fileStats.getSizeBytes());
    }
    if (pageIndexEntries != null && !pageIndexEntries.isEmpty()) {
      coverage.setPagesIndexed(pageIndexEntries.size());
      coverage.setRowGroupsIndexed(
          pageIndexEntries.stream()
              .map(FloecatConnector.ParquetPageIndexEntry::rowGroup)
              .distinct()
              .count());
    }
    return coverage.build();
  }

  private static String indexArtifactTargetStorageId(String filePath) {
    return "file:" + filePath;
  }

  private static String indexedColumnsProperty(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return "";
    }
    return pageIndexEntries.stream()
        .map(FloecatConnector.ParquetPageIndexEntry::columnName)
        .filter(name -> name != null && !name.isBlank())
        .map(String::trim)
        .distinct()
        .sorted()
        .collect(java.util.stream.Collectors.joining(","));
  }

  private static Set<String> normalizeIndexSelectors(Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) {
      return Set.of();
    }
    LinkedHashSet<String> normalized = new LinkedHashSet<>();
    for (String selector : selectors) {
      if (selector == null || selector.isBlank()) {
        continue;
      }
      String trimmed = selector.trim();
      if (trimmed.startsWith("#")) {
        return null;
      }
      normalized.add(trimmed);
    }
    return Set.copyOf(normalized);
  }

  private static Set<String> persistedIndexSelectors(IndexArtifactRecord record) {
    if (record == null) {
      return Set.of();
    }
    String encoded = record.getPropertiesOrDefault("indexed_columns", "");
    if (encoded.isBlank()) {
      return Set.of();
    }
    LinkedHashSet<String> selectors = new LinkedHashSet<>();
    for (String token : encoded.split(",")) {
      if (token != null && !token.isBlank()) {
        selectors.add(token.trim());
      }
    }
    return Set.copyOf(selectors);
  }

  private static byte[] writeIndexSidecar(
      FileTargetStats fileStats, List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    var output = new InMemoryOutputFile();
    Configuration configuration = new Configuration(false);
    var groupFactory = new SimpleGroupFactory(BOOTSTRAP_INDEX_SIDECAR_SCHEMA);
    List<FloecatConnector.ParquetPageIndexEntry> sortedEntries =
        sortedPageIndexEntries(pageIndexEntries);
    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(output)
            .withConf(configuration)
            .withType(BOOTSTRAP_INDEX_SIDECAR_SCHEMA)
            .withExtraMetaData(Map.of("sidecar.data_file_path", fileStats.getFilePath()))
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()) {
      for (FloecatConnector.ParquetPageIndexEntry entry : sortedEntries) {
        writer.write(toGroup(groupFactory, entry));
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to write index sidecar for " + fileStats.getFilePath(), e);
    }
    return output.toByteArray();
  }

  private static Group toGroup(
      SimpleGroupFactory groupFactory, FloecatConnector.ParquetPageIndexEntry entry) {
    // OSS Java intentionally prioritizes offset/dictionary metadata compatibility first.
    // Typed page min/max columns remain sparsely populated until we add a lightweight decoder pass.
    Group group =
        groupFactory
            .newGroup()
            .append("column_name", entry.columnName())
            .append("row_group", entry.rowGroup())
            .append("page_ordinal", entry.pageOrdinal())
            .append("first_row_index", entry.firstRowIndex())
            .append("row_count", entry.rowCount())
            .append("live_row_count", entry.liveRowCount())
            .append("page_total_compressed_size", entry.pageTotalCompressedSize())
            .append("requires_dictionary_page", entry.requiresDictionaryPage())
            .append("parquet_physical_type", entry.parquetPhysicalType())
            .append("parquet_compression", entry.parquetCompression())
            .append("parquet_max_def_level", (int) entry.parquetMaxDefLevel())
            .append("parquet_max_rep_level", (int) entry.parquetMaxRepLevel());
    if (entry.pageHeaderOffset() != null) {
      group.append("page_header_offset", entry.pageHeaderOffset());
    }
    if (entry.dictionaryPageHeaderOffset() != null) {
      group.append("dictionary_page_header_offset", entry.dictionaryPageHeaderOffset());
    }
    if (entry.dictionaryPageTotalCompressedSize() != null) {
      group.append(
          "dictionary_page_total_compressed_size", entry.dictionaryPageTotalCompressedSize());
    }
    if (entry.decimalPrecision() != null) {
      group.append("decimal_precision", entry.decimalPrecision());
    }
    if (entry.decimalScale() != null) {
      group.append("decimal_scale", entry.decimalScale());
    }
    if (entry.decimalBits() != null) {
      group.append("decimal_bits", entry.decimalBits());
    }
    if (entry.minI32() != null) {
      group.append("min_i32", entry.minI32());
    }
    if (entry.maxI32() != null) {
      group.append("max_i32", entry.maxI32());
    }
    if (entry.minI64() != null) {
      group.append("min_i64", entry.minI64());
    }
    if (entry.maxI64() != null) {
      group.append("max_i64", entry.maxI64());
    }
    if (entry.minF32() != null) {
      group.append("min_f32", entry.minF32());
    }
    if (entry.maxF32() != null) {
      group.append("max_f32", entry.maxF32());
    }
    if (entry.minF64() != null) {
      group.append("min_f64", entry.minF64());
    }
    if (entry.maxF64() != null) {
      group.append("max_f64", entry.maxF64());
    }
    if (entry.minBool() != null) {
      group.append("min_bool", entry.minBool());
    }
    if (entry.maxBool() != null) {
      group.append("max_bool", entry.maxBool());
    }
    if (entry.minUtf8() != null) {
      group.append("min_utf8", entry.minUtf8());
    }
    if (entry.maxUtf8() != null) {
      group.append("max_utf8", entry.maxUtf8());
    }
    if (entry.minDecimal128Unscaled() != null) {
      group.append(
          "min_decimal128_unscaled", Binary.fromConstantByteArray(entry.minDecimal128Unscaled()));
    }
    if (entry.maxDecimal128Unscaled() != null) {
      group.append(
          "max_decimal128_unscaled", Binary.fromConstantByteArray(entry.maxDecimal128Unscaled()));
    }
    if (entry.minDecimal256Unscaled() != null) {
      group.append(
          "min_decimal256_unscaled", Binary.fromConstantByteArray(entry.minDecimal256Unscaled()));
    }
    if (entry.maxDecimal256Unscaled() != null) {
      group.append(
          "max_decimal256_unscaled", Binary.fromConstantByteArray(entry.maxDecimal256Unscaled()));
    }
    return group;
  }

  private static List<FloecatConnector.ParquetPageIndexEntry> sortedPageIndexEntries(
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries) {
    if (pageIndexEntries == null || pageIndexEntries.isEmpty()) {
      return List.of();
    }
    return pageIndexEntries.stream()
        .sorted(
            Comparator.comparing(FloecatConnector.ParquetPageIndexEntry::columnName)
                .thenComparingInt(FloecatConnector.ParquetPageIndexEntry::rowGroup)
                .thenComparingInt(FloecatConnector.ParquetPageIndexEntry::pageOrdinal))
        .toList();
  }

  private static String sha256B64(byte[] data) {
    try {
      var messageDigest = MessageDigest.getInstance("SHA-256");
      return Base64.getEncoder().encodeToString(messageDigest.digest(data));
    } catch (Exception e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private static String base64ToHex(String base64) {
    byte[] digest = Base64.getDecoder().decode(base64);
    StringBuilder sb = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  private static final class InMemoryOutputFile implements OutputFile {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      buffer.reset();
      return new InMemoryPositionOutputStream(buffer);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      buffer.reset();
      return new InMemoryPositionOutputStream(buffer);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0L;
    }

    byte[] toByteArray() {
      return buffer.toByteArray();
    }
  }

  private static final class InMemoryPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream delegate;

    private InMemoryPositionOutputStream(ByteArrayOutputStream delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getPos() {
      return delegate.size();
    }

    @Override
    public void write(int b) {
      delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      delegate.write(b, off, len);
    }
  }
}
