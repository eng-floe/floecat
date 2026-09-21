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

package ai.floedb.floecat.client.trino;


import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.RelationServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.engine.catalog.RelationResults;
import com.google.inject.Inject;
import com.google.protobuf.Timestamp;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.TypeManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types.NestedField;

public class FloecatMetadata implements ConnectorMetadata {

  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceService;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryService;
  private final RelationServiceGrpc.RelationServiceBlockingStub relationService;
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotService;
  private final CatalogName catalogName;
  private final CatalogHandle catalogHandle;
  private final TypeManager typeManager;

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(FloecatMetadata.class);

  @Inject
  public FloecatMetadata(
      FloecatClient client,
      CatalogName catalogName,
      CatalogHandle catalogHandle,
      TypeManager typeManager) {
    this(
        client.namespaces(),
        client.directory(),
        client.relations(),
        client.snapshots(),
        catalogName,
        catalogHandle,
        typeManager);
  }

  // Testing/helper constructor to allow direct stub injection
  FloecatMetadata(
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceService,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryService,
      RelationServiceGrpc.RelationServiceBlockingStub relationService,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotService,
      CatalogName catalogName,
      CatalogHandle catalogHandle,
      TypeManager typeManager) {
    this.namespaceService = namespaceService;
    this.directoryService = directoryService;
    this.relationService = relationService;
    this.snapshotService = snapshotService;
    this.catalogName = catalogName;
    this.catalogHandle = catalogHandle;
    this.typeManager = typeManager;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return namespaceEntries().stream().map(NamespaceEntry::schemaName).toList();
  }

  private record NamespaceEntry(ResourceId id, String schemaName) {}

  private ResourceId catalogId() {
    return directoryService
        .resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(catalogName.toString()).build())
                .build())
        .getResourceId();
  }

  /** Every namespace in the catalog, nested ones included, flattened to dotted Trino schemas. */
  private List<NamespaceEntry> namespaceEntries() {
    ResourceId catalogId = catalogId();
    var out = new ArrayList<NamespaceEntry>();
    String token = "";
    do {
      var response =
          namespaceService.listNamespaces(
              ListNamespacesRequest.newBuilder()
                  .setCatalogId(catalogId)
                  .setRecursive(true)
                  .setPage(PageRequest.newBuilder().setPageToken(token).build())
                  .build());
      for (Namespace ns : response.getNamespacesList()) {
        out.add(new NamespaceEntry(ns.getResourceId(), schemaNameOf(ns)));
      }
      token = response.getPage().getNextPageToken();
    } while (!token.isEmpty());
    return out;
  }

  private static String schemaNameOf(Namespace ns) {
    if (ns.getParentsCount() == 0) {
      return ns.getDisplayName();
    }
    String parentPath = String.join(".", ns.getParentsList());
    return parentPath.isEmpty() ? ns.getDisplayName() : parentPath + "." + ns.getDisplayName();
  }

  /** The dotted schema a relation sits in, from the path on its resolved name. */
  private static String schemaOf(Relation relation, String fallback) {
    List<String> path = relation.getName().getPathList();
    return path.isEmpty() ? fallback : String.join(".", path);
  }

  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    var request = ListRelationsRequest.newBuilder().addKinds(ResourceKind.RK_TABLE);
    String fallbackSchema = schemaName.orElse("");

    if (schemaName.isPresent()) {
      Optional<NamespaceEntry> namespace =
          namespaceEntries().stream()
              .filter(entry -> entry.schemaName().equals(schemaName.get()))
              .findFirst();
      if (namespace.isEmpty()) {
        return List.of();
      }
      request.setNamespaceId(namespace.get().id());
    } else {
      request.setCatalogId(catalogId()).setRecursive(true);
    }

    var out = new ArrayList<SchemaTableName>();
    String token = "";
    do {
      var response =
          relationService.listRelations(
              request.setPage(PageRequest.newBuilder().setPageToken(token)).build());
      var page = RelationResults.read(response);
      RelationResults.requireComplete(page);
      for (Relation relation : page.relations()) {
        out.add(
            new SchemaTableName(schemaOf(relation, fallbackSchema), relation.getDisplayName()));
      }
      token = page.nextPageToken();
    } while (!token.isEmpty());
    return out;
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {

    NameRef nameRef =
        NameMapper.nameRef(
            catalogName.toString(), tableName.getSchemaName(), tableName.getTableName());
    var resolved =
        relationService.resolveRelations(
            ResolveRelationsRequest.newBuilder()
                .addReferences(RelationReference.newBuilder().addCandidates(nameRef))
                .setIncludeSchema(true)
                .build());

    Relation relation;
    try {
      relation = RelationResults.requireResolved(resolved);
    } catch (RelationResults.RelationResolutionException e) {
      if (e.isNotFound()) {
        return null;
      }
      throw e;
    }
    if (relation.getResourceId().getKind() != ResourceKind.RK_TABLE || !relation.hasTable()) {
      return null;
    }

    var details = relation.getTable();
    if (!details.hasUpstream()) {
      return null;
    }

    ResourceId tableId = relation.getResourceId();
    String tableUri = details.getUpstream().getUri();
    String schemaJson = details.getSchemaJson();
    if (tableUri == null || tableUri.isEmpty() || schemaJson == null || schemaJson.isEmpty()) {
      return null;
    }

    List<String> partitionKeys = details.getUpstream().getPartitionKeysList();

    Optional<Long> snapshotId = FloecatSessionProperties.getSnapshotId(session);
    Optional<Long> asOfMillis = FloecatSessionProperties.getAsOfEpochMillis(session);
    if (snapshotId.isPresent() && asOfMillis.isPresent()) {
      throw new IllegalArgumentException(
          "Only one of snapshot_id or as_of_epoch_millis may be set");
    }

    if (snapshotId.isPresent() || asOfMillis.isPresent()) {
      Timestamp asOfTs = null;
      if (asOfMillis.isPresent()) {
        long ms = asOfMillis.get();
        asOfTs =
            Timestamp.newBuilder()
                .setSeconds(Math.floorDiv(ms, 1000))
                .setNanos((int) ((ms % 1000) * 1_000_000))
                .build();
      }

      var snapRefBuilder = SnapshotRef.newBuilder();
      if (snapshotId.isPresent()) {
        snapRefBuilder.setSnapshotId(snapshotId.get());
      } else if (asOfTs != null) {
        snapRefBuilder.setAsOf(asOfTs);
      } else {
        snapRefBuilder.setSpecial(SpecialSnapshot.SS_CURRENT);
      }

      var snapReq =
          GetSnapshotRequest.newBuilder()
              .setTableId(tableId)
              .setSnapshot(snapRefBuilder.build())
              .build();
      var snapResp = snapshotService.getSnapshot(snapReq);
      if (snapResp.hasSnapshot() && !snapResp.getSnapshot().getSchemaJson().isBlank()) {
        schemaJson = snapResp.getSnapshot().getSchemaJson();
      }
    }

    PartitionSpec partitionSpec = buildPartitionSpec(schemaJson, partitionKeys);

    return new FloecatTableHandle(
        tableName,
        tableId.getId(),
        tableId.getAccountId(),
        tableId.getKind().name(),
        tableUri,
        schemaJson,
        PartitionSpecParser.toJson(partitionSpec),
        details.getUpstream().getFormat().name(),
        catalogHandle.getId(),
        TupleDomain.all(),
        Set.of(),
        snapshotId.orElse(null),
        asOfMillis.orElse(null));
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    FloecatTableHandle handle = (FloecatTableHandle) tableHandle;
    Schema schema = SchemaParser.fromJson(handle.getSchemaJson());
    Map<String, ColumnMetadata> columns = buildColumns(schema);

    return new ConnectorTableMetadata(
        handle.getSchemaTableName(), columns.values().stream().toList());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      ConnectorSession session, SchemaTablePrefix prefix) {
    List<SchemaTableName> tables =
        listTables(session, prefix.getSchema().map(Optional::of).orElse(Optional.empty()));
    Map<SchemaTableName, List<ColumnMetadata>> map = new LinkedHashMap<>();
    for (SchemaTableName table : tables) {
      ConnectorTableHandle handle =
          getTableHandle(session, table, Optional.empty(), Optional.empty());
      if (handle == null) {
        continue;
      }
      ConnectorTableMetadata meta = getTableMetadata(session, handle);
      map.put(table, meta.getColumns());
    }
    return map;
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    FloecatTableHandle handle = (FloecatTableHandle) tableHandle;
    Schema schema = SchemaParser.fromJson(handle.getSchemaJson());
    Map<String, ColumnMetadata> cols = buildColumns(schema);
    Map<String, ColumnHandle> handles = new LinkedHashMap<>();
    for (ColumnMetadata col : cols.values()) {
      NestedField field = schema.findField(col.getName());
      if (field == null) {
        continue;
      }
      ColumnIdentity identity = ColumnIdentity.createColumnIdentity(field);
      IcebergColumnHandle icebergCol =
          new IcebergColumnHandle(
              identity,
              col.getType(),
              List.of(),
              col.getType(),
              field.isOptional(),
              Optional.ofNullable(col.getComment()));
      handles.put(col.getName(), icebergCol);
    }
    return handles;
  }

  @Override
  public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
      ConnectorSession session,
      ConnectorTableHandle table,
      List<ConnectorExpression> projections,
      Map<String, ColumnHandle> assignments) {

    FloecatTableHandle handle = (FloecatTableHandle) table;
    Set<String> projected =
        assignments.values().stream()
            .map(ch -> ((IcebergColumnHandle) ch).getName())
            .collect(Collectors.toSet());

    FloecatTableHandle newHandle =
        new FloecatTableHandle(
            handle.getSchemaTableName(),
            handle.getTableId(),
            handle.getTableAccountId(),
            handle.getTableKind(),
            handle.getUri(),
            handle.getSchemaJson(),
            handle.getPartitionSpecJson(),
            handle.getFormat(),
            handle.getCatalogHandleId(),
            handle.getEnforcedConstraint(),
            projected,
            handle.getSnapshotId(),
            handle.getAsOfEpochMillis());

    List<Assignment> projectionAssignments =
        assignments.entrySet().stream()
            .map(
                e ->
                    new Assignment(
                        e.getKey(), e.getValue(), ((IcebergColumnHandle) e.getValue()).getType()))
            .toList();

    return Optional.of(
        new ProjectionApplicationResult<>(newHandle, projections, projectionAssignments, false));
  }

  @Override
  public ColumnMetadata getColumnMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
    IcebergColumnHandle col = (IcebergColumnHandle) columnHandle;
    return ColumnMetadata.builder()
        .setName(col.getName())
        .setType(col.getType())
        .setNullable(col.isNullable())
        .setComment(col.getComment())
        .build();
  }

  private Map<String, ColumnMetadata> buildColumns(Schema schema) {
    Map<String, ColumnMetadata> columns = new LinkedHashMap<>();
    for (NestedField field : schema.columns()) {
      columns.put(
          field.name(),
          ColumnMetadata.builder()
              .setName(field.name())
              .setType(TypeConverter.toTrinoType(field.type(), typeManager))
              .setNullable(field.isOptional())
              .setComment(Optional.ofNullable(field.doc()))
              .setHidden(false)
              .build());
    }
    return columns;
  }

  PartitionSpec buildPartitionSpec(String schemaJson, List<String> partitionKeys) {
    Schema schema = SchemaParser.fromJson(schemaJson);
    Builder builder = PartitionSpec.builderFor(schema);
    for (String key : partitionKeys) {
      builder.identity(key);
    }
    return builder.build();
  }

  @Override
  public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
      ConnectorSession session, ConnectorTableHandle table, Constraint constraint) {
    FloecatTableHandle handle = (FloecatTableHandle) table;
    TupleDomain<IcebergColumnHandle> incoming =
        constraint.getSummary().transformKeys(ch -> (IcebergColumnHandle) ch);
    TupleDomain<IcebergColumnHandle> current = handle.getEnforcedConstraint();
    TupleDomain<IcebergColumnHandle> domain = current.intersect(incoming);

    if (domain.equals(current)) {
      return Optional.empty();
    }

    FloecatTableHandle newHandle =
        new FloecatTableHandle(
            handle.getSchemaTableName(),
            handle.getTableId(),
            handle.getTableAccountId(),
            handle.getTableKind(),
            handle.getUri(),
            handle.getSchemaJson(),
            handle.getPartitionSpecJson(),
            handle.getFormat(),
            handle.getCatalogHandleId(),
            domain,
            handle.getProjectedColumns(),
            handle.getSnapshotId(),
            handle.getAsOfEpochMillis());
    ConnectorExpression remainingExpr =
        constraint.getExpression() == null
            ? new Constant(Boolean.TRUE, BooleanType.BOOLEAN)
            : constraint.getExpression();

    LOG.debug(
        "applyFilter: incoming summary={} current={} new={}",
        constraint.getSummary(),
        current,
        domain);

    return Optional.of(
        new ConstraintApplicationResult<>(
            newHandle, constraint.getSummary(), remainingExpr, false));
  }
}
