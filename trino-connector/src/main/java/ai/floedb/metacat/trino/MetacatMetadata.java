package ai.floedb.metacat.trino;

import static io.trino.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static java.util.stream.Collectors.toList;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import com.google.inject.Inject;
import io.grpc.StatusRuntimeException;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.TrinoException;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpec.Builder;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types.NestedField;

/**
 * Core metadata implementation. Fetches schemas and tables from the Metacat gRPC service and builds
 * Iceberg-friendly handles, columns, and metadata.
 */
public class MetacatMetadata implements ConnectorMetadata {

  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceService;
  private final TableServiceGrpc.TableServiceBlockingStub tableService;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryService;
  private final CatalogName catalogName;
  private final CatalogHandle catalogHandle;
  private final TypeManager typeManager;

  @Inject
  public MetacatMetadata(
      MetacatClient client,
      CatalogName catalogName,
      CatalogHandle catalogHandle,
      TypeManager typeManager) {
    this.namespaceService = client.namespaces();
    this.tableService = client.tables();
    this.directoryService = client.directory();
    this.catalogName = catalogName;
    this.catalogHandle = catalogHandle;
    this.typeManager = typeManager;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    String catName = "sales";

    try {
      var catResponse =
          directoryService.resolveCatalog(
              ResolveCatalogRequest.newBuilder()
                  .setRef(NameRef.newBuilder().setCatalog(catName).build())
                  .build());

      var nsResponse =
          namespaceService.listNamespaces(
              ListNamespacesRequest.newBuilder().setCatalogId(catResponse.getResourceId()).build());

      return nsResponse.getNamespacesList().stream()
          .map(
              ns -> {
                if (ns.getParentsCount() == 0) {
                  return ns.getDisplayName();
                }
                String parentPath = String.join(".", ns.getParentsList());
                return parentPath.isEmpty()
                    ? ns.getDisplayName()
                    : parentPath + "." + ns.getDisplayName();
              })
          .toList();
    } catch (StatusRuntimeException e) {
      // This will show you the real gRPC status + description in Trino
      throw new TrinoException(
          REMOTE_TASK_ERROR,
          "Metacat listSchemaNames failed for catalog '"
              + catName
              + "': "
              + e.getStatus()
              + " - "
              + e.getStatus().getDescription(),
          e);
    } catch (Exception e) {
      // Anything else that blows up locally
      throw new TrinoException(
          REMOTE_TASK_ERROR,
          "Metacat listSchemaNames failed for catalog '" + catName + "': " + e.getMessage(),
          e);
    }
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    ResolveFQTablesRequest request =
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(NameMapper.prefixFromSchema(schemaName.orElse(null)))
            .build();
    ResolveFQTablesResponse response = directoryService.resolveFQTables(request);

    return response.getTablesList().stream().map(NameMapper::toSchemaTableName).collect(toList());
  }

  @Override
  public ConnectorTableHandle getTableHandle(
      ConnectorSession session,
      SchemaTableName tableName,
      Optional<ConnectorTableVersion> startVersion,
      Optional<ConnectorTableVersion> endVersion) {
    ResolveTableRequest resolveRequest =
        ResolveTableRequest.newBuilder()
            .setRef(NameMapper.nameRef(tableName.getSchemaName(), tableName.getTableName()))
            .build();
    ResourceId tableId = directoryService.resolveTable(resolveRequest).getResourceId();

    if (tableId == null || tableId.getId().isEmpty()) {
      return null;
    }

    GetTableRequest request = GetTableRequest.newBuilder().setTableId(tableId).build();
    var response = tableService.getTable(request);

    if (!response.hasTable() || !response.getTable().hasUpstream()) {
      return null;
    }

    String tableUri = response.getTable().getUpstream().getUri();
    String schemaJson = response.getTable().getSchemaJson();
    if (tableUri == null || tableUri.isEmpty() || schemaJson == null || schemaJson.isEmpty()) {
      return null;
    }

    List<String> partitionKeys = response.getTable().getUpstream().getPartitionKeysList();
    Map<String, Integer> fieldIds = response.getTable().getUpstream().getFieldIdByPathMap();

    PartitionSpec partitionSpec = buildPartitionSpec(schemaJson, partitionKeys, fieldIds);

    return new MetacatTableHandle(
        tableName,
        tableId,
        tableUri,
        schemaJson,
        PartitionSpecParser.toJson(partitionSpec),
        response.getTable().getUpstream().getFormat().name(),
        catalogHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    MetacatTableHandle handle = (MetacatTableHandle) tableHandle;
    Schema schema = SchemaParser.fromJson(handle.getSchemaJson());
    Map<String, ColumnMetadata> columns = buildColumns(schema);

    return new ConnectorTableMetadata(
        handle.getSchemaTableName(), columns.values().stream().toList());
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(
      ConnectorSession session, ConnectorTableHandle tableHandle) {
    MetacatTableHandle handle = (MetacatTableHandle) tableHandle;
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

  private PartitionSpec buildPartitionSpec(
      String schemaJson, List<String> partitionKeys, Map<String, Integer> fieldIds) {
    Schema schema = SchemaParser.fromJson(schemaJson);
    Builder builder = PartitionSpec.builderFor(schema);
    for (String key : partitionKeys) {
      builder.identity(key);
    }
    return builder.build();
  }
}
