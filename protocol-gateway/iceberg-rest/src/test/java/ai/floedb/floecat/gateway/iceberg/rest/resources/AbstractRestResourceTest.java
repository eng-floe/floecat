package ai.floedb.floecat.gateway.iceberg.rest.resources;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.DeleteConnectorResponse;
import ai.floedb.floecat.connector.rpc.SyncCaptureResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableRepository;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableDropCleanupService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc.QueryServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.test.InjectMock;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;

public abstract class AbstractRestResourceTest {

  @InjectMock protected GrpcWithHeaders grpc;
  @InjectMock protected GrpcClients clients;
  @InjectMock protected TableMetadataImportService metadataImportService;
  @InjectMock protected TableDropCleanupService tableDropCleanupService;
  @Inject protected StagedTableRepository stageRepository;
  @Inject protected ViewMetadataService viewMetadataService;

  protected TableServiceGrpc.TableServiceBlockingStub tableStub;
  protected DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub;
  protected NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceStub;
  protected ViewServiceGrpc.ViewServiceBlockingStub viewStub;
  protected SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub;
  protected TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statsStub;
  protected QueryServiceBlockingStub queryStub;
  protected QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub;
  protected QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchemaStub;
  protected ConnectorsGrpc.ConnectorsBlockingStub connectorsStub;
  protected RequestSpecification defaultSpec;

  @BeforeEach
  void setUpCommon() {
    tableStub = Mockito.mock(TableServiceGrpc.TableServiceBlockingStub.class);
    directoryStub = Mockito.mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
    namespaceStub = Mockito.mock(NamespaceServiceGrpc.NamespaceServiceBlockingStub.class);
    viewStub = Mockito.mock(ViewServiceGrpc.ViewServiceBlockingStub.class);
    snapshotStub = Mockito.mock(SnapshotServiceGrpc.SnapshotServiceBlockingStub.class);
    statsStub = Mockito.mock(TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub.class);
    queryStub = Mockito.mock(QueryServiceGrpc.QueryServiceBlockingStub.class);
    queryScanStub = Mockito.mock(QueryScanServiceGrpc.QueryScanServiceBlockingStub.class);
    querySchemaStub = Mockito.mock(QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub.class);
    connectorsStub = Mockito.mock(ConnectorsGrpc.ConnectorsBlockingStub.class);

    Mockito.when(clients.table()).thenReturn(tableStub);
    Mockito.when(clients.directory()).thenReturn(directoryStub);
    Mockito.when(clients.namespace()).thenReturn(namespaceStub);
    Mockito.when(clients.view()).thenReturn(viewStub);
    Mockito.when(clients.snapshot()).thenReturn(snapshotStub);
    Mockito.when(clients.stats()).thenReturn(statsStub);
    Mockito.when(clients.query()).thenReturn(queryStub);
    Mockito.when(clients.queryScan()).thenReturn(queryScanStub);
    Mockito.when(clients.querySchema()).thenReturn(querySchemaStub);
    Mockito.when(clients.connectors()).thenReturn(connectorsStub);
    Mockito.when(grpc.raw()).thenReturn(clients);
    Mockito.when(grpc.withHeaders(tableStub)).thenReturn(tableStub);
    Mockito.when(grpc.withHeaders(directoryStub)).thenReturn(directoryStub);
    Mockito.when(grpc.withHeaders(namespaceStub)).thenReturn(namespaceStub);
    Mockito.when(grpc.withHeaders(viewStub)).thenReturn(viewStub);
    Mockito.when(grpc.withHeaders(snapshotStub)).thenReturn(snapshotStub);
    Mockito.when(grpc.withHeaders(statsStub)).thenReturn(statsStub);
    Mockito.when(grpc.withHeaders(queryStub)).thenReturn(queryStub);
    Mockito.when(grpc.withHeaders(queryScanStub)).thenReturn(queryScanStub);
    Mockito.when(grpc.withHeaders(querySchemaStub)).thenReturn(querySchemaStub);
    Mockito.when(grpc.withHeaders(connectorsStub)).thenReturn(connectorsStub);
    Mockito.when(querySchemaStub.describeInputs(Mockito.any()))
        .thenReturn(DescribeInputsResponse.getDefaultInstance());
    Mockito.when(snapshotStub.createSnapshot(Mockito.any()))
        .thenReturn(CreateSnapshotResponse.newBuilder().build());
    Mockito.when(snapshotStub.deleteSnapshot(Mockito.any()))
        .thenReturn(DeleteSnapshotResponse.newBuilder().build());
    Mockito.when(snapshotStub.listSnapshots(Mockito.any()))
        .thenReturn(ListSnapshotsResponse.getDefaultInstance());
    Mockito.when(snapshotStub.getSnapshot(Mockito.any()))
        .thenReturn(GetSnapshotResponse.getDefaultInstance());
    Mockito.when(viewStub.getView(Mockito.any())).thenReturn(GetViewResponse.getDefaultInstance());
    Mockito.when(connectorsStub.triggerReconcile(Mockito.any()))
        .thenReturn(TriggerReconcileResponse.newBuilder().setJobId("job").build());
    Mockito.when(connectorsStub.syncCapture(Mockito.any()))
        .thenReturn(SyncCaptureResponse.newBuilder().build());
    Mockito.when(queryStub.getQuery(Mockito.any()))
        .thenAnswer(
            inv -> {
              var request = inv.getArgument(0, GetQueryRequest.class);
              String queryId = request == null ? "" : request.getQueryId();
              return GetQueryResponse.newBuilder()
                  .setQuery(QueryDescriptor.newBuilder().setQueryId(queryId).build())
                  .build();
            });
    Mockito.when(connectorsStub.deleteConnector(Mockito.any()))
        .thenReturn(DeleteConnectorResponse.newBuilder().build());
    Mockito.when(metadataImportService.importMetadata(Mockito.any(), Mockito.any()))
        .thenAnswer(
            inv -> {
              String metadataLocation = inv.getArgument(0, String.class);
              String tableLocation = metadataLocation;
              if (tableLocation != null) {
                int idx = tableLocation.indexOf("/metadata");
                if (idx > 0) {
                  tableLocation = tableLocation.substring(0, idx);
                } else {
                  int slash = tableLocation.lastIndexOf('/');
                  if (slash > 0) {
                    tableLocation = tableLocation.substring(0, slash);
                  }
                }
              }
              return new ImportedMetadata(
                  "{\"type\":\"struct\",\"fields\":[]}", Map.of(), tableLocation, null, List.of());
            });
    ResourceId catalogId = ResourceId.newBuilder().setId("cat:default").build();
    Mockito.when(directoryStub.resolveCatalog(Mockito.any()))
        .thenReturn(ResolveCatalogResponse.newBuilder().setResourceId(catalogId).build());
    Mockito.when(directoryStub.resolveNamespace(Mockito.any()))
        .thenAnswer(
            inv -> {
              ResolveNamespaceRequest request = inv.getArgument(0, ResolveNamespaceRequest.class);
              if (request == null || !request.hasRef()) {
                return ResolveNamespaceResponse.getDefaultInstance();
              }
              ResourceId id =
                  buildResourceId(
                      request.getRef().getCatalog(), request.getRef().getPathList(), null);
              return ResolveNamespaceResponse.newBuilder().setResourceId(id).build();
            });
    Mockito.when(directoryStub.resolveTable(Mockito.any()))
        .thenAnswer(
            inv -> {
              ResolveTableRequest request = inv.getArgument(0, ResolveTableRequest.class);
              if (request == null || !request.hasRef()) {
                return ResolveTableResponse.getDefaultInstance();
              }
              ResourceId id =
                  buildResourceId(
                      request.getRef().getCatalog(),
                      request.getRef().getPathList(),
                      request.getRef().getName());
              return ResolveTableResponse.newBuilder().setResourceId(id).build();
            });
    Mockito.when(directoryStub.resolveView(Mockito.any()))
        .thenAnswer(
            inv -> {
              ResolveViewRequest request = inv.getArgument(0, ResolveViewRequest.class);
              if (request == null || !request.hasRef()) {
                return ResolveViewResponse.getDefaultInstance();
              }
              ResourceId id =
                  buildResourceId(
                      request.getRef().getCatalog(),
                      request.getRef().getPathList(),
                      request.getRef().getName());
              return ResolveViewResponse.newBuilder().setResourceId(id).build();
            });
    defaultSpec =
        new RequestSpecBuilder()
            .addHeader("x-tenant-id", "account1")
            .addHeader("authorization", "Bearer token")
            .build();
    RestAssured.requestSpecification = defaultSpec;
    stageRepository.clear();
  }

  private ResourceId buildResourceId(String catalog, List<String> path, String leafName) {
    StringBuilder builder = new StringBuilder("cat");
    if (catalog != null && !catalog.isBlank()) {
      builder.append(':').append(catalog);
    }
    if (path != null) {
      for (String part : path) {
        if (part != null && !part.isBlank()) {
          builder.append(':').append(part);
        }
      }
    }
    if (leafName != null && !leafName.isBlank()) {
      builder.append(':').append(leafName);
    }
    return ResourceId.newBuilder().setId(builder.toString()).build();
  }

  protected String stageCreateRequestWithoutLocation(String tableName) {
    return """
    {
      "name": "%s",
      "schema": {
        "schema-id": 0,
        "type": "struct",
        "fields": [
          {
            "id": 1,
            "name": "i",
            "required": false,
            "type": "int"
          }
        ]
      },
      "partition-spec": {
        "spec-id": 0,
        "fields": []
      },
      "write-order": {
        "order-id": 0,
        "fields": []
      },
      "properties": {},
      "location": null,
      "stage-create": true
    }
    """
        .formatted(tableName);
  }

  protected String stageCreateRequest(String tableName) {
    return """
    {
      "name": "%s",
      "schema": {
        "schema-id": 1,
        "last-column-id": 1,
        "type": "struct",
        "fields": [
          {
            "id": 1,
            "name": "id",
            "required": true,
            "type": "int"
          }
        ]
      },
      "partition-spec": {
        "spec-id": 0,
        "fields": [
          {
            "name": "id",
            "field-id": 1,
            "source-id": 1,
            "transform": "identity"
          }
        ]
      },
      "write-order": {
        "sort-order-id": 0,
        "fields": [
          {
            "source-id": 1
          }
        ]
      },
      "properties": {
        "metadata-location": "s3://bucket/%s/metadata/00000-abc.metadata.json",
        "io-impl": "org.apache.iceberg.inmemory.InMemoryFileIO"
      },
      "location": "s3://bucket/%s",
      "stage-create": true
    }
    """
        .formatted(tableName, tableName, tableName);
  }

  protected static IcebergMetadata metadataFromSpec(SnapshotSpec spec) {
    try {
      return IcebergMetadata.parseFrom(spec.getFormatMetadataOrThrow("iceberg"));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to parse Iceberg metadata", e);
    }
  }
}
