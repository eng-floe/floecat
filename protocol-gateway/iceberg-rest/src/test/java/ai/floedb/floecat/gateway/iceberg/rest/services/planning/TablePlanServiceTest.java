package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanTasksResponseDto;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TablePlanServiceTest {
  private final TablePlanService service = new TablePlanService();
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final GrpcClients clients = mock(GrpcClients.class);
  private final QueryServiceGrpc.QueryServiceBlockingStub queryStub =
      mock(QueryServiceGrpc.QueryServiceBlockingStub.class);
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
      mock(QueryScanServiceGrpc.QueryScanServiceBlockingStub.class);
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaStub =
      mock(QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub.class);

  @BeforeEach
  void setUp() {
    service.mapper = new ObjectMapper();
    service.queryClient =
        new ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient(grpc);
    service.querySchemaClient =
        new ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient(grpc);
    when(grpc.raw()).thenReturn(clients);
    when(clients.query()).thenReturn(queryStub);
    when(clients.queryScan()).thenReturn(scanStub);
    when(clients.querySchema()).thenReturn(schemaStub);
    when(grpc.withHeaders(queryStub)).thenReturn(queryStub);
    when(grpc.withHeaders(scanStub)).thenReturn(scanStub);
    when(grpc.withHeaders(schemaStub)).thenReturn(schemaStub);
  }

  @Test
  void startPlanRegistersInputsAndReturnsHandle() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-1").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    TablePlanService.PlanHandle handle =
        service.startPlan(
            "cat",
            tableId,
            List.of("id", "total"),
            null,
            null,
            7L,
            List.of("rows"),
            null,
            true,
            false,
            null);

    assertEquals("plan-1", handle.queryId());

    ArgumentCaptor<DescribeInputsRequest> describe =
        ArgumentCaptor.forClass(DescribeInputsRequest.class);
    verify(schemaStub).describeInputs(describe.capture());
    DescribeInputsRequest sent = describe.getValue();
    assertEquals("plan-1", sent.getQueryId());
    assertEquals(tableId, sent.getInputs(0).getTableId());
    assertEquals(7L, sent.getInputs(0).getSnapshot().getSnapshotId());
  }

  @Test
  void fetchPlanBuildsTasksAndPredicates() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-2").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    when(queryStub.getQuery(any()))
        .thenReturn(GetQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Map<String, Object> filter =
        Map.of(
            "type",
            "and",
            "expressions",
            List.of(
                Map.of(
                    "type",
                    "equal",
                    "term",
                    Map.of("type", "reference", "name", "CustomerId"),
                    "literal",
                    Map.of("type", "long", "value", 5)),
                Map.of("type", "is-null", "term", "DeletedFlag")));

    service.startPlan(
        "cat", tableId, List.of("id"), null, null, 3L, null, filter, false, true, 10L);

    ScanFile data =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/data.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(10)
            .setRecordCount(5)
            .setPartitionDataJson("{\"partitionValues\":[1,\"A\"]}")
            .build();
    ScanFile deleteFile =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/delete.parquet")
            .setFileFormat("PARQUET")
            .build();
    ScanBundle bundle =
        ScanBundle.newBuilder().addDataFiles(data).addDeleteFiles(deleteFile).build();
    when(scanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    List<StorageCredentialDto> credentials =
        List.of(new StorageCredentialDto("s3", Map.of("role", "arn:aws:iam::123:role/Test")));
    TablePlanResponseDto response = service.fetchPlan("plan-2", credentials);

    assertEquals("completed", response.status());
    assertEquals("plan-2", response.planId());
    assertIterableEquals(List.of("plan-2"), response.planTasks());
    assertEquals(credentials, response.storageCredentials());
    assertEquals(1, response.fileScanTasks().size());
    assertEquals(List.of(1, "A"), response.fileScanTasks().get(0).dataFile().partition());
    assertEquals("s3://bucket/delete.parquet", response.deleteFiles().get(0).filePath());

    ArgumentCaptor<FetchScanBundleRequest> fetch =
        ArgumentCaptor.forClass(FetchScanBundleRequest.class);
    verify(scanStub).fetchScanBundle(fetch.capture());
    FetchScanBundleRequest sent = fetch.getValue();
    assertEquals(tableId, sent.getTableId());
    assertEquals(1, sent.getRequiredColumnsCount());
    assertEquals("id", sent.getRequiredColumns(0));
    assertEquals(2, sent.getPredicatesCount());
    assertEquals("customerid", sent.getPredicates(0).getColumn());
    assertEquals("DeletedFlag".toLowerCase(), sent.getPredicates(1).getColumn());
  }

  @Test
  void fetchTasksReturnsBundleAndClearsContext() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-3").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    service.startPlan("cat", tableId, null, null, null, null, null, null, true, false, null);

    ScanBundle bundle = ScanBundle.newBuilder().build();
    when(scanStub.fetchScanBundle(any()))
        .thenReturn(FetchScanBundleResponse.newBuilder().setBundle(bundle).build());

    TablePlanTasksResponseDto tasks = service.fetchTasks("plan-3");
    assertTrue(tasks.fileScanTasks().isEmpty());

    assertThrows(IllegalArgumentException.class, () -> service.fetchTasks("plan-3"));
  }

  @Test
  void cancelPlanEndsQueryAndDropsContext() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-4").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    service.startPlan("cat", tableId, null, null, null, null, null, null, true, false, null);

    service.cancelPlan("plan-4");

    ArgumentCaptor<EndQueryRequest> endCaptor = ArgumentCaptor.forClass(EndQueryRequest.class);
    verify(queryStub).endQuery(endCaptor.capture());
    assertEquals("plan-4", endCaptor.getValue().getQueryId());
    assertFalse(endCaptor.getValue().getCommit());
    assertThrows(
        IllegalArgumentException.class,
        () -> service.fetchPlan("plan-4", List.of(new StorageCredentialDto("s3", Map.of()))));
  }

  @Test
  void invalidFilterExpressionThrows() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-5").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    Map<String, Object> filter = Map.of("type", "or");

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.startPlan(
                "cat", tableId, null, null, null, null, null, filter, false, false, null));
    verify(queryStub, times(0)).beginQuery(any());
  }
}
