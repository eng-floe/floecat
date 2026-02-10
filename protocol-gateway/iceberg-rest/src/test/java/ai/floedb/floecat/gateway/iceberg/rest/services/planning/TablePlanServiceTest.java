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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanTasksResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QueryClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.QuerySchemaClient;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DataFile;
import ai.floedb.floecat.query.rpc.DataFileBatch;
import ai.floedb.floecat.query.rpc.DeleteFile;
import ai.floedb.floecat.query.rpc.DeleteFileBatch;
import ai.floedb.floecat.query.rpc.DeleteRef;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.InitScanRequest;
import ai.floedb.floecat.query.rpc.InitScanResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.query.rpc.TableInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Empty;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
    service.queryClient = new QueryClient(grpc);
    service.querySchemaClient = new QuerySchemaClient(grpc);
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
            ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
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
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        List.of("id"),
        null,
        null,
        3L,
        null,
        filter,
        false,
        true,
        10L);

    ScanFile data =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/data.parquet")
            .setFileFormat("PARQUET")
            .setFileSizeInBytes(10)
            .setRecordCount(5)
            .setPartitionDataJson("{\"partitionValues\":[1,\"A\"]}")
            .addDeleteFileIndices(0)
            .build();
    ScanFile deleteFile =
        ScanFile.newBuilder()
            .setFilePath("s3://bucket/delete.parquet")
            .setFileFormat("PARQUET")
            .build();
    ScanBundle bundle =
        ScanBundle.newBuilder().addDataFiles(data).addDeleteFiles(deleteFile).build();
    ScanHandle handle = mockScanForBundle(tableId, bundle);

    List<StorageCredentialDto> credentials =
        List.of(new StorageCredentialDto("s3", Map.of("role", "arn:aws:iam::123:role/Test")));
    TablePlanResponseDto response = service.fetchPlan("plan-2", credentials);

    assertEquals("completed", response.status());
    assertEquals("plan-2", response.planId());
    assertIterableEquals(List.of("plan-2"), response.planTasks());
    assertEquals(credentials, response.storageCredentials());
    assertEquals(1, response.fileScanTasks().size());
    assertEquals("s3://bucket/data.parquet", response.fileScanTasks().get(0).dataFile().filePath());
    assertEquals("s3://bucket/delete.parquet", response.deleteFiles().get(0).filePath());

    ArgumentCaptor<InitScanRequest> fetch = ArgumentCaptor.forClass(InitScanRequest.class);
    verify(scanStub).initScan(fetch.capture());
    InitScanRequest sent = fetch.getValue();
    assertEquals(tableId, sent.getTableId());
    assertEquals(1, sent.getRequiredColumnsCount());
    assertEquals("id", sent.getRequiredColumns(0));
    assertEquals(2, sent.getPredicatesCount());
    assertEquals("customerid", sent.getPredicates(0).getColumn());
    assertEquals("DeletedFlag".toLowerCase(), sent.getPredicates(1).getColumn());
    verify(scanStub).streamDeleteFiles(handle);
    verify(scanStub).streamDataFiles(handle);
    verify(scanStub).closeScan(handle);
  }

  @Test
  void fetchPlanSkipsUnsupportedLogicalFilters() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-4").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    when(queryStub.getQuery(any()))
        .thenReturn(GetQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Map<String, Object> filter =
        Map.of(
            "type",
            "or",
            "left",
            Map.of("type", "eq", "term", "id", "value", 7),
            "right",
            Map.of("type", "not", "child", Map.of("type", "eq", "term", "id", "value", 9)));

    service.startPlan(
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        null,
        null,
        null,
        3L,
        null,
        filter,
        true,
        false,
        null);

    ScanBundle bundle = ScanBundle.newBuilder().build();
    ScanHandle handle = mockScanForBundle(tableId, bundle);

    service.fetchPlan("plan-4", List.of());

    ArgumentCaptor<InitScanRequest> initFetch = ArgumentCaptor.forClass(InitScanRequest.class);
    verify(scanStub).initScan(initFetch.capture());
    assertEquals(0, initFetch.getValue().getPredicatesCount());
  }

  @Test
  void fetchPlanAcceptsTransformTerms() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-5").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    when(queryStub.getQuery(any()))
        .thenReturn(GetQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    Map<String, Object> filter =
        Map.of(
            "type",
            "eq",
            "term",
            Map.of("type", "transform", "transform", "bucket[16]", "term", "OrderId"),
            "value",
            11);

    service.startPlan(
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        null,
        null,
        null,
        3L,
        null,
        filter,
        true,
        false,
        null);

    ScanBundle bundle = ScanBundle.newBuilder().build();
    ScanHandle handle = mockScanForBundle(tableId, bundle);

    service.fetchPlan("plan-5", List.of());

    ArgumentCaptor<InitScanRequest> initFetch = ArgumentCaptor.forClass(InitScanRequest.class);
    verify(scanStub).initScan(initFetch.capture());
    assertEquals(1, initFetch.getValue().getPredicatesCount());
    assertEquals("OrderId", initFetch.getValue().getPredicates(0).getColumn());
  }

  @Test
  void fetchTasksReturnsBundleAndClearsContext() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-3").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();
    service.startPlan(
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        false,
        null);

    ScanBundle bundle = ScanBundle.newBuilder().build();
    mockScanForBundle(tableId, bundle);

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
    service.startPlan(
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        false,
        null);

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
  void unsupportedLogicalFilterDoesNotFailPlanning() {
    QueryDescriptor descriptor = QueryDescriptor.newBuilder().setQueryId("plan-5").build();
    when(queryStub.beginQuery(any()))
        .thenReturn(BeginQueryResponse.newBuilder().setQuery(descriptor).build());
    ResourceId tableId = ResourceId.newBuilder().setId("cat:db:orders").build();

    Map<String, Object> filter = Map.of("type", "or");

    service.startPlan(
        ResourceId.newBuilder().setId("cat").setKind(ResourceKind.RK_CATALOG).build(),
        tableId,
        null,
        null,
        null,
        null,
        null,
        filter,
        false,
        false,
        null);
    verify(queryStub, times(1)).beginQuery(any());
  }

  private ScanHandle mockScanForBundle(ResourceId tableId, ScanBundle bundle) {
    ScanHandle handle = ScanHandle.newBuilder().setId("handle").build();
    InitScanResponse initResp =
        InitScanResponse.newBuilder()
            .setHandle(handle)
            .setTableInfo(TableInfo.newBuilder().setTableId(tableId).build())
            .build();
    when(scanStub.initScan(any())).thenReturn(initResp);
    AtomicInteger deleteId = new AtomicInteger(0);
    List<DeleteFile> deleteFiles =
        bundle.getDeleteFilesList().stream()
            .map(
                file ->
                    DeleteFile.newBuilder()
                        .setDeleteId(deleteId.getAndIncrement())
                        .setFile(file)
                        .build())
            .toList();
    when(scanStub.streamDeleteFiles(handle))
        .thenReturn(
            List.of(DeleteFileBatch.newBuilder().addAllItems(deleteFiles).build()).iterator());
    List<DataFile> dataFiles =
        bundle.getDataFilesList().stream()
            .map(
                file ->
                    DataFile.newBuilder()
                        .setFile(file)
                        .setDeletes(DeleteRef.newBuilder().setAllDeletes(true))
                        .build())
            .toList();
    when(scanStub.streamDataFiles(handle))
        .thenReturn(List.of(DataFileBatch.newBuilder().addAllItems(dataFiles).build()).iterator());
    when(scanStub.closeScan(handle)).thenReturn(Empty.newBuilder().build());
    return handle;
  }
}
