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

package ai.floedb.floecat.gateway.iceberg.minimal.services.planning;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanBundle;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TablePlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.dto.TablePlanTasksResponseDto;
import ai.floedb.floecat.gateway.iceberg.minimal.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.EndQueryResponse;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TablePlanningServiceTest {
  private final GrpcWithHeaders grpc = Mockito.mock(GrpcWithHeaders.class);
  private final GrpcClients clients = Mockito.mock(GrpcClients.class);
  private final QueryServiceGrpc.QueryServiceBlockingStub query =
      Mockito.mock(QueryServiceGrpc.QueryServiceBlockingStub.class);
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema =
      Mockito.mock(QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub.class);
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan =
      Mockito.mock(QueryScanServiceGrpc.QueryScanServiceBlockingStub.class);
  private final MinimalGatewayConfig config = Mockito.mock(MinimalGatewayConfig.class);
  private TablePlanningService service;

  TablePlanningServiceTest() {
    when(grpc.raw()).thenReturn(clients);
    when(clients.query()).thenReturn(query);
    when(clients.querySchema()).thenReturn(querySchema);
    when(clients.queryScan()).thenReturn(queryScan);
    when(grpc.withHeaders(query)).thenReturn(query);
    when(grpc.withHeaders(querySchema)).thenReturn(querySchema);
    when(grpc.withHeaders(queryScan)).thenReturn(queryScan);
  }

  @BeforeEach
  void setUp() {
    when(config.planTaskTtl()).thenReturn(Duration.ofMinutes(5));
    when(config.planTaskFilesPerTask()).thenReturn(10);
    service = new TablePlanningService(grpc, new ObjectMapper(), new PlanTaskManager(config));
  }

  @Test
  void rejectsMixedSnapshotModes() {
    Response response =
        service.plan(
            table(),
            "ns",
            "orders",
            new PlanRequests.Plan(10L, 1L, 2L, null, null, null, null, null, null),
            List.of());

    assertEquals(400, response.getStatus());
    verify(query, never()).beginQuery(any());
  }

  @Test
  void returnsCompletedPlanAndCanFetchAndCancelIt() {
    when(query.beginQuery(any()))
        .thenReturn(
            BeginQueryResponse.newBuilder()
                .setQuery(QueryDescriptor.newBuilder().setQueryId("query-1"))
                .build());
    when(querySchema.describeInputs(any())).thenReturn(DescribeInputsResponse.newBuilder().build());
    when(queryScan.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(
                            ScanFile.newBuilder()
                                .setFilePath("s3://bucket/data.parquet")
                                .setFileFormat("PARQUET")
                                .setFileSizeInBytes(10L)
                                .setRecordCount(2L)
                                .setFileContent(ScanFileContent.SCAN_FILE_CONTENT_DATA)
                                .setPartitionSpecId(0)
                                .setPartitionDataJson("{\"partitionValues\":[\"2026-03-16\"]}")
                                .build()))
                .build());
    when(query.endQuery(any()))
        .thenReturn(EndQueryResponse.newBuilder().setQueryId("query-1").build());

    Response planned = service.plan(table(), "ns", "orders", PlanRequests.Plan.empty(), List.of());

    assertEquals(200, planned.getStatus());
    TablePlanResponseDto dto = (TablePlanResponseDto) planned.getEntity();
    assertEquals("completed", dto.status());
    assertEquals(1, dto.planTasks().size());
    assertEquals(1, dto.fileScanTasks().size());
    assertEquals("s3://bucket/data.parquet", dto.fileScanTasks().getFirst().dataFile().filePath());

    Response fetched = service.fetchPlan(dto.planId());
    assertEquals(200, fetched.getStatus());
    TablePlanResponseDto fetchedDto = (TablePlanResponseDto) fetched.getEntity();
    assertEquals("completed", fetchedDto.status());
    assertEquals(null, fetchedDto.planId());

    assertEquals(204, service.cancelPlan(dto.planId()).getStatus());
    Response cancelled = service.fetchPlan(dto.planId());
    assertEquals(200, cancelled.getStatus());
    TablePlanResponseDto cancelledDto = (TablePlanResponseDto) cancelled.getEntity();
    assertEquals("cancelled", cancelledDto.status());
  }

  @Test
  void consumesPlanTasksForCompletedPlan() {
    when(query.beginQuery(any()))
        .thenReturn(
            BeginQueryResponse.newBuilder()
                .setQuery(QueryDescriptor.newBuilder().setQueryId("query-1"))
                .build());
    when(querySchema.describeInputs(any())).thenReturn(DescribeInputsResponse.newBuilder().build());
    when(queryScan.fetchScanBundle(any()))
        .thenReturn(
            FetchScanBundleResponse.newBuilder()
                .setBundle(
                    ScanBundle.newBuilder()
                        .addDataFiles(
                            ScanFile.newBuilder()
                                .setFilePath("s3://bucket/data.parquet")
                                .setFileFormat("PARQUET")
                                .setFileSizeInBytes(10L)
                                .setRecordCount(2L)
                                .setFileContent(ScanFileContent.SCAN_FILE_CONTENT_DATA)
                                .setPartitionSpecId(0)
                                .setPartitionDataJson("{\"partitionValues\":[\"2026-03-16\"]}")
                                .build()))
                .build());
    when(query.endQuery(any()))
        .thenReturn(EndQueryResponse.newBuilder().setQueryId("query-1").build());

    Response planned = service.plan(table(), "ns", "orders", PlanRequests.Plan.empty(), List.of());
    TablePlanResponseDto dto = (TablePlanResponseDto) planned.getEntity();

    Response taskResponse = service.consumeTask("ns", "orders", dto.planTasks().getFirst());
    assertEquals(200, taskResponse.getStatus());
    TablePlanTasksResponseDto taskDto = (TablePlanTasksResponseDto) taskResponse.getEntity();
    assertEquals(1, taskDto.fileScanTasks().size());
  }

  private Table table() {
    return Table.newBuilder()
        .setResourceId(ResourceId.newBuilder().setId("tbl-1").setAccountId("acct"))
        .setCatalogId(ResourceId.newBuilder().setId("cat-1").setAccountId("acct"))
        .setDisplayName("orders")
        .build();
  }
}
