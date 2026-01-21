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

package ai.floedb.floecat.service.execution.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ScanBundleServiceTest {

  @Test
  void streamPagesThroughAllFileStats() {
    ScanBundleService service = new ScanBundleService();
    service.tables = mock(TableServiceGrpc.TableServiceBlockingStub.class);
    TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats =
        mock(TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub.class);

    ResourceId tableId = ResourceId.newBuilder().setId("tbl").build();
    Table table = Table.newBuilder().setResourceId(tableId).build();
    when(service.tables.getTable(any()))
        .thenReturn(GetTableResponse.newBuilder().setTable(table).build());

    FileColumnStats first =
        FileColumnStats.newBuilder().setFilePath("s3://bucket/file-1.parquet").build();
    FileColumnStats second =
        FileColumnStats.newBuilder().setFilePath("s3://bucket/file-2.parquet").build();
    FileColumnStats third =
        FileColumnStats.newBuilder().setFilePath("s3://bucket/file-3.parquet").build();

    var page1 =
        ListFileColumnStatsResponse.newBuilder()
            .addFileColumns(first)
            .addFileColumns(second)
            .setPage(PageResponse.newBuilder().setNextPageToken("next").build())
            .build();
    var page2 =
        ListFileColumnStatsResponse.newBuilder()
            .addFileColumns(third)
            .setPage(PageResponse.newBuilder().setNextPageToken("").build())
            .build();

    when(stats.listFileColumnStats(any())).thenReturn(page1, page2);

    List<ScanFile> files = new ArrayList<>();
    service.stream("corr", tableId, SnapshotPin.newBuilder().setSnapshotId(99L).build(), stats)
        .forEach(files::add);

    assertThat(files).hasSize(3);
    assertThat(files.get(0).getFilePath()).isEqualTo("s3://bucket/file-1.parquet");
    assertThat(files.get(1).getFilePath()).isEqualTo("s3://bucket/file-2.parquet");
    assertThat(files.get(2).getFilePath()).isEqualTo("s3://bucket/file-3.parquet");

    ArgumentCaptor<ListFileColumnStatsRequest> captor =
        ArgumentCaptor.forClass(ListFileColumnStatsRequest.class);
    org.mockito.Mockito.verify(stats, org.mockito.Mockito.times(2))
        .listFileColumnStats(captor.capture());
    List<ListFileColumnStatsRequest> calls = captor.getAllValues();
    assertThat(calls.get(0).getPage().getPageToken()).isEmpty();
    assertThat(calls.get(1).getPage().getPageToken()).isEqualTo("next");
  }
}
