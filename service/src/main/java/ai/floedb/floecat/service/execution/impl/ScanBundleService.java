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

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.execution.rpc.ScanFileContent;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@ApplicationScoped
public class ScanBundleService {
  private static final int PAGE_SIZE = 1000;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  public Iterable<ScanFile> stream(
      String correlationId,
      ResourceId tableId,
      SnapshotPin snapshotPin,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats) {

    // Load table metadata
    Table table =
        tables.getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();

    // SnapshotPin: snapshot_id is a proto3 scalar, no hasSnapshotId()
    long snapshotId = snapshotPin.getSnapshotId();
    if (snapshotId == 0L) {
      throw GrpcErrors.invalidArgument(
          correlationId, "query.snapshot.required", Map.of("table_id", tableId.getId()));
    }

    return () -> new FileColumnStatsIterator(stats, table.getResourceId(), snapshotId);
  }

  private static ScanFile toScanFile(FileColumnStats fcs) {
    ScanFile.Builder builder =
        ScanFile.newBuilder()
            .setFilePath(fcs.getFilePath())
            .setFileFormat(fcs.getFileFormat())
            .setFileSizeInBytes(fcs.getSizeBytes())
            .setRecordCount(fcs.getRowCount())
            .setPartitionDataJson(fcs.getPartitionDataJson())
            .setPartitionSpecId(fcs.getPartitionSpecId())
            .addAllEqualityFieldIds(fcs.getEqualityFieldIdsList())
            .setFileContent(mapContent(fcs.getFileContent()))
            .addAllColumns(fcs.getColumnsList());
    if (fcs.hasSequenceNumber()) {
      builder.setSequenceNumber(fcs.getSequenceNumber());
    }
    return builder.build();
  }

  //  Direct mapping from catalog FileContent -> execution ScanFileContent
  private static ScanFileContent mapContent(FileContent fc) {
    return switch (fc) {
      case FC_EQUALITY_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES;
      case FC_POSITION_DELETES -> ScanFileContent.SCAN_FILE_CONTENT_POSITION_DELETES;
      default -> ScanFileContent.SCAN_FILE_CONTENT_DATA;
    };
  }

  private static final class FileColumnStatsIterator implements Iterator<ScanFile> {
    private final TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;
    private final ResourceId tableId;
    private final long snapshotId;
    private List<FileColumnStats> page = List.of();
    private int index = 0;
    private String pageToken = "";
    private boolean finished = false;
    private boolean finalPage = false;

    private FileColumnStatsIterator(
        TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats,
        ResourceId tableId,
        long snapshotId) {
      this.stats = stats;
      this.tableId = tableId;
      this.snapshotId = snapshotId;
    }

    @Override
    public boolean hasNext() {
      if (index < page.size()) {
        return true;
      }
      if (finished || finalPage) {
        return false;
      }
      loadNextPage();
      return index < page.size();
    }

    @Override
    public ScanFile next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more scan files");
      }
      return toScanFile(page.get(index++));
    }

    private void loadNextPage() {
      while (true) {
        var req =
            ListFileColumnStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                .setPage(PageRequest.newBuilder().setPageSize(PAGE_SIZE).setPageToken(pageToken))
                .build();

        var resp = stats.listFileColumnStats(req);
        page = resp.getFileColumnsList();
        index = 0;
        pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
        finalPage = pageToken.isBlank();

        if (!page.isEmpty()) {
          return;
        }
        if (pageToken.isBlank()) {
          finished = true;
          return;
        }
      }
    }
  }
}
