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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableCommitMetadataMutator;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Set;

@ApplicationScoped
public class CommitResponseBuilder {
  @Inject GrpcServiceFacade snapshotClient;
  @Inject TableCommitMetadataMutator metadataMutator;

  public Set<Long> removedSnapshotIds(TableRequests.Commit req) {
    return CommitUpdateInspector.inspect(req).removedSnapshotIdsSet();
  }

  public CommitTableResponseDto buildInitialResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      IcebergMetadata metadata) {
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(snapshotClient, tableId, SnapshotLister.Mode.ALL, metadata);
    String metadataLocation =
        tableSupport == null ? null : tableSupport.loadCurrentMetadataLocation(committedTable);
    Set<Long> removedSnapshotIds = parsed.removedSnapshotIdsSet();
    if (!removedSnapshotIds.isEmpty() && snapshotList != null && !snapshotList.isEmpty()) {
      snapshotList =
          snapshotList.stream()
              .filter(s -> !removedSnapshotIds.contains(s.getSnapshotId()))
              .toList();
    }
    CommitTableResponseDto initialResponse =
        TableResponseMapper.toCommitResponse(
            tableName, committedTable, metadata, snapshotList, metadataLocation);
    return applyRequestMetadataMutations(initialResponse, req);
  }

  public CommitTableResponseDto buildFinalResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      Set<Long> removedSnapshotIds) {
    IcebergMetadata refreshedMetadata = tableSupport.loadCurrentMetadata(committedTable);
    String refreshedMetadataLocation = tableSupport.loadCurrentMetadataLocation(committedTable);
    List<Snapshot> refreshedSnapshots =
        SnapshotLister.fetchSnapshots(
            snapshotClient, tableId, SnapshotLister.Mode.ALL, refreshedMetadata);
    if (!removedSnapshotIds.isEmpty()
        && refreshedSnapshots != null
        && !refreshedSnapshots.isEmpty()) {
      refreshedSnapshots =
          refreshedSnapshots.stream()
              .filter(s -> !removedSnapshotIds.contains(s.getSnapshotId()))
              .toList();
    }
    CommitTableResponseDto finalResponse =
        TableResponseMapper.toCommitResponse(
            tableName,
            committedTable,
            refreshedMetadata,
            refreshedSnapshots,
            refreshedMetadataLocation);
    return applyRequestMetadataMutations(finalResponse, req);
  }

  private CommitTableResponseDto applyRequestMetadataMutations(
      CommitTableResponseDto response, TableRequests.Commit req) {
    if (response == null || response.metadata() == null || req == null || req.updates() == null) {
      return response;
    }
    var metadata = metadataMutator.apply(response.metadata(), req);
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }
}
