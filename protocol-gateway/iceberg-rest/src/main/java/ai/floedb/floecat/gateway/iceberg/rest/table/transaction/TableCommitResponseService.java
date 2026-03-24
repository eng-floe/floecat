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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TableCommitResponseService {
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject GrpcServiceFacade grpcClient;

  Response buildCommitResponse(
      TransactionCommitService.CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry) {
    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            command.catalogName(), command.namespacePath(), command.table());
    ai.floedb.floecat.catalog.rpc.Table committedTable = tableGatewaySupport.getTable(tableId);
    TableGatewaySupport tableSupport = command.tableSupport();
    IcebergMetadata metadata =
        icebergMetadataService.resolveCurrentIcebergMetadata(committedTable, tableSupport);
    var resolved =
        icebergMetadataService.resolveMetadata(
            committedTable == null ? null : committedTable.getDisplayName(),
            committedTable,
            metadata,
            tableSupport.defaultFileIoProperties(),
            () ->
                SnapshotLister.fetchSnapshots(
                    grpcClient, committedTable.getResourceId(), SnapshotLister.Mode.ALL, metadata));
    CommitTableResponseDto response = responseForMetadata(resolved.metadataView());
    if (response.metadata() == null
        || response.metadataLocation() == null
        || response.metadataLocation().isBlank()) {
      return IcebergErrorResponses.failure(
          "Commit response missing metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(response).build();
  }

  private CommitTableResponseDto responseForMetadata(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }
}
