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

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.table.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitResponseService {
  private static final Logger LOG = Logger.getLogger(TableCommitResponseService.class);

  @Inject IcebergMetadataService icebergMetadataService;
  @Inject GrpcServiceFacade grpcClient;

  Response buildCommitResponse(
      TransactionCommitService.CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry) {
    // Hydration must use the same request-scoped support used by the commit path so metadata reads
    // cannot drift to a different catalog or table resolution context after the durable commit.
    TableGatewaySupport tableSupport = command.tableSupport();
    try {
      var tableId =
          tableSupport.resolveTableId(
              command.catalogName(), command.namespacePath(), command.table());
      ai.floedb.floecat.catalog.rpc.Table committedTable = tableSupport.getTable(tableId);
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
                      grpcClient,
                      committedTable.getResourceId(),
                      SnapshotLister.Mode.ALL,
                      metadata));
      CommitTableResponseDto response = responseForMetadata(resolved.metadataView());
      if (response.metadata() != null
          && response.metadataLocation() != null
          && !response.metadataLocation().isBlank()) {
        return Response.ok(response).build();
      }
      LOG.warnf(
          "Commit already applied for %s.%s but hydrated response metadata was incomplete; returning minimal success response",
          String.join(".", command.namespacePath()), command.table());
    } catch (RuntimeException e) {
      // The commit is already durable when this method is called. Failures here are post-commit
      // enrichment failures, so prefer a minimal success response over surfacing a misleading 5xx.
      LOG.warnf(
          e,
          "Commit already applied for %s.%s but response hydration failed; returning minimal success response",
          String.join(".", command.namespacePath()),
          command.table());
    }
    return Response.ok(minimalSuccessfulResponse(req, stagedEntry)).build();
  }

  private CommitTableResponseDto responseForMetadata(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }

  private CommitTableResponseDto minimalSuccessfulResponse(
      TableRequests.Commit req, StagedTableEntry stagedEntry) {
    String metadataLocation = fallbackMetadataLocation(req, stagedEntry);
    return new CommitTableResponseDto(metadataLocation, null);
  }

  private String fallbackMetadataLocation(TableRequests.Commit req, StagedTableEntry stagedEntry) {
    String requested = CommitUpdateInspector.inspect(req).requestedMetadataLocation();
    if (requested != null && !requested.isBlank()) {
      return requested;
    }
    if (stagedEntry != null && stagedEntry.request() != null) {
      return MetadataLocationUtil.metadataLocation(stagedEntry.request().properties());
    }
    return null;
  }
}
