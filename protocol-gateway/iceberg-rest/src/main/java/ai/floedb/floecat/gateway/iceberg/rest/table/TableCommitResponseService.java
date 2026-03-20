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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitResponseService {
  private static final Logger LOG = Logger.getLogger(TableCommitResponseService.class);

  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject GrpcServiceFacade grpcClient;

  public record StageCommitResult(
      ai.floedb.floecat.catalog.rpc.Table table,
      LoadTableResultDto loadResult,
      boolean tableCreated) {}

  Response buildCommitResponse(
      TransactionCommitService.CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry) {
    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            command.catalogName(), command.namespacePath(), command.table());
    ai.floedb.floecat.catalog.rpc.Table committedTable = tableGatewaySupport.getTable(tableId);
    TableGatewaySupport tableSupport = command.tableSupport();
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    StageCommitResult stagedResponse =
        buildStagedCommitResult(command, parsed, stagedEntry, committedTable);

    CommitTableResponseDto finalResponse;
    if (!parsed.containsSnapshotUpdates() && stagedResponse != null) {
      finalResponse =
          preferRequestedMetadata(
              responseForStage(null, stagedResponse), parsed.requestedMetadataLocation());
    } else {
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
      finalResponse =
          resolved.metadataView() == null
              ? null
              : ensureMetadataLocation(
                  responseForMetadata(resolved.metadataView()),
                  committedTable,
                  resolved.icebergMetadata());
      if (!parsed.containsSnapshotUpdates()) {
        finalResponse = responseForStage(finalResponse, stagedResponse);
      }
      finalResponse = preferRequestedMetadata(finalResponse, parsed.requestedMetadataLocation());
    }
    if (finalResponse == null
        || finalResponse.metadata() == null
        || finalResponse.metadataLocation() == null
        || finalResponse.metadataLocation().isBlank()) {
      return IcebergErrorResponses.failure(
          "Commit response missing metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return Response.ok(finalResponse).build();
  }

  private StageCommitResult buildStagedCommitResult(
      TransactionCommitService.CommitCommand command,
      CommitUpdateInspector.Parsed parsed,
      StagedTableEntry stagedEntry,
      ai.floedb.floecat.catalog.rpc.Table table) {
    if (stagedEntry == null || stagedEntry.request() == null || table == null) {
      return null;
    }
    if (parsed.containsSnapshotUpdates()) {
      return null;
    }
    TableGatewaySupport tableSupport = command.tableSupport();
    List<StorageCredentialDto> credentials =
        tableSupport == null ? List.of() : tableSupport.defaultCredentials();
    LoadTableResultDto loadResult =
        TableResponseMapper.toLoadResult(
            TableMetadataBuilder.fromCreateRequest(command.table(), table, stagedEntry.request()),
            tableSupport == null ? Map.of() : tableSupport.defaultTableConfig(),
            credentials);
    return new StageCommitResult(table, loadResult, false);
  }

  private CommitTableResponseDto responseForStage(
      CommitTableResponseDto response, StageCommitResult stageMaterialization) {
    if (stageMaterialization == null || stageMaterialization.loadResult() == null) {
      return response;
    }
    LoadTableResultDto staged = stageMaterialization.loadResult();
    TableMetadataView stagedMetadata = staged.metadata();
    String stagedLocation = staged.metadataLocation();
    if ((stagedLocation == null || stagedLocation.isBlank())
        && stagedMetadata != null
        && stagedMetadata.metadataLocation() != null
        && !stagedMetadata.metadataLocation().isBlank()) {
      stagedLocation = stagedMetadata.metadataLocation();
    }
    if (stagedLocation == null || stagedLocation.isBlank()) {
      if (stagedMetadata == null) {
        return response;
      }
      boolean responseIncomplete =
          response == null
              || response.metadata() == null
              || response.metadata().formatVersion() == null
              || response.metadata().schemas() == null
              || response.metadata().schemas().isEmpty();
      if (!responseIncomplete) {
        return response;
      }
      return responseForMetadata(stagedMetadata);
    }
    String originalLocation = response == null ? "<null>" : response.metadataLocation();
    LOG.infof(
        "Stage metadata evaluation stagedLocation=%s originalLocation=%s",
        stagedLocation, originalLocation);
    if (stagedMetadata != null) {
      stagedMetadata = stagedMetadata.withMetadataLocation(stagedLocation);
    }
    if (response != null
        && stagedLocation.equals(response.metadataLocation())
        && Objects.equals(stagedMetadata, response.metadata())) {
      return response;
    }
    LOG.infof("Preferring staged metadata location %s over %s", stagedLocation, originalLocation);
    return responseForMetadata(stagedMetadata);
  }

  private CommitTableResponseDto preferRequestedMetadata(
      CommitTableResponseDto response, String requestedMetadataLocation) {
    if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
      return response;
    }
    TableMetadataView metadata = response == null ? null : response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(requestedMetadataLocation);
    }
    if (response != null && requestedMetadataLocation.equals(response.metadataLocation())) {
      if (Objects.equals(metadata, response.metadata())) {
        return response;
      }
    }
    return responseForMetadata(metadata);
  }

  private CommitTableResponseDto responseForMetadata(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }

  private CommitTableResponseDto ensureMetadataLocation(
      CommitTableResponseDto response,
      ai.floedb.floecat.catalog.rpc.Table committedTable,
      IcebergMetadata metadata) {
    if (response == null) {
      return null;
    }
    String resolved =
        firstNonBlank(
            response.metadataLocation(),
            response.metadata() == null ? null : response.metadata().metadataLocation(),
            committedTable == null
                ? null
                : MetadataLocationUtil.metadataLocation(committedTable.getPropertiesMap()),
            metadata == null ? null : metadata.getMetadataLocation());
    if (resolved == null || resolved.isBlank()) {
      return response;
    }
    TableMetadataView metadataView = response.metadata();
    if (metadataView != null && !resolved.equals(metadataView.metadataLocation())) {
      metadataView = metadataView.withMetadataLocation(resolved);
    }
    if (resolved.equals(response.metadataLocation())
        && Objects.equals(metadataView, response.metadata())) {
      return response;
    }
    return new CommitTableResponseDto(resolved, metadataView);
  }
}
