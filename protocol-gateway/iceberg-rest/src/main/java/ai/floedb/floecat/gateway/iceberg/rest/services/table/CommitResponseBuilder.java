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
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMetadataBuilder;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitResponseBuilder {
  private static final Logger LOG = Logger.getLogger(CommitResponseBuilder.class);

  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject GrpcServiceFacade snapshotClient;

  public String resolveRequestedMetadataLocation(TableRequests.Commit req) {
    return CommitUpdateInspector.inspect(req).requestedMetadataLocation();
  }

  public CommitTableResponseDto buildFinalResponse(
      Table committedTable,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport) {
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    IcebergMetadata refreshedMetadata = tableSupport.loadCurrentMetadata(committedTable);
    CommitTableResponseDto finalResponse =
        buildCommitResponse(committedTable, refreshedMetadata, tableSupport);
    if (!parsed.containsSnapshotUpdates()) {
      finalResponse = preferStageMetadata(finalResponse, stageMaterialization);
    }
    finalResponse = normalizeMetadataLocation(finalResponse);
    return preferRequestedMetadata(finalResponse, parsed.requestedMetadataLocation());
  }

  public CommitTableResponseDto preferStageMetadata(
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
      return commitResponse(stagedMetadata);
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
    return commitResponse(stagedMetadata);
  }

  public CommitTableResponseDto preferRequestedMetadata(
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
    return commitResponse(metadata);
  }

  public CommitTableResponseDto normalizeMetadataLocation(CommitTableResponseDto response) {
    return response;
  }

  public CommitTableResponseDto commitResponse(TableMetadataView metadata) {
    if (metadata == null) {
      return new CommitTableResponseDto(null, null);
    }
    return new CommitTableResponseDto(metadata.metadataLocation(), metadata);
  }

  private CommitTableResponseDto buildCommitResponse(
      Table committedTable, IcebergMetadata metadata, TableGatewaySupport tableSupport) {
    TableMetadataView canonicalView =
        tableMetadataImportService.importMetadataView(
            committedTable, metadata, tableSupport.defaultFileIoProperties());
    if (canonicalView != null) {
      return ensureMetadataLocation(
          TableResponseMapper.toCommitResponse(canonicalView), committedTable, metadata);
    }
    List<Snapshot> snapshots =
        SnapshotLister.fetchSnapshots(
            snapshotClient, committedTable.getResourceId(), SnapshotLister.Mode.ALL, metadata);
    TableMetadataView fallbackView =
        TableMetadataBuilder.fromCatalog(
            committedTable.getDisplayName(),
            committedTable,
            new LinkedHashMap<>(committedTable.getPropertiesMap()),
            metadata,
            snapshots);
    return fallbackView == null
        ? null
        : ensureMetadataLocation(
            TableResponseMapper.toCommitResponse(fallbackView), committedTable, metadata);
  }

  private CommitTableResponseDto ensureMetadataLocation(
      CommitTableResponseDto response, Table committedTable, IcebergMetadata metadata) {
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

  private String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }
}
