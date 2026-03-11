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
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitResponseBuilder {
  private static final Logger LOG = Logger.getLogger(CommitResponseBuilder.class);

  @Inject SnapshotClient snapshotClient;
  @Inject TableCommitMetadataMutator metadataMutator;

  void setSnapshotClient(SnapshotClient snapshotClient) {
    this.snapshotClient = snapshotClient;
  }

  public String resolveRequestedMetadataLocation(TableRequests.Commit req) {
    return CommitUpdateInspector.inspect(req).requestedMetadataLocation();
  }

  public Set<Long> removedSnapshotIds(TableRequests.Commit req) {
    return CommitUpdateInspector.inspect(req).removedSnapshotIdsSet();
  }

  public boolean containsSnapshotUpdates(TableRequests.Commit req) {
    return CommitUpdateInspector.inspect(req).containsSnapshotUpdates();
  }

  public CommitTableResponseDto buildInitialResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      IcebergMetadata metadata) {
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(snapshotClient, tableId, SnapshotLister.Mode.ALL, metadata);
    Set<Long> removedSnapshotIds = parsed.removedSnapshotIdsSet();
    if (!removedSnapshotIds.isEmpty() && snapshotList != null && !snapshotList.isEmpty()) {
      snapshotList =
          snapshotList.stream()
              .filter(s -> !removedSnapshotIds.contains(s.getSnapshotId()))
              .toList();
    }
    CommitTableResponseDto initialResponse =
        TableResponseMapper.toCommitResponse(tableName, committedTable, metadata, snapshotList);
    CommitTableResponseDto stageAwareResponse =
        parsed.containsSnapshotUpdates()
            ? initialResponse
            : preferStageMetadata(initialResponse, stageMaterialization);
    CommitTableResponseDto mutatedResponse = applyRequestMetadataMutations(stageAwareResponse, req);
    mutatedResponse = normalizeMetadataLocation(mutatedResponse);
    return preferRequestedMetadata(mutatedResponse, parsed.requestedMetadataLocation());
  }

  public CommitTableResponseDto buildFinalResponse(
      String tableName,
      Table committedTable,
      ResourceId tableId,
      StageCommitResult stageMaterialization,
      TableRequests.Commit req,
      TableGatewaySupport tableSupport,
      Set<Long> removedSnapshotIds) {
    CommitUpdateInspector.Parsed parsed = CommitUpdateInspector.inspect(req);
    IcebergMetadata refreshedMetadata = tableSupport.loadCurrentMetadata(committedTable);
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
            tableName, committedTable, refreshedMetadata, refreshedSnapshots);
    if (!parsed.containsSnapshotUpdates()) {
      finalResponse = preferStageMetadata(finalResponse, stageMaterialization);
    }
    finalResponse = applyRequestMetadataMutations(finalResponse, req);
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

  private CommitTableResponseDto applyRequestMetadataMutations(
      CommitTableResponseDto response, TableRequests.Commit req) {
    if (response == null || response.metadata() == null || req == null || req.updates() == null) {
      return response;
    }
    return commitResponse(metadataMutator.apply(response.metadata(), req));
  }
}
