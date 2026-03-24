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

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.asStringMap;
import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.SnapshotMetadataUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.storage.kv.Keys;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.jboss.logging.Logger;

@ApplicationScoped
public class SnapshotTxChangeBuilder {
  private static final Logger LOG = Logger.getLogger(SnapshotTxChangeBuilder.class);
  private static final String ICEBERG_METADATA_KEY = "iceberg";

  @Inject IcebergMetadataService icebergMetadataService;
  @Inject TransactionOutcomePolicy outcomePolicy;
  @Inject GrpcServiceFacade grpcClient;

  public record Result(
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges, Response error) {}

  public Result build(
      String accountId,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      ParsedCommit commit,
      boolean alreadyApplied) {
    if (alreadyApplied || tableId == null || commit.addedSnapshots().isEmpty()) {
      return new Result(List.of(), null);
    }
    String resolvedAccountId = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccountId == null || resolvedAccountId.isBlank()) {
      return new Result(
          List.of(),
          IcebergErrorResponses.failure(
              "missing account id for snapshot atomic changes",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE));
    }
    ResourceId scopedTableId =
        resolvedAccountId.equals(tableId.getAccountId())
            ? tableId
            : tableId.toBuilder().setAccountId(resolvedAccountId).build();
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    for (Map<String, Object> snapshotMap : commit.addedSnapshots()) {
      if (snapshotMap == null || snapshotMap.isEmpty()) {
        return new Result(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot"));
      }
      Long snapshotId = TableMappingUtil.asLong(snapshotMap.get("snapshot-id"));
      if (snapshotId == null || snapshotId < 0) {
        return new Result(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot-id"));
      }

      Snapshot snapshot;
      try {
        snapshot =
            fetchOrBuildSnapshotPayload(
                scopedTableId, table, committedMetadata, tableSupport, snapshotId, snapshotMap);
      } catch (StatusRuntimeException e) {
        return new Result(List.of(), outcomePolicy.mapPrepareFailure(e));
      } catch (RuntimeException e) {
        return new Result(
            List.of(),
            IcebergErrorResponses.failure(
                "failed to fetch or build snapshot payload",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE));
      }

      long upstreamCreatedMs =
          snapshot.hasUpstreamCreatedAt()
              ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
              : System.currentTimeMillis();
      String byIdKey =
          Keys.snapshotPointerById(
              resolvedAccountId, scopedTableId.getId(), snapshot.getSnapshotId());
      String byTimeKey =
          Keys.snapshotPointerByTime(
              resolvedAccountId,
              scopedTableId.getId(),
              snapshot.getSnapshotId(),
              upstreamCreatedMs);
      ByteString payload = ByteString.copyFrom(snapshot.toByteArray());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(byIdKey)
              .setPayload(payload)
              .build());
      out.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(byTimeKey)
              .setPayload(payload)
              .build());
    }
    return new Result(List.copyOf(out), null);
  }

  private Snapshot fetchOrBuildSnapshotPayload(
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Map<String, Object> snapshotMap) {
    try {
      var resp =
          grpcClient.getSnapshot(
              ai.floedb.floecat.catalog.rpc.GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                  .build());
      if (resp != null && resp.hasSnapshot()) {
        Snapshot existing = resp.getSnapshot();
        if (!existing.getFormatMetadataMap().containsKey(ICEBERG_METADATA_KEY)) {
          Map<String, String> existingSummary = SnapshotMetadataUtil.snapshotSummary(existing);
          String existingOperation = SnapshotMetadataUtil.snapshotOperation(existing);
          IcebergMetadata snapshotIcebergMetadata =
              buildSnapshotIcebergMetadata(
                  table,
                  committedMetadata,
                  tableSupport,
                  snapshotId,
                  existing.getSequenceNumber(),
                  existingOperation,
                  existingSummary);
          if (snapshotIcebergMetadata != null) {
            return existing.toBuilder()
                .putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString())
                .build();
          }
        }
        return existing;
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    Snapshot.Builder builder = Snapshot.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    Long upstreamCreated = TableMappingUtil.asLong(snapshotMap.get("timestamp-ms"));
    if (upstreamCreated != null && upstreamCreated > 0) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreated));
    }
    Long parentId = TableMappingUtil.asLong(snapshotMap.get("parent-snapshot-id"));
    if (parentId != null) {
      builder.setParentSnapshotId(parentId);
    }
    Long sequenceNumber = TableMappingUtil.asLong(snapshotMap.get("sequence-number"));
    if (sequenceNumber != null && sequenceNumber > 0) {
      builder.setSequenceNumber(sequenceNumber);
    }
    String manifestList = TableMappingUtil.asString(snapshotMap.get("manifest-list"));
    if (manifestList != null && !manifestList.isBlank()) {
      builder.addManifestList(manifestList);
    }
    Integer schemaId = TableMappingUtil.asInteger(snapshotMap.get("schema-id"));
    if (schemaId != null && schemaId >= 0) {
      builder.setSchemaId(schemaId);
    }
    String schemaJson = TableMappingUtil.asString(snapshotMap.get("schema-json"));
    if (schemaJson != null && !schemaJson.isBlank()) {
      builder.setSchemaJson(schemaJson);
    }
    Map<String, String> summary = asStringMap(snapshotMap.get("summary"));
    String operation = TableMappingUtil.asString(snapshotMap.get("operation"));
    if (operation != null && !operation.isBlank() && !summary.containsKey("operation")) {
      summary = new LinkedHashMap<>(summary);
      summary.put("operation", operation);
    }
    String resolvedOperation =
        operation != null && !operation.isBlank() ? operation : summary.get("operation");
    IcebergMetadata snapshotIcebergMetadata =
        buildSnapshotIcebergMetadata(
            table,
            committedMetadata,
            tableSupport,
            snapshotId,
            sequenceNumber,
            resolvedOperation,
            summary);
    if (snapshotIcebergMetadata != null) {
      builder.putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString());
    }
    if (!builder.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()));
    }
    return builder.build();
  }

  private IcebergMetadata buildSnapshotIcebergMetadata(
      ai.floedb.floecat.catalog.rpc.Table table,
      TableMetadata committedMetadata,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Long sequenceNumber,
      String operation,
      Map<String, String> summary) {
    if (committedMetadata == null) {
      LOG.debugf(
          "Skipping snapshot format metadata synthesis without canonical TableMetadata tableId=%s snapshotId=%d",
          table == null || !table.hasResourceId() ? "<missing>" : table.getResourceId().getId(),
          snapshotId);
      return null;
    }
    IcebergMetadata base =
        icebergMetadataService.toIcebergMetadata(
            committedMetadata, committedMetadata.metadataFileLocation());
    if (base == null) {
      LOG.debugf(
          "Skipping snapshot format metadata synthesis without readable Iceberg metadata tableId=%s snapshotId=%d",
          table == null || !table.hasResourceId() ? "<missing>" : table.getResourceId().getId(),
          snapshotId);
      return null;
    }
    IcebergMetadata.Builder builder = base.toBuilder().setCurrentSnapshotId(snapshotId);
    if (operation != null && !operation.isBlank()) {
      builder.setOperation(operation);
    }
    if (summary != null && !summary.isEmpty()) {
      builder.putAllSummary(summary);
    }
    return builder.build();
  }
}
