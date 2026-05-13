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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asStringMap;

import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
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
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitSnapshotSupport {
  private static final Logger LOG = Logger.getLogger(TransactionCommitSnapshotSupport.class);

  @Inject TransactionCommitExecutionSupport transactionCommitExecutionSupport;
  @Inject GrpcServiceFacade grpcClient;

  record SnapshotChangePlan(
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges, Response error) {}

  SnapshotChangePlan planAtomicSnapshotChanges(
      String accountId,
      String txId,
      ResourceId tableId,
      Table table,
      TableGatewaySupport tableSupport,
      String metadataLocation,
      List<Map<String, Object>> updates,
      List<Long> removedSnapshotIds) {
    if (tableId == null) {
      return new SnapshotChangePlan(List.of(), null);
    }
    String resolvedAccountId = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccountId == null || resolvedAccountId.isBlank()) {
      return new SnapshotChangePlan(
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
    String requestedMetadataLocation =
        CommitUpdateInspector.inspectUpdates(updates).requestedMetadataLocation();
    String resolvedMetadataLocation =
        firstNonBlank(requestedMetadataLocation, trimToNull(metadataLocation));
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    boolean writesNewSnapshot = false;
    if (updates != null && !updates.isEmpty()) {
      for (Map<String, Object> update : updates) {
        String action = CommitUpdateInspector.actionOf(update);
        if (!CommitUpdateInspector.ACTION_ADD_SNAPSHOT.equals(action)) {
          continue;
        }
        writesNewSnapshot = true;
        Map<String, Object> snapshotMap = TableMappingUtil.asObjectMap(update.get("snapshot"));
        if (snapshotMap == null || snapshotMap.isEmpty()) {
          return new SnapshotChangePlan(
              List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot"));
        }
        Long snapshotId = TableMappingUtil.asLong(snapshotMap.get("snapshot-id"));
        if (snapshotId == null || snapshotId < 0) {
          return new SnapshotChangePlan(
              List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot-id"));
        }

        Snapshot snapshot;
        try {
          snapshot =
              buildSnapshotPayload(
                  scopedTableId,
                  table,
                  tableSupport,
                  snapshotId,
                  snapshotMap,
                  resolvedMetadataLocation);
        } catch (RuntimeException e) {
          return new SnapshotChangePlan(
              List.of(),
              IcebergErrorResponses.failure(
                  "failed to build snapshot payload",
                  "CommitStateUnknownException",
                  Response.Status.SERVICE_UNAVAILABLE));
        }

        long upstreamCreatedMs =
            snapshot.hasUpstreamCreatedAt()
                ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
                : clockMillis();
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
    }
    if (!writesNewSnapshot
        && resolvedMetadataLocation != null
        && !resolvedMetadataLocation.isBlank()) {
      SnapshotChangePlan metadataOnlyPlan =
          planCurrentSnapshotMetadataLocationUpdate(
              resolvedAccountId,
              txId,
              scopedTableId,
              table,
              tableSupport,
              resolvedMetadataLocation);
      if (metadataOnlyPlan.error() != null) {
        return metadataOnlyPlan;
      }
      out.addAll(metadataOnlyPlan.txChanges());
    }
    if (removedSnapshotIds != null && !removedSnapshotIds.isEmpty()) {
      for (Long snapshotId : removedSnapshotIds) {
        if (snapshotId == null || snapshotId < 0) {
          continue;
        }
        Snapshot snapshot;
        try {
          var resp =
              grpcClient.getSnapshot(
                  GetSnapshotRequest.newBuilder()
                      .setTableId(scopedTableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                      .build());
          if (resp == null || !resp.hasSnapshot()) {
            continue;
          }
          snapshot = resp.getSnapshot();
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            continue;
          }
          return new SnapshotChangePlan(
              List.of(), transactionCommitExecutionSupport.mapPrepareFailure(e));
        } catch (RuntimeException e) {
          return new SnapshotChangePlan(
              List.of(),
              IcebergErrorResponses.failure(
                  "failed to load snapshot payload for transactional delete",
                  "CommitStateUnknownException",
                  Response.Status.SERVICE_UNAVAILABLE));
        }
        long upstreamCreatedMs =
            snapshot.hasUpstreamCreatedAt()
                ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
                : clockMillis();
        out.add(
            snapshotDeleteChange(
                resolvedAccountId,
                txId,
                scopedTableId.getId(),
                snapshot.getSnapshotId(),
                upstreamCreatedMs,
                true));
        out.add(
            snapshotDeleteChange(
                resolvedAccountId,
                txId,
                scopedTableId.getId(),
                snapshot.getSnapshotId(),
                upstreamCreatedMs,
                false));
      }
    }
    return new SnapshotChangePlan(out, null);
  }

  private SnapshotChangePlan planCurrentSnapshotMetadataLocationUpdate(
      String accountId,
      String txId,
      ResourceId tableId,
      Table table,
      TableGatewaySupport tableSupport,
      String metadataLocation) {
    Snapshot currentSnapshot;
    try {
      var response =
          grpcClient.getSnapshot(
              GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(
                      SnapshotRef.newBuilder()
                          .setSpecial(ai.floedb.floecat.common.rpc.SpecialSnapshot.SS_CURRENT)
                          .build())
                  .build());
      if (response == null || !response.hasSnapshot()) {
        return new SnapshotChangePlan(List.of(), null);
      }
      currentSnapshot = response.getSnapshot();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return new SnapshotChangePlan(List.of(), null);
      }
      return new SnapshotChangePlan(
          List.of(), transactionCommitExecutionSupport.mapPrepareFailure(e));
    } catch (RuntimeException e) {
      return new SnapshotChangePlan(
          List.of(),
          IcebergErrorResponses.failure(
              "failed to load current snapshot payload for metadata-only commit",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE));
    }

    Snapshot.Builder updatedSnapshot = currentSnapshot.toBuilder();
    updatedSnapshot.setMetadataLocation(metadataLocation);
    long upstreamCreatedMs =
        updatedSnapshot.hasUpstreamCreatedAt()
            ? Timestamps.toMillis(updatedSnapshot.getUpstreamCreatedAt())
            : clockMillis();
    String byIdKey =
        Keys.snapshotPointerById(accountId, tableId.getId(), updatedSnapshot.getSnapshotId());
    String byTimeKey =
        Keys.snapshotPointerByTime(
            accountId, tableId.getId(), updatedSnapshot.getSnapshotId(), upstreamCreatedMs);
    ByteString payload = ByteString.copyFrom(updatedSnapshot.build().toByteArray());
    return new SnapshotChangePlan(
        List.of(
            ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
                .setTargetPointerKey(byIdKey)
                .setPayload(payload)
                .build(),
            ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
                .setTargetPointerKey(byTimeKey)
                .setPayload(payload)
                .build()),
        null);
  }

  private ai.floedb.floecat.transaction.rpc.TxChange snapshotDeleteChange(
      String accountId,
      String txId,
      String tableId,
      long snapshotId,
      long upstreamCreatedMs,
      boolean byId) {
    String targetPointerKey =
        byId
            ? Keys.snapshotPointerById(accountId, tableId, snapshotId)
            : Keys.snapshotPointerByTime(accountId, tableId, snapshotId, upstreamCreatedMs);
    return ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
        .setTargetPointerKey(targetPointerKey)
        .setIntendedBlobUri(Keys.transactionDeleteSentinelUri(accountId, txId, targetPointerKey))
        .build();
  }

  private Snapshot buildSnapshotPayload(
      ResourceId tableId,
      Table table,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Map<String, Object> snapshotMap,
      String requestedMetadataLocation) {
    String explicitMetadataLocation =
        trimToNull(TableMappingUtil.asString(snapshotMap.get("metadata-location")));

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
      builder.setManifestList(manifestList);
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
    if (!summary.isEmpty()) {
      builder.putAllSummary(summary);
    }
    String metadataLocation =
        firstNonBlank(explicitMetadataLocation, trimToNull(requestedMetadataLocation));
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    if (!builder.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(clockMillis()));
    }
    return builder.build();
  }

  private String trimToNull(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static String firstNonBlank(String... values) {
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

  private long clockMillis() {
    return System.currentTimeMillis();
  }
}
