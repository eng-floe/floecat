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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitSideEffectService {
  private static final Logger LOG = Logger.getLogger(TableCommitSideEffectService.class);

  private final GrpcWithHeaders grpc;

  @Inject
  public TableCommitSideEffectService(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public void schedulePostCommitStatsSync(
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      LOG.infof(
          "Post-commit stats sync skipped table=%s namespace=%s reason=no_connector",
          tableName, namespacePath == null ? "<null>" : String.join(".", namespacePath));
      return;
    }
    if (snapshotIds == null || snapshotIds.isEmpty()) {
      LOG.infof(
          "Post-commit stats sync skipped table=%s connectorId=%s namespace=%s reason=no_snapshots",
          tableName,
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath));
      return;
    }
    try {
      var reconcile = grpc.withHeaders(grpc.raw().reconcile());
      StartCaptureResponse response =
          reconcile.startCapture(
              StartCaptureRequest.newBuilder()
                  .setScope(
                      CaptureScope.newBuilder()
                          .setConnectorId(connectorId)
                          .addDestinationNamespacePaths(
                              NamespacePath.newBuilder().addAllSegments(namespacePath).build())
                          .setDestinationTableDisplayName(tableName)
                          .addAllDestinationSnapshotIds(snapshotIds)
                          .build())
                  .setMode(CaptureMode.CM_STATS_ONLY)
                  .setFullRescan(true)
                  .build());
      LOG.infof(
          "Scheduled post-commit stats job table=%s connectorId=%s namespace=%s snapshots=%s jobId=%s",
          tableName,
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          snapshotIds,
          response.getJobId());
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Connector stats-only sync failed connectorId=%s namespace=%s table=%s jobId=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName,
          "");
    }
  }

  public boolean pruneRemovedSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    if (tableId == null || snapshotIds == null || snapshotIds.isEmpty()) {
      return true;
    }
    try {
      var snapshotStub = grpc.withHeaders(grpc.raw().snapshot());
      for (Long snapshotId : snapshotIds) {
        if (snapshotId == null) {
          continue;
        }
        try {
          snapshotStub.deleteSnapshot(
              DeleteSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshotId(snapshotId)
                  .build());
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            continue;
          }
          throw e;
        }
      }
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Snapshot prune failed tableId=%s snapshotIds=%s", tableId.getId(), snapshotIds);
      return false;
    }
  }
}
