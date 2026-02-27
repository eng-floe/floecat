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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitSideEffectService {
  private static final Logger LOG = Logger.getLogger(TableCommitSideEffectService.class);
  @Inject SnapshotMetadataService snapshotMetadataService;

  public void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    runConnectorSyncAttempt(tableSupport, connectorId, namespacePath, tableName);
  }

  public ConnectorSyncResult runConnectorSyncAttempt(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (!tableSupport.connectorIntegrationEnabled()) {
      return ConnectorSyncResult.skippedResult();
    }
    if (connectorId == null || connectorId.getId().isBlank()) {
      return ConnectorSyncResult.skippedResult();
    }
    boolean captureOk = true;
    boolean reconcileOk = true;
    try {
      tableSupport.runSyncMetadataCapture(connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      captureOk = false;
      LOG.warnf(
          e,
          "Connector sync metadata capture failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
    }
    try {
      tableSupport.triggerScopedReconcile(connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      reconcileOk = false;
      LOG.warnf(
          e,
          "Connector reconcile trigger failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
    }
    return new ConnectorSyncResult(captureOk, reconcileOk, false);
  }

  public ConnectorSyncResult runConnectorStatsSyncAttempt(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (!tableSupport.connectorIntegrationEnabled()) {
      return ConnectorSyncResult.skippedResult();
    }
    if (connectorId == null || connectorId.getId().isBlank()) {
      return ConnectorSyncResult.skippedResult();
    }
    boolean statsOk = true;
    try {
      tableSupport.runSyncStatisticsCapture(connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      statsOk = false;
      LOG.warnf(
          e,
          "Connector stats-only sync failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
    }
    return new ConnectorSyncResult(statsOk, true, false);
  }

  public ConnectorSyncResult runPostCommitStatsSyncAttempt(
      TableGatewaySupport tableSupport,
      List<String> namespacePath,
      String tableName,
      Table tableRecord) {
    return runConnectorStatsSyncAttempt(
        tableSupport, resolveConnectorId(tableRecord), namespacePath, tableName);
  }

  ResourceId resolveConnectorId(Table tableRecord) {
    if (tableRecord == null
        || !tableRecord.hasUpstream()
        || !tableRecord.getUpstream().hasConnectorId()) {
      return null;
    }
    ResourceId connectorId = tableRecord.getUpstream().getConnectorId();
    return connectorId == null || connectorId.getId().isBlank() ? null : connectorId;
  }

  public void pruneRemovedSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    if (tableId == null || snapshotIds == null || snapshotIds.isEmpty()) {
      return;
    }
    try {
      snapshotMetadataService.deleteSnapshots(tableId, snapshotIds);
    } catch (RuntimeException e) {
      LOG.warnf(e, "Snapshot prune failed tableId=%s snapshotIds=%s", tableId.getId(), snapshotIds);
    }
  }

  public record ConnectorSyncResult(boolean captureOk, boolean reconcileOk, boolean skipped) {
    static ConnectorSyncResult skippedResult() {
      return new ConnectorSyncResult(true, true, true);
    }

    public boolean success() {
      return skipped || (captureOk && reconcileOk);
    }
  }
}
