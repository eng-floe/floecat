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

  public boolean runPostCommitStatsSyncAttempt(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds) {
    return runStatsSyncAttempt(
        tableSupport, connectorId, namespacePath, tableName, snapshotIds, true);
  }

  private boolean runStatsSyncAttempt(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds,
      boolean fullRescan) {
    if (!tableSupport.connectorIntegrationEnabled()) {
      return true;
    }
    if (connectorId == null || connectorId.getId().isBlank()) {
      return true;
    }
    if (snapshotIds == null || snapshotIds.isEmpty()) {
      return true;
    }
    try {
      tableSupport.runSyncStatisticsCapture(
          connectorId, namespacePath, tableName, snapshotIds, fullRescan);
      return true;
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Connector stats-only sync failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
      return false;
    }
  }

  public boolean pruneRemovedSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    if (tableId == null || snapshotIds == null || snapshotIds.isEmpty()) {
      return true;
    }
    try {
      snapshotMetadataService.deleteSnapshots(tableId, snapshotIds);
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Snapshot prune failed tableId=%s snapshotIds=%s", tableId.getId(), snapshotIds);
      return false;
    }
  }
}
