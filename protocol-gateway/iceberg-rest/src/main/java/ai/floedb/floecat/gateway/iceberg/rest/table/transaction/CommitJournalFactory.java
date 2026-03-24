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
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import java.util.List;

final class CommitJournalFactory {
  private static final int COMMIT_JOURNAL_VERSION = 1;
  private static final int COMMIT_OUTBOX_VERSION = 1;

  private CommitJournalFactory() {}

  static IcebergCommitJournalEntry buildJournalEntry(
      String txId,
      String requestHash,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds,
      ai.floedb.floecat.catalog.rpc.Table table,
      long createdAtMs) {
    IcebergCommitJournalEntry.Builder builder =
        IcebergCommitJournalEntry.newBuilder()
            .setVersion(COMMIT_JOURNAL_VERSION)
            .setTxId(txId == null ? "" : txId)
            .setRequestHash(requestHash == null ? "" : requestHash)
            .setCreatedAtMs(Math.max(0L, createdAtMs));
    if (tableId != null) {
      builder.setTableId(tableId);
    }
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.addAllNamespacePath(namespacePath);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setTableName(tableName);
    }
    if (connectorId != null && !connectorId.getId().isBlank()) {
      builder.setConnectorId(connectorId);
    }
    if (addedSnapshotIds != null && !addedSnapshotIds.isEmpty()) {
      builder.addAllAddedSnapshotIds(addedSnapshotIds);
    }
    if (removedSnapshotIds != null && !removedSnapshotIds.isEmpty()) {
      builder.addAllRemovedSnapshotIds(removedSnapshotIds);
    }
    String metadataLocation =
        table == null ? null : MetadataLocationUtil.metadataLocation(table.getPropertiesMap());
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    String tableUuid = table == null ? null : table.getPropertiesMap().get("table-uuid");
    if (tableUuid != null && !tableUuid.isBlank()) {
      builder.setTableUuid(tableUuid);
    }
    return builder.build();
  }

  static IcebergCommitOutboxEntry buildOutboxEntry(
      String txId, String requestHash, String accountId, String tableId, long createdAtMs) {
    return IcebergCommitOutboxEntry.newBuilder()
        .setVersion(COMMIT_OUTBOX_VERSION)
        .setTxId(txId == null ? "" : txId)
        .setRequestHash(requestHash == null ? "" : requestHash)
        .setAccountId(accountId == null ? "" : accountId)
        .setTableId(tableId == null ? "" : tableId)
        .setCreatedAtMs(Math.max(0L, createdAtMs))
        .setAttemptCount(0)
        .setNextAttemptAtMs(0L)
        .setLastAttemptAtMs(0L)
        .setDeadLetteredAtMs(0L)
        .build();
  }
}
