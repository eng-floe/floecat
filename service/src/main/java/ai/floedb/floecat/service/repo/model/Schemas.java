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

package ai.floedb.floecat.service.repo.model;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.util.ConstraintNormalizer;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class Schemas {

  public static final ResourceSchema<Account, AccountKey> ACCOUNT =
      ResourceSchema.<Account, AccountKey>of(
              "account",
              key -> Keys.accountPointerById(key.accountId()),
              key -> Keys.accountBlobUri(key.accountId(), key.sha256()),
              v -> Map.of("byName", Keys.accountPointerByName(v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new AccountKey(v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Catalog, CatalogKey> CATALOG =
      ResourceSchema.<Catalog, CatalogKey>of(
              "catalog",
              key -> Keys.catalogPointerById(key.accountId(), key.catalogId()),
              key -> Keys.catalogBlobUri(key.accountId(), key.catalogId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.catalogPointerByName(
                          v.getResourceId().getAccountId(), v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new CatalogKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<StorageAuthority, StorageAuthorityKey> STORAGE_AUTHORITY =
      ResourceSchema.<StorageAuthority, StorageAuthorityKey>of(
              "storage-authority",
              key -> Keys.storageAuthorityPointerById(key.accountId(), key.authorityId()),
              key -> Keys.storageAuthorityBlobUri(key.accountId(), key.authorityId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.storageAuthorityPointerByName(
                          v.getResourceId().getAccountId(), v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new StorageAuthorityKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Namespace, NamespaceKey> NAMESPACE =
      ResourceSchema.<Namespace, NamespaceKey>of(
              "namespace",
              key -> Keys.namespacePointerById(key.accountId(), key.namespaceId()),
              key -> Keys.namespaceBlobUri(key.accountId(), key.namespaceId(), key.sha256()),
              v -> {
                List<String> fullPath = new ArrayList<>(v.getParentsList());
                fullPath.add(v.getDisplayName());
                return Map.of(
                    "byPath",
                    Keys.namespacePointerByPath(
                        v.getResourceId().getAccountId(), v.getCatalogId().getId(), fullPath));
              },
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new NamespaceKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Table, TableKey> TABLE =
      ResourceSchema.<Table, TableKey>of(
              "table",
              key -> Keys.tablePointerById(key.accountId(), key.tableId()),
              key -> Keys.tableBlobUri(key.accountId(), key.tableId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.tablePointerByName(
                          v.getResourceId().getAccountId(),
                          v.getCatalogId().getId(),
                          v.getNamespaceId().getId(),
                          v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new TableKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Snapshot, SnapshotKey> SNAPSHOT =
      ResourceSchema.<Snapshot, SnapshotKey>of(
              "snapshot",
              key -> Keys.snapshotPointerById(key.accountId(), key.tableId(), key.snapshotId()),
              key ->
                  Keys.snapshotBlobUri(
                      key.accountId(), key.tableId(), key.snapshotId(), key.sha256()),
              v ->
                  Map.of(
                      "byId",
                          Keys.snapshotPointerById(
                              v.getTableId().getAccountId(),
                              v.getTableId().getId(),
                              v.getSnapshotId()),
                      "byTime",
                          Keys.snapshotPointerByTime(
                              v.getTableId().getAccountId(), v.getTableId().getId(),
                              v.getSnapshotId(), Timestamps.toMillis(v.getUpstreamCreatedAt()))),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new SnapshotKey(
                    v.getTableId().getAccountId(), v.getTableId().getId(), v.getSnapshotId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<TargetStatsRecord, TargetStatsKey> TARGET_STATS =
      ResourceSchema.<TargetStatsRecord, TargetStatsKey>of(
              "target-stats",
              (TargetStatsKey key) ->
                  Keys.snapshotTargetStatsPointer(
                      key.accountId(), key.tableId(), key.snapshotId(), key.targetId()),
              (TargetStatsKey key) ->
                  Keys.snapshotTargetStatsBlobUri(
                      key.accountId(), key.tableId(), key.targetId(), key.sha256()),
              (TargetStatsRecord v) -> Map.of(),
              (TargetStatsRecord v) -> {
                var target = v.getTarget();
                if (target.getTargetCase() == StatsTarget.TargetCase.TARGET_NOT_SET) {
                  throw new IllegalArgumentException("target must be set on TargetStatsRecord");
                }
                validateTargetValueCompatibility(target, v.getValueCase());
                var normalizedTarget =
                    target.getTargetCase() == StatsTarget.TargetCase.EXPRESSION
                        ? StatsTarget.newBuilder()
                            .setExpression(
                                StatsTargetIdentity.normalizeExpressionTarget(
                                    target.getExpression()))
                            .build()
                        : target;
                var canonical =
                    switch (v.getValueCase()) {
                      case TABLE, SCALAR, FILE -> v.toBuilder().setTarget(normalizedTarget).build();
                      case VALUE_NOT_SET ->
                          throw new IllegalArgumentException(
                              "value must be set on TargetStatsRecord");
                    };
                var sha = Hashing.sha256Hex(canonical.toByteArray());
                return new TargetStatsKey(
                    v.getTableId().getAccountId(),
                    v.getTableId().getId(),
                    v.getSnapshotId(),
                    StatsTargetIdentity.storageId(normalizedTarget),
                    sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<IndexArtifactRecord, IndexArtifactKey> INDEX_ARTIFACT =
      ResourceSchema.<IndexArtifactRecord, IndexArtifactKey>of(
              "index-artifact",
              (IndexArtifactKey key) ->
                  Keys.snapshotIndexArtifactPointer(
                      key.accountId(), key.tableId(), key.snapshotId(), key.targetId()),
              (IndexArtifactKey key) ->
                  Keys.snapshotIndexArtifactBlobUri(
                      key.accountId(), key.tableId(), key.targetId(), key.sha256()),
              (IndexArtifactRecord v) -> Map.of(),
              (IndexArtifactRecord v) -> {
                if (!v.hasTableId() || v.getTableId().getId().isBlank()) {
                  throw new IllegalArgumentException("table_id must be set on IndexArtifactRecord");
                }
                if (!v.hasTarget()
                    || v.getTarget().getTargetCase() == IndexTarget.TargetCase.TARGET_NOT_SET) {
                  throw new IllegalArgumentException("target must be set on IndexArtifactRecord");
                }
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new IndexArtifactKey(
                    v.getTableId().getAccountId(),
                    v.getTableId().getId(),
                    v.getSnapshotId(),
                    indexArtifactTargetStorageId(v.getTarget()),
                    sha);
              })
          .withCasBlobs();

  private static void validateTargetValueCompatibility(
      StatsTarget target, TargetStatsRecord.ValueCase valueCase) {
    switch (target.getTargetCase()) {
      case TABLE -> {
        if (valueCase != TargetStatsRecord.ValueCase.TABLE) {
          throw new IllegalArgumentException(
              "incompatible target/value: table target requires table value");
        }
      }
      case COLUMN, EXPRESSION -> {
        if (valueCase != TargetStatsRecord.ValueCase.SCALAR) {
          throw new IllegalArgumentException(
              "incompatible target/value: column/expression target requires scalar value");
        }
      }
      case FILE -> {
        if (valueCase != TargetStatsRecord.ValueCase.FILE) {
          throw new IllegalArgumentException(
              "incompatible target/value: file target requires file value");
        }
      }
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on TargetStatsRecord");
    }
  }

  private static String indexArtifactTargetStorageId(IndexTarget target) {
    return switch (target.getTargetCase()) {
      case FILE -> "file:" + target.getFile().getFilePath();
      case TARGET_NOT_SET ->
          throw new IllegalArgumentException("target must be set on IndexArtifactRecord");
    };
  }

  public static final ResourceSchema<SnapshotConstraints, SnapshotConstraintsKey>
      SNAPSHOT_CONSTRAINTS =
          ResourceSchema.<SnapshotConstraints, SnapshotConstraintsKey>of(
                  "snapshot-constraints",
                  (SnapshotConstraintsKey key) ->
                      Keys.snapshotConstraintsPointer(
                          key.accountId(), key.tableId(), key.snapshotId()),
                  (SnapshotConstraintsKey key) ->
                      Keys.snapshotConstraintsBlobUri(
                          key.accountId(), key.tableId(), key.snapshotId(), key.sha256()),
                  (SnapshotConstraints v) ->
                      Map.of(
                          "bySnapshotStats",
                          Keys.snapshotConstraintsStatsPointer(
                              v.getTableId().getAccountId(),
                              v.getTableId().getId(),
                              v.getSnapshotId())),
                  (SnapshotConstraints v) -> {
                    var norm = ConstraintNormalizer.normalize(v);
                    var sha = Hashing.sha256Hex(norm.toByteArray());
                    return new SnapshotConstraintsKey(
                        v.getTableId().getAccountId(),
                        v.getTableId().getId(),
                        v.getSnapshotId(),
                        sha);
                  })
              .withCasBlobs();

  public static final ResourceSchema<View, ViewKey> VIEW =
      ResourceSchema.<View, ViewKey>of(
              "view",
              key -> Keys.viewPointerById(key.accountId(), key.viewId()),
              key -> Keys.viewBlobUri(key.accountId(), key.viewId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.viewPointerByName(
                          v.getResourceId().getAccountId(),
                          v.getCatalogId().getId(),
                          v.getNamespaceId().getId(),
                          v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new ViewKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Connector, ConnectorKey> CONNECTOR =
      ResourceSchema.<Connector, ConnectorKey>of(
              "connector",
              key -> Keys.connectorPointerById(key.accountId(), key.connectorId()),
              key -> Keys.connectorBlobUri(key.accountId(), key.connectorId(), key.sha256()),
              v ->
                  Map.of(
                      "byName",
                      Keys.connectorPointerByName(
                          v.getResourceId().getAccountId(), v.getDisplayName())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new ConnectorKey(
                    v.getResourceId().getAccountId(), v.getResourceId().getId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<Transaction, TransactionKey> TRANSACTION =
      ResourceSchema.<Transaction, TransactionKey>of(
              "transaction",
              key -> Keys.transactionPointerById(key.accountId(), key.txId()),
              key -> Keys.transactionBlobUri(key.accountId(), key.txId(), key.sha256()),
              v -> Map.of(),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new TransactionKey(v.getAccountId(), v.getTxId(), sha);
              })
          .withCasBlobs();

  public static final ResourceSchema<TransactionIntent, TransactionIntentKey> TRANSACTION_INTENT =
      ResourceSchema.<TransactionIntent, TransactionIntentKey>of(
              "transaction-intent",
              key -> Keys.transactionIntentPointerByTarget(key.accountId(), key.targetPointerKey()),
              key -> Keys.transactionIntentBlobUri(key.accountId(), key.txId(), key.sha256()),
              v ->
                  Map.of(
                      "byTx",
                      Keys.transactionIntentPointerByTx(
                          v.getAccountId(), v.getTxId(), v.getTargetPointerKey())),
              v -> {
                var sha = Hashing.sha256Hex(v.toByteArray());
                return new TransactionIntentKey(
                    v.getAccountId(), v.getTxId(), v.getTargetPointerKey(), sha);
              })
          .withCasBlobs();
}
