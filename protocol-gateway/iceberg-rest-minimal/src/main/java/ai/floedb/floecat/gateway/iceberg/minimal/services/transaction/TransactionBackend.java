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

import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.transaction.rpc.AbortTransactionResponse;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.TxChange;
import java.time.Duration;
import java.util.List;

public interface TransactionBackend {
  ResolveCatalogResponse resolveCatalog(String prefix);

  ResolveNamespaceResponse resolveNamespace(String prefix, List<String> namespacePath);

  ResolveTableResponse resolveTable(String prefix, List<String> namespacePath, String tableName);

  GetTableResponse getTable(ResourceId tableId);

  ListSnapshotsResponse listSnapshots(ResourceId tableId);

  default BeginTransactionResponse beginTransaction(String idempotencyKey, String requestHash) {
    return beginTransaction(idempotencyKey, requestHash, null);
  }

  BeginTransactionResponse beginTransaction(
      String idempotencyKey, String requestHash, Duration ttl);

  GetTransactionResponse getTransaction(String txId);

  PrepareTransactionResponse prepareTransaction(
      String txId, String idempotencyKey, List<TxChange> changes);

  CommitTransactionResponse commitTransaction(String txId, String idempotencyKey);

  AbortTransactionResponse abortTransaction(String txId, String reason);
}
