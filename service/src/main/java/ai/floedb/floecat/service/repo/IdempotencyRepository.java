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

package ai.floedb.floecat.service.repo;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import java.util.Optional;

public interface IdempotencyRepository {
  Optional<IdempotencyRecord> get(String key);

  boolean createPending(
      String accountId,
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt);

  /**
   * Reserves the stable identity of a resource before its create transaction is attempted. The
   * reserved identity lets the resource and its immutable success receipt commit atomically.
   */
  default boolean createPending(
      String accountId,
      String key,
      String opName,
      String requestHash,
      ResourceId resourceId,
      Timestamp createdAt,
      Timestamp expiresAt) {
    return createPending(accountId, key, opName, requestHash, createdAt, expiresAt);
  }

  void finalizeSuccess(
      String accountId,
      String key,
      String opName,
      String requestHash,
      ResourceId resourceId,
      MutationMeta meta,
      byte[] payloadBytes,
      Timestamp createdAt,
      Timestamp expiresAt);

  default PointerStore.CasUpsert prepareSuccess(
      String accountId,
      String key,
      String opName,
      String requestHash,
      ResourceId resourceId,
      MutationMeta meta,
      byte[] payloadBytes,
      Timestamp createdAt,
      Timestamp expiresAt) {
    throw new UnsupportedOperationException("atomic idempotency completion is not supported");
  }

  boolean delete(String key);
}
