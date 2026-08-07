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
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.Timestamp;
import java.util.Optional;

public interface IdempotencyRepository {
  Optional<IdempotencyRecord> get(String key);

  PendingClaim createPending(
      String accountId,
      String key,
      String opName,
      String requestHash,
      Timestamp createdAt,
      Timestamp expiresAt);

  void finalizeSuccess(
      String accountId,
      String key,
      PendingClaim pendingClaim,
      String opName,
      String requestHash,
      ResourceId resourceId,
      MutationMeta meta,
      byte[] payloadBytes,
      Timestamp createdAt,
      Timestamp expiresAt);

  boolean delete(String key);

  boolean deletePending(String key, PendingClaim pendingClaim);

  CleanupResult deleteAccountResources(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress);

  record CleanupResult(int pointersDeleted, int blobsDeleted) {}

  /** Exact durable PENDING row owned by one invocation. */
  record PendingClaim(
      boolean created, long pointerVersion, String claimMarkerKey, long claimMarkerVersion) {
    public PendingClaim {
      boolean validCreated =
          pointerVersion > 0L
              && claimMarkerKey != null
              && !claimMarkerKey.isBlank()
              && claimMarkerVersion > 0L;
      boolean validLost =
          pointerVersion == 0L
              && (claimMarkerKey == null || claimMarkerKey.isBlank())
              && claimMarkerVersion == 0L;
      if (created ? !validCreated : !validLost) {
        throw new IllegalArgumentException(
            "a created claim must name positive pointer and marker versions; a lost claim must be empty");
      }
    }
  }
}
