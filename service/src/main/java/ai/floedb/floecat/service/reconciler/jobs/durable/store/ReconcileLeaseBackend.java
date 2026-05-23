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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

import java.util.List;
import java.util.Optional;

public interface ReconcileLeaseBackend {
  sealed interface LeaseWriteOp
      permits LeaseRecordUpsert,
          LeaseRecordDelete,
          LeaseExpiryUpsert,
          LeaseExpiryDelete,
          LeaseOwnerUpsert,
          LeaseOwnerDelete {}

  record LeaseRecordUpsert(
      String accountId, String jobId, long expectedVersion, String encodedLease)
      implements LeaseWriteOp {}

  record LeaseRecordDelete(String accountId, String jobId, long expectedVersion)
      implements LeaseWriteOp {}

  record LeaseExpiryUpsert(String leaseExpiryKey, long expectedVersion, String canonicalPointerKey)
      implements LeaseWriteOp {}

  record LeaseExpiryDelete(String leaseExpiryKey, long expectedVersion) implements LeaseWriteOp {}

  record LeaseOwnerUpsert(String ownerKey, long expectedVersion, String canonicalPointerKey)
      implements LeaseWriteOp {}

  record LeaseOwnerDelete(String ownerKey, long expectedVersion) implements LeaseWriteOp {}

  record LeaseWriteBatch(List<LeaseWriteOp> writes) {
    public static LeaseWriteBatch empty() {
      return new LeaseWriteBatch(List.of());
    }
  }

  record LeaseRecordSnapshot(String encodedLease, long version) {}

  record LeaseExpirySnapshot(String leaseExpiryKey, String canonicalPointerKey, long version) {}

  record LeaseOwnerSnapshot(String ownerKey, String canonicalPointerKey, long version) {}

  Optional<LeaseRecordSnapshot> loadLease(String accountId, String jobId);

  Optional<LeaseExpirySnapshot> loadLeaseExpiry(String leaseExpiryKey);

  Optional<LeaseOwnerSnapshot> loadOwner(String ownerKey);

  ReconcileLeaseStore.LeaseExpiryScanPage scanExpiredLeaseEntries(int limit, String pageToken);

  boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch jobIndexBatch, LeaseWriteBatch leaseBatch);
}
