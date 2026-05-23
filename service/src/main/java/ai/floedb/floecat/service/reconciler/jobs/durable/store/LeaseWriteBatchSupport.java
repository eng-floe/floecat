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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.ArrayList;
import java.util.List;

public final class LeaseWriteBatchSupport {
  private LeaseWriteBatchSupport() {}

  public static List<CasOp> toCasOps(ReconcileLeaseBackend.LeaseWriteBatch batch) {
    List<CasOp> ops = new ArrayList<>(batch.writes().size());
    for (ReconcileLeaseBackend.LeaseWriteOp write : batch.writes()) {
      if (write instanceof ReconcileLeaseBackend.LeaseRecordUpsert upsert) {
        String key = Keys.reconcileJobLeasePointerById(upsert.accountId(), upsert.jobId());
        ops.add(
            new CasUpsert(
                key,
                upsert.expectedVersion(),
                Pointer.newBuilder()
                    .setKey(key)
                    .setBlobUri(upsert.encodedLease())
                    .setVersion(upsert.expectedVersion() + 1L)
                    .build()));
      } else if (write instanceof ReconcileLeaseBackend.LeaseRecordDelete delete) {
        ops.add(
            new CasDelete(
                Keys.reconcileJobLeasePointerById(delete.accountId(), delete.jobId()),
                delete.expectedVersion()));
      } else if (write instanceof ReconcileLeaseBackend.LeaseExpiryUpsert upsert) {
        ops.add(
            new CasUpsert(
                upsert.leaseExpiryKey(),
                upsert.expectedVersion(),
                Pointer.newBuilder()
                    .setKey(upsert.leaseExpiryKey())
                    .setBlobUri(upsert.canonicalPointerKey())
                    .setVersion(upsert.expectedVersion() + 1L)
                    .build()));
      } else if (write instanceof ReconcileLeaseBackend.LeaseExpiryDelete delete) {
        ops.add(new CasDelete(delete.leaseExpiryKey(), delete.expectedVersion()));
      } else if (write instanceof ReconcileLeaseBackend.LeaseOwnerUpsert upsert) {
        ops.add(
            new CasUpsert(
                upsert.ownerKey(),
                upsert.expectedVersion(),
                Pointer.newBuilder()
                    .setKey(upsert.ownerKey())
                    .setBlobUri(upsert.canonicalPointerKey())
                    .setVersion(upsert.expectedVersion() + 1L)
                    .build()));
      } else if (write instanceof ReconcileLeaseBackend.LeaseOwnerDelete delete) {
        ops.add(new CasDelete(delete.ownerKey(), delete.expectedVersion()));
      }
    }
    return ops;
  }
}
