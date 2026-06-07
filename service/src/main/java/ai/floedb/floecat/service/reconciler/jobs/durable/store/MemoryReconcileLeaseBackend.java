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
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileLeaseBackend implements ReconcileLeaseBackend {
  private PointerStore pointerStore;

  @Inject
  public MemoryReconcileLeaseBackend(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public MemoryReconcileLeaseBackend() {}

  public void bind(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  @Override
  public Optional<LeaseRecordSnapshot> loadLease(String accountId, String jobId) {
    return pointerStore
        .get(LeaseBackendSupport.leasePointerKey(accountId, jobId))
        .map(pointer -> new LeaseRecordSnapshot(pointer.getBlobUri(), pointer.getVersion()));
  }

  @Override
  public Optional<LeaseExpirySnapshot> loadLeaseExpiry(String leaseExpiryKey) {
    return pointerStore
        .get(leaseExpiryKey)
        .map(
            pointer ->
                new LeaseExpirySnapshot(
                    leaseExpiryKey, pointer.getBlobUri(), pointer.getVersion()));
  }

  @Override
  public Optional<LeaseOwnerSnapshot> loadOwner(String ownerKey) {
    return pointerStore
        .get(ownerKey)
        .map(
            pointer ->
                new LeaseOwnerSnapshot(ownerKey, pointer.getBlobUri(), pointer.getVersion()));
  }

  @Override
  public ReconcileLeaseStore.LeaseExpiryScanPage scanExpiredLeaseEntries(
      int limit, String pageToken) {
    StringBuilder nextPageToken = new StringBuilder();
    List<Pointer> pointers =
        pointerStore.listPointersByPrefix(
            LeaseBackendSupport.LEASE_EXPIRY_POINTER_PREFIX, limit, pageToken, nextPageToken);
    List<ReconcileLeaseStore.LeaseExpiryEntry> entries = new ArrayList<>(pointers.size());
    for (Pointer pointer : pointers) {
      entries.add(new ReconcileLeaseStore.LeaseExpiryEntry(pointer.getKey(), pointer.getBlobUri()));
    }
    return new ReconcileLeaseStore.LeaseExpiryScanPage(
        List.copyOf(entries), nextPageToken.toString());
  }

  @Override
  public boolean compareAndSetBatch(
      ReconcileJobIndexStore.JobIndexWriteBatch jobIndexBatch, LeaseWriteBatch leaseBatch) {
    List<CasOp> ops =
        new ArrayList<>(
            JobIndexWriteBatchSupport.toCasOps(
                jobIndexBatch,
                key ->
                    pointerStore
                        .get(key)
                        .map(
                            pointer ->
                                new JobIndexEntrySnapshot(
                                    pointer.getKey(),
                                    pointer.getBlobUri(),
                                    pointer.getVersion()))));
    if (leaseBatch != null) {
      for (LeaseWriteOp write : leaseBatch.writes()) {
        if (write instanceof LeaseRecordCondition condition) {
          var current =
              pointerStore.get(
                  LeaseBackendSupport.leasePointerKey(condition.accountId(), condition.jobId()));
          if (condition.expectedVersion() == 0L) {
            if (current.isPresent()) {
              return false;
            }
          } else if (current.isEmpty()
              || current.get().getVersion() != condition.expectedVersion()) {
            return false;
          }
        } else if (write instanceof LeaseRecordUpsert upsert) {
          String key = LeaseBackendSupport.leasePointerKey(upsert.accountId(), upsert.jobId());
          ops.add(
              new CasUpsert(
                  key,
                  upsert.expectedVersion(),
                  PointerReferences.asInlineJsonPointer(
                          Pointer.newBuilder()
                              .setKey(key)
                              .setVersion(upsert.expectedVersion() + 1L),
                          upsert.encodedLease())
                      .build()));
        } else if (write instanceof LeaseRecordDelete delete) {
          ops.add(
              new CasDelete(
                  LeaseBackendSupport.leasePointerKey(delete.accountId(), delete.jobId()),
                  delete.expectedVersion()));
        } else if (write instanceof LeaseExpiryUpsert upsert) {
          ops.add(
              new CasUpsert(
                  upsert.leaseExpiryKey(),
                  upsert.expectedVersion(),
                  PointerReferences.asPointerKeyPointer(
                          Pointer.newBuilder()
                              .setKey(upsert.leaseExpiryKey())
                              .setVersion(upsert.expectedVersion() + 1L),
                          upsert.canonicalPointerKey())
                      .build()));
        } else if (write instanceof LeaseExpiryDelete delete) {
          ops.add(new CasDelete(delete.leaseExpiryKey(), delete.expectedVersion()));
        } else if (write instanceof LeaseOwnerUpsert upsert) {
          ops.add(
              new CasUpsert(
                  upsert.ownerKey(),
                  upsert.expectedVersion(),
                  PointerReferences.asPointerKeyPointer(
                          Pointer.newBuilder()
                              .setKey(upsert.ownerKey())
                              .setVersion(upsert.expectedVersion() + 1L),
                          upsert.canonicalPointerKey())
                      .build()));
        } else if (write instanceof LeaseOwnerDelete delete) {
          ops.add(new CasDelete(delete.ownerKey(), delete.expectedVersion()));
        }
      }
    }
    return pointerStore.compareAndSetBatch(ops);
  }
}
