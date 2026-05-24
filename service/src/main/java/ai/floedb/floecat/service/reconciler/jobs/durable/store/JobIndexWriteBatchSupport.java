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
import ai.floedb.floecat.storage.spi.PointerStore.CasDelete;
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public final class JobIndexWriteBatchSupport {
  private JobIndexWriteBatchSupport() {}

  public static List<CasOp> toCasOps(ReconcileJobIndexStore.JobIndexWriteBatch batch) {
    return toCasOps(batch, key -> Optional.empty());
  }

  public static List<CasOp> toCasOps(
      ReconcileJobIndexStore.JobIndexWriteBatch batch,
      Function<String, Optional<JobIndexEntrySnapshot>> loadStoredPointer) {
    List<CasOp> ops = new ArrayList<>(batch.writes().size());
    for (ReconcileJobIndexStore.JobIndexWriteOp write : batch.writes()) {
      if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
        ops.add(
            new CasUpsert(
                upsert.pointerKey(),
                upsert.expectedVersion(),
                Pointer.newBuilder()
                    .setKey(upsert.pointerKey())
                    .setBlobUri(upsert.blobUri())
                    .setVersion(upsert.expectedVersion() + 1L)
                    .build()));
      } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
        ops.add(new CasDelete(delete.pointerKey(), delete.expectedVersion()));
      }
    }
    for (ReconcileJobIndexStore.ReadyQueueWrite readyUpsert : batch.readyMutation().upserts()) {
      JobIndexEntrySnapshot existing =
          loadStoredPointer.apply(readyUpsert.readyPointerKey()).orElse(null);
      long expectedVersion = existing == null ? 0L : existing.version();
      ops.add(
          new CasUpsert(
              readyUpsert.readyPointerKey(),
              expectedVersion,
              Pointer.newBuilder()
                  .setKey(readyUpsert.readyPointerKey())
                  .setBlobUri(readyUpsert.canonicalPointerKey())
                  .setVersion(expectedVersion + 1L)
                  .build()));
    }
    for (String readyDeleteKey : batch.readyMutation().deletes()) {
      JobIndexEntrySnapshot existing = loadStoredPointer.apply(readyDeleteKey).orElse(null);
      if (existing != null) {
        ops.add(new CasDelete(readyDeleteKey, existing.version()));
      }
    }
    return ops;
  }
}
