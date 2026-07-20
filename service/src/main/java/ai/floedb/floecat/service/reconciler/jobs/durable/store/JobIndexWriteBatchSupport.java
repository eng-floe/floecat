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

import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheck;
import ai.floedb.floecat.storage.spi.PointerStore.CasCheckAbsent;
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
    if (batch == null) {
      return List.of();
    }
    List<CasOp> ops = new ArrayList<>(batch.writes().size());
    for (ReconcileJobIndexStore.JobIndexWriteOp write : batch.writes()) {
      if (write instanceof ReconcileJobIndexStore.JobIndexUpsert upsert) {
        ops.add(
            new CasUpsert(
                upsert.pointerKey(),
                upsert.expectedVersion(),
                switch (upsert.referenceKind()) {
                  case PRK_BLOB_URI ->
                      PointerReferences.blobPointer(
                          upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                  case PRK_INLINE_JSON ->
                      PointerReferences.inlineJsonPointer(
                          upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                  case PRK_POINTER_KEY ->
                      PointerReferences.pointerKeyPointer(
                          upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                  case PRK_OPAQUE_MARKER ->
                      PointerReferences.opaqueMarkerPointer(
                          upsert.pointerKey(), upsert.blobUri(), upsert.expectedVersion() + 1L);
                  case PRK_UNSPECIFIED, UNRECOGNIZED ->
                      throw new IllegalStateException(
                          "missing pointer reference kind for " + upsert.pointerKey());
                }));
      } else if (write instanceof ReconcileJobIndexStore.JobIndexDelete delete) {
        if (!delete.allowAbsent() || loadStoredPointer.apply(delete.pointerKey()).isPresent()) {
          ops.add(new CasDelete(delete.pointerKey(), delete.expectedVersion()));
        }
      } else if (write instanceof ReconcileJobIndexStore.JobIndexCheck check) {
        ops.add(new CasCheck(check.pointerKey(), check.expectedVersion()));
      } else if (write instanceof ReconcileJobIndexStore.JobIndexCheckAbsent check) {
        ops.add(new CasCheckAbsent(check.pointerKey()));
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
              switch (readyUpsert.referenceKind()) {
                case PRK_BLOB_URI ->
                    PointerReferences.blobPointer(
                        readyUpsert.readyPointerKey(),
                        readyUpsert.canonicalPointerKey(),
                        expectedVersion + 1L);
                case PRK_INLINE_JSON ->
                    PointerReferences.inlineJsonPointer(
                        readyUpsert.readyPointerKey(),
                        readyUpsert.canonicalPointerKey(),
                        expectedVersion + 1L);
                case PRK_POINTER_KEY ->
                    PointerReferences.pointerKeyPointer(
                        readyUpsert.readyPointerKey(),
                        readyUpsert.canonicalPointerKey(),
                        expectedVersion + 1L);
                case PRK_OPAQUE_MARKER ->
                    PointerReferences.opaqueMarkerPointer(
                        readyUpsert.readyPointerKey(),
                        readyUpsert.canonicalPointerKey(),
                        expectedVersion + 1L);
                case PRK_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalStateException(
                        "missing pointer reference kind for " + readyUpsert.readyPointerKey());
              }));
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
