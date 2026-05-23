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
import ai.floedb.floecat.storage.spi.PointerStore.CasOp;
import ai.floedb.floecat.storage.spi.PointerStore.CasUpsert;
import java.util.ArrayList;
import java.util.List;

public final class ProjectionWriteBatchSupport {
  private ProjectionWriteBatchSupport() {}

  public static List<CasOp> toCasOps(ReconcileProjectionBackend.ProjectionWriteBatch batch) {
    List<CasOp> ops = new ArrayList<>(batch.upserts().size());
    for (ReconcileProjectionBackend.ProjectionUpsert upsert : batch.upserts()) {
      ops.add(
          new CasUpsert(
              upsert.pointerKey(),
              upsert.expectedVersion(),
              Pointer.newBuilder()
                  .setKey(upsert.pointerKey())
                  .setBlobUri(upsert.blobUri())
                  .setVersion(upsert.expectedVersion() + 1L)
                  .build()));
    }
    return ops;
  }
}
