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

package ai.floedb.floecat.service.catalog.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.SNAPSHOT;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class CurrentSnapshotPointerService {
  @Inject SnapshotRepository snapshotRepo;

  public void maybeAdvance(ResourceId tableId, long snapshotId, String corr) {
    Snapshot candidate =
        snapshotRepo
            .getById(tableId, snapshotId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr,
                        SNAPSHOT,
                        Map.of(
                            "table_id", tableId.getId(),
                            "id", Long.toString(snapshotId))));
    maybeAdvance(tableId, candidate, corr);
  }

  public void maybeAdvance(ResourceId tableId, Snapshot candidate, String corr) {
    var result = snapshotRepo.maybeAdvanceCurrentSnapshotPointer(tableId, candidate);
    if (result == null) {
      return;
    }
    switch (result) {
      case UPDATED, UNCHANGED -> {
        return;
      }
      case TABLE_MISSING -> throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
      case CONFLICT -> throw GrpcErrors.aborted(corr, Map.of("id", tableId.getId()));
    }
  }
}
