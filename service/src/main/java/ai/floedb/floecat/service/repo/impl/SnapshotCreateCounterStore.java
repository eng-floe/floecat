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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@ApplicationScoped
public class SnapshotCreateCounterStore {
  private static final String STATE_PREFIX = "counter:";

  private final PointerStore pointerStore;

  @Inject
  public SnapshotCreateCounterStore(PointerStore pointerStore) {
    this.pointerStore = Objects.requireNonNull(pointerStore, "pointerStore");
  }

  public record CreateIncrement(String accountId) {}

  public List<PointerStore.CasOp> planIncrementOps(List<CreateIncrement> increments) {
    if (increments == null || increments.isEmpty()) {
      return List.of();
    }

    Map<String, Integer> createsByAccount = new LinkedHashMap<>();
    for (CreateIncrement increment : increments) {
      if (increment == null) {
        continue;
      }
      createsByAccount.merge(increment.accountId(), 1, Integer::sum);
    }

    List<PointerStore.CasOp> ops = new ArrayList<>();
    for (Map.Entry<String, Integer> entry : createsByAccount.entrySet()) {
      String accountId = entry.getKey();
      int createCount = entry.getValue();
      if (createCount <= 0) {
        continue;
      }

      String stateKey = Keys.snapshotCreateCounterStatePointer(accountId);
      Pointer state = pointerStore.get(stateKey).orElse(null);
      long expectedVersion = state == null ? 0L : state.getVersion();
      long currentCounter = state == null ? 0L : parseStateCounter(state, stateKey);
      long nextCounter = currentCounter + createCount;

      ops.add(
          new PointerStore.CasUpsert(
              stateKey,
              expectedVersion,
              PointerReferences.opaqueMarkerPointer(
                  stateKey, STATE_PREFIX + nextCounter, expectedVersion + 1L)));
    }
    return ops;
  }

  public long currentCounter(String accountId) {
    String stateKey = Keys.snapshotCreateCounterStatePointer(accountId);
    return pointerStore
        .get(stateKey)
        .map(pointer -> parseStateCounter(pointer, stateKey))
        .orElse(0L);
  }

  private static long parseStateCounter(Pointer pointer, String stateKey) {
    if (!PointerReferences.isOpaqueMarkerPointer(pointer)) {
      throw new BaseResourceRepository.CorruptionException(
          "snapshot create counter state has wrong pointer kind: " + stateKey);
    }
    String payload = pointer.getBlobUri();
    if (payload == null || !payload.startsWith(STATE_PREFIX)) {
      throw new BaseResourceRepository.CorruptionException(
          "snapshot create counter state has invalid payload: " + stateKey);
    }
    try {
      return Long.parseLong(payload.substring(STATE_PREFIX.length()));
    } catch (NumberFormatException e) {
      throw new BaseResourceRepository.CorruptionException(
          "snapshot create counter state is not numeric: " + stateKey, e);
    }
  }
}
