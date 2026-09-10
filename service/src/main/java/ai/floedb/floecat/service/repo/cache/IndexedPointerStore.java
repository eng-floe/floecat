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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One PointerStore seam: planner reads use the complete index, operational reads use durable KV.
 *
 * <p>The owner contract is a precondition: one Floecat instance owns an account at a time, and a
 * handoff stops the old instance's mutations before the new instance loads the partition. This lets
 * the in-memory index and durable KV be one state machine rather than two independently expiring
 * replicas.
 */
public class IndexedPointerStore implements PointerStore {
  private final PointerStore durable;
  private final PlanningPointerIndex index;

  public IndexedPointerStore(PointerStore durable, PlanningPointerIndex index) {
    this.durable = java.util.Objects.requireNonNull(durable, "durable");
    this.index = java.util.Objects.requireNonNull(index, "index");
  }

  @Override
  public Optional<Pointer> get(String key) {
    return index.get(key);
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    return index.getBatch(keys);
  }

  @Override
  public Optional<Pointer> getConsistent(String key) {
    return durable.getConsistent(key);
  }

  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    return durable.getBatchConsistent(keys);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String token, StringBuilder next) {
    return index.list(prefix, limit, token, next);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String token, StringBuilder next) {
    return durable.listPointersByPrefixConsistent(prefix, limit, token, next);
  }

  @Override
  public int countByPrefix(String prefix) {
    return index.count(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    return durable.countByPrefixConsistent(prefix);
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return index.pageTokenAfterKey(key);
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return index.mutate(
        List.of(key),
        () -> durable.compareAndSet(key, expectedVersion, next),
        won -> {
          if (won) publish(key, next, expectedVersion + 1L);
          else index.refresh(key, durable.getConsistent(key));
        });
  }

  @Override
  public boolean delete(String key) {
    return index.mutate(
        List.of(key),
        () -> durable.delete(key),
        deleted -> {
          if (deleted) index.remove(key);
          else index.refresh(key, durable.getConsistent(key));
        });
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return index.mutate(
        List.of(key),
        () -> durable.compareAndDelete(key, expectedVersion),
        deleted -> {
          if (deleted) index.remove(key);
          else index.refresh(key, durable.getConsistent(key));
        });
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) return durable.compareAndSetBatch(ops);
    List<String> keys = ops.stream().map(CasOp::key).distinct().toList();
    return index.mutate(
        keys,
        () -> durable.compareAndSetBatch(ops),
        won -> {
          if (!won) {
            for (String key : keys) index.refresh(key, durable.getConsistent(key));
            return;
          }
          for (CasOp op : ops) {
            switch (op) {
              case CasUpsert upsert ->
                  publish(upsert.key(), upsert.next(), upsert.expectedVersion() + 1L);
              case UnconditionalUpsert upsert ->
                  publish(upsert.key(), upsert.next(), upsert.next().getVersion());
              case CasDelete delete -> index.remove(delete.key());
              case CasCheckAbsent absent -> index.remove(absent.key());
              case CasCheck ignored -> {}
            }
          }
        });
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return index.mutate(
        List.of(prefix),
        () -> durable.deleteByPrefix(prefix),
        ignored -> index.removePrefix(prefix, null));
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    return index.mutate(
        List.of(prefix),
        () -> durable.deleteByPrefixExcluding(prefix, excludedKey),
        ignored -> index.removePrefix(prefix, excludedKey));
  }

  @Override
  public boolean isEmpty() {
    return durable.isEmpty();
  }

  @Override
  public void dump(String header) {
    durable.dump(header);
  }

  private void publish(String key, Pointer next, long version) {
    index.publish(key, next.toBuilder().setKey(key).setVersion(version).build());
  }
}
