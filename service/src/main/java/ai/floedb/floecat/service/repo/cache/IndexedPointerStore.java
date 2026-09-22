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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * One PointerStore seam: planner reads use the complete index, operational reads use durable KV.
 *
 * <p>When the index is disabled, reads go straight to durable KV while mutations keep running
 * through it, so they keep the partition and key locks that order a publish against a load. Routing
 * the reads is an optimisation rather than a correctness boundary: a disabled index never loads, so
 * every partition stays incomplete and the index would fall through to the same durable call
 * anyway. Skipping it saves the ownership check and the partition lookup on every read.
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
    return index.enabled() ? index.get(key) : durable.get(key);
  }

  @Override
  public Map<String, Pointer> getBatch(List<String> keys) {
    return index.enabled() ? index.getBatch(keys) : durable.getBatch(keys);
  }

  @Override
  public Optional<Pointer> getConsistent(String key) {
    return index.enabled() ? index.getConsistent(key) : durable.getConsistent(key);
  }

  @Override
  public Map<String, Pointer> getBatchConsistent(List<String> keys) {
    return index.enabled() ? index.getBatchConsistent(keys) : durable.getBatchConsistent(keys);
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String token, StringBuilder next) {
    return index.enabled()
        ? index.list(prefix, limit, token, next)
        : durable.listPointersByPrefix(prefix, limit, token, next);
  }

  @Override
  public List<Pointer> listPointersByPrefixConsistent(
      String prefix, int limit, String token, StringBuilder next) {
    return index.enabled()
        ? index.listConsistent(prefix, limit, token, next)
        : durable.listPointersByPrefixConsistent(prefix, limit, token, next);
  }

  @Override
  public int countByPrefix(String prefix) {
    return index.enabled() ? index.count(prefix) : durable.countByPrefix(prefix);
  }

  @Override
  public int countByPrefixConsistent(String prefix) {
    return index.enabled()
        ? index.countConsistent(prefix)
        : durable.countByPrefixConsistent(prefix);
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return index.enabled() ? index.pageTokenAfterKey(key) : durable.pageTokenAfterKey(key);
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    return index.mutateKeys(
        List.of(key),
        () -> durable.compareAndSet(key, expectedVersion, next),
        won -> {
          if (won) publish(key, next, expectedVersion + 1L);
          else index.refresh(key, durable.getConsistent(key));
        });
  }

  @Override
  public boolean delete(String key) {
    return index.mutateKeys(
        List.of(key),
        () -> durable.delete(key),
        deleted -> {
          if (deleted) index.remove(key);
          else index.refresh(key, durable.getConsistent(key));
        });
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    return index.mutateKeys(
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
    return index.mutateKeys(
        keys,
        () -> durable.compareAndSetBatch(ops),
        won -> {
          if (!won) {
            // Same ordering rule as the winning path: refresh is publish-if-present and
            // remove-if-absent, so resyncing in key order could remove a name before
            // republishing the other one.
            Map<String, Optional<Pointer>> fresh = new LinkedHashMap<>();
            for (String key : keys) fresh.put(key, durable.getConsistent(key));
            fresh.forEach(
                (key, pointer) -> {
                  if (pointer.isPresent()) index.refresh(key, pointer);
                });
            fresh.forEach(
                (key, pointer) -> {
                  if (pointer.isEmpty()) index.refresh(key, pointer);
                });
            return;
          }
          // Every insertion before any removal, whatever order the batch arrived in. Listing
          // paths hold the partition write lock and never see a partial batch, but a caller
          // doing two point lookups can land between them, and absence in the index is
          // authoritative. A rename may show both names to such a caller; it must never show
          // neither. Both switches stay exhaustive so a new CasOp cannot be silently dropped.
          for (CasOp op : ops) {
            switch (op) {
              case CasUpsert upsert ->
                  publish(upsert.key(), upsert.next(), upsert.expectedVersion() + 1L);
              case UnconditionalUpsert upsert ->
                  publish(upsert.key(), upsert.next(), upsert.next().getVersion());
              case CasDelete ignored -> {}
              case CasCheckAbsent ignored -> {}
              case CasCheck ignored -> {}
            }
          }
          // Splitting the passes cannot reorder two ops against each other: only planner-key
          // batches reach the index, and the assemblers that build those reject duplicate keys.
          for (CasOp op : ops) {
            switch (op) {
              case CasDelete delete -> index.remove(delete.key());
              case CasCheckAbsent absent -> index.remove(absent.key());
              case CasUpsert ignored -> {}
              case UnconditionalUpsert ignored -> {}
              case CasCheck ignored -> {}
            }
          }
        });
  }

  @Override
  public int deleteByPrefix(String prefix) {
    return index.mutatePrefix(
        prefix, () -> durable.deleteByPrefix(prefix), ignored -> index.removePrefix(prefix, null));
  }

  @Override
  public int deleteByPrefixExcluding(String prefix, String excludedKey) {
    return index.mutatePrefix(
        prefix,
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
