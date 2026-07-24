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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.annotation.Priority;
import jakarta.decorator.Decorator;
import jakarta.decorator.Delegate;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Adds durable, per-account storage counters to successful pointer mutations.
 *
 * <p>The counter is itself an opaque pointer row and is updated through the undecorated delegate,
 * so accounting never recurses. Counter rows are deliberately excluded from their own totals.
 */
@Decorator
@Priority(10)
public abstract class StorageAccountingPointerStore implements PointerStore {
  private static final Logger LOG = Logger.getLogger(StorageAccountingPointerStore.class);
  private static final String USAGE_SUFFIX = "/metrics/storage-usage";
  private static final String PAYLOAD_VERSION = "v1";
  private static final int CAS_MAX = 32;

  @Inject @Delegate PointerStore delegate;
  @Inject BlobStore blobStore;

  public record AccountUsage(long pointers, long bytes) {
    public AccountUsage {
      pointers = Math.max(0L, pointers);
      bytes = Math.max(0L, bytes);
    }
  }

  @Override
  public Optional<Pointer> get(String key) {
    return delegate.get(key);
  }

  @Override
  public boolean compareAndSet(String key, long expectedVersion, Pointer next) {
    Pointer before = expectedVersion == 0L ? null : delegate.get(key).orElse(null);
    Pointer enriched = enrichSize(next);
    boolean changed = delegate.compareAndSet(key, expectedVersion, enriched);
    if (changed) {
      adjustForMutation(key, before, enriched);
    }
    return changed;
  }

  @Override
  public boolean delete(String key) {
    Pointer before = delegate.get(key).orElse(null);
    boolean changed = delegate.delete(key);
    if (changed) {
      adjustForMutation(key, before, null);
    }
    return changed;
  }

  @Override
  public boolean compareAndDelete(String key, long expectedVersion) {
    Pointer before = delegate.get(key).orElse(null);
    boolean changed = delegate.compareAndDelete(key, expectedVersion);
    if (changed) {
      adjustForMutation(key, before, null);
    }
    return changed;
  }

  @Override
  public boolean compareAndSetBatch(List<CasOp> ops) {
    if (ops == null || ops.isEmpty()) {
      return true;
    }
    List<CasOp> enriched = new ArrayList<>(ops.size());
    Map<String, Pointer> beforeByKey = new LinkedHashMap<>();
    for (CasOp op : ops) {
      if (op instanceof CasUpsert upsert) {
        if (upsert.expectedVersion() > 0L) {
          beforeByKey.put(upsert.key(), delegate.get(upsert.key()).orElse(null));
        }
        enriched.add(
            new CasUpsert(upsert.key(), upsert.expectedVersion(), enrichSize(upsert.next())));
      } else if (op instanceof UnconditionalUpsert upsert) {
        beforeByKey.put(upsert.key(), delegate.get(upsert.key()).orElse(null));
        enriched.add(new UnconditionalUpsert(upsert.key(), enrichSize(upsert.next())));
      } else if (op instanceof CasDelete delete) {
        beforeByKey.put(delete.key(), delegate.get(delete.key()).orElse(null));
        enriched.add(op);
      } else {
        enriched.add(op);
      }
    }
    boolean changed = delegate.compareAndSetBatch(enriched);
    if (!changed) {
      return false;
    }
    Map<String, long[]> deltas = new LinkedHashMap<>();
    for (CasOp op : enriched) {
      if (op instanceof CasUpsert upsert) {
        collectDelta(deltas, upsert.key(), beforeByKey.get(upsert.key()), upsert.next());
      } else if (op instanceof UnconditionalUpsert upsert) {
        collectDelta(deltas, upsert.key(), beforeByKey.get(upsert.key()), upsert.next());
      } else if (op instanceof CasDelete delete) {
        collectDelta(deltas, delete.key(), beforeByKey.get(delete.key()), null);
      }
    }
    applyDeltas(deltas);
    return true;
  }

  @Override
  public List<Pointer> listPointersByPrefix(
      String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    return delegate.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
  }

  @Override
  public String pageTokenAfterKey(String key) {
    return delegate.pageTokenAfterKey(key);
  }

  @Override
  public int deleteByPrefix(String prefix) {
    Map<String, long[]> deltas = new LinkedHashMap<>();
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      List<Pointer> page = delegate.listPointersByPrefix(prefix, 500, token, next);
      for (Pointer pointer : page) {
        collectDelta(deltas, pointer.getKey(), pointer, null);
      }
      token = next.toString();
    } while (!token.isBlank());
    int deleted = delegate.deleteByPrefix(prefix);
    if (deleted > 0) {
      applyDeltas(deltas);
    }
    return deleted;
  }

  @Override
  public int countByPrefix(String prefix) {
    return delegate.countByPrefix(prefix);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public void dump(String header) {
    delegate.dump(header);
  }

  private Pointer enrichSize(Pointer pointer) {
    if (pointer == null || pointer.hasReferencedObjectSizeBytes()) {
      return pointer;
    }
    if (!PointerReferences.isBlobPointer(pointer) || pointer.getBlobUri().isBlank()) {
      return pointer.toBuilder().setReferencedObjectSizeBytes(0L).build();
    }
    try {
      return blobStore
          .head(pointer.getBlobUri())
          .filter(header -> header.getContentLength() >= 0L)
          .map(
              header ->
                  pointer.toBuilder()
                      .setReferencedObjectSizeBytes(header.getContentLength())
                      .build())
          .orElseGet(() -> pointer.toBuilder().setReferencedObjectSizeBytes(0L).build());
    } catch (RuntimeException e) {
      LOG.warnf(
          e, "Failed to record object size for storage accounting uri=%s", pointer.getBlobUri());
      return pointer.toBuilder().setReferencedObjectSizeBytes(0L).build();
    }
  }

  private void adjustForMutation(String key, Pointer before, Pointer after) {
    Map<String, long[]> deltas = new LinkedHashMap<>();
    collectDelta(deltas, key, before, after);
    applyDeltas(deltas);
  }

  private static void collectDelta(
      Map<String, long[]> deltas, String key, Pointer before, Pointer after) {
    String usageKey = usageKeyFor(key);
    if (usageKey == null) {
      return;
    }
    long pointerDelta = (isAccounted(before) ? -1L : 0L) + (isAccounted(after) ? 1L : 0L);
    long byteDelta = accountedBytes(after) - accountedBytes(before);
    if (pointerDelta == 0L && byteDelta == 0L) {
      return;
    }
    long[] delta = deltas.computeIfAbsent(usageKey, ignored -> new long[2]);
    delta[0] += pointerDelta;
    delta[1] += byteDelta;
  }

  private void applyDeltas(Map<String, long[]> deltas) {
    deltas.forEach((key, delta) -> adjustUsage(key, delta[0], delta[1]));
  }

  private void adjustUsage(String usageKey, long pointerDelta, long byteDelta) {
    for (int attempt = 0; attempt < CAS_MAX; attempt++) {
      Pointer current = delegate.get(usageKey).orElse(null);
      AccountUsage usage = decodeUsage(current);
      AccountUsage nextUsage =
          new AccountUsage(usage.pointers() + pointerDelta, usage.bytes() + byteDelta);
      long expectedVersion = current == null ? 0L : current.getVersion();
      Pointer next =
          PointerReferences.opaqueMarkerPointer(
              usageKey, encodeUsage(nextUsage), expectedVersion + 1L);
      if (delegate.compareAndSet(usageKey, expectedVersion, next)) {
        return;
      }
    }
    LOG.errorf(
        "Failed to update storage usage counter key=%s pointerDelta=%d byteDelta=%d",
        usageKey, Long.valueOf(pointerDelta), Long.valueOf(byteDelta));
  }

  public static AccountUsage decodeUsage(Pointer pointer) {
    if (pointer == null || !PointerReferences.isOpaqueMarkerPointer(pointer)) {
      return new AccountUsage(0L, 0L);
    }
    String[] fields = pointer.getBlobUri().split("\\n", -1);
    if (fields.length != 3 || !PAYLOAD_VERSION.equals(fields[0])) {
      return new AccountUsage(0L, 0L);
    }
    try {
      return new AccountUsage(Long.parseLong(fields[1]), Long.parseLong(fields[2]));
    } catch (NumberFormatException ignored) {
      return new AccountUsage(0L, 0L);
    }
  }

  public static String encodeUsage(AccountUsage usage) {
    AccountUsage value = usage == null ? new AccountUsage(0L, 0L) : usage;
    return PAYLOAD_VERSION + "\n" + value.pointers() + "\n" + value.bytes();
  }

  static String usageKeyFor(String pointerKey) {
    if (pointerKey == null
        || !pointerKey.startsWith("/accounts/")
        || pointerKey.startsWith("/accounts/by-id/")
        || pointerKey.startsWith("/accounts/by-name/")
        || pointerKey.endsWith(USAGE_SUFFIX)) {
      return null;
    }
    int accountEnd = pointerKey.indexOf('/', "/accounts/".length());
    if (accountEnd < 0) {
      return null;
    }
    return pointerKey.substring(0, accountEnd) + USAGE_SUFFIX;
  }

  private static long accountedBytes(Pointer pointer) {
    return pointer == null || !PointerReferences.isBlobPointer(pointer)
        ? 0L
        : Math.max(0L, pointer.getReferencedObjectSizeBytes());
  }

  private static boolean isAccounted(Pointer pointer) {
    return pointer != null && pointer.hasReferencedObjectSizeBytes();
  }
}
