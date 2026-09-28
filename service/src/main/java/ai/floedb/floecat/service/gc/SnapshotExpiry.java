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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.StatsVisibilityGate;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

/**
 * Enforces snapshot retention by dropping expired snapshots from their table, as DeleteSnapshot
 * does: their pointers go first, then their root entries. Everything the table no longer references
 * is then ordinary garbage for the reachability collectors.
 */
@ApplicationScoped
public class SnapshotExpiry {

  private static final Logger LOG = Logger.getLogger(SnapshotExpiry.class);

  /** Bounds one pass's pointer transactions and manifest rewrite per table. */
  static final int MAX_EXPIRED_PER_PASS = 500;

  private final PointerStore pointers;
  private final TableRootRepository roots;
  private final SnapshotRepository snapshots;
  private final TableRootWriter rootWriter;
  private final StatsStore statsStore;
  private final SnapshotRetentionPolicy retention;
  private final Map<String, String> resumeTokens = new ConcurrentHashMap<>();
  // Package-private so tests can page one table at a time.
  int tablePageSize = 200;

  @Inject
  public SnapshotExpiry(
      PointerStore pointers,
      TableRootRepository roots,
      SnapshotRepository snapshots,
      TableRootWriter rootWriter,
      StatsStore statsStore,
      SnapshotRetentionPolicy retention) {
    this.pointers = pointers;
    this.roots = roots;
    this.snapshots = snapshots;
    this.rootWriter = rootWriter;
    this.statsStore = statsStore;
    this.retention = retention;
  }

  /**
   * Drops the expired snapshots of every table in the account; returns how many. A pass cut short
   * by the deadline resumes at the same table page next time.
   */
  public int expireAccount(String accountId, long deadlineMs) {
    if (!retention.isRetentionEnabled()) {
      return 0;
    }
    String prefix = Keys.tablePointerByIdPrefix(accountId);
    String token = resumeTokens.getOrDefault(accountId, "");
    int expired = 0;
    while (true) {
      if (System.currentTimeMillis() >= deadlineMs) {
        resumeTokens.put(accountId, token);
        return expired;
      }
      StringBuilder next = new StringBuilder();
      for (Pointer table : pointers.listPointersByPrefix(prefix, tablePageSize, token, next)) {
        if (System.currentTimeMillis() >= deadlineMs) {
          resumeTokens.put(accountId, token);
          return expired;
        }
        String tableId = Keys.idAfterPrefix(prefix, table.getKey());
        if (tableId != null) {
          expired += expireSafely(accountId, tableId, deadlineMs);
        }
        // Persist progress after every table, not only after the store page. A single slow page
        // must not make the scheduler revisit its first table forever.
        token = pointers.pageTokenAfterKey(table.getKey());
      }
      token = next.toString();
      if (token.isEmpty()) {
        resumeTokens.remove(accountId);
        return expired;
      }
    }
  }

  /** Drops continuation state for accounts no longer present in the ownership listing. */
  void pruneAccounts(Set<String> activeAccountIds) {
    resumeTokens.keySet().removeIf(accountId -> !activeAccountIds.contains(accountId));
  }

  private int expireSafely(String accountId, String tableId, long deadlineMs) {
    try {
      return expire(
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(tableId)
              .setKind(ResourceKind.RK_TABLE)
              .build(),
          deadlineMs);
    } catch (RuntimeException e) {
      // One unreadable table must not keep the account's other tables from expiring.
      LOG.warnf(e, "snapshot expiry skipped table %s of account %s", tableId, accountId);
      return 0;
    }
  }

  /**
   * Drops up to {@link #MAX_EXPIRED_PER_PASS} of the table's snapshots retention no longer keeps
   * and returns how many; the rest, and entries left unjudged at the deadline, wait for the next
   * pass.
   */
  public int expire(ResourceId tableId, long deadlineMs) {
    if (!retention.isRetentionEnabled()) {
      return 0;
    }
    var rootMeta = roots.pointerMetaForSafe(tableId);
    TableRoot root =
        rootMeta.getBlobUri().isBlank()
            ? null
            : roots.getByBlobUri(rootMeta.getBlobUri()).orElse(null);
    if (root == null) {
      return 0;
    }
    var chain = SnapshotManifests.chain(roots, null, root.getSnapshotManifestRef());
    Set<Long> protectedIds =
        retention.protectedSnapshotIds(chain, root, StatsVisibilityGate.gateOnFinalize(statsStore));
    Set<Long> expired = new LinkedHashSet<>();
    chain.forEachEntryWhile(
        entry -> {
          if (expired.size() >= MAX_EXPIRED_PER_PASS || System.currentTimeMillis() >= deadlineMs) {
            return false;
          }
          if (entry.hasSnapshotRef()
              && !protectedIds.contains(entry.getSnapshotId())
              && retention.collectable(snapshots.publishedAt(tableId, entry))) {
            expired.add(entry.getSnapshotId());
          }
          return true;
        });
    Set<Long> released = new LinkedHashSet<>();
    try {
      for (long snapshotId : expired) {
        if (System.currentTimeMillis() >= deadlineMs) {
          break;
        }
        if (snapshots.deleteUnlessCurrent(tableId, snapshotId)) {
          released.add(snapshotId);
        }
      }
    } finally {
      // Keep the root manifest converged even when a later deletion fails or the deadline is hit.
      if (!released.isEmpty()) {
        rootWriter.removeSnapshots(tableId, released);
      }
    }
    return released.size();
  }
}
