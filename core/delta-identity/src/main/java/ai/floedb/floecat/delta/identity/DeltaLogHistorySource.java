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

package ai.floedb.floecat.delta.identity;

import ai.floedb.floecat.schema.identity.HistoryTier;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Delta transaction-log implementation of {@link DeltaHistorySource}. */
public final class DeltaLogHistorySource implements DeltaHistorySource {
  private final String tableName;
  private final String tablePath;
  private final Table table;
  private final Engine engine;
  private final long headVersion;
  private final long earliestRecreatableVersion;
  private final Map<Long, Optional<Snapshot>> snapshots = new ConcurrentHashMap<>();

  public DeltaLogHistorySource(String tableName, String tablePath, Table table, Engine engine) {
    this.tableName = tableName;
    this.tablePath = tablePath;
    this.table = table;
    this.engine = engine;
    Snapshot latest = KernelAdapter.latestSnapshot(table, engine);
    this.headVersion = latest == null ? -1L : latest.getVersion();
    this.earliestRecreatableVersion =
        latest == null ? 0L : KernelAdapter.earliestRecreatableCommit(engine, tablePath);
  }

  @Override
  public String tableName() {
    return tableName;
  }

  @Override
  public long headVersion() {
    return headVersion;
  }

  @Override
  public HistoryTier historyTier() {
    return HistoryTier.VERSIONED_LOG;
  }

  @Override
  public Optional<Snapshot> snapshot(long version) {
    if (version < earliestRecreatableVersion) {
      return Optional.empty();
    }
    return snapshots.computeIfAbsent(
        version,
        requested ->
            Optional.ofNullable(
                KernelAdapter.loadExactSnapshot(table, engine, tablePath, requested)));
  }

  @Override
  public MetadataHistory metadataVersions(long firstVersion, long lastVersion) {
    if (firstVersion > lastVersion) {
      return MetadataHistory.complete(java.util.List.of());
    }
    if (firstVersion < earliestRecreatableVersion) {
      return MetadataHistory.gap("history-before-earliest-recreatable-version");
    }
    long commitCount = lastVersion - firstVersion + 1L;
    if (commitCount <= 0L || commitCount > MAX_IDENTITY_HISTORY_COMMITS) {
      return MetadataHistory.gap("history-bound-exceeded");
    }
    try {
      return MetadataHistory.complete(
          KernelAdapter.metadataVersions(engine, tablePath, firstVersion, lastVersion));
    } catch (Exception unavailable) {
      return MetadataHistory.gap("history-unavailable: " + unavailable.getMessage());
    }
  }
}
