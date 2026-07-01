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

package ai.floedb.floecat.stats.spi;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import com.google.protobuf.Timestamp;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Internal SPI for authoritative stats persistence.
 *
 * <p>This interface is designed for Floecat storage-layer implementations. Custom engine authors
 * should not implement this interface; inject it via CDI when a {@link StatsCaptureEngine}
 * implementation needs to read or write persisted stats.
 */
public interface StatsStore {
  /** Upserts a target stats record. */
  void putTargetStats(TargetStatsRecord value);

  /**
   * Persists a batch of target stats records for one table snapshot.
   *
   * <p>The default implementation preserves legacy semantics by delegating to {@link
   * #putTargetStats(TargetStatsRecord)} record-by-record. Implementations may override this to
   * reuse snapshot-scoped state and reduce storage round-trips.
   */
  default void putTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    for (TargetStatsRecord record : records == null ? List.<TargetStatsRecord>of() : records) {
      putTargetStats(record);
    }
  }

  /**
   * Creates a target stats record only when the exact table/snapshot/target key is absent.
   *
   * <p>Returns {@code true} only when this call created the record. Returns {@code false} when an
   * equal or conflicting record already owns the target key.
   */
  boolean putTargetStatsIfAbsent(TargetStatsRecord value);

  /** Returns the target stats record for the exact table/snapshot/target key, if present. */
  Optional<TargetStatsRecord> getTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target);

  /**
   * Returns a stale target stats record when exact stats for {@code snapshotId} are unavailable.
   *
   * <p>Implementations should prefer the newest available stats at or before {@code snapshotId}.
   * The default returns empty for stores that cannot enumerate snapshot-scoped stats.
   */
  default Optional<TargetStatsRecord> getStaleTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    return Optional.empty();
  }

  /**
   * Batch stale lookup: returns the newest available stats for each target at or before {@code
   * snapshotId}.
   *
   * <p>The default calls {@link #getStaleTargetStats} per target (N independent prefix scans).
   * Implementations that maintain a latest-snapshot index should override this for O(1) lookup.
   */
  default Map<String, Optional<TargetStatsRecord>> getStaleTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }
    Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
    for (StatsTarget target : targets) {
      out.put(
          StatsTargetIdentity.storageId(target), getStaleTargetStats(tableId, snapshotId, target));
    }
    return Collections.unmodifiableMap(out);
  }

  /**
   * Batch read of specific targets for a snapshot.
   *
   * <p>Returns a map from {@link ai.floedb.floecat.stats.identity.StatsTargetIdentity#storageId} to
   * the optional record for that target. Missing entries in the returned map are treated as absent
   * (same as {@code Optional.empty()}).
   *
   * <p>The default implementation makes N sequential single-target calls. Stores with real batch
   * read support (e.g. DynamoDB {@code BatchGetItem}) should override this method for efficiency.
   */
  default Map<String, Optional<TargetStatsRecord>> getTargetStatsBatch(
      ResourceId tableId, long snapshotId, List<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }
    Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
    for (StatsTarget target : targets) {
      out.put(StatsTargetIdentity.storageId(target), getTargetStats(tableId, snapshotId, target));
    }
    return Collections.unmodifiableMap(out);
  }

  /** Deletes the exact table/snapshot/target record and returns true iff a record was deleted. */
  boolean deleteTargetStats(ResourceId tableId, long snapshotId, StatsTarget target);

  /**
   * Lists target stats for a table snapshot with optional target-type filtering.
   *
   * <p>{@code targetType=Optional.empty()} means "all target types". {@code pageToken=""} means
   * first page.
   */
  StatsStorePage listTargetStats(
      ResourceId tableId,
      long snapshotId,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken);

  /**
   * Counts target stats records for a table snapshot with optional target-type filtering.
   *
   * <p>{@code targetType=Optional.empty()} means "all target types".
   */
  int countTargetStats(ResourceId tableId, long snapshotId, Optional<StatsTargetType> targetType);

  /** Deletes all target stats for a table snapshot and returns true iff anything was deleted. */
  boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId);

  /**
   * Replaces all target stats for a table snapshot.
   *
   * <p>Implementations should publish the replacement set atomically when the backing store
   * supports multi-pointer CAS. The default implementation preserves correctness for simple test
   * stores.
   */
  default void replaceAllStatsForSnapshot(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    deleteAllStatsForSnapshot(tableId, snapshotId);
    for (TargetStatsRecord record : records == null ? List.<TargetStatsRecord>of() : records) {
      putTargetStats(record);
    }
  }

  /**
   * Returns mutation metadata for an exact table/snapshot/target key.
   *
   * <p>{@code nowTs} is used to stamp metadata when no record exists yet.
   */
  MutationMeta metaForTargetStats(
      ResourceId tableId, long snapshotId, StatsTarget target, Timestamp nowTs);

  /**
   * Immutable page container for {@link #listTargetStats}.
   *
   * <p>Records are defensively copied; {@code nextPageToken} is normalized to empty-string when
   * null.
   */
  record StatsStorePage(List<TargetStatsRecord> records, String nextPageToken) {
    public StatsStorePage {
      records = records == null ? List.of() : List.copyOf(records);
      nextPageToken = nextPageToken == null ? "" : nextPageToken;
    }
  }
}
