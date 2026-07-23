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
import java.util.ArrayList;
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
  enum UnpublishedGenerationDeleteResult {
    DELETED,
    NOT_DELETABLE_PUBLISHED,
    RETRYABLE_IN_PROGRESS
  }

  /**
   * Signals that a frozen generation token cannot be resolved before any target-specific read.
   * Callers may handle this once for the whole generation-scoped batch rather than isolating
   * individual targets. Retryable storage failures must retain their retryable exception type.
   */
  final class GenerationUnavailableException extends RuntimeException {
    public GenerationUnavailableException(String message) {
      super(message);
    }

    public GenerationUnavailableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

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

  /**
   * Creates target stats records only when each exact table/snapshot/target key is absent.
   *
   * <p>Returns the records that were actually created. Existing equal or conflicting target keys
   * are skipped.
   */
  default List<TargetStatsRecord> putTargetStatsBatchIfAbsent(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> created = new ArrayList<>();
    for (TargetStatsRecord record : records == null ? List.<TargetStatsRecord>of() : records) {
      if (record == null) {
        continue;
      }
      if (!tableId.equals(record.getTableId()) || record.getSnapshotId() != snapshotId) {
        throw new IllegalArgumentException("target stats do not match table snapshot");
      }
      if (putTargetStatsIfAbsent(record)) {
        created.add(record);
      }
    }
    return List.copyOf(created);
  }

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

  /**
   * Returns the target stats record for the exact table/snapshot/target key within the frozen stats
   * generation named by {@code generationToken}. Stores that do not track generations serve the
   * live exact-snapshot record.
   */
  default Optional<TargetStatsRecord> getTargetStatsInGeneration(
      ResourceId tableId, long snapshotId, String generationToken, StatsTarget target) {
    return getTargetStats(tableId, snapshotId, target);
  }

  /**
   * Batch variant of {@link #getTargetStatsInGeneration}. Tracking stores must read the immutable
   * keyspace named by {@code generationToken}, not the live active generation for the snapshot.
   */
  default Map<String, Optional<TargetStatsRecord>> getTargetStatsBatchInGeneration(
      ResourceId tableId, long snapshotId, String generationToken, List<StatsTarget> targets) {
    if (targets == null || targets.isEmpty()) {
      return Map.of();
    }
    Map<String, Optional<TargetStatsRecord>> out = new LinkedHashMap<>(targets.size());
    for (StatsTarget target : targets) {
      out.put(
          StatsTargetIdentity.storageId(target),
          getTargetStatsInGeneration(tableId, snapshotId, generationToken, target));
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
   * Whether this store tracks stats generations at all. When {@code false}, {@link
   * #activeStatsGeneration} is meaningless (always empty) and readers skip generation checks
   * entirely. When {@code true}, an empty {@link #activeStatsGeneration} result means specifically
   * "this snapshot has no stats generation yet" — a real, comparable state, distinct from "the
   * store cannot say" — so readers must still compare it page to page (a generation appearing
   * mid-pagination is a change like any other).
   */
  default boolean tracksStatsGenerations() {
    return false;
  }

  /**
   * Opaque token identifying the currently-active stats generation for a table snapshot, if the
   * store tracks generations (see {@link #tracksStatsGenerations}). {@link
   * #replaceAllStatsForSnapshot} and {@link #deleteAllStatsForSnapshot} change the token;
   * per-record upserts within a generation do not have to. A multi-page reader captures the token
   * (or its absence) before its first page and compares before consuming each page, so a
   * mid-pagination replacement — or a first generation appearing under a scan that started with
   * none — is detected instead of silently mixing pages from different generations.
   */
  default Optional<String> activeStatsGeneration(ResourceId tableId, long snapshotId) {
    return Optional.empty();
  }

  /**
   * Like {@link #listTargetStats}, but reads within the specific generation named by {@code
   * generationToken} — a token previously returned by {@link #activeStatsGeneration} — instead of
   * resolving the live active generation. A reader that froze its generation at first touch keeps a
   * deterministic view of that immutable keyspace while newer generations publish (superseded
   * generations are retained until GC finds them unreferenced). Stores that do not track
   * generations serve the live state; the token is meaningless to them. A tracking store must fail
   * loudly when the token's generation is gone rather than falling back to the live one.
   */
  default StatsStorePage listTargetStatsInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationToken,
      Optional<StatsTargetType> targetType,
      int limit,
      String pageToken) {
    return listTargetStats(tableId, snapshotId, targetType, limit, pageToken);
  }

  /**
   * Replaces target-scoped records inside an unpublished generation.
   *
   * <p>{@code targetsToReplace} defines the target keys owned by the writer. Implementations delete
   * those target keys from {@code generationId}, then write {@code records} into that same
   * generation. The generation must not become visible to normal readers until {@link
   * #publishStatsGeneration(ResourceId, long, String, List)} succeeds.
   */
  default void replaceTargetStatsInGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<StatsTarget> targetsToReplace,
      List<TargetStatsRecord> records) {
    throw new UnsupportedOperationException("unpublished stats generations are not supported");
  }

  /**
   * Publishes an unpublished generation as the active stats generation for a table snapshot.
   *
   * <p>{@code finalRecords} are written into the generation immediately before publication; callers
   * use this for records produced during finalization, such as aggregate stats.
   */
  default void publishStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<TargetStatsRecord> finalRecords) {
    throw new UnsupportedOperationException("unpublished stats generations are not supported");
  }

  /**
   * Publishes an unpublished generation, optionally enriching it from the active generation.
   *
   * <p>Full rescans pass {@code false}: their output is authoritative and must not read payloads
   * from the generation being replaced. Incremental publication retains the default enrichment
   * behavior.
   */
  default void publishStatsGeneration(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      List<TargetStatsRecord> finalRecords,
      boolean carryForwardSupersededSketches) {
    publishStatsGeneration(tableId, snapshotId, generationId, finalRecords);
  }

  /**
   * Deletes an unpublished stats generation.
   *
   * <p>Implementations must not delete a generation that has already been published as the active
   * generation. {@link UnpublishedGenerationDeleteResult#RETRYABLE_IN_PROGRESS} means the
   * generation is not conclusively published, but a writer may still be publishing it; callers must
   * not treat that result as durable cleanup completion.
   */
  default UnpublishedGenerationDeleteResult deleteUnpublishedStatsGeneration(
      ResourceId tableId, long snapshotId, String generationId) {
    return UnpublishedGenerationDeleteResult.NOT_DELETABLE_PUBLISHED;
  }

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

  /** Replaces a snapshot generation, optionally enriching it from the generation being replaced. */
  default void replaceAllStatsForSnapshot(
      ResourceId tableId,
      long snapshotId,
      List<TargetStatsRecord> records,
      boolean carryForwardSupersededSketches) {
    replaceAllStatsForSnapshot(tableId, snapshotId, records);
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
