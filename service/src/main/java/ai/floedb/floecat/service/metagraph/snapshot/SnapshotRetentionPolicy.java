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

package ai.floedb.floecat.service.metagraph.snapshot;

import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The one retention policy for every versioned catalog object: snapshots of a table, and table
 * definitions, stats generations, and constraints bundles. A version is kept while it is live or
 * published within retention plus grace. Zero retention keeps every snapshot; unreferenced
 * artifacts still age out after grace.
 */
@ApplicationScoped
public final class SnapshotRetentionPolicy {

  private final Clock clock;
  private final Duration retention;
  private final Duration grace;

  @Inject
  public SnapshotRetentionPolicy(
      @ConfigProperty(name = "floecat.snapshot.retention") Duration retention,
      @ConfigProperty(name = "floecat.snapshot.retention-grace") Duration grace) {
    this(Clock.systemUTC(), retention, grace);
  }

  public SnapshotRetentionPolicy(Clock clock, Duration retention, Duration grace) {
    if (retention.isNegative() || grace.isNegative()) {
      throw new IllegalArgumentException("snapshot retention and grace must be >= 0");
    }
    this.clock = clock;
    this.retention = retention;
    this.grace = grace;
  }

  /** Retention off, for instances built without CDI. */
  public static SnapshotRetentionPolicy disabled() {
    return new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ZERO, Duration.ofDays(7));
  }

  /** Whether historical snapshots expire at all. */
  public boolean isRetentionEnabled() {
    return !retention.isZero();
  }

  /** Age after which an unreachable artifact may be collected. */
  public long retentionAndGraceMillis() {
    return retention.plus(grace).toMillis();
  }

  /** Snapshots kept regardless of age: the committed and queryable current snapshots. */
  public Set<Long> protectedSnapshotIds(
      SnapshotManifests.Chain chain, TableRoot root, boolean gateOnFinalize) {
    var current = SnapshotManifests.currentSnapshots(chain, root, gateOnFinalize);
    Set<Long> ids = new HashSet<>();
    current.committed().ifPresent(e -> ids.add(e.getSnapshotId()));
    current.queryable().ifPresent(e -> ids.add(e.getSnapshotId()));
    return ids;
  }

  /**
   * Publication time of a manifest entry. Entries written before {@code ingested_at} existed fall
   * back to the snapshot pointer, then to the snapshot blob's write time, which is never earlier.
   * Empty only when none is recorded and the blob is gone.
   */
  public static Optional<Timestamp> publishedAt(
      ResourceId tableId, SnapshotManifestEntry entry, PointerStore pointers, BlobStore blobs) {
    if (entry.hasIngestedAt()) {
      return Optional.of(entry.getIngestedAt());
    }
    var pointer =
        pointers.get(
            Keys.snapshotPointerById(
                tableId.getAccountId(), tableId.getId(), entry.getSnapshotId()));
    if (pointer.isPresent() && pointer.get().hasIngestedAt()) {
      return Optional.of(pointer.get().getIngestedAt());
    }
    if (!entry.hasSnapshotRef() || entry.getSnapshotRef().getUri().isBlank()) {
      return Optional.empty();
    }
    return blobs.head(entry.getSnapshotRef().getUri()).map(BlobHeader::getLastModifiedAt);
  }

  /** Whether a new selection may resolve to a snapshot published at {@code publishedAt}. */
  public boolean visible(Timestamp publishedAt) {
    return !isRetentionEnabled()
        || publishedAt != null && !before(publishedAt, clock.instant().minus(retention));
  }

  /**
   * Whether a snapshot published at {@code publishedAt} is past retention plus grace: collectable,
   * and no longer readable by a query that selected it. Unknown publication never expires.
   */
  public boolean expired(Timestamp publishedAt) {
    return isRetentionEnabled()
        && publishedAt != null
        && before(publishedAt, clock.instant().minus(retention).minus(grace));
  }

  /**
   * Whether GC may drop a snapshot retention does not protect: past retention plus grace, or with
   * no publication record and no blob left to date it by.
   */
  public boolean collectable(Optional<Timestamp> publishedAt) {
    return isRetentionEnabled() && publishedAt.map(this::expired).orElse(true);
  }

  private static boolean before(Timestamp publishedAt, Instant cutoff) {
    return Timestamps.toMillis(publishedAt) < cutoff.toEpochMilli();
  }
}
