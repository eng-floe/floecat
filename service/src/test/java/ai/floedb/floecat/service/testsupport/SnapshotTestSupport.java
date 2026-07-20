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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Test helpers for snapshot-related fixtures. */
public final class SnapshotTestSupport {

  private SnapshotTestSupport() {}

  /**
   * A blob-backed CURRENT table pin for {@code (tableId, snapshotId)} — the only pin shape
   * production ever stores (every pin captures immutable blob identity at construction). The
   * synthesized URIs and etags match {@link FakeSnapshotRepository}'s scheme so seeded pins and the
   * fake repositories agree when used together. {@code constraints_version} is UNKNOWN: nothing was
   * authoritatively captured.
   */
  public static TablePin blobBackedPin(ResourceId tableId, long snapshotId) {
    return TablePin.newBuilder()
        .setTableId(tableId)
        .setPinKind(PinKind.PIN_KIND_CURRENT)
        .setSnapshotId(snapshotId)
        .setTableBlobUri("s3://" + tableId.getId() + "/table.pb")
        .setTableBlobVersion("etag-t")
        .setSnapshotBlobUri("s3://" + tableId.getId() + "/snap-" + snapshotId + ".pb")
        .setSnapshotBlobVersion("etag-s" + snapshotId)
        .build();
  }

  /**
   * A blob-backed pin whose manifest entry carried a read-schema fingerprint (see
   * SnapshotManifestEntry.schema_fingerprint). Two pins at different snapshots with the SAME
   * fingerprint model a data-only ingest; different fingerprints model a snapshot-backed schema
   * change. The no-fingerprint {@link #blobBackedPin} models a legacy pre-fingerprint entry.
   */
  public static TablePin blobBackedPin(ResourceId tableId, long snapshotId, String schemaFingerprint) {
    return blobBackedPin(tableId, snapshotId).toBuilder()
        .setSchemaFingerprint(schemaFingerprint)
        .build();
  }

  /** The relation-pin set resolution stores on a query context, from blob-backed pins. */
  public static RelationPinSet relationPins(TablePin... pins) {
    RelationPinSet.Builder set = RelationPinSet.newBuilder();
    for (TablePin pin : pins) {
      set.addPins(QueryPins.ofTable(pin));
    }
    return set.build();
  }

  public static final class FakeSnapshotRepository extends SnapshotRepository {

    private final Map<ResourceId, Map<Long, Snapshot>> snapshots = new HashMap<>();

    public FakeSnapshotRepository() {
      this(new InMemoryPointerStore(), new InMemoryBlobStore());
    }

    private FakeSnapshotRepository(InMemoryPointerStore pointers, InMemoryBlobStore blobs) {
      super(pointers, blobs, new TableRepository(pointers, blobs));
    }

    @Override
    public void create(Snapshot snapshot) {
      put(snapshot.getTableId(), snapshot);
    }

    public void put(ResourceId tableId, Snapshot snapshot) {
      snapshots
          .computeIfAbsent(tableId, ignored -> new HashMap<>())
          .put(snapshot.getSnapshotId(), snapshot);
    }

    @Override
    public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
      return Optional.ofNullable(snapshots.getOrDefault(tableId, Map.of()).get(snapshotId));
    }

    /**
     * Resolve a stored snapshot by the synthesized immutable blob URI ({@code
     * s3://<table>/snap-<id> .pb}), mirroring {@link #metaForSafe}. Lets the pinned-snapshot-blob
     * read paths (schema) hit the fake by URI instead of the live pointer.
     */
    @Override
    public Optional<Snapshot> getByBlobUri(String blobUri) {
      if (blobUri == null || !blobUri.startsWith("s3://") || !blobUri.endsWith(".pb")) {
        return Optional.empty();
      }
      int snapIdx = blobUri.indexOf("/snap-");
      if (snapIdx < 0) {
        return Optional.empty();
      }
      String tableId = blobUri.substring("s3://".length(), snapIdx);
      String snapPart =
          blobUri.substring(snapIdx + "/snap-".length(), blobUri.length() - ".pb".length());
      long snapshotId;
      try {
        snapshotId = Long.parseLong(snapPart);
      } catch (NumberFormatException e) {
        return Optional.empty();
      }
      return snapshots.entrySet().stream()
          .filter(e -> e.getKey().getId().equals(tableId))
          .map(e -> e.getValue().get(snapshotId))
          .filter(java.util.Objects::nonNull)
          .findFirst();
    }

    /**
     * The fake keeps no resident-decode layer, so the cache-bypassing "live" read resolves through
     * the same synthesized-URI lookup: a seeded snapshot's blob is live, everything else is gone.
     */
    @Override
    public Optional<Snapshot> getByBlobUriLive(String blobUri) {
      return getByBlobUri(blobUri);
    }

    // Mirror the real repository's newest-first, id-descending-on-tie ordering (at millisecond
    // granularity) so tests observe the same snapshot selection the production by-time index gives.
    private static final Comparator<Snapshot> NEWEST_FIRST =
        Comparator.comparingLong(FakeSnapshotRepository::createdMillisOf)
            .thenComparingLong(Snapshot::getSnapshotId);

    @Override
    public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
      return snapshots.getOrDefault(tableId, Map.of()).values().stream().max(NEWEST_FIRST);
    }

    @Override
    public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
      long targetMillis = com.google.protobuf.util.Timestamps.toMillis(asOf);
      return snapshots.getOrDefault(tableId, Map.of()).values().stream()
          .filter(s -> createdMillisOf(s) <= targetMillis)
          .max(NEWEST_FIRST);
    }

    /** Synthesize immutable blob identity for a stored snapshot so pin resolution can complete. */
    @Override
    public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
      if (!snapshots.getOrDefault(tableId, Map.of()).containsKey(snapshotId)) {
        return null;
      }
      return MutationMeta.newBuilder()
          .setBlobUri("s3://" + tableId.getId() + "/snap-" + snapshotId + ".pb")
          .setEtag("etag-s" + snapshotId)
          .build();
    }

    /**
     * Report the etag of a synthesized snapshot blob by its URI, mirroring {@link #metaForSafe} so
     * pin validation (which now probes the immutable blob URI rather than the live pointer) sees
     * the same identity. Returns {@code null} for an unknown/unstored blob, exactly like the real
     * HEAD.
     */
    @Override
    public String blobEtag(String blobUri) {
      if (blobUri == null || !blobUri.startsWith("s3://") || !blobUri.endsWith(".pb")) {
        return null;
      }
      int snapIdx = blobUri.indexOf("/snap-");
      if (snapIdx < 0) {
        return null;
      }
      String tableId = blobUri.substring("s3://".length(), snapIdx);
      String snapPart =
          blobUri.substring(snapIdx + "/snap-".length(), blobUri.length() - ".pb".length());
      long snapshotId;
      try {
        snapshotId = Long.parseLong(snapPart);
      } catch (NumberFormatException e) {
        return null;
      }
      boolean present =
          snapshots.entrySet().stream()
              .anyMatch(
                  e -> e.getKey().getId().equals(tableId) && e.getValue().containsKey(snapshotId));
      return present ? "etag-s" + snapshotId : null;
    }

    private static long createdMillisOf(Snapshot snapshot) {
      Timestamp ts = snapshot.getUpstreamCreatedAt();
      return ts.getSeconds() * 1000L + ts.getNanos() / 1_000_000L;
    }
  }
}
