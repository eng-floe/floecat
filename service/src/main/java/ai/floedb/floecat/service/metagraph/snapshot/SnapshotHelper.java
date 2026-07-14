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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.TableRoot;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.catalog.impl.StatsVisibilityGate;
import ai.floedb.floecat.service.catalog.impl.TableRootCommitter;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph.SchemaResolution;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Centralized snapshot handling ----------------------------------------- - Snapshot override
 * resolution - AS OF timestamp fallback - Default "CURRENT" snapshot semantics - Schema resolution
 * based on effective snapshot
 */
@ApplicationScoped
public class SnapshotHelper {

  private final SnapshotRepository snapshots;
  private final TableRootRepository roots;
  private final TableRootCommitter rootCommitter;
  private final StatsStore statsStore;

  /**
   * {@code rootCommitter} materializes a legacy table's root at first touch ({@code ensureRoot}),
   * so a pre-existing deployment migrates lazily as its tables are queried. Pins are built from a
   * just-read root blob and validated by every consumption through the {@link PinValidator}
   * contract, so construction performs no extra validation round-trip.
   */
  public SnapshotHelper(
      SnapshotRepository snapshots,
      TableRootRepository roots,
      TableRootCommitter rootCommitter,
      StatsStore statsStore) {
    this.snapshots = snapshots;
    this.roots = roots;
    this.rootCommitter = rootCommitter;
    this.statsStore = statsStore;
  }

  /**
   * Whether snapshots gate visibility on finalize: with a generation-tracking store, an entry
   * without its generation ref is not yet query-ready (its file list, indexes, and stats have not
   * published) and no pin kind may resolve to it. Stores that track no generations cannot express
   * readiness and are exempt.
   */
  private boolean gateOnFinalize() {
    return StatsVisibilityGate.gateOnFinalize(statsStore);
  }

  // ----------------------------------------------------------------------
  // Snapshot pinning
  // ----------------------------------------------------------------------

  /**
   * Build the coherent {@link TablePin} for one table by resolving through the table's immutable
   * root — the single object that names the definition blob, every snapshot's blob identity, and
   * the current-snapshot selection. This is the pin the query context stores and downstream reads
   * reuse.
   *
   * <ul>
   *   <li>CURRENT pins the root's {@code current_snapshot_id}; a table with no current snapshot is
   *       NOT_FOUND.
   *   <li>Explicit {@code snapshot_id} resolves against the root's snapshot manifest (user error
   *       when the snapshot is unknown).
   *   <li>{@code AS_OF} resolves once to the manifest entry with the greatest {@code
   *       upstream_created_at} at or before the timestamp (snapshot id breaks ties, matching the
   *       advance rule); the original timestamp is kept only as provenance.
   * </ul>
   *
   * <p>Because every leg of the pin is a ref out of one immutable root, the pinned (definition,
   * snapshot, stats, constraints) state is coherent by construction — there is no cross-pointer
   * pair to keep in step. A legacy table's root is synthesized at first touch ({@code ensureRoot}),
   * so pre-existing data needs no migration step.
   */
  public TablePin tablePinFor(
      String cid, ResourceId tableId, SnapshotRef override, Optional<Timestamp> asOfDefault) {

    // A SPECIAL selector other than SS_CURRENT (e.g. an as-yet-unimplemented "oldest"/"first") has
    // no defined pin resolution. Reject it rather than silently pinning CURRENT, which would hide
    // the unsupported request behind a plausible-looking result. SS_CURRENT falls through to the
    // CURRENT path below.
    if (override != null
        && override.getWhichCase() == SnapshotRef.WhichCase.SPECIAL
        && override.getSpecial() != SpecialSnapshot.SS_CURRENT) {
      throw GrpcErrors.invalidArgument(
          cid, SNAPSHOT_SPECIAL_UNSUPPORTED, Map.of("requested", override.getSpecial().name()));
    }

    // Steady state is one pointer read + one blob read: the root pointer names the blob, and the
    // blob at that URI is immutable. Only a table with no root yet (legacy, un-migrated) pays the
    // lazy synthesis, once.
    MutationMeta rootMeta = roots.metaForSafe(tableId);
    if (rootMeta == null || rootMeta.getBlobUri().isEmpty()) {
      rootCommitter.ensureRoot(tableId);
      rootMeta = roots.metaForSafe(tableId);
    }
    TableRoot root = loadRoot(rootMeta);
    if (root == null && rootMeta != null && !rootMeta.getBlobUri().isEmpty()) {
      // The root was superseded and its blob swept between the pointer read and the blob read —
      // the pointer has necessarily moved on, so follow it once instead of failing a live table.
      // The retry MUST bypass the TTL pointer cache (which would hand back the same dead URI for
      // the rest of the TTL); metaForSafeLive also invalidates the stale entry as a side effect.
      // Still null after the retry means the re-read pointer is also unreadable (corruption, or a
      // pathological second race): fall through to the per-pin-kind not-found handling below.
      rootMeta = roots.metaForSafeLive(tableId);
      root = loadRoot(rootMeta);
    }

    if (override != null && override.hasSnapshotId()) {
      long snapshotId = override.getSnapshotId();
      SnapshotManifestEntry entry =
          (root == null
                  ? Optional.<SnapshotManifestEntry>empty()
                  : SnapshotManifests.findEntry(roots, manifestHead(root), snapshotId))
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          SNAPSHOT,
                          Map.of(
                              "table_id", tableId.getId(),
                              "snapshot_id", Long.toString(snapshotId))));
      if (gateOnFinalize() && !entry.hasStatsGenerationRef()) {
        // Registered but not yet finalized: its file list, indexes, and stats have not published,
        // so no pin may resolve to it — the snapshot exists, it is just not query-ready yet.
        throw GrpcErrors.preconditionFailed(
            cid,
            QUERY_SNAPSHOT_NOT_FINALIZED,
            Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
      }
      return pinFromEntry(cid, tableId, PinKind.PIN_KIND_SNAPSHOT_ID, entry, root, rootMeta, null);
    }

    Timestamp asOf =
        (override != null && override.hasAsOf()) ? override.getAsOf() : asOfDefault.orElse(null);
    if (asOf != null) {
      SnapshotManifestEntry entry =
          (root == null ? Optional.<SnapshotManifestEntry>empty() : entryAsOf(root, asOf))
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          QUERY_SNAPSHOT_NOT_FOUND_AT_TIME,
                          Map.of(
                              "table_id", tableId.getId(),
                              "as_of",
                                  Instant.ofEpochSecond(asOf.getSeconds(), asOf.getNanos())
                                      .toString())));
      return pinFromEntry(cid, tableId, PinKind.PIN_KIND_AS_OF, entry, root, rootMeta, asOf);
    }

    // CURRENT: the root's current_snapshot_id is the authoritative selection. A table with no
    // current snapshot (freshly created with no data yet, or its current snapshot was deleted) is
    // an expected, client-reachable state — NOT_FOUND.
    if (root == null || !root.hasCurrentSnapshotId()) {
      throw GrpcErrors.notFound(cid, SNAPSHOT, Map.of("table_id", tableId.getId()));
    }
    long currentId = root.getCurrentSnapshotId();
    SnapshotManifestEntry entry =
        SnapshotManifests.findEntry(roots, manifestHead(root), currentId)
            .orElseThrow(
                // Currency pointing at a snapshot the manifest does not carry is a broken root
                // invariant (removal clears currency in the same commit), never a client state.
                () ->
                    GrpcErrors.internal(
                        cid,
                        QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
                        Map.of(
                            "table_id", tableId.getId(),
                            "snapshot_id", Long.toString(currentId))));
    if (gateOnFinalize() && !entry.hasStatsGenerationRef()) {
      // Committed currency can move before the generation publishes. Rather than reporting no
      // current for the whole append->finalize window, pin the newest FINALIZED snapshot at or
      // before the committed current — snapshot isolation on the latest fully-queryable state,
      // consistent with metadata surfaces (getCommittedCurrent*) still exposing the committed id.
      // NOT_FOUND only when nothing at or before it is finalized yet (pre-first-finalize).
      entry =
          SnapshotManifests.latestQueryableCurrent(roots, manifestHead(root), entry)
              .orElseThrow(
                  () -> GrpcErrors.notFound(cid, SNAPSHOT, Map.of("table_id", tableId.getId())));
    }
    return pinFromEntry(cid, tableId, PinKind.PIN_KIND_CURRENT, entry, root, rootMeta, null);
  }

  private TableRoot loadRoot(MutationMeta rootMeta) {
    return (rootMeta == null || rootMeta.getBlobUri().isEmpty())
        ? null
        : roots.getByBlobUri(rootMeta.getBlobUri()).orElse(null);
  }

  /**
   * The manifest entry AS OF {@code asOf}: greatest {@code upstream_created_at} at or before the
   * timestamp, snapshot id breaking ties — the same ordering the advance rule applies. The manifest
   * walk is bounded by the table's history and happens once per query pin.
   */
  private Optional<SnapshotManifestEntry> entryAsOf(TableRoot root, Timestamp asOf) {
    long asOfMs = Timestamps.toMillis(asOf);
    boolean gate = gateOnFinalize();
    SnapshotManifestEntry[] best = new SnapshotManifestEntry[1];
    SnapshotManifests.forEachEntry(
        roots,
        manifestHead(root),
        entry -> {
          if (!entry.hasUpstreamCreatedAt()
              || Timestamps.toMillis(entry.getUpstreamCreatedAt()) > asOfMs) {
            return;
          }
          if (gate && !entry.hasStatsGenerationRef()) {
            return; // not yet finalized: invisible to AS_OF like every other pin kind
          }
          if (best[0] == null || SnapshotManifests.newer(entry, best[0])) {
            best[0] = entry;
          }
        });
    return Optional.ofNullable(best[0]);
  }

  private static ai.floedb.floecat.catalog.rpc.BlobRef manifestHead(TableRoot root) {
    return root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null;
  }

  /**
   * Materialize the pin from a resolved manifest entry: every leg — the root identity, the pinned
   * definition blob, and the snapshot blob — comes from the one immutable root, so the pin is
   * coherent by construction. A root without a definition ref cannot serve table-scoped reads and
   * fails as a catalog-integrity error rather than walking to the live pointer. No validation HEAD
   * happens here: every leg was just read out of the root blob at exactly the pinned version, and
   * every consumption re-validates through the {@link PinValidator} contract.
   */
  private TablePin pinFromEntry(
      String cid,
      ResourceId tableId,
      PinKind pinKind,
      SnapshotManifestEntry entry,
      TableRoot root,
      MutationMeta rootMeta,
      Timestamp originalAsOf) {
    if (!root.hasDefinitionRef() || root.getDefinitionRef().getUri().isEmpty()) {
      throw GrpcErrors.internal(
          cid, QUERY_PINNED_TABLE_BLOB_MISSING, Map.of("table_id", tableId.getId()));
    }
    if (!entry.hasSnapshotRef() || entry.getSnapshotRef().getUri().isEmpty()) {
      // Every writer records a snapshot ref with the entry; its absence is a broken root
      // invariant. Failing here names the real problem instead of pinning an empty URI that a
      // downstream requirePinnedSnapshotBlob would report as a generic internal error.
      throw GrpcErrors.internal(
          cid,
          QUERY_PINNED_SNAPSHOT_BLOB_MISSING,
          Map.of(
              "table_id", tableId.getId(),
              "snapshot_id", Long.toString(entry.getSnapshotId())));
    }
    TablePin.Builder pin =
        TablePin.newBuilder()
            .setTableId(tableId)
            .setPinKind(pinKind)
            .setSnapshotId(entry.getSnapshotId())
            .setRootUri(rootMeta.getBlobUri())
            .setRootVersion(rootMeta.getEtag())
            .setTableBlobUri(root.getDefinitionRef().getUri())
            .setTableBlobVersion(root.getDefinitionRef().getVersion())
            .setSnapshotBlobUri(entry.getSnapshotRef().getUri())
            .setSnapshotBlobVersion(entry.getSnapshotRef().getVersion());
    if (entry.hasConstraintsRef()) {
      pin.setConstraintsRefUri(entry.getConstraintsRef().getUri())
          .setConstraintsRefVersion(entry.getConstraintsRef().getVersion());
    }
    if (entry.hasStatsGenerationRef()) {
      // Freeze the generation the pinned root referenced so the scan's file list matches the pin,
      // not whatever a later finalize made live between BeginQuery and InitScan. Under the gate a
      // pinnable snapshot is always finalized, so this ref is always present here.
      pin.setStatsGenerationRefUri(entry.getStatsGenerationRef().getUri());
    }
    if (originalAsOf != null) {
      pin.setOriginalAsOf(originalAsOf);
    }
    return pin.build();
  }

  // ----------------------------------------------------------------------
  // Schema resolution
  // ----------------------------------------------------------------------

  public SchemaResolution schemaFor(
      String cid,
      UserTableNode tbl,
      SnapshotRef ref,
      java.util.function.Supplier<String> supplier) {

    return new SchemaResolution(tbl, schemaJsonFor(cid, tbl, ref, supplier));
  }

  public String schemaJsonFor(
      String cid,
      UserTableNode tbl,
      SnapshotRef ref,
      java.util.function.Supplier<String> supplier) {
    return schemaJsonFor(cid, tbl, ref, "", supplier);
  }

  /**
   * Schema JSON for a table, preferring the pinned snapshot blob. When {@code snapshotBlobUri}
   * names a pinned snapshot blob, the schema is read from that immutable, content-addressed blob
   * directly ({@code getByBlobUri}) — never re-hydrated through {@link #resolveSnapshot}, which
   * reads the live {@code (table, snapshot id)} pointer that an in-place {@code UpdateSnapshot} can
   * repoint to a new blob after the pin was validated. Empty uri keeps the legacy behaviour of
   * resolving the reference against the live pointer.
   */
  public String schemaJsonFor(
      String cid,
      UserTableNode tbl,
      SnapshotRef ref,
      String snapshotBlobUri,
      java.util.function.Supplier<String> supplier) {

    if (snapshotBlobUri != null && !snapshotBlobUri.isEmpty()) {
      Snapshot snap =
          PinValidator.requirePinnedSnapshotBlob(
              // LIVE: this read's emptiness is the pin-integrity detector (requirePinned*); a
              // still-resident decode must not mask a swept pinned blob.
              snapshots.getByBlobUriLive(snapshotBlobUri), cid, tbl.id());
      return snap.getSchemaJson().isBlank() ? supplier.get() : snap.getSchemaJson();
    }

    if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      return supplier.get();
    }

    Snapshot snap = resolveSnapshot(cid, tbl.id(), ref);

    if (snap == null || snap.getSchemaJson().isBlank()) {
      return supplier.get();
    }

    return snap.getSchemaJson();
  }

  private Snapshot resolveSnapshot(String cid, ResourceId tableId, SnapshotRef ref) {

    return switch (ref.getWhichCase()) {
      case SNAPSHOT_ID ->
          snapshots
              .getById(tableId, ref.getSnapshotId())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          SNAPSHOT,
                          Map.of(
                              "table_id", tableId.getId(),
                              "snapshot_id", Long.toString(ref.getSnapshotId()))));

      case AS_OF ->
          snapshots
              .getAsOf(tableId, ref.getAsOf())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          cid,
                          SNAPSHOT,
                          Map.of(
                              "table_id", tableId.getId(),
                              "as_of",
                                  Instant.ofEpochSecond(
                                          ref.getAsOf().getSeconds(), ref.getAsOf().getNanos())
                                      .toString())));

      case SPECIAL -> {
        if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(
              cid, SNAPSHOT_SPECIAL_UNSUPPORTED, Map.of("requested", ref.getSpecial().name()));
        }

        yield currentSnapshot(cid, tableId);
      }

      case WHICH_NOT_SET ->
          throw GrpcErrors.invalidArgument(
              cid, SNAPSHOT_MISSING, Map.of("table_id", tableId.getId()));

      default ->
          throw GrpcErrors.invalidArgument(
              cid, SNAPSHOT_MISSING, Map.of("table_id", tableId.getId()));
    };
  }

  private Snapshot currentSnapshot(String cid, ResourceId tableId) {
    return snapshots
        .getCurrentSnapshot(tableId)
        .orElseThrow(() -> GrpcErrors.notFound(cid, SNAPSHOT, Map.of("table_id", tableId.getId())));
  }
}
