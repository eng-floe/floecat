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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestPage;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Operations over the snapshot-manifest page chain (newest entries first, pages chained oldward via
 * {@code prev_page_ref}). Pages are immutable and content-addressed: a mutation rewrites the
 * touched page and re-chains every page newer than it (their prev refs change), while older pages
 * are shared structurally between roots. All writes go through {@link
 * TableRootRepository#putManifestPage}, so identical content converges on identical URIs and
 * retried mutators are idempotent.
 *
 * <p>A {@link Chain} bundles one {@code (roots, tableId, head)} walk context and caches every page
 * it touches — pages are content-addressed, so a cached page can never be stale. A mutator that
 * finds, upserts, and re-checks currency against the same head reads each page blob at most once
 * instead of once per operation. The static methods are one-shot conveniences over a fresh chain.
 */
public final class SnapshotManifests {

  /** Bounds a page so the common reads (current, recent AS_OF) touch one blob. */
  public static final int PAGE_ENTRY_BOUND = 256;

  /**
   * Sentinel fingerprint for a snapshot whose schema_json is blank: the read schema is then the
   * table definition's default, already covered by the root's definition ref, so every such
   * snapshot shares one constant fingerprint. Distinct from "" (a legacy entry written before the
   * field existed), which readers treat as unknown and fall back on.
   */
  public static final String SCHEMA_FINGERPRINT_DEFINITION_DEFAULT = "definition-default";

  private SnapshotManifests() {}

  private static final ObjectMapper FINGERPRINT_JSON = new ObjectMapper();

  /**
   * Content fingerprint of a snapshot's READ SCHEMA, stamped on its manifest entry at write time
   * (see {@code SnapshotManifestEntry.schema_fingerprint}). Two snapshots with the same logical
   * read schema share a fingerprint, so a data-only ingest does not move it; a snapshot-backed
   * schema change does.
   *
   * <p>The fingerprint is over a CANONICALIZED form of schema_json, not the raw bytes: the value is
   * stored verbatim as the ingesting client supplied it, and a client that re-serializes an
   * unchanged schema with different object-key order or whitespace on each ingest would otherwise
   * churn the fingerprint every ingest — silently degrading the warm path to cold-on-every-ingest
   * (never stale, just never warm). Canonicalization sorts object keys and drops insignificant
   * whitespace while PRESERVING array order (column order is semantic). schema_json that does not
   * parse as JSON falls back to the raw string — still never stale, just not churn-resilient for
   * that input.
   */
  public static String schemaFingerprint(ai.floedb.floecat.catalog.rpc.Snapshot snapshot) {
    String schemaJson = snapshot.getSchemaJson();
    if (schemaJson.isBlank()) {
      return SCHEMA_FINGERPRINT_DEFINITION_DEFAULT;
    }
    return ai.floedb.floecat.types.Hashing.sha256Hex(canonicalizeSchemaJson(schemaJson));
  }

  /**
   * Re-serialize JSON with object keys sorted and array order preserved, so a logically-identical
   * schema hashes the same regardless of the producer's key ordering or whitespace. Scalars are
   * left exactly as parsed (no number/string reformatting). Non-JSON input returns unchanged.
   */
  private static String canonicalizeSchemaJson(String json) {
    try {
      return FINGERPRINT_JSON.writeValueAsString(canonicalize(FINGERPRINT_JSON.readTree(json)));
    } catch (JacksonException e) {
      return json;
    }
  }

  private static JsonNode canonicalize(JsonNode node) {
    if (node.isObject()) {
      ObjectNode sorted = FINGERPRINT_JSON.createObjectNode();
      TreeSet<String> names = new TreeSet<>();
      node.fieldNames().forEachRemaining(names::add);
      for (String name : names) {
        sorted.set(name, canonicalize(node.get(name)));
      }
      return sorted;
    }
    if (node.isArray()) {
      ArrayNode ordered = FINGERPRINT_JSON.createArrayNode();
      for (JsonNode element : node) {
        ordered.add(canonicalize(element)); // preserve order: column position is semantic
      }
      return ordered;
    }
    return node;
  }

  /** One walk context over a fixed head, with a content-addressed page cache. */
  public static Chain chain(TableRootRepository roots, ResourceId tableId, BlobRef head) {
    return new Chain(roots, tableId, head);
  }

  public static final class Chain {
    private final TableRootRepository roots;
    private final ResourceId tableId;
    private final BlobRef head;
    private final Map<String, SnapshotManifestPage> pages = new HashMap<>();

    private Chain(TableRootRepository roots, ResourceId tableId, BlobRef head) {
      this.roots = roots;
      this.tableId = tableId;
      this.head = head;
    }

    /**
     * Finds the entry for {@code snapshotId}, walking newest to oldest. A missing mid-chain page is
     * corruption and throws like the mutation paths do — treating it as end-of-chain would let a
     * finalize silently no-op ("snapshot unknown") and AS_OF resolve to a too-new snapshot.
     */
    public Optional<SnapshotManifestEntry> findEntry(long snapshotId) {
      BlobRef cursor = head;
      while (isPresent(cursor)) {
        SnapshotManifestPage page = page(cursor);
        Optional<SnapshotManifestEntry> match = entryIn(page, snapshotId);
        if (match.isPresent()) {
          return match;
        }
        cursor = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
      }
      return Optional.empty();
    }

    /**
     * Visits every entry, newest page first (entries within a page are newest first). Fails closed
     * on a missing page like {@link #findEntry}.
     */
    public void forEachEntry(Consumer<SnapshotManifestEntry> visitor) {
      BlobRef cursor = head;
      while (isPresent(cursor)) {
        SnapshotManifestPage page = page(cursor);
        page.getEntriesList().forEach(visitor);
        cursor = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
      }
    }

    /**
     * Inserts or replaces the entry for {@code entry.snapshot_id} and returns the new head ref. A
     * new snapshot prepends to the head page (spilling a full head into the chain); an existing
     * snapshot's entry is replaced where it sits, rewriting only that page and the pages newer than
     * it.
     */
    public BlobRef upsert(SnapshotManifestEntry entry) {
      List<SnapshotManifestPage> newerThanTarget = new ArrayList<>();
      BlobRef cursor = head;
      while (isPresent(cursor)) {
        SnapshotManifestPage page = page(cursor);
        for (int i = 0; i < page.getEntriesCount(); i++) {
          if (page.getEntries(i).getSnapshotId() == entry.getSnapshotId()) {
            SnapshotManifestPage replaced = page.toBuilder().setEntries(i, entry).build();
            return relink(newerThanTarget, put(replaced));
          }
        }
        newerThanTarget.add(page);
        cursor = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
      }

      // Not present: prepend. Only the head page is involved; no newer pages exist above it.
      return prepend(entry);
    }

    /**
     * Prepends {@code entry} WITHOUT the existing-id walk — the caller guarantees the id is not in
     * the chain (the synthesizer folds by-id-unique legacy pointers). Touches only the head page,
     * so a fold of N prepends over {@link #withHead}-chained instances costs O(N) page reads
     * instead of the O(N^2) that per-entry {@link #upsert} walks would.
     */
    public BlobRef prepend(SnapshotManifestEntry entry) {
      if (!isPresent(head)) {
        return put(SnapshotManifestPage.newBuilder().addEntries(entry).build());
      }
      SnapshotManifestPage headPage = page(head);
      if (headPage.getEntriesCount() >= PAGE_ENTRY_BOUND) {
        return put(
            SnapshotManifestPage.newBuilder().addEntries(entry).setPrevPageRef(head).build());
      }
      return put(
          headPage.toBuilder()
              .clearEntries()
              .addEntries(entry)
              .addAllEntries(headPage.getEntriesList())
              .build());
    }

    /**
     * A chain over {@code newHead} sharing this chain's content-addressed page cache. For folds
     * that repeatedly extend the head they just produced (legacy-history synthesis): every page the
     * fold wrote is already cached, so re-walking the new head reads nothing from the store.
     */
    public Chain withHead(BlobRef newHead) {
      Chain next = new Chain(roots, tableId, newHead);
      next.pages.putAll(pages);
      return next;
    }

    /**
     * Removes the entry for {@code snapshotId}, returning the new head ref: {@code head} unchanged
     * when the id is absent, {@code null} when the removal empties the whole chain. A page emptied
     * mid-chain is collapsed out by re-chaining past it.
     */
    public BlobRef remove(long snapshotId) {
      List<SnapshotManifestPage> newerThanTarget = new ArrayList<>();
      BlobRef cursor = head;
      while (isPresent(cursor)) {
        SnapshotManifestPage page = page(cursor);
        for (int i = 0; i < page.getEntriesCount(); i++) {
          if (page.getEntries(i).getSnapshotId() == snapshotId) {
            SnapshotManifestPage without = page.toBuilder().removeEntries(i).build();
            if (without.getEntriesCount() > 0) {
              return relink(newerThanTarget, put(without));
            }
            // Page emptied: collapse it out by linking the newer pages straight to its elder.
            return relink(
                newerThanTarget, without.hasPrevPageRef() ? without.getPrevPageRef() : null);
          }
        }
        newerThanTarget.add(page);
        cursor = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
      }
      return head;
    }

    /**
     * Rewrites the walked newer pages (oldest of them first) so their prev chain lands on {@code
     * target}, returning the new head ref. {@code target == null} clears the elder link.
     */
    private BlobRef relink(List<SnapshotManifestPage> newerPages, BlobRef target) {
      BlobRef prev = target;
      for (int i = newerPages.size() - 1; i >= 0; i--) {
        SnapshotManifestPage.Builder b = newerPages.get(i).toBuilder();
        if (prev == null) {
          b.clearPrevPageRef();
        } else {
          b.setPrevPageRef(prev);
        }
        prev = put(b.build());
      }
      return prev;
    }

    private SnapshotManifestPage page(BlobRef ref) {
      SnapshotManifestPage cached = pages.get(ref.getUri());
      if (cached != null) {
        return cached;
      }
      SnapshotManifestPage loaded =
          // Mutation chains (tableId set) run inside the commit funnel and read pages LIVE; the
          // read one-shots (tableId null) serve content and stay on the decoded cache.
          (tableId != null ? roots.getManifestPageLive(ref) : roots.getManifestPage(ref))
              .orElseThrow(
                  () ->
                      new BaseResourceRepository.CorruptionException(
                          "manifest page missing: " + ref.getUri(), null));
      pages.put(ref.getUri(), loaded);
      return loaded;
    }

    private BlobRef put(SnapshotManifestPage page) {
      BlobRef ref = roots.putManifestPage(tableId.getAccountId(), tableId.getId(), page);
      pages.put(ref.getUri(), page);
      return ref;
    }
  }

  /** One-shot {@link Chain#upsert}. */
  public static BlobRef upsert(
      TableRootRepository roots, ResourceId tableId, BlobRef head, SnapshotManifestEntry entry) {
    return chain(roots, tableId, head).upsert(entry);
  }

  /** One-shot {@link Chain#remove}. */
  public static BlobRef remove(
      TableRootRepository roots, ResourceId tableId, BlobRef head, long snapshotId) {
    return chain(roots, tableId, head).remove(snapshotId);
  }

  /**
   * One-shot {@link Chain#findEntry}; reads need no table identity. The head page is probed first:
   * the hottest lookups (CURRENT, a recent AS_OF) match there, and serving them from one (cached)
   * page read means a cold decoded cache never pays a full-chain build for them — a long-history
   * table is hundreds of pages whose refs resolve serially, and that walk would land exactly on the
   * latency-sensitive pin/planning path. Only a lookup that has to go deeper builds (and caches)
   * the per-head {@code snapshotId → entry} index; when caching is off it falls back to the
   * fail-closed page walk.
   */
  public static Optional<SnapshotManifestEntry> findEntry(
      TableRootRepository roots, BlobRef head, long snapshotId) {
    if (isPresent(head)) {
      Optional<SnapshotManifestPage> headPage = roots.getManifestPage(head);
      if (headPage.isPresent()) {
        Optional<SnapshotManifestEntry> match = entryIn(headPage.get(), snapshotId);
        if (match.isPresent()) {
          return match;
        }
        if (!headPage.get().hasPrevPageRef()) {
          return Optional.empty(); // single-page chain fully scanned
        }
      }
      // A missing head page falls through: the index build / page walk below fails closed on it.
    }
    var index = roots.manifestEntryIndex(head);
    if (index != null) {
      return Optional.ofNullable(index.get(snapshotId));
    }
    return chain(roots, null, head).findEntry(snapshotId);
  }

  /** Newest-first scan of one page for {@code snapshotId}. */
  private static Optional<SnapshotManifestEntry> entryIn(
      SnapshotManifestPage page, long snapshotId) {
    for (SnapshotManifestEntry e : page.getEntriesList()) {
      if (e.getSnapshotId() == snapshotId) {
        return Optional.of(e);
      }
    }
    return Optional.empty();
  }

  /** One-shot {@link Chain#forEachEntry}; reads need no table identity. */
  public static void forEachEntry(
      TableRootRepository roots, BlobRef head, Consumer<SnapshotManifestEntry> visitor) {
    chain(roots, null, head).forEachEntry(visitor);
  }

  /**
   * The query-visible current entry when the committed current is not yet finalized: the newest
   * FINALIZED entry (carrying a stats-generation ref) that is NOT newer than {@code
   * committedCurrent}.
   *
   * <p>This is the snapshot-isolation fallback — a query reads the latest fully-queryable state
   * rather than seeing "no current" during the append→finalize window. Bounding by "not newer than
   * the committed current" keeps a rollback honest: currency rolled back to an older, still-
   * finalizing snapshot never serves a newer one. Returns empty when nothing at or before the
   * committed current is finalized yet (a brand-new table before its first finalize).
   */
  public static Optional<SnapshotManifestEntry> latestQueryableCurrent(
      TableRootRepository roots, BlobRef head, SnapshotManifestEntry committedCurrent) {
    SnapshotManifestEntry[] best = {null};
    forEachEntry(
        roots,
        head,
        e -> {
          if (!e.hasStatsGenerationRef() || newer(e, committedCurrent)) {
            return;
          }
          if (best[0] == null || newer(e, best[0])) {
            best[0] = e;
          }
        });
    return Optional.ofNullable(best[0]);
  }

  /**
   * The advance rule's ordering, shared by every consumer (currency advance, AS_OF resolution,
   * legacy-currency import): newest {@code upstream_created_at} wins, snapshot id breaks ties; an
   * entry without a timestamp sorts oldest.
   */
  public static boolean newer(SnapshotManifestEntry a, SnapshotManifestEntry b) {
    long aMs = createdMillis(a);
    long bMs = createdMillis(b);
    if (aMs != bMs) {
      return aMs > bMs;
    }
    return a.getSnapshotId() > b.getSnapshotId();
  }

  private static long createdMillis(SnapshotManifestEntry entry) {
    return entry.hasUpstreamCreatedAt()
        ? com.google.protobuf.util.Timestamps.toMillis(entry.getUpstreamCreatedAt())
        : Long.MIN_VALUE;
  }

  private static boolean isPresent(BlobRef ref) {
    return ref != null && !ref.getUri().isEmpty();
  }
}
