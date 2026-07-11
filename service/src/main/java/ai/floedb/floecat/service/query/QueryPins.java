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

package ai.floedb.floecat.service.query;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.QUERY_TABLE_PIN_CONFLICT;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.PinKind;
import ai.floedb.floecat.query.rpc.RelationPin;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.types.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Query-pin helpers shared by the resolver and the relation-resolution RPCs.
 *
 * <p>Three concerns live here:
 *
 * <ul>
 *   <li>Projections between the coherent {@link RelationPin} the query context stores and the
 *       snapshot-selector {@link SnapshotPin} the older read paths (obligations, schema-describe,
 *       scan/stats pin lookup) still speak. The stored representation is always {@code
 *       RelationPinSet}; richer paths consume the {@code TablePin} directly.
 *   <li>The table-pin conflict rule ({@link #compatible} / {@link #reconcile} / {@link
 *       #mergeSets}): first-touch wins, compatible later resolutions reuse the stored pin, and
 *       incompatible temporal intents fail planning.
 *   <li>The opaque planner-facing {@link RelationPinIdentity} built by {@link #identity}.
 * </ul>
 */
public final class QueryPins {
  private QueryPins() {}

  public static RelationPin ofTable(TablePin tablePin) {
    return RelationPin.newBuilder().setTablePin(tablePin).build();
  }

  /**
   * Every immutable blob URI this pin references: the pinned ROOT (the object all reads follow refs
   * out of, and whose chain expansion protects the manifest pages and per-entry refs), plus the
   * copied table/snapshot/constraints refs. This is the ONE definition shared by the transient
   * resolving-window registration and the committed context's GC roots — the two must never
   * diverge, or a pin's blobs are unprotected exactly in the window between resolution and context
   * commit.
   */
  public static List<String> gcRootUris(TablePin pin) {
    List<String> uris = new ArrayList<>(4);
    addUriIfPresent(uris, pin.getRootUri());
    addUriIfPresent(uris, pin.getTableBlobUri());
    addUriIfPresent(uris, pin.getSnapshotBlobUri());
    addUriIfPresent(uris, pin.getConstraintsRefUri());
    return uris;
  }

  private static void addUriIfPresent(List<String> uris, String uri) {
    if (uri != null && !uri.isEmpty()) {
      uris.add(uri);
    }
  }

  /** Find the pinned table for {@code tableId}, matched by account + id. */
  public static Optional<TablePin> findTablePin(RelationPinSet pins, ResourceId tableId) {
    return pins.getPinsList().stream()
        .filter(RelationPin::hasTablePin)
        .map(RelationPin::getTablePin)
        .filter(t -> t.hasTableId() && sameTable(t.getTableId(), tableId))
        .findFirst();
  }

  /**
   * Project a table pin to the snapshot-selector shape still spoken by the wire contract and the
   * older read paths. Every pin is resolved to a concrete snapshot at construction (an AS_OF pin
   * resolves once, keeping its timestamp only as provenance), so the projection always carries the
   * snapshot id.
   */
  public static SnapshotPin toSnapshotPin(TablePin tablePin) {
    return SnapshotPin.newBuilder()
        .setTableId(tablePin.getTableId())
        .setSnapshotId(tablePin.getSnapshotId())
        .build();
  }

  public static SnapshotSet toSnapshotSet(RelationPinSet pins) {
    SnapshotSet.Builder b = SnapshotSet.newBuilder();
    for (RelationPin p : pins.getPinsList()) {
      if (p.hasTablePin()) {
        b.addPins(toSnapshotPin(p.getTablePin()));
      }
    }
    return b.build();
  }

  /**
   * The table-pin conflict rule. A later resolution of an already-pinned table is compatible when:
   *
   * <ul>
   *   <li>it carries no temporal intent (CURRENT / unspecified) — it simply reuses the stored pin;
   *   <li>its temporal intent (explicit snapshot id, or a resolved AS_OF) resolves to the same
   *       physical identity as the stored pin.
   * </ul>
   *
   * <p>Identity is the immutable DATA the pin names: (snapshot id, snapshot blob version) — not the
   * snapshot id alone, and deliberately NOT the table blob version. Snapshot ids are always
   * resolved to a concrete value at pin time, so they are compared directly (0 is a real snapshot
   * id). The snapshot blob version is compared only when both sides carry one: an empty version is
   * an etag a pin never captured and cannot prove a conflict.
   *
   * <p>The table blob version is intentionally excluded. It tracks the MUTABLE current table
   * pointer (an explicit/AS_OF pin captures whatever the current table blob is at pin time — see
   * {@code SnapshotHelper.resolvedPin}), so folding it into identity would make two references to
   * the same immutable snapshot conflict merely because the table was ALTERed between them —
   * resolution-order dependent, and the opposite of what first-touch is for. It is carried on the
   * pin as first-touch provenance (used to read the pinned table blob and validate it), not as
   * identity. Compatibility never rests on {@code original_as_of}; the stored pin is never mutated
   * or upgraded.
   */
  static boolean compatible(TablePin existing, TablePin incoming) {
    PinKind kind = incoming.getPinKind();
    if (kind == PinKind.PIN_KIND_CURRENT || kind == PinKind.PIN_KIND_UNSPECIFIED) {
      return true;
    }
    return existing.getSnapshotId() == incoming.getSnapshotId()
        && sameCapturedVersion(
            existing.getSnapshotBlobVersion(), incoming.getSnapshotBlobVersion());
  }

  /** Blob versions match unless one side never captured its etag (empty), which proves nothing. */
  private static boolean sameCapturedVersion(String a, String b) {
    return a.isEmpty() || b.isEmpty() || a.equals(b);
  }

  /**
   * Reconcile an incoming pin against the existing pin for the same table. Returns the pin to keep
   * (always the existing one, per first-touch semantics) or throws a query-consistency error when
   * the two carry incompatible temporal intents.
   */
  public static TablePin reconcile(TablePin existing, TablePin incoming, String correlationId) {
    if (compatible(existing, incoming)) {
      return existing;
    }
    throw GrpcErrors.preconditionFailed(
        correlationId,
        QUERY_TABLE_PIN_CONFLICT,
        Map.of(
            "table_id", existing.getTableId().getId(),
            "pinned_snapshot", Long.toString(existing.getSnapshotId()),
            "requested_snapshot", Long.toString(incoming.getSnapshotId())));
  }

  /**
   * Merge two relation-pin sets keyed by table id, applying {@link #reconcile}. Existing pins are
   * preserved in place; incoming pins for new tables are appended; incompatible temporal intents
   * for an already-pinned table fail planning. Insertion order is stable.
   */
  public static RelationPinSet mergeSets(
      RelationPinSet existing, RelationPinSet incoming, String correlationId) {
    if (incoming.getPinsCount() == 0) {
      return existing;
    }
    // Keyed by table identity. Only table pins participate: the set is table pins today (views pin
    // their base tables), and skipping non-table pins on both sides avoids collapsing several of
    // them onto the empty key relationPinKey returns for a non-table pin.
    Map<String, RelationPin> merged = new LinkedHashMap<>();
    // Non-table pins (a reserved ViewPin arm, none constructed today) are carried through
    // verbatim rather than dropped, so a future pin kind cannot be silently lost on merge.
    List<RelationPin> carryThrough = new ArrayList<>();
    for (RelationPin pin : existing.getPinsList()) {
      if (pin.hasTablePin()) {
        merged.put(relationPinKey(pin), pin);
      } else {
        carryThrough.add(pin);
      }
    }
    for (RelationPin pin : incoming.getPinsList()) {
      if (!pin.hasTablePin()) {
        continue;
      }
      String key = relationPinKey(pin);
      RelationPin current = merged.get(key);
      if (current == null) {
        merged.put(key, pin);
      } else {
        reconcile(current.getTablePin(), pin.getTablePin(), correlationId);
        // Compatible: keep the first pin unchanged.
      }
    }
    return RelationPinSet.newBuilder().addAllPins(carryThrough).addAllPins(merged.values()).build();
  }

  /**
   * Build the opaque, planner-facing identity for a resolved pin. The fingerprint is a stable hash
   * of the pin's immutable DATA identity — relation kind, table id, snapshot id, snapshot blob
   * version (see {@link #fingerprint}). It deliberately excludes {@code pin_kind}, the {@code
   * table_blob_version}, and the {@code constraints_ref_version}: CURRENT, AS_OF, and
   * explicit-snapshot requests that resolve to the same physical data are the same pin, so they
   * must share one cache identity rather than fragmenting it, and the table blob and constraints
   * versions are per-touch provenance that would fragment the key across a benign ALTER or
   * constraints write. All three are still carried as separate fields on the identity for
   * provenance. Carries no blob URIs.
   */
  public static RelationPinIdentity identity(TablePin tablePin) {
    RelationPinIdentity.Builder b =
        RelationPinIdentity.newBuilder()
            .setPinKind(tablePin.getPinKind())
            .setTableBlobVersion(tablePin.getTableBlobVersion())
            .setSnapshotId(tablePin.getSnapshotId())
            .setSnapshotBlobVersion(tablePin.getSnapshotBlobVersion())
            .setConstraintsRefVersion(tablePin.getConstraintsRefVersion())
            .setPinFingerprint(fingerprint(tablePin));
    if (tablePin.hasOriginalAsOf()) {
      b.setOriginalAsOf(tablePin.getOriginalAsOf());
    }
    return b.build();
  }

  /** Build the planner-facing identities for every table pin in a set, in stored order. */
  public static List<RelationPinIdentity> identities(RelationPinSet pins) {
    List<RelationPinIdentity> out = new ArrayList<>(pins.getPinsCount());
    for (RelationPin p : pins.getPinsList()) {
      if (p.hasTablePin()) {
        out.add(identity(p.getTablePin()));
      }
    }
    return out;
  }

  private static String fingerprint(TablePin tablePin) {
    ResourceId id = tablePin.getTableId();
    // Hashes the immutable data identity only — account, table id, snapshot id, snapshot blob
    // version — matching compatible(). The table blob version is excluded: it is mutable per-touch
    // provenance, not identity, so including it would fragment the cache key across a benign ALTER.
    String canonical =
        String.join(
            "\u0000",
            "TABLE",
            id.getAccountId(),
            id.getId(),
            Long.toString(tablePin.getSnapshotId()),
            tablePin.getSnapshotBlobVersion());
    return Hashing.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8));
  }

  /** Stable per-query map key for a table: account + kind + id. */
  public static String pinKey(ResourceId id) {
    return String.join(":", id.getAccountId(), id.getKind().name(), id.getId());
  }

  private static String relationPinKey(RelationPin pin) {
    return pin.hasTablePin() ? pinKey(pin.getTablePin().getTableId()) : "";
  }

  private static boolean sameTable(ResourceId a, ResourceId b) {
    // Strict identity on both legs: a pin lookup must carry the fully-resolved (account, id). A
    // blank accountId on one side is a DIFFERENT identity, never a wildcard match — see
    // QueryContextTest.requireSnapshotPinDoesNotMatchWhenAccountIsMissingOnOneSide.
    return a.getAccountId().equals(b.getAccountId()) && a.getId().equals(b.getId());
  }
}
