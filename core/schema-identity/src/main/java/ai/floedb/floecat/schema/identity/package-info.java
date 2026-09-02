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

/**
 * Canonical column identity for every supported table format.
 *
 * <h2>The guarantee</h2>
 *
 * For any supported format, given a canonical schema-node path, resolve its stable column id; and
 * given a stable column id, resolve its canonical path — at every nesting level. That holds equally
 * for Iceberg, for Delta with column mapping, and for Delta without; and equally for struct fields,
 * array elements, map keys, and map values.
 *
 * <h2>The pieces</h2>
 *
 * <dl>
 *   <dt>{@link ai.floedb.floecat.schema.identity.ColumnPath}
 *   <dd>A structured path — the canonical identity of a node within one schema version. Never a
 *       dotted string: {@code FIELD("a.b")} and {@code FIELD("a"), FIELD("b")} are different nodes
 *       that render identically.
 *   <dt>{@link ai.floedb.floecat.schema.identity.ResolvedSchema}
 *   <dd>What this schema version contains. Produced by a format producer; carries no canonical ids.
 *   <dt>{@link ai.floedb.floecat.schema.identity.FormatIdentity}
 *   <dd>What the source format said about a node's identity. An input, never the identity itself.
 *   <dt>{@link ai.floedb.floecat.schema.identity.SchemaIdentityMap}
 *   <dd>How Floecat identifies the nodes of this schema version, in both directions. Construction
 *       proves the guarantee above.
 * </dl>
 *
 * <p>Producers live in the {@code delta} and {@code iceberg} subpackages and answer exactly one
 * question: where, if anywhere, does this node's id come from? Everything format-specific —
 * physical names, column mapping modes, nested ids, Parquet wrapper encodings — stays there.
 *
 * <h2>Contract for the identity assigner</h2>
 *
 * {@link ai.floedb.floecat.schema.identity.NativeIdentityAssigner} handles only the easy case:
 * schemas where the format supplied an id for every node. The reconciling assigner that fills gaps
 * and carries identity from one schema version to the next is deliberately not in this package yet.
 * When it arrives it must hold to the following, which is why they are written down here rather
 * than left to be rediscovered.
 *
 * <h3>For Delta, canonical identity virtualizes Delta's own column mapping</h3>
 *
 * <p>Delta's column mapping (see the Delta protocol's Column Mapping section) already defines a
 * stable per-node identity scheme: a monotonically allocated integer id per node, tracked by {@code
 * delta.columnMapping.maxColumnId}, paired with a physical name that survives renames. Floecat's
 * aim is not to invent a competing scheme but to behave as though that one had always been enabled
 * — using Delta's own ids where the table supplies them, and maintaining the equivalent externally
 * where it does not.
 *
 * <p>Two consequences for the assigner:
 *
 * <ul>
 *   <li><b>Allocate in Delta's space.</b> Ids minted for Delta nodes must be positive and fit in a
 *       signed 32-bit integer, allocated monotonically from a persisted counter that mirrors {@code
 *       maxColumnId}. Canonical ids are {@code long} in this package for headroom across formats;
 *       that width is not licence to leave Delta's range.
 *   <li><b>A virtual physical identity is internal.</b> Where Delta declares no physical name,
 *       Floecat may maintain a stable one of its own so that identity survives a rename. It is not
 *       a key for reading data: the files of an unmapped table hold logical names, and if real
 *       column mapping is later enabled Delta will mint its own {@code col-<uuid>} names that will
 *       not match Floecat's. Use it for identity continuity only, never to locate a column in a
 *       file.
 * </ul>
 *
 * <p>Neither belongs in {@link ai.floedb.floecat.schema.identity.ResolvedSchema} or {@link
 * ai.floedb.floecat.schema.identity.ColumnIdentity}. The high-water counter and any virtual
 * physical names are persisted identity state that sits alongside the map, not facts about the
 * source schema — which is why {@link ai.floedb.floecat.schema.identity.SchemaNode} reports only
 * the physical names the source actually declares.
 *
 * <p><b>Canonical ids are immutable once assigned to a logical lineage.</b> A node's {@code
 * FormatIdentity} may appear, disappear, or change as the table's mapping mode and format features
 * change. Its canonical id does not. Enabling column mapping on an unmapped Delta table, or turning
 * on Iceberg compatibility so that collection interiors gain nested ids, adds provenance — it never
 * renumbers an established identity.
 *
 * <p><b>A persisted high-water mark, not tombstones, guarantees non-reuse.</b> Store {@code
 * maxAssignedColumnId} alongside the current identities and allocate with {@code
 * ++maxAssignedColumnId}. Dropping a node removes it from the current map but never lowers the
 * mark, so a retired id can never be handed to a different node. Tombstones would only be needed to
 * recognise a dropped column that later reappears; absent a requirement for that kind of
 * resurrection, they are complexity without benefit.
 *
 * <p><b>After canonical identity exists, the canonical namespace is authoritative.</b> {@link
 * ai.floedb.floecat.schema.identity.NativeIdentityAssigner} may seed canonical ids from native ones
 * when a table's identity set is first created, but that is an initialization shortcut and nothing
 * more: the source format knows nothing about ids Floecat minted itself and can legitimately issue
 * the same integer later. Concretely, if Delta has used ids 1..50 and Floecat allocated 51 for a
 * node with no native id, Delta may subsequently assign native id 51 to something else. So for an
 * existing table: a node matched to an existing identity keeps its canonical id and merely updates
 * its provenance; a genuinely new node may adopt its native id only if that value has never been
 * allocated in the canonical namespace; otherwise it takes a fresh id above the high-water mark.
 *
 * <p><b>Continuity comes only from what the format makes authoritative.</b> Match in this order and
 * no other:
 *
 * <ol>
 *   <li><b>Authoritative native id</b>, where both versions have one. The strongest evidence, and
 *       what makes a mapped rename preserve identity: Delta keeps the id stable while the display
 *       name moves.
 *   <li><b>Collection interiors under a parent matched in step 1.</b> Deductive, not inferred: an
 *       array has exactly one element node, a map exactly one key and one value, so a proven parent
 *       lineage determines its collection child's with no candidate to choose between. This is what
 *       keeps {@code orders[]} alive when {@code orders} is renamed to {@code purchases} under
 *       column mapping without nested ids — the common case, since nested ids are written mainly
 *       for Iceberg compatibility. Propagation continues down a chain of collection roles
 *       ({@code array -> element -> map -> value}) and <b>stops at the first FIELD</b>: a named
 *       field beneath a matched element inherits nothing from its position and must earn its own
 *       match in step 1 or 3.
 *   <li><b>Exact canonical structured path.</b> The continuity mechanism wherever no native id
 *       exists.
 *   <li><b>Allocate fresh.</b>
 * </ol>
 *
 * <p>Nothing else counts as evidence.
 *
 * <p><b>Reconcile only against the immediately preceding version.</b> Path matching consults the
 * previous live map alone, which is exactly what makes a dropped-then-readded path take a new id.
 * Skipping a version breaks that: reconciling v3 against v1 would resurrect an identity v2
 * retired. The Delta state therefore records the source version it represents and the reconciler
 * rejects input that is not its immediate successor. This is a Delta-side precondition, not a
 * neutral invariant — nothing in this package assumes a cross-format version concept.
 *
 * <p><b>Renames are not inferred.</b> For unmapped Delta the schema alone cannot distinguish
 * renaming {@code a} to {@code b} from dropping {@code a} and adding {@code b}, because Delta
 * persists nothing that records the difference. So a path change with no native id behind it
 * produces a new lineage and a new id.
 *
 * <p>That loses continuity across an unmapped rename, and the trade is deliberate, resting on an
 * asymmetry worth stating plainly: <b>a false positive silently attaches a dropped column's
 * statistics and metadata to a different column, while a false negative only costs a new identity
 * and a recapture.</b> One corrupts query plans invisibly; the other is visible and self-healing.
 *
 * <p>In particular, <b>ordinal is not identity evidence</b> — not alone, and not as one term in a
 * similarity score. Two nodes sharing a sibling position across versions are not thereby the same
 * node. {@link ai.floedb.floecat.schema.identity.SchemaNode#ordinal()} describes the shape of the
 * schema in front of you and gives the traversal a deterministic order for allocation; it says
 * nothing about lineage. Any future rename inference must rest on evidence outside the schema —
 * Delta operation metadata or transaction history — with its confidence stated explicitly, not on
 * structural resemblance.
 */
package ai.floedb.floecat.schema.identity;
