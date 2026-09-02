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

package ai.floedb.floecat.schema.identity;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Seeds a canonical identity map from the source format's own ids, for a table that has no
 * canonical mapping yet.
 *
 * <h2>Seeding, not continuity</h2>
 *
 * <p>This is an initialization shortcut, and only that. When Floecat first sees a schema whose
 * every node already carries a native id, adopting those ids is the cheapest correct starting
 * point: it costs nothing and it makes Floecat's ids agree with what the format itself uses.
 *
 * <p>It must not be read as the general rule. Once a canonical mapping exists for a table,
 * canonical ids are authoritative and the reconciling assigner owns continuity: a node matched to
 * an existing identity keeps its canonical id no matter what happens to its native one. Re-running
 * this seeder over a later schema version would renumber columns to whatever the format currently
 * says, which is precisely the mistake the canonical layer exists to prevent. See the package
 * documentation for the rules that govern the second and every subsequent version.
 *
 * <h2>When it applies</h2>
 *
 * <p>Iceberg always qualifies — it assigns an id to every node. Delta qualifies when column mapping
 * is on <em>and</em> nested ids cover every collection interior; a mapped Delta table missing those
 * does not, because some of its nodes have no native id at all. Where a node has no native id this
 * seeder refuses rather than inventing one, since inventing ids safely requires the allocation and
 * reconciliation state it has no access to.
 */
public final class NativeIdentityAssigner {

  private NativeIdentityAssigner() {}

  /** True when this seeder applies — that is, every node already has a native id. */
  public static boolean canSeed(ResolvedSchema schema) {
    return schema.hasTotalNativeIds();
  }

  /**
   * Seeds a canonical map by adopting every node's native id as its canonical column id.
   *
   * <p>Only for a table with no existing canonical mapping — see the class note.
   *
   * @throws IllegalArgumentException if any node lacks a native id
   */
  public static SchemaIdentityMap seed(ResolvedSchema schema) {
    List<SchemaNode> ungoverned = schema.nodesWithoutNativeIds();
    if (!ungoverned.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot seed canonical ids from native ids for "
              + schema.format()
              + ": "
              + ungoverned.size()
              + " node(s) have none, starting with '"
              + ungoverned.get(0).path().display()
              + "'. These need ids allocated and reconciled against the table's previous schema.");
    }
    List<ColumnIdentity> identities = new ArrayList<>(schema.nodes().size());
    for (SchemaNode node : schema.nodes()) {
      int nativeId = node.nativeFieldId().orElseThrow();
      identities.add(
          new ColumnIdentity(
              node.path(),
              nativeId,
              node.kind(),
              Optional.of(new FormatIdentity(schema.format(), nativeId))));
    }
    return SchemaIdentityMap.of(schema.format(), schema, identities);
  }
}
