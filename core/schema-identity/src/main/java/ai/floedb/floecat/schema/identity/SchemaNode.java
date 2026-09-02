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

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * One node of a resolved schema tree, in format-neutral terms.
 *
 * <p>Every node a table's schema contains is one of these: struct fields and the interiors of
 * collections alike. Iceberg and Delta produce the same node universe for the same logical schema,
 * so a consumer can walk either without knowing which it has.
 *
 * @param path the canonical structured path — the node's identity within its schema version
 * @param kind what the node is structurally. Always equal to {@code path.last().kind()} — the
 *     constructor enforces it — so the field is an ergonomic accessor rather than independent
 *     state, and anything persisting a node can store the path alone and derive this.
 * @param ordinal 1-based position among its siblings; an array element is 1, a map key 1 and its
 *     value 2
 * @param leaf true when the node has no children
 * @param nativeFieldId the id the source format assigned this node, when it assigned one. Iceberg
 *     assigns one to every node; Delta assigns them to struct fields under column mapping and to
 *     collection interiors only when nested ids are present. This is an <em>input</em> to canonical
 *     identity, never the identity itself.
 * @param sourcePhysicalPath the physical names the <em>source schema actually declares</em>, as the
 *     same structural tree with each field's physical name substituted for its logical one. Present
 *     only where the format supplies them — Delta under column mapping — and empty everywhere else,
 *     including Iceberg, which has no per-node analogue, and unmapped Delta, which declares none.
 *     <p>Never synthesized. A resolver reports what the source contains; where Delta has no
 *     physical name, this is empty rather than a copy of the logical path. Floecat's own stable
 *     physical identity for such a node is maintained identity state, not source metadata, and
 *     belongs with the persisted mapping rather than here.
 *     <p><b>Not a Parquet path.</b> Only {@link NodeKind#FIELD} elements carry substituted names;
 *     array elements and map keys and values stay structural. Parquet spells those out as {@code
 *     list.element} and {@code key_value.key}, and translating between the two is the job of the
 *     Delta producer's Parquet index.
 */
public record SchemaNode(
    ColumnPath path,
    NodeKind kind,
    int ordinal,
    boolean leaf,
    OptionalInt nativeFieldId,
    Optional<ColumnPath> sourcePhysicalPath) {

  public SchemaNode {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(nativeFieldId, "nativeFieldId");
    Objects.requireNonNull(sourcePhysicalPath, "sourcePhysicalPath");
    if (path.isRoot()) {
      throw new IllegalArgumentException("A schema node cannot sit at the root path");
    }
    if (kind != path.last().kind()) {
      throw new IllegalArgumentException(
          "Node kind " + kind + " contradicts its path '" + path.display() + "', which ends in "
              + path.last().kind());
    }
    if (ordinal <= 0) {
      throw new IllegalArgumentException("Ordinal must be 1-based, got " + ordinal);
    }
  }

  /** True when the source format supplied an id for this node. */
  public boolean hasNativeFieldId() {
    return nativeFieldId.isPresent();
  }

  /** The node's logical name, for a field. Empty for a collection interior, which has none. */
  public Optional<String> name() {
    return kind == NodeKind.FIELD ? Optional.of(path.last().name()) : Optional.empty();
  }
}
