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

/**
 * The canonical identity of one schema node: its path and the Floecat column id that names it.
 *
 * <p>{@code columnId} is Floecat's own identity for the node and is always present. {@code
 * formatIdentity} records what the source format contributed, which may be nothing.
 *
 * <p>Deliberately carries no physical path. Two different notions would compete for such a field:
 * the physical names the source declares, which belong to {@link SchemaNode#sourcePhysicalPath()}
 * and describe the schema rather than the identity; and the stable physical identity Floecat
 * maintains for a Delta table that has no column mapping of its own, which is persisted identity
 * state living alongside the map rather than inside it. Keeping both out leaves this container
 * unambiguous.
 *
 * @param path the canonical structured path
 * @param columnId the Floecat column id, always positive
 * @param kind what the node is structurally. Always equal to {@code path.last().kind()} — the
 *     constructor enforces it — so persisted identity need only carry the path and derive this.
 * @param formatIdentity the source format's own id for this node, when it had one
 */
public record ColumnIdentity(
    ColumnPath path, long columnId, NodeKind kind, Optional<FormatIdentity> formatIdentity) {

  public ColumnIdentity {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(formatIdentity, "formatIdentity");
    if (path.isRoot()) {
      throw new IllegalArgumentException("An identity cannot sit at the root path");
    }
    if (kind != path.last().kind()) {
      throw new IllegalArgumentException(
          "Identity kind " + kind + " contradicts its path '" + path.display() + "', which ends in "
              + path.last().kind());
    }
    if (columnId <= 0L) {
      throw new IllegalArgumentException("A column id must be positive, got " + columnId);
    }
  }
}
