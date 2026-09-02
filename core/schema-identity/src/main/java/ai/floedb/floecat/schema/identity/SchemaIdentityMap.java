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

import ai.floedb.floecat.catalog.rpc.TableFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The canonical name-to-id mapping for one schema version, in both directions.
 *
 * <h2>Invariant</h2>
 *
 * For every schema node Floecat represents, in any supported format:
 *
 * <ul>
 *   <li>{@code canonicalPath -> columnId} is <b>total</b> — every node has an id — and
 *       <b>unique</b> — no two nodes share one.
 *   <li>{@code columnId -> canonicalPath} is total and unique, the exact inverse.
 * </ul>
 *
 * <p>This holds equally for Iceberg, for Delta with column mapping, and for Delta without; and
 * equally for struct fields, array elements, map keys, and map values. Construction enforces it:
 * {@link #of(TableFormat, ResolvedSchema, List)} checks the identities against the schema they
 * describe and refuses to build a map that is partial or ambiguous in either direction.
 *
 * <h2>Scope</h2>
 *
 * <p>One map describes one schema version. Across versions a rename moves a path while the id
 * persists, and a dropped column's id must never be handed to a different node later. Carrying
 * identity across versions belongs to the assigner, not here, so never cache a map as though it
 * described the table rather than one of its schemas.
 */
public final class SchemaIdentityMap {

  private final TableFormat format;
  private final List<ColumnIdentity> identities;
  private final Map<ColumnPath, ColumnIdentity> byCanonicalPath;
  private final Map<Long, ColumnIdentity> byId;

  private SchemaIdentityMap(
      TableFormat format,
      List<ColumnIdentity> identities,
      Map<ColumnPath, ColumnIdentity> byCanonicalPath,
      Map<Long, ColumnIdentity> byId) {
    this.format = format;
    this.identities = List.copyOf(identities);
    this.byCanonicalPath = Collections.unmodifiableMap(byCanonicalPath);
    this.byId = Collections.unmodifiableMap(byId);
  }

  /**
   * Builds the map and enforces the invariant against the schema it describes.
   *
   * <p>This is the boundary where identity is proved, so the checks are exhaustive rather than
   * indicative. Beyond the two-way totality and uniqueness of path and id, every identity must
   * actually describe the node it names: same {@link NodeKind}, and format provenance consistent
   * with what the producer found. An identity claiming a native id the node does not have, or
   * omitting one it does, is rejected — otherwise a caller could assert provenance the source
   * format never supplied.
   *
   * @throws IllegalArgumentException if the map is partial, ambiguous, or inconsistent with the
   *     schema in any of those respects
   */
  public static SchemaIdentityMap of(
      TableFormat format, ResolvedSchema schema, List<ColumnIdentity> identities) {
    Objects.requireNonNull(format, "format");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(identities, "identities");

    if (format != schema.format()) {
      throw new IllegalArgumentException(
          "Identity map format " + format + " does not match schema format " + schema.format());
    }

    Map<ColumnPath, ColumnIdentity> byCanonicalPath = new LinkedHashMap<>();
    Map<Long, ColumnIdentity> byId = new LinkedHashMap<>();
    List<String> problems = new ArrayList<>();

    for (ColumnIdentity identity : identities) {
      String where = "'" + identity.path().display() + "'";
      if (byCanonicalPath.putIfAbsent(identity.path(), identity) != null) {
        problems.add("two identities share the canonical path " + where);
      }
      ColumnIdentity idClash = byId.putIfAbsent(identity.columnId(), identity);
      if (idClash != null) {
        problems.add(
            "column id "
                + identity.columnId()
                + " is shared by '"
                + idClash.path().display()
                + "' and "
                + where);
      }
      SchemaNode node = schema.byPath(identity.path()).orElse(null);
      if (node == null) {
        problems.add("identity " + where + " names no node in the schema");
        continue;
      }
      if (identity.kind() != node.kind()) {
        problems.add(
            "identity "
                + where
                + " claims kind "
                + identity.kind()
                + " but the node is "
                + node.kind());
      }
      checkProvenance(identity, node, format, where, problems);
    }
    for (SchemaNode node : schema.nodes()) {
      if (!byCanonicalPath.containsKey(node.path())) {
        problems.add("node '" + node.path().display() + "' has no identity");
      }
    }
    if (!problems.isEmpty()) {
      throw new IllegalArgumentException(
          "Schema identity map is inconsistent with its schema: " + String.join("; ", problems));
    }
    return new SchemaIdentityMap(format, identities, byCanonicalPath, byId);
  }

  /**
   * Checks that an identity's recorded provenance matches what the producer actually found: present
   * exactly when the node has a native id, naming this format, and carrying that same id.
   */
  private static void checkProvenance(
      ColumnIdentity identity,
      SchemaNode node,
      TableFormat format,
      String where,
      List<String> problems) {
    Optional<FormatIdentity> provenance = identity.formatIdentity();
    if (provenance.isEmpty()) {
      if (node.hasNativeFieldId()) {
        problems.add(
            "identity "
                + where
                + " records no format provenance but the node has native id "
                + node.nativeFieldId().orElseThrow());
      }
      return;
    }
    FormatIdentity found = provenance.orElseThrow();
    if (found.format() != format) {
      problems.add(
          "identity " + where + " records provenance from " + found.format() + ", not " + format);
    }
    if (!node.hasNativeFieldId()) {
      problems.add(
          "identity " + where + " records native id " + found.fieldId() + " but the node has none");
    } else if (node.nativeFieldId().orElseThrow() != found.fieldId()) {
      problems.add(
          "identity "
              + where
              + " records native id "
              + found.fieldId()
              + " but the node has "
              + node.nativeFieldId().orElseThrow());
    }
  }

  public TableFormat format() {
    return format;
  }

  public List<ColumnIdentity> identities() {
    return identities;
  }

  public int size() {
    return identities.size();
  }

  /** Canonical path to identity. Total over the schema this map was built from. */
  public Optional<ColumnIdentity> byCanonicalPath(ColumnPath path) {
    return Optional.ofNullable(byCanonicalPath.get(path));
  }

  /** Column id to identity. The exact inverse of {@link #byCanonicalPath(ColumnPath)}. */
  public Optional<ColumnIdentity> byId(long columnId) {
    return Optional.ofNullable(byId.get(columnId));
  }
}
