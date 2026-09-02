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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.schema.identity.delta.ColumnMappingMode;
import java.util.Locale;
import java.util.Map;

/**
 * Produces the legacy {@code UpstreamRef.column_id_algorithm} value recorded against a table.
 *
 * <h2>This is provenance. Do not dispatch on it.</h2>
 *
 * <p>{@link ColumnIdAlgorithm} predates the canonical identity model and describes neither half of
 * it correctly.
 *
 * <p><b>{@code CID_FIELD_ID} is not a per-node instruction.</b> It says one thing about a whole
 * table, whereas native ids are a property of each node:
 *
 * <ul>
 *   <li><b>Iceberg</b> assigns a native id to every node, collection interiors included.
 *   <li><b>Delta with column mapping</b> assigns one to every struct field, but a collection
 *       interior only has an id when {@code delta.columnMapping.nested.ids} is present — and
 *       writers emit that only when something needs it. So a mapped Delta table routinely holds
 *       fields with native ids alongside array elements and map keys without them.
 *   <li><b>Delta without column mapping</b> assigns none at all.
 * </ul>
 *
 * <p>Reading {@code CID_FIELD_ID} off a mapped Delta table and applying it to every node is
 * therefore wrong, and is the precise bug the canonical model exists to prevent: the nodes with no
 * native id would all resolve to id 0, which is also the value meaning "uncomputed".
 *
 * <p><b>{@code CID_PATH_ORDINAL} is not an identity algorithm either.</b> It names how ids for
 * unmapped Delta columns were <em>historically</em> derived, by hashing a path together with an
 * ordinal. A table still carrying that stamp records how its stored ids may once have been
 * computed; it says nothing about how identity is determined now, and the value must never be read
 * as an instruction to recompute anything.
 *
 * <p>Ordinals play no part in the current model. Identity continuity comes from the source format's
 * own evidence: a native Delta id where column mapping is active, and otherwise the exact canonical
 * structured path. A node whose path changes with no native id to vouch for it is a new lineage,
 * not a renamed one — Delta records nothing that could prove otherwise, and guessing from sibling
 * position would silently hand a dropped column's statistics to an unrelated new one. Ordinal
 * survives on {@link ai.floedb.floecat.schema.identity.SchemaNode} as a description of the schema's
 * current shape and to make traversal deterministic, never as identity evidence.
 *
 * <p>To identify a column, resolve the schema through a producer and consult the resulting {@link
 * SchemaIdentityMap}, which records where each node's id actually came from in its {@link
 * FormatIdentity}. Use this class only to fill the persisted enum field, and read that field only
 * as a record of how a table was last reconciled.
 */
public final class ColumnIdAlgorithmStamp {

  private ColumnIdAlgorithmStamp() {}

  /**
   * The value to record on a table's {@code UpstreamRef}.
   *
   * <p>Approximate by construction — see the class note. It reports roughly where a table's native
   * ids come from, not how any particular node is identified.
   *
   * @throws IllegalArgumentException if the format is unsupported, or the table declares a column
   *     mapping mode this build does not understand
   */
  public static ColumnIdAlgorithm forTable(TableFormat format, Map<String, String> properties) {
    return switch (format) {
      case TF_ICEBERG -> ColumnIdAlgorithm.CID_FIELD_ID;
      case TF_DELTA ->
          ColumnMappingMode.fromProperties(properties).isEnabled()
              ? ColumnIdAlgorithm.CID_FIELD_ID
              : ColumnIdAlgorithm.CID_PATH_ORDINAL;
      default -> throw new IllegalArgumentException("Unsupported upstream table format=" + format);
    };
  }

  /** As {@link #forTable(TableFormat, Map)}, from the raw format string discovery carries. */
  public static ColumnIdAlgorithm forTable(String format, Map<String, String> properties) {
    return forTable(tableFormat(format), properties);
  }

  /** Maps a discovery format string onto {@link TableFormat}. */
  public static TableFormat tableFormat(String format) {
    if (format == null || format.isBlank()) {
      throw new IllegalArgumentException("Upstream table format must not be blank");
    }
    return switch (format.trim().toUpperCase(Locale.ROOT)) {
      case "ICEBERG" -> TableFormat.TF_ICEBERG;
      case "DELTA" -> TableFormat.TF_DELTA;
      default -> throw new IllegalArgumentException("Unsupported upstream table format=" + format);
    };
  }
}
