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

package ai.floedb.floecat.schema.identity.delta;

import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.List;
import java.util.Optional;

/**
 * A {@link ResolvedSchema} produced from Delta, plus the Delta-specific lookups that only make
 * sense for this format.
 *
 * <p>The format-neutral schema is what the identity layer consumes. The extra lookups here —
 * resolving a Parquet column path and resolving a Delta log statistics name — exist because those
 * inputs speak Delta's physical names and Delta's structural conventions, and nothing in the
 * neutral model should have to know about either.
 */
public final class DeltaResolvedSchema {

  private final ResolvedSchema schema;
  private final ColumnMappingMode mode;
  private final DeltaStatsNameIndex statsNames;
  private final DeltaParquetPathIndex parquetPaths;

  DeltaResolvedSchema(
      ResolvedSchema schema,
      ColumnMappingMode mode,
      DeltaStatsNameIndex statsNames,
      DeltaParquetPathIndex parquetPaths) {
    this.schema = schema;
    this.mode = mode;
    this.statsNames = statsNames;
    this.parquetPaths = parquetPaths;
  }

  /** The format-neutral schema. This is what the identity layer consumes. */
  public ResolvedSchema schema() {
    return schema;
  }

  /** The mapping mode this schema was resolved for, or empty when it was not supplied. */
  public Optional<ColumnMappingMode> effectiveMappingMode() {
    return Optional.ofNullable(mode);
  }

  /**
   * Resolves a Parquet column path — raw, in Parquet's own notation — by walking the schema
   * structurally.
   *
   * <p>Each segment is matched against the current node's children before being read as a
   * structural wrapper, so {@code list}, {@code element}, {@code key_value}, {@code key} and {@code
   * value} keep working as ordinary Delta field names. A struct {@code a} holding a struct {@code
   * list} holding a field {@code element} resolves to that field rather than to an array element,
   * because the walk knows {@code a} is a struct. Both the standard three-level encodings and the
   * bare {@code element}/{@code key}/{@code value} shorthand are accepted.
   */
  public Optional<SchemaNode> nodeForParquetPath(String parquetPath) {
    return parquetPaths.resolve(parquetPath);
  }

  /**
   * Resolves a Delta log statistics column reference.
   *
   * <p>Delta expresses nested statistics as a multi-part name — {@code ["s", "a"]} for {@code s.a}
   * — and descends through struct fields only, never into a collection. Which spelling each segment
   * is matched against follows the mapping mode: physical names once mapping is enabled, since that
   * is what the writer emitted; logical names when it is off; and, only when the mode was not
   * supplied, either spelling with the physical one winning a collision. Returns empty when any
   * segment is unknown, rather than attributing the value to an ancestor.
   */
  public Optional<SchemaNode> nodeForStatsNames(List<String> names) {
    if (names == null
        || names.isEmpty()
        || names.stream().anyMatch(n -> n == null || n.isEmpty())) {
      return Optional.empty();
    }
    return statsNames.resolve(names);
  }
}
