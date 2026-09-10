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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.delta.identity.ColumnMappingMode;
import ai.floedb.floecat.connector.delta.identity.DeltaResolvedSchema;
import ai.floedb.floecat.connector.delta.identity.DeltaSchemaResolver;
import ai.floedb.floecat.schema.identity.SchemaNode;
import io.delta.kernel.Snapshot;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.internal.tablefeatures.TableFeatures;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The bridge between Delta Kernel and Floecat's format-neutral column identity.
 *
 * <p>Column mapping splits a Delta table into two namespaces: the logical names users and the
 * catalog speak, and the physical names that data files, checkpoints and file statistics carry.
 * This class is the only place in the connector that knows which namespace a given Kernel artifact
 * is expressed in, so planners can work in logical names throughout.
 *
 * <p>Everything here is a thin Kernel adapter. The decisions themselves live in {@link
 * ColumnMappingMode} and {@link DeltaResolvedSchema}, which are Kernel-free and directly testable.
 */
final class DeltaColumnMapping {

  private DeltaColumnMapping() {}

  /** Resolves a snapshot's schema together with the mapping mode its readers may trust. */
  static DeltaResolvedSchema resolveSchema(Snapshot snapshot) {
    Objects.requireNonNull(snapshot, "snapshot");
    return DeltaSchemaResolver.resolve(snapshot.getSchema(), effectiveMode(snapshot));
  }

  /**
   * The mapping mode a reader may act on, which is the configured mode only when the table protocol
   * actually supports the feature.
   */
  static ColumnMappingMode effectiveMode(Snapshot snapshot) {
    return ColumnMappingMode.effectiveFromTableProperties(
        snapshot.getTableProperties(), () -> supportsColumnMapping(protocolOf(snapshot)));
  }

  static boolean supportsColumnMapping(Protocol protocol) {
    Objects.requireNonNull(protocol, "protocol");
    return protocol.supportsFeature(TableFeatures.COLUMN_MAPPING_RW_FEATURE);
  }

  /**
   * The schema that Delta writes file statistics against, which column mapping makes physical.
   *
   * <p>Reading statistics against the logical schema of a mapped table would match nothing, so a
   * mapped snapshot that cannot produce its physical schema is an error rather than a silent
   * fallback.
   */
  static StructType statsDataSchema(Snapshot snapshot, DeltaResolvedSchema resolvedSchema) {
    if (!resolvedSchema.effectiveMappingMode().isEnabled()) {
      return snapshot.getSchema();
    }
    if (snapshot instanceof SnapshotImpl snapshotImpl && snapshotImpl.getMetadata() != null) {
      return snapshotImpl.getMetadata().getPhysicalSchema();
    }
    throw new IllegalArgumentException(
        "A column-mapped Delta snapshot must expose its physical schema");
  }

  /**
   * Narrows a statistics schema to the requested columns, keeping the parents of any selected
   * nested column so the surviving struct stays reachable.
   *
   * <p>Names are matched in the namespace of {@code schema}, so callers must pass keys already
   * translated by {@link DeltaResolvedSchema#statsKeysFor(Set)}. Selecting nothing yields an empty
   * struct rather than the whole schema, so an empty selection cannot widen into reading every
   * column's statistics.
   */
  static StructType projectedStatsDataSchema(StructType schema, Set<String> includeColumns) {
    if (schema == null || includeColumns == null) {
      return schema;
    }
    if (includeColumns.isEmpty()) {
      return new StructType();
    }
    return projectedStatsDataSchema(schema, "", includeColumns);
  }

  private static StructType projectedStatsDataSchema(
      StructType schema, String prefix, Set<String> includeColumns) {
    StructType projected = new StructType();
    for (StructField field : schema.fields()) {
      String path = prefix.isEmpty() ? field.getName() : prefix + "." + field.getName();
      if (includeColumns.contains(path)) {
        projected = projected.add(field);
      } else if (field.getDataType() instanceof StructType nested) {
        StructType projectedNested = projectedStatsDataSchema(nested, path, includeColumns);
        if (projectedNested.length() > 0) {
          projected = projected.add(field.withDataType(projectedNested));
        }
      }
    }
    return projected;
  }

  /**
   * Maps a column named by Delta log statistics back to the logical key the planner reports, or
   * {@code null} when it resolves to no column the caller asked for.
   */
  static String logicalNameForStats(
      Column statsColumn, DeltaResolvedSchema schema, Set<String> columnSet) {
    String[] names = statsColumn == null ? null : statsColumn.getNames();
    if (names == null || names.length == 0) {
      return null;
    }
    return logicalName(schema.nodeForStatsNames(Arrays.asList(names)), columnSet);
  }

  /**
   * Maps a Parquet footer column back to the logical key the planner reports, or {@code null} when
   * it resolves to no column the caller asked for.
   */
  static String logicalNameForFooter(
      List<String> parquetPath,
      Integer fieldId,
      DeltaResolvedSchema schema,
      Set<String> columnSet) {
    return logicalName(schema.nodeForFooterColumn(parquetPath, fieldId), columnSet);
  }

  private static String logicalName(Optional<SchemaNode> node, Set<String> columnSet) {
    return node.map(value -> value.path().legacyDottedKey())
        .filter(columnSet::contains)
        .orElse(null);
  }

  /** The key Delta stores a field's physical name under. */
  static final String PHYSICAL_NAME_KEY = DeltaSchemaResolver.PHYSICAL_NAME;

  /**
   * Reads a field's physical name from its Kernel metadata, or {@code null} when the field carries
   * none. Absence is normal for an unmapped table, where logical and physical names coincide.
   */
  static String physicalName(FieldMetadata metadata) {
    return metadata == null ? null : metadata.getString(PHYSICAL_NAME_KEY);
  }

  private static Protocol protocolOf(Snapshot snapshot) {
    if (snapshot instanceof SnapshotImpl snapshotImpl) {
      return snapshotImpl.getProtocol();
    }
    throw new IllegalArgumentException(
        "A Delta snapshot with column mapping configured must expose its protocol");
  }
}
