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

package ai.floedb.floecat.connector.common.resolver;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.*;

/**
 * LogicalSchemaMapper -------------------
 *
 * <p>Converts physical table metadata (Iceberg/Delta or generic catalog schemas) into the unified
 * logical SchemaDescriptor consumed by the query planner.
 */
public class LogicalSchemaMapper {

  /**
   * Build a logical SchemaDescriptor from catalog metadata and a schema JSON string.
   *
   * <p>Caller is responsible for selecting the correct schemaJson (table schema vs snapshot
   * schema).
   */
  public SchemaDescriptor map(Table table, String schemaJson) {
    if (schemaJson == null || schemaJson.isBlank()) {
      return SchemaDescriptor.getDefaultInstance();
    }

    TableFormat fmt = table.getUpstream().getFormat();
    ColumnIdAlgorithm cid_algo = table.getUpstream().getColumnIdAlgorithm();
    Set<String> partitionKeys = new HashSet<>(table.getUpstream().getPartitionKeysList());

    return mapInternal(cid_algo, fmt, schemaJson, partitionKeys);
  }

  /** Builds a logical schema descriptor directly from a cached {@link UserTableNode}. */
  public SchemaDescriptor map(UserTableNode node, String overrideSchemaJson) {
    String schemaJson =
        (overrideSchemaJson == null || overrideSchemaJson.isBlank())
            ? node.schemaJson()
            : overrideSchemaJson;
    if (schemaJson == null || schemaJson.isBlank()) {
      return SchemaDescriptor.getDefaultInstance();
    }
    return mapInternal(
        node.columnIdAlgorithm(), node.format(), schemaJson, new HashSet<>(node.partitionKeys()));
  }

  public SchemaDescriptor map(UserTableNode node) {
    return map(node, node.schemaJson());
  }

  public SchemaDescriptor mapRaw(
      ColumnIdAlgorithm cid_algo,
      TableFormat format,
      String schemaJson,
      Set<String> partitionKeys) {

    if (schemaJson == null || schemaJson.isBlank()) {
      return SchemaDescriptor.getDefaultInstance();
    }

    Set<String> pk = (partitionKeys == null) ? Set.of() : partitionKeys;
    return mapInternal(cid_algo, format, schemaJson, new HashSet<>(pk));
  }

  private SchemaDescriptor mapInternal(
      ColumnIdAlgorithm cid_algo,
      TableFormat format,
      String schemaJson,
      Set<String> partitionKeys) {
    return switch (format) {
      case TF_ICEBERG -> IcebergSchemaMapper.map(cid_algo, schemaJson, partitionKeys);
      case TF_DELTA -> DeltaSchemaMapper.map(cid_algo, schemaJson, partitionKeys);
      default -> GenericSchemaMapper.map(cid_algo, schemaJson);
    };
  }

  /**
   * Build a map of column name -> ordinal for all columns (nested included) from a schema.
   *
   * <p>This is critical for CID_PATH_ORDINAL: ColumnIdComputer requires ordinal > 0 to compute a
   * valid column_id. Stats engines include nested columns (e.g., "struct.field"), so ordinals must
   * be extracted for all of them from the schema, not just top-level fields.
   *
   * <p>This is a shared utility for all connectors needing to extract ordinals from schemas.
   *
   * @param cid_algo column ID algorithm (typically CID_PATH_ORDINAL for Delta/generic)
   * @param format table format (TF_DELTA, TF_ICEBERG, etc.)
   * @param schemaJson schema as JSON string
   * @return map of column name (including nested paths like "struct.field") to 1-based ordinal
   */
  public static Map<String, Integer> buildColumnOrdinals(
      ColumnIdAlgorithm cid_algo, TableFormat format, String schemaJson) {
    var ordinals = new LinkedHashMap<String, Integer>();

    try {
      // Use mapRaw to parse the schema and extract all columns with ordinals.
      // This handles nested structures (struct, array, map) correctly.
      var mapper = new LogicalSchemaMapper();
      var schemaDesc = mapper.mapRaw(cid_algo, format, schemaJson, Set.of());

      if (schemaDesc != null) {
        // Iterate through all columns (top-level and nested) and extract ordinals.
        for (var col : schemaDesc.getColumnsList()) {
          if (col.getOrdinal() > 0) {
            ordinals.put(col.getName(), col.getOrdinal());
          }
        }
      }
    } catch (Exception e) {
      // If schema mapping fails, log warning and fall back to empty ordinals.
      // This ensures stats are skipped (with ordinal=0) rather than causing exceptions.
      // Callers should detect empty results and handle appropriately.
      System.err.println(
          "WARN: Failed to extract column ordinals from schema (format="
              + format
              + "): "
              + e.getMessage());
    }

    return ordinals;
  }
}
