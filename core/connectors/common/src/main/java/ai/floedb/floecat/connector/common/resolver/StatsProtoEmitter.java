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
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * Converts connector stats objects (ColumnStatsView, FileColumnStatsView) into RPC protos
 * (ColumnStats, FileColumnStats), computing stable column IDs based on schema.
 *
 * <p>Handles both FIELD_ID and PATH_ORDINAL column ID algorithms.
 */
public final class StatsProtoEmitter {
  private static final Logger LOG = Logger.getLogger(StatsProtoEmitter.class);

  private StatsProtoEmitter() {}

  /**
   * Build a map from canonical path to column ID, derived from schema using the configured
   * algorithm.
   */
  private static Map<String, Long> buildIdByCanonicalPath(
      ColumnIdAlgorithm algo, SchemaDescriptor schema) {

    if (schema == null || schema.getColumnsCount() == 0) {
      return Map.of();
    }

    var out = new LinkedHashMap<String, Long>(schema.getColumnsCount());
    for (SchemaColumn c : schema.getColumnsList()) {
      if (c == null) continue;

      String p = c.getPhysicalPath();
      if (p == null || p.isBlank()) {
        p = c.getName();
      }
      if (p == null || p.isBlank()) {
        continue;
      }

      String key = ColumnIdComputer.canonicalizePath(p);
      long id = ColumnIdComputer.compute(algo, c);
      if (id != 0L) {
        out.put(key, id);
      }
    }
    return out;
  }

  /**
   * Resolve a column's stable ID from its ColumnRef using the configured algorithm.
   *
   * <p>Returns 0L if resolution fails, which signals the stats ingester to skip that column.
   * Unresolvable columns are logged at WARN level for observability.
   */
  private static long resolveId(
      ColumnIdAlgorithm algo, Map<String, Long> idByPath, FloecatConnector.ColumnRef ref) {

    if (ref == null) return 0L;

    // FIELD_ID policy: use field_id only; do NOT look at path/name/ordinal
    if (algo == ColumnIdAlgorithm.CID_FIELD_ID) {
      if (ref.fieldId() > 0) {
        return (long) ref.fieldId();
      }
      // Field ID not available; cannot resolve under FIELD_ID algorithm
      LOG.warn(
          "Cannot resolve column ID: FIELD_ID algorithm requires fieldId > 0, got: "
              + ref.fieldId()
              + " for column: "
              + ref.name());
      return 0L;
    }

    // PATH_ORDINAL policy: resolve by canonical schema path (preferred); fallback to name
    String path = ref.physicalPath();
    if (path == null || path.isBlank()) {
      path = ref.name();
    }
    if (path == null || path.isBlank()) {
      // Both physicalPath and name are missing; cannot compute stable ID
      LOG.warn("Cannot resolve column ID: both physicalPath and name are blank/null");
      return 0L;
    }

    String canonicalPath = ColumnIdComputer.canonicalizePath(path);
    Long id = idByPath.get(canonicalPath);
    if (id != null) {
      return id.longValue();
    }
    // Path not found in schema; column may have been dropped or renamed
    LOG.warn(
        "Cannot resolve column ID: path '"
            + canonicalPath
            + "' not found in schema (may be dropped/renamed column: "
            + ref.name()
            + ")");
    return 0L;
  }

  /**
   * Convert connector ColumnStatsView objects to RPC ColumnStats protos, computing stable column
   * IDs.
   */
  public static List<ColumnStats> toColumnStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.ColumnStatsView> in) {

    var upstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            toTableFormat(format),
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(Math.max(0L, upstreamCreatedAtMs)),
            Map.of());

    Map<String, Long> idByPath = buildIdByCanonicalPath(algo, schema);

    var out = new ArrayList<ColumnStats>(in.size());
    for (var v : in) {
      if (v == null || v.ref() == null) continue;

      long colId = resolveId(algo, idByPath, v.ref());
      if (colId == 0L) {
        // If we can't resolve, skip (safe: you'll store no stats for that column)
        continue;
      }

      var b =
          ColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setColumnId(colId)
              .setColumnName(v.ref().name() == null ? "" : v.ref().name())
              .setUpstream(upstream);

      if (v.valueCount() != null) b.setValueCount(v.valueCount());
      if (v.nullCount() != null) b.setNullCount(v.nullCount());
      if (v.nanCount() != null) b.setNanCount(v.nanCount());
      if (v.logicalType() != null && !v.logicalType().isBlank()) b.setLogicalType(v.logicalType());
      if (v.min() != null) b.setMin(v.min());
      if (v.max() != null) b.setMax(v.max());
      if (v.ndv() != null) b.setNdv(v.ndv());
      if (v.properties() != null && !v.properties().isEmpty()) b.putAllProperties(v.properties());

      out.add(b.build());
    }
    return Collections.unmodifiableList(out);
  }

  /**
   * Convert connector FileColumnStatsView objects to RPC FileColumnStats protos, including
   * per-column stats.
   */
  public static List<FileColumnStats> toFileColumnStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.FileColumnStatsView> in) {

    if (in == null || in.isEmpty()) {
      return List.of();
    }

    var out = new ArrayList<FileColumnStats>(in.size());

    for (var fv : in) {
      if (fv == null) continue;

      var cols =
          (fv.columns() == null || fv.columns().isEmpty())
              ? List.<ColumnStats>of()
              : toColumnStats(
                  tableId, snapshotId, upstreamCreatedAtMs, format, algo, schema, fv.columns());

      var fb =
          FileColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setFilePath(fv.filePath() == null ? "" : fv.filePath())
              .setFileFormat(fv.fileFormat() == null ? "" : fv.fileFormat())
              .setRowCount(fv.rowCount())
              .setSizeBytes(fv.sizeBytes())
              .setPartitionDataJson(fv.partitionDataJson() == null ? "" : fv.partitionDataJson())
              .setPartitionSpecId(Math.max(0, fv.partitionSpecId()))
              .setFileContent(fv.fileContent() == null ? FileContent.FC_DATA : fv.fileContent())
              .addAllColumns(cols);

      if (fv.equalityFieldIds() != null && !fv.equalityFieldIds().isEmpty()) {
        fb.addAllEqualityFieldIds(fv.equalityFieldIds());
      }
      if (fv.sequenceNumber() != null && fv.sequenceNumber() > 0) {
        fb.setSequenceNumber(fv.sequenceNumber());
      }

      out.add(fb.build());
    }

    return Collections.unmodifiableList(out);
  }

  /** Helper to convert ConnectorFormat to TableFormat proto. */
  private static TableFormat toTableFormat(ConnectorFormat fmt) {
    return switch (fmt) {
      case CF_ICEBERG -> TableFormat.TF_ICEBERG;
      case CF_DELTA -> TableFormat.TF_DELTA;
      default -> TableFormat.TF_UNKNOWN;
    };
  }
}
