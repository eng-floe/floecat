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
import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
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
 * Converts connector stats objects (ColumnStatsView, FileColumnStatsView) into target-native RPC
 * records (`TargetStatsRecord`) with canonical target identities and scalar/file payloads.
 *
 * <p>Handles both FIELD_ID and PATH_ORDINAL column ID algorithms.
 */
public final class StatsProtoEmitter {
  private static final Logger LOG = Logger.getLogger(StatsProtoEmitter.class);
  private static final StatsMetadata DEFAULT_CONNECTOR_METADATA =
      StatsMetadata.newBuilder()
          .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
          .setCompleteness(StatsCompleteness.SC_COMPLETE)
          .setCaptureMode(StatsCaptureMode.SCM_ASYNC)
          .build();

  private StatsProtoEmitter() {}

  public static TargetStatsRecord tableStatsToTargetRecord(
      ResourceId tableId, long snapshotId, TableValueStats tableValueStats) {
    return tableStatsToTargetRecord(tableId, snapshotId, tableValueStats, null);
  }

  public static TargetStatsRecord tableStatsToTargetRecord(
      ResourceId tableId,
      long snapshotId,
      TableValueStats tableValueStats,
      StatsMetadata metadata) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
        .setMetadata(metadata == null ? DEFAULT_CONNECTOR_METADATA : metadata)
        .setTable(tableValueStats)
        .build();
  }

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

  public static List<TargetStatsRecord> toTargetColumnStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.ColumnStatsView> in) {
    return toTargetColumnStats(
        tableId, snapshotId, upstreamCreatedAtMs, format, algo, schema, in, null);
  }

  public static List<TargetStatsRecord> toTargetColumnStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.ColumnStatsView> in,
      StatsMetadata metadata) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }

    Map<String, Long> idByPath = buildIdByCanonicalPath(algo, schema);
    var upstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            toTableFormat(format),
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(Math.max(0L, upstreamCreatedAtMs)),
            Map.of());
    List<TargetStatsRecord> out = new ArrayList<>(in.size());
    for (var view : in) {
      if (view == null || view.ref() == null) {
        continue;
      }
      long columnId = resolveId(algo, idByPath, view.ref());
      if (columnId <= 0L) {
        continue;
      }

      ScalarStats.Builder scalar =
          ScalarStats.newBuilder()
              .setDisplayName(view.ref().name() == null ? "" : view.ref().name())
              .setLogicalType(view.logicalType() == null ? "" : view.logicalType())
              .setValueCount(view.valueCount() == null ? 0L : view.valueCount())
              .setUpstream(upstream)
              .putAllProperties(view.properties() == null ? Map.of() : view.properties());
      if (view.nullCount() != null) {
        scalar.setNullCount(view.nullCount());
      }
      if (view.nanCount() != null) {
        scalar.setNanCount(view.nanCount());
      }
      if (view.min() != null) {
        scalar.setMin(view.min());
      }
      if (view.max() != null) {
        scalar.setMax(view.max());
      }
      if (view.ndv() != null) {
        scalar.setNdv(view.ndv());
      }

      out.add(
          TargetStatsRecord.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setTarget(
                  StatsTarget.newBuilder()
                      .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId)))
              .setMetadata(metadata == null ? DEFAULT_CONNECTOR_METADATA : metadata)
              .setScalar(scalar)
              .build());
    }
    return Collections.unmodifiableList(out);
  }

  public static List<TargetStatsRecord> toTargetFileStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.FileColumnStatsView> in) {
    return toTargetFileStats(
        tableId, snapshotId, upstreamCreatedAtMs, format, algo, schema, in, null);
  }

  public static List<TargetStatsRecord> toTargetFileStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      ConnectorFormat format,
      ColumnIdAlgorithm algo,
      SchemaDescriptor schema,
      List<FloecatConnector.FileColumnStatsView> in,
      StatsMetadata metadata) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }

    var upstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            toTableFormat(format),
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(Math.max(0L, upstreamCreatedAtMs)),
            Map.of());
    Map<String, Long> idByPath = buildIdByCanonicalPath(algo, schema);

    List<TargetStatsRecord> out = new ArrayList<>(in.size());
    for (var fv : in) {
      if (fv == null) {
        continue;
      }

      List<FileColumnStats> cols = new ArrayList<>();
      if (fv.columns() != null && !fv.columns().isEmpty()) {
        for (var v : fv.columns()) {
          if (v == null || v.ref() == null) {
            continue;
          }
          long colId = resolveId(algo, idByPath, v.ref());
          if (colId == 0L) {
            continue;
          }
          var b =
              ScalarStats.newBuilder()
                  .setDisplayName(v.ref().name() == null ? "" : v.ref().name())
                  .setUpstream(upstream);
          if (v.valueCount() != null) b.setValueCount(v.valueCount());
          if (v.nullCount() != null) b.setNullCount(v.nullCount());
          if (v.nanCount() != null) b.setNanCount(v.nanCount());
          if (v.logicalType() != null && !v.logicalType().isBlank()) {
            b.setLogicalType(v.logicalType());
          }
          if (v.min() != null) b.setMin(v.min());
          if (v.max() != null) b.setMax(v.max());
          if (v.ndv() != null) b.setNdv(v.ndv());
          if (v.properties() != null && !v.properties().isEmpty()) {
            b.putAllProperties(v.properties());
          }
          cols.add(FileColumnStats.newBuilder().setColumnId(colId).setScalar(b.build()).build());
        }
      }

      var file =
          FileTargetStats.newBuilder()
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
        file.addAllEqualityFieldIds(fv.equalityFieldIds());
      }
      if (fv.sequenceNumber() != null && fv.sequenceNumber() > 0) {
        file.setSequenceNumber(fv.sequenceNumber());
      }
      FileTargetStats fileStats = file.build();

      out.add(
          TargetStatsRecord.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setTarget(
                  StatsTarget.newBuilder()
                      .setFile(FileStatsTarget.newBuilder().setFilePath(fileStats.getFilePath())))
              .setMetadata(metadata == null ? DEFAULT_CONNECTOR_METADATA : metadata)
              .setFile(fileStats)
              .build());
    }
    return Collections.unmodifiableList(out);
  }

  public static List<TargetStatsRecord> toTargetColumnStatsFromViews(
      ResourceId tableId,
      long snapshotId,
      ColumnIdAlgorithm algo,
      List<FloecatConnector.ColumnStatsView> in) {
    return toTargetColumnStatsFromViews(tableId, snapshotId, algo, in, null);
  }

  public static List<TargetStatsRecord> toTargetColumnStatsFromViews(
      ResourceId tableId,
      long snapshotId,
      ColumnIdAlgorithm algo,
      List<FloecatConnector.ColumnStatsView> in,
      StatsMetadata metadata) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }
    List<TargetStatsRecord> out = new ArrayList<>(in.size());
    for (var view : in) {
      if (view == null || view.ref() == null) {
        continue;
      }
      long columnId =
          ColumnIdComputer.compute(
              algo,
              view.ref().name(),
              view.ref().physicalPath(),
              view.ref().ordinal(),
              view.ref().fieldId());
      if (columnId <= 0L) {
        continue;
      }
      ScalarStats.Builder scalar =
          ScalarStats.newBuilder()
              .setDisplayName(view.ref().name() == null ? "" : view.ref().name())
              .setLogicalType(view.logicalType() == null ? "" : view.logicalType())
              .setValueCount(view.valueCount() == null ? 0L : view.valueCount())
              .putAllProperties(view.properties() == null ? Map.of() : view.properties());
      if (view.nullCount() != null) {
        scalar.setNullCount(view.nullCount());
      }
      if (view.nanCount() != null) {
        scalar.setNanCount(view.nanCount());
      }
      if (view.min() != null) {
        scalar.setMin(view.min());
      }
      if (view.max() != null) {
        scalar.setMax(view.max());
      }
      if (view.ndv() != null) {
        scalar.setNdv(view.ndv());
      }

      out.add(
          TargetStatsRecord.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setTarget(
                  StatsTarget.newBuilder()
                      .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId)))
              .setMetadata(metadata == null ? DEFAULT_CONNECTOR_METADATA : metadata)
              .setScalar(scalar)
              .build());
    }
    return Collections.unmodifiableList(out);
  }

  public static List<TargetStatsRecord> toTargetFileStatsFromViews(
      ResourceId tableId,
      long snapshotId,
      ColumnIdAlgorithm algo,
      List<FloecatConnector.FileColumnStatsView> in) {
    return toTargetFileStatsFromViews(tableId, snapshotId, algo, in, null);
  }

  public static List<TargetStatsRecord> toTargetFileStatsFromViews(
      ResourceId tableId,
      long snapshotId,
      ColumnIdAlgorithm algo,
      List<FloecatConnector.FileColumnStatsView> in,
      StatsMetadata metadata) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }
    List<TargetStatsRecord> out = new ArrayList<>(in.size());
    for (var fileView : in) {
      if (fileView == null) {
        continue;
      }
      List<FileColumnStats> columns = new ArrayList<>();
      if (fileView.columns() != null && !fileView.columns().isEmpty()) {
        for (var columnView : fileView.columns()) {
          if (columnView == null || columnView.ref() == null) {
            continue;
          }
          long columnId =
              ColumnIdComputer.compute(
                  algo,
                  columnView.ref().name(),
                  columnView.ref().physicalPath(),
                  columnView.ref().ordinal(),
                  columnView.ref().fieldId());
          if (columnId <= 0L) {
            continue;
          }
          ScalarStats.Builder column =
              ScalarStats.newBuilder()
                  .setDisplayName(columnView.ref().name() == null ? "" : columnView.ref().name())
                  .setLogicalType(columnView.logicalType() == null ? "" : columnView.logicalType())
                  .setValueCount(columnView.valueCount() == null ? 0L : columnView.valueCount())
                  .putAllProperties(
                      columnView.properties() == null ? Map.of() : columnView.properties());
          if (columnView.nullCount() != null) {
            column.setNullCount(columnView.nullCount());
          }
          if (columnView.nanCount() != null) {
            column.setNanCount(columnView.nanCount());
          }
          if (columnView.min() != null) {
            column.setMin(columnView.min());
          }
          if (columnView.max() != null) {
            column.setMax(columnView.max());
          }
          if (columnView.ndv() != null) {
            column.setNdv(columnView.ndv());
          }
          columns.add(
              FileColumnStats.newBuilder().setColumnId(columnId).setScalar(column.build()).build());
        }
      }

      FileTargetStats.Builder file =
          FileTargetStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setFilePath(fileView.filePath() == null ? "" : fileView.filePath())
              .setFileFormat(fileView.fileFormat() == null ? "" : fileView.fileFormat())
              .setRowCount(fileView.rowCount())
              .setSizeBytes(fileView.sizeBytes())
              .setFileContent(
                  fileView.fileContent() == null ? FileContent.FC_DATA : fileView.fileContent())
              .setPartitionDataJson(
                  fileView.partitionDataJson() == null ? "" : fileView.partitionDataJson())
              .setPartitionSpecId(Math.max(0, fileView.partitionSpecId()))
              .addAllColumns(columns);
      if (fileView.equalityFieldIds() != null && !fileView.equalityFieldIds().isEmpty()) {
        file.addAllEqualityFieldIds(fileView.equalityFieldIds());
      }
      if (fileView.sequenceNumber() != null && fileView.sequenceNumber() > 0) {
        file.setSequenceNumber(fileView.sequenceNumber());
      }

      out.add(
          TargetStatsRecord.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setTarget(
                  StatsTarget.newBuilder()
                      .setFile(FileStatsTarget.newBuilder().setFilePath(file.getFilePath())))
              .setMetadata(metadata == null ? DEFAULT_CONNECTOR_METADATA : metadata)
              .setFile(file)
              .build());
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
