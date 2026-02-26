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

package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ndv.NdvSketch;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import ai.floedb.floecat.types.ValueEncoders;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class ConnectorStatsViewBuilder {

  private ConnectorStatsViewBuilder() {}

  public static TableStats toTableStats(
      ResourceId tableId,
      long snapshotId,
      long upstreamCreatedAtMs,
      TableFormat format,
      StatsEngine.Result<?> result) {

    var upstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            format,
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(upstreamCreatedAtMs),
            Map.of());

    return TableStats.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setUpstream(upstream)
        .setRowCount(result.totalRowCount())
        .setDataFileCount(result.fileCount())
        .setTotalSizeBytes(result.totalSizeBytes())
        .build();
  }

  /**
   * Populates {@link FloecatConnector.ColumnRef} with name + physicalPath + ordinal + fieldId when
   * available. This enables stable column_id resolution for CID_FIELD_ID and CID_PATH_ORDINAL.
   *
   * @param tableTotalRows fallback for NDV rowsTotal when not available or zero in the model
   */
  public static <K> List<FloecatConnector.ColumnStatsView> toColumnStatsView(
      Map<K, StatsEngine.ColumnAgg> columns,
      java.util.function.Function<K, String> nameOf,
      java.util.function.Function<K, String> physicalPathOf,
      java.util.function.ToIntFunction<K> ordinalOf,
      java.util.function.ToIntFunction<K> fieldIdOf,
      java.util.function.Function<K, LogicalType> typeOf,
      long tableTotalRows) {

    List<FloecatConnector.ColumnStatsView> out = new ArrayList<>(columns.size());
    for (var e : columns.entrySet()) {
      K key = e.getKey();
      var agg = e.getValue();

      String name = nameOf.apply(key);
      String physicalPath = physicalPathOf == null ? "" : physicalPathOf.apply(key);
      int ordinal = ordinalOf == null ? 0 : Math.max(0, ordinalOf.applyAsInt(key));
      int fieldId = fieldIdOf == null ? 0 : Math.max(0, fieldIdOf.applyAsInt(key));
      LogicalType lt = typeOf.apply(key);

      // Populate ref as fully as we can.
      var ref =
          new FloecatConnector.ColumnRef(
              name == null ? "" : name, physicalPath == null ? "" : physicalPath, ordinal, fieldId);

      String logicalTypeStr = (lt == null) ? "" : LogicalTypeProtoAdapter.encodeLogicalType(lt);

      String min = null;
      String max = null;
      if (lt != null && LogicalComparators.isOrderable(lt)) {
        if (agg.min() != null) min = ValueEncoders.encodeToString(lt, agg.min());
        if (agg.max() != null) max = ValueEncoders.encodeToString(lt, agg.max());
      }

      Ndv ndv = null;
      if (agg.ndvExact() != null) {
        ndv = Ndv.newBuilder().setExact(agg.ndvExact()).build();
      } else if (agg.ndv() != null) {
        var model = agg.ndv();
        var ndvBuilder = Ndv.newBuilder();
        boolean hasPayload = false;

        if (model.approx != null) {
          var ndvApprox = model.approx;
          var approx = NdvApprox.newBuilder();
          if (ndvApprox.estimate != null) approx.setEstimate(ndvApprox.estimate);
          if (ndvApprox.rse != null) approx.setRelativeStandardError(ndvApprox.rse);
          if (ndvApprox.ciLower != null) approx.setConfidenceLower(ndvApprox.ciLower);
          if (ndvApprox.ciUpper != null) approx.setConfidenceUpper(ndvApprox.ciUpper);
          if (ndvApprox.ciLevel != null) approx.setConfidenceLevel(ndvApprox.ciLevel);
          if (ndvApprox.rowsSeen != null) approx.setRowsSeen(ndvApprox.rowsSeen);
          if (ndvApprox.rowsTotal != null && ndvApprox.rowsTotal > 0) {
            approx.setRowsTotal(ndvApprox.rowsTotal);
          } else if (tableTotalRows > 0) {
            approx.setRowsTotal(tableTotalRows);
          }
          if (ndvApprox.method != null) approx.setMethod(ndvApprox.method);
          if (ndvApprox.params != null && !ndvApprox.params.isEmpty())
            approx.putAllProperties(ndvApprox.params);
          ndvBuilder.setApprox(approx);
          hasPayload = true;
        }

        if (model.sketches != null && !model.sketches.isEmpty()) {
          for (NdvSketch s : model.sketches) {
            var sb = ai.floedb.floecat.catalog.rpc.NdvSketch.newBuilder();
            if (s.type != null) sb.setType(s.type);
            if (s.data != null) sb.setData(ByteString.copyFrom(s.data));
            if (s.encoding != null) sb.setEncoding(s.encoding);
            if (s.compression != null) sb.setCompression(s.compression);
            if (s.version != null) sb.setVersion(s.version);
            if (s.params != null && !s.params.isEmpty()) sb.putAllProperties(s.params);
            ndvBuilder.addSketches(sb);
          }
          hasPayload = true;
        }

        if (hasPayload) ndv = ndvBuilder.build();
      }

      out.add(
          new FloecatConnector.ColumnStatsView(
              ref,
              logicalTypeStr,
              agg.valueCount(),
              agg.nullCount(),
              agg.nanCount(),
              min,
              max,
              ndv,
              Map.of()));
    }
    return out;
  }

  /**
   * Simplified conversion for connectors that only provide name and type. Uses empty values for
   * physicalPath, ordinal, and fieldId.
   */
  public static <K> List<FloecatConnector.FileColumnStatsView> toFileColumnStatsView(
      List<StatsEngine.FileAgg<K>> files,
      java.util.function.Function<K, String> nameOf,
      java.util.function.Function<K, LogicalType> typeOf) {
    return toFileColumnStatsView(files, nameOf, null, null, null, typeOf);
  }

  /**
   * Converts file aggregates to FileColumnStatsView records with full column reference support.
   * This enables stable column_id resolution for CID_FIELD_ID and CID_PATH_ORDINAL across all
   * connectors (Delta, Iceberg, etc.) by consolidating the per-file stats transformation logic.
   *
   * @param files the file aggregates from StatsEngine
   * @param nameOf function to extract column name from key
   * @param physicalPathOf optional function to extract physical path from key (null ok)
   * @param ordinalOf optional function to extract ordinal from key (null ok)
   * @param fieldIdOf optional function to extract field ID from key (null ok)
   * @param typeOf function to extract LogicalType from key
   * @return list of FileColumnStatsView records with ColumnRef fully populated
   */
  public static <K> List<FloecatConnector.FileColumnStatsView> toFileColumnStatsView(
      List<StatsEngine.FileAgg<K>> files,
      java.util.function.Function<K, String> nameOf,
      java.util.function.Function<K, String> physicalPathOf,
      java.util.function.ToIntFunction<K> ordinalOf,
      java.util.function.ToIntFunction<K> fieldIdOf,
      java.util.function.Function<K, LogicalType> typeOf) {

    if (files == null || files.isEmpty()) return List.of();

    List<FloecatConnector.FileColumnStatsView> out = new ArrayList<>(files.size());
    for (var fa : files) {
      // Reuse toColumnStatsView to convert per-file columns, which delegates to our full ColumnRef
      // building logic
      var cols =
          toColumnStatsView(
              fa.columns(), nameOf, physicalPathOf, ordinalOf, fieldIdOf, typeOf, fa.rowCount());

      out.add(
          new FloecatConnector.FileColumnStatsView(
              fa.path(),
              fa.format(),
              fa.rowCount(),
              fa.sizeBytes(),
              fa.isDelete()
                  ? (fa.isEqualityDelete()
                      ? FileContent.FC_EQUALITY_DELETES
                      : FileContent.FC_POSITION_DELETES)
                  : FileContent.FC_DATA,
              fa.partitionDataJson(),
              Math.max(0, fa.partitionSpecId()),
              // Extract equality field IDs from FileAgg; typically populated only for equality
              // delete files in Iceberg.
              fa.equalityFieldIds() == null ? List.of() : fa.equalityFieldIds(),
              fa.sequenceNumber(),
              java.util.Collections.unmodifiableList(cols)));
    }

    return out;
  }
}
