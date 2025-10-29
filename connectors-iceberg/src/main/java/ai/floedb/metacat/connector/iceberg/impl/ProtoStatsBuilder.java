package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.Ndv;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.types.LogicalType;
import ai.floedb.metacat.types.LogicalTypeProtoAdapter;
import ai.floedb.metacat.types.ValueEncoders;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class ProtoStatsBuilder {

  static TableStats toTableStats(
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

  static <K> List<ColumnStats> toColumnStats(
      ResourceId tableId,
      long snapshotId,
      TableFormat format,
      Map<K, StatsEngine.ColumnAgg> columns,
      Function<K, String> nameOf,
      Function<K, LogicalType> typeOf) {

    List<ColumnStats> list = new ArrayList<>(columns.size());
    for (var column : columns.entrySet()) {
      K id = column.getKey();
      var columnAgg = column.getValue();
      var name = nameOf.apply(id);
      var logicalType = typeOf.apply(id);

      var builder =
          ColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setColumnId(String.valueOf(id))
              .setColumnName(name == null ? "" : name)
              .setUpstream(
                  LogicalTypeProtoAdapter.upstreamStamp(
                      format,
                      "",
                      Long.toString(snapshotId),
                      Timestamps.fromMillis(System.currentTimeMillis()),
                      java.util.Map.of()));

      if (columnAgg.nullCount() != null) {
        builder.setNullCount(columnAgg.nullCount());
      }

      if (columnAgg.nanCount() != null) {
        builder.setNanCount(columnAgg.nanCount());
      }

      if (logicalType != null) {
        builder.setLogicalType(LogicalTypeProtoAdapter.encodeLogicalType(logicalType));
      }

      if (columnAgg.ndvExact() != null) {
        builder.setNdv(Ndv.newBuilder().setExact(columnAgg.ndvExact()).build());
      } else if (columnAgg.ndvHll() != null) {
        builder.setNdv(
            Ndv.newBuilder().setHllSketch(ByteString.copyFrom(columnAgg.ndvHll())).build());
      }

      if (logicalType != null) {
        if (columnAgg.min() != null) {
          builder.setMin(ValueEncoders.encodeToString(logicalType, columnAgg.min()));
        }

        if (columnAgg.max() != null) {
          builder.setMax(ValueEncoders.encodeToString(logicalType, columnAgg.max()));
        }
      }

      list.add(builder.build());
    }
    return list;
  }
}
