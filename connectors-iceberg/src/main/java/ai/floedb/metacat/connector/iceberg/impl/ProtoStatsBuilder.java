package ai.floedb.metacat.connector.iceberg.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.Ndv;
import ai.floedb.metacat.catalog.rpc.NdvApprox;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.common.StatsEngine;
import ai.floedb.metacat.connector.common.ndv.NdvSketch;
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
      Function<K, LogicalType> typeOf,
      long upstreamCreatedAtMs,
      long tableTotalRows) {
    var commonUpstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            format,
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(upstreamCreatedAtMs),
            Map.of());

    List<ColumnStats> list = new ArrayList<>(columns.size());
    for (var columnAgg : columns.entrySet()) {
      K id = columnAgg.getKey();
      var agg = columnAgg.getValue();
      var name = nameOf.apply(id);
      var logicalType = typeOf.apply(id);

      boolean wroteNdv = false;

      var columnStatBuilder =
          ColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setColumnId(String.valueOf(id))
              .setColumnName(name == null ? "" : name)
              .setUpstream(commonUpstream);

      if (agg.nullCount() != null) {
        columnStatBuilder.setNullCount(agg.nullCount());
      }

      if (agg.nanCount() != null) {
        columnStatBuilder.setNanCount(agg.nanCount());
      }

      if (logicalType != null)
        columnStatBuilder.setLogicalType(LogicalTypeProtoAdapter.encodeLogicalType(logicalType));

      if (agg.ndvExact() != null) {
        columnStatBuilder.setNdv(Ndv.newBuilder().setExact(agg.ndvExact()).build());
        wroteNdv = true;
      } else if (agg.ndv() != null) {
        var model = agg.ndv();
        var ndvBuilder = Ndv.newBuilder();
        boolean hasPayload = false;

        if (model.approx != null) {
          var ndvApprox = model.approx;
          var approx = NdvApprox.newBuilder();

          if (ndvApprox.estimate != null) {
            approx.setEstimate(ndvApprox.estimate);
          }
          if (ndvApprox.rse != null) {
            approx.setRelativeStandardError(ndvApprox.rse);
          }
          if (ndvApprox.ciLower != null) {
            approx.setConfidenceLower(ndvApprox.ciLower);
          }
          if (ndvApprox.ciUpper != null) {
            approx.setConfidenceUpper(ndvApprox.ciUpper);
          }
          if (ndvApprox.ciLevel != null) {
            approx.setConfidenceLevel(ndvApprox.ciLevel);
          }
          if (ndvApprox.rowsSeen != null) {
            approx.setRowsSeen(ndvApprox.rowsSeen);
          }
          if (ndvApprox.rowsTotal != null) {
            approx.setRowsTotal(ndvApprox.rowsTotal);
          } else if (tableTotalRows > 0) {
            approx.setRowsTotal(tableTotalRows);
          }
          if (ndvApprox.method != null) {
            approx.setMethod(ndvApprox.method);
          }
          if (ndvApprox.params != null && !ndvApprox.params.isEmpty()) {
            approx.putAllParams(ndvApprox.params);
          }
          ndvBuilder.setApprox(approx);
          hasPayload = true;
        }

        if (model.sketches != null && !model.sketches.isEmpty()) {
          for (NdvSketch s : model.sketches) {
            var sb = ai.floedb.metacat.catalog.rpc.NdvSketch.newBuilder();

            if (s.type != null) {
              sb.setType(s.type);
            }
            if (s.data != null) {
              sb.setData(ByteString.copyFrom(s.data));
            }
            if (s.encoding != null) {
              sb.setEncoding(s.encoding);
            }
            if (s.compression != null) {
              sb.setCompression(s.compression);
            }
            if (s.version != null) {
              sb.setVersion(s.version);
            }
            if (s.params != null && !s.params.isEmpty()) {
              sb.putAllParams(s.params);
            }
            ndvBuilder.addSketches(sb);
          }
          hasPayload = true;
        }

        if (hasPayload) {
          columnStatBuilder.setNdv(ndvBuilder.build());
          wroteNdv = true;
        }
      }

      if (logicalType != null) {
        if (agg.min() != null) {
          columnStatBuilder.setMin(ValueEncoders.encodeToString(logicalType, agg.min()));
        }

        if (agg.max() != null) {
          columnStatBuilder.setMax(ValueEncoders.encodeToString(logicalType, agg.max()));
        }
      }

      list.add(columnStatBuilder.build());
    }

    return list;
  }
}
