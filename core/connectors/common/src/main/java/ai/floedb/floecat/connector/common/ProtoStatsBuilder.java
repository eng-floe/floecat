package ai.floedb.floecat.connector.common;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.ndv.NdvSketch;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import ai.floedb.floecat.types.ValueEncoders;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class ProtoStatsBuilder {

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

  public static <K> List<ColumnStats> toColumnStats(
      ResourceId tableId,
      long snapshotId,
      TableFormat format,
      Map<K, StatsEngine.ColumnAgg> columns,
      Function<K, String> nameOf,
      Function<K, Integer> idOf,
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
      K key = columnAgg.getKey();
      var agg = columnAgg.getValue();
      var name = nameOf.apply(key);
      var columnId = idOf.apply(key);
      var logicalType = typeOf.apply(key);

      boolean wroteNdv = false;

      var columnStatBuilder =
          ColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setColumnId(columnId)
              .setColumnName(name == null ? "" : name)
              .setUpstream(commonUpstream);

      if (agg.valueCount() != null) {
        columnStatBuilder.setValueCount(agg.valueCount());
      }

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
            approx.putAllProperties(ndvApprox.params);
          }
          ndvBuilder.setApprox(approx);
          hasPayload = true;
        }

        if (model.sketches != null && !model.sketches.isEmpty()) {
          for (NdvSketch s : model.sketches) {
            var sb = ai.floedb.floecat.catalog.rpc.NdvSketch.newBuilder();

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
              sb.putAllProperties(s.params);
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
          String encoded = ValueEncoders.encodeToString(logicalType, agg.min());
          if (encoded != null) {
            columnStatBuilder.setMin(encoded);
          }
        }

        if (agg.max() != null) {
          String encoded = ValueEncoders.encodeToString(logicalType, agg.max());
          if (encoded != null) {
            columnStatBuilder.setMax(encoded);
          }
        }
      }

      list.add(columnStatBuilder.build());
    }

    return list;
  }

  public static <K> List<FileColumnStats> toFileColumnStats(
      ResourceId tableId,
      long snapshotId,
      TableFormat format,
      List<StatsEngine.FileAgg<K>> files,
      Function<K, String> nameOf,
      Function<K, Integer> idOf,
      Function<K, LogicalType> typeOf,
      long upstreamCreatedAtMs) {

    if (files == null || files.isEmpty()) {
      return List.of();
    }

    var commonUpstream =
        LogicalTypeProtoAdapter.upstreamStamp(
            format,
            "",
            Long.toString(snapshotId),
            Timestamps.fromMillis(upstreamCreatedAtMs),
            Map.of());

    List<FileColumnStats> out = new ArrayList<>(files.size());

    for (StatsEngine.FileAgg<K> fa : files) {
      List<ColumnStats> cols = new ArrayList<>(fa.columns().size());

      for (var entry : fa.columns().entrySet()) {
        K colKey = entry.getKey();
        StatsEngine.ColumnAgg agg = entry.getValue();

        String name = nameOf.apply(colKey);
        Integer columnId = idOf.apply(colKey);
        LogicalType logicalType = typeOf.apply(colKey);

        var colBuilder =
            ColumnStats.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(snapshotId)
                .setColumnId(columnId)
                .setColumnName(name == null ? "" : name)
                .setUpstream(commonUpstream);

        if (agg.valueCount() != null) {
          colBuilder.setValueCount(agg.valueCount());
        }
        if (agg.nullCount() != null) {
          colBuilder.setNullCount(agg.nullCount());
        }
        if (agg.nanCount() != null) {
          colBuilder.setNanCount(agg.nanCount());
        }

        if (logicalType != null) {
          colBuilder.setLogicalType(LogicalTypeProtoAdapter.encodeLogicalType(logicalType));

          if (agg.min() != null) {
            String encoded = ValueEncoders.encodeToString(logicalType, agg.min());
            if (encoded != null) {
              colBuilder.setMin(encoded);
            }
          }
          if (agg.max() != null) {
            String encoded = ValueEncoders.encodeToString(logicalType, agg.max());
            if (encoded != null) {
              colBuilder.setMax(encoded);
            }
          }
        }

        if (agg.ndvExact() != null) {
          colBuilder.setNdv(Ndv.newBuilder().setExact(agg.ndvExact()).build());
        } else if (agg.ndv() != null) {
          // ignore for now
        }

        cols.add(colBuilder.build());
      }

      out.add(
          FileColumnStats.newBuilder()
              .setTableId(tableId)
              .setSnapshotId(snapshotId)
              .setFilePath(fa.path())
              .setFileFormat(fa.format())
              .setRowCount(fa.rowCount())
              .setSizeBytes(fa.sizeBytes())
              .setPartitionDataJson(fa.partitionDataJson() == null ? "" : fa.partitionDataJson())
              .setPartitionSpecId(Math.max(0, fa.partitionSpecId()))
              .setFileContent(
                  fa.isDelete()
                      ? (fa.isEqualityDelete()
                          ? FileContent.FC_EQUALITY_DELETES
                          : FileContent.FC_POSITION_DELETES)
                      : FileContent.FC_DATA)
              .addAllColumns(cols)
              .build());
    }

    return out;
  }
}
