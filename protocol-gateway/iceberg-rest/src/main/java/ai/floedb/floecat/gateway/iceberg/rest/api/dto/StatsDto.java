package ai.floedb.floecat.gateway.iceberg.rest.api.dto;

import java.util.List;
import java.util.Map;

public record StatsDto(
    TableStatsDto table, List<ColumnStatsDto> columns, List<FileColumnStatsDto> files) {

  public record TableStatsDto(
      long snapshotId,
      long rowCount,
      long dataFileCount,
      long totalSizeBytes,
      Map<String, String> properties,
      UpstreamStampDto upstream,
      NdvDto ndv) {}

  public record ColumnStatsDto(
      int columnId,
      String columnName,
      String logicalType,
      long valueCount,
      long nullCount,
      long nanCount,
      UpstreamStampDto upstream,
      NdvDto ndv,
      String min,
      String max) {}

  public record FileColumnStatsDto(
      String filePath,
      long rowCount,
      long sizeBytes,
      List<ColumnStatsDto> columns,
      FileContentDto content,
      String partitionDataJson,
      int partitionSpecId,
      List<Integer> equalityFieldIds) {}

  public record NdvDto(long exact, NdvApproxDto approx, List<NdvSketchDto> sketches) {}

  public record NdvApproxDto(
      double estimate,
      double relativeStandardError,
      double confidenceLower,
      double confidenceUpper,
      double confidenceLevel,
      long rowsSeen,
      long rowsTotal,
      String method) {}

  public record NdvSketchDto(String type, String encoding, String compression, long version) {}

  public record UpstreamStampDto(
      String system,
      String tableNativeId,
      String commitRef,
      String fetchedAt,
      Map<String, String> properties) {}

  public enum FileContentDto {
    DATA,
    POSITION_DELETES,
    EQUALITY_DELETES,
    UNSPECIFIED
  }
}
