package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.FileColumnStats;
import ai.floedb.metacat.catalog.rpc.FileContent;
import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListFileColumnStatsResponse;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/snapshots/{snapshot}/stats")
public class StatsResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;

  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("snapshot") long snapshot,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    String catalog = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalog, NamespacePaths.split(namespace), table);
    SnapshotRef ref = SnapshotRef.newBuilder().setSnapshotId(snapshot).build();
    TableStatisticsServiceBlockingStub stub = grpc.withHeaders(grpc.raw().stats());

    PageRequest.Builder page = PageRequest.newBuilder();
    if (pageToken != null) page.setPageToken(pageToken);
    if (pageSize != null) page.setPageSize(pageSize);

    GetTableStatsResponse tableResp =
        stub.getTableStats(
            GetTableStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(ref)
                .setPage(page)
                .build());
    ListColumnStatsResponse colResp =
        stub.listColumnStats(
            ListColumnStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(ref)
                .setPage(page)
                .build());
    ListFileColumnStatsResponse fileResp =
        stub.listFileColumnStats(
            ListFileColumnStatsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(ref)
                .setPage(page)
                .build());

    StatsDto stats =
        new StatsDto(
            toDto(tableResp.getStats()),
            colResp.getColumnsList().stream().map(this::toDto).collect(Collectors.toList()),
            fileResp.getFileColumnsList().stream().map(this::toDto).collect(Collectors.toList()));
    return Response.ok(stats).build();
  }

  private TableStatsDto toDto(ai.floedb.metacat.catalog.rpc.TableStats stats) {
    return new TableStatsDto(
        stats.getSnapshotId(),
        stats.getRowCount(),
        stats.getDataFileCount(),
        stats.getTotalSizeBytes(),
        stats.getPropertiesMap(),
        toUpstream(stats.getUpstream()),
        toNdv(stats.getNdv()));
  }

  private ColumnStatsDto toDto(ai.floedb.metacat.catalog.rpc.ColumnStats stats) {
    return new ColumnStatsDto(
        stats.getColumnId(),
        stats.getColumnName(),
        stats.getLogicalType(),
        stats.getValueCount(),
        stats.getNullCount(),
        stats.getNanCount(),
        toUpstream(stats.getUpstream()),
        toNdv(stats.getNdv()),
        stats.getMin(),
        stats.getMax());
  }

  private FileColumnStatsDto toDto(FileColumnStats stats) {
    return new FileColumnStatsDto(
        stats.getFilePath(),
        stats.getRowCount(),
        stats.getSizeBytes(),
        stats.getColumnsList().stream().map(this::toDto).collect(Collectors.toList()),
        fileContent(stats.getFileContent()),
        stats.getPartitionDataJson(),
        stats.getPartitionSpecId(),
        stats.getEqualityFieldIdsList());
  }

  private NdvDto toNdv(ai.floedb.metacat.catalog.rpc.Ndv ndv) {
    if (ndv == null) {
      return null;
    }
    ai.floedb.metacat.catalog.rpc.NdvApprox approx = ndv.getApprox();
    NdvApproxDto approxDto =
        approx == null
            ? null
            : new NdvApproxDto(
                approx.getEstimate(),
                approx.getRelativeStandardError(),
                approx.getConfidenceLower(),
                approx.getConfidenceUpper(),
                approx.getConfidenceLevel(),
                approx.getRowsSeen(),
                approx.getRowsTotal(),
                approx.getMethod());
    List<NdvSketchDto> sketches =
        ndv.getSketchesList().stream()
            .map(
                s ->
                    new NdvSketchDto(
                        s.getType(), s.getEncoding(), s.getCompression(), s.getVersion()))
            .collect(Collectors.toList());
    return new NdvDto(ndv.getExact(), approxDto, sketches);
  }

  private UpstreamStampDto toUpstream(ai.floedb.metacat.catalog.rpc.UpstreamStamp stamp) {
    if (stamp == null) {
      return null;
    }
    return new UpstreamStampDto(
        stamp.getSystem().name(),
        stamp.getTableNativeId(),
        stamp.getCommitRef(),
        stamp.hasFetchedAt() ? stamp.getFetchedAt().toString() : null,
        stamp.getPropertiesMap());
  }

  private FileContentDto fileContent(FileContent content) {
    return switch (content) {
      case FC_DATA -> FileContentDto.DATA;
      case FC_POSITION_DELETES -> FileContentDto.POSITION_DELETES;
      case FC_EQUALITY_DELETES -> FileContentDto.EQUALITY_DELETES;
      default -> FileContentDto.UNSPECIFIED;
    };
  }

  private String resolveCatalog(String prefix) {
    return Optional.ofNullable(config.catalogMapping().get(prefix)).orElse(prefix);
  }
}
