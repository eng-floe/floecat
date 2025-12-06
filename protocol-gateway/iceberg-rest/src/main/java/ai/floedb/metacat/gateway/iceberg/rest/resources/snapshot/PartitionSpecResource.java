package ai.floedb.metacat.gateway.iceberg.rest.resources.snapshot;

import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PageDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PartitionSpecDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PartitionSpecHistoryDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PartitionSpecsResponse;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.SnapshotSupport;
import com.google.protobuf.Timestamp;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/partition-specs")
@Produces(MediaType.APPLICATION_JSON)
public class PartitionSpecResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    ResourceId tableId = SnapshotSupport.resolveTableId(grpc, config, prefix, namespace, table);
    ListSnapshotsRequest.Builder req = SnapshotSupport.listSnapshots(tableId, pageToken, pageSize);

    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    var resp = stub.listSnapshots(req.build());
    List<PartitionSpecHistoryDto> specs =
        resp.getSnapshotsList().stream()
            .map(
                s ->
                    new PartitionSpecHistoryDto(
                        s.getSnapshotId(),
                        PartitionSpecDto.fromProto(s.getPartitionSpec()),
                        toIso(s.getUpstreamCreatedAt()),
                        toIso(s.getIngestedAt())))
            .collect(Collectors.toList());
    return Response.ok(new PartitionSpecsResponse(specs, toDto(resp.getPage()))).build();
  }

  private PageDto toDto(PageResponse page) {
    return new PageDto(page.getNextPageToken(), page.getTotalSize());
  }

  private String toIso(Timestamp ts) {
    if (ts == null) {
      return null;
    }
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()).toString();
  }
}
