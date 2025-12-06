package ai.floedb.metacat.gateway.iceberg.rest.resources.snapshot;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PageDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PartitionSpecDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.SnapshotDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.SnapshotsResponse;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.SnapshotRequests;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.SnapshotSupport;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import com.google.protobuf.Timestamp;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/tables/{table}/snapshots")
@Produces(MediaType.APPLICATION_JSON)
public class SnapshotResource {
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
    List<SnapshotDto> snapshots =
        resp.getSnapshotsList().stream().map(this::toDto).collect(Collectors.toList());
    return Response.ok(new SnapshotsResponse(snapshots, toDto(resp.getPage()))).build();
  }

  @Path("/{snapshotId}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("snapshotId") long snapshotId) {
    ResourceId tableId = SnapshotSupport.resolveTableId(grpc, config, prefix, namespace, table);
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    var resp =
        stub.getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(
                    ai.floedb.metacat.common.rpc.SnapshotRef.newBuilder().setSnapshotId(snapshotId))
                .build());
    return Response.ok(toDto(resp.getSnapshot())).build();
  }

  @POST
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      SnapshotRequests.Create req) {
    ResourceId tableId = SnapshotSupport.resolveTableId(grpc, config, prefix, namespace, table);

    SnapshotSpec.Builder spec = SnapshotSpec.newBuilder().setTableId(tableId);
    if (req != null) {
      if (req.snapshotId() != null) {
        spec.setSnapshotId(req.snapshotId());
      }
      if (req.parentSnapshotId() != null) {
        spec.setParentSnapshotId(req.parentSnapshotId());
      }
      if (req.upstreamCreatedAt() != null) {
        spec.setUpstreamCreatedAt(fromIso(req.upstreamCreatedAt()));
      }
      if (req.ingestedAt() != null) {
        spec.setIngestedAt(fromIso(req.ingestedAt()));
      }
    }

    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    var created = stub.createSnapshot(CreateSnapshotRequest.newBuilder().setSpec(spec).build());
    return Response.status(Response.Status.CREATED).entity(toDto(created.getSnapshot())).build();
  }

  @Path("/{snapshotId}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("snapshotId") long snapshotId) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    stub.deleteSnapshot(
        DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId).build());
    return Response.noContent().build();
  }

  @Path("/{snapshotId}/rollback")
  @POST
  public Response rollback(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("snapshotId") long snapshotId) {
    return IcebergErrorResponses.unsupported("rollback not implemented");
  }

  private SnapshotDto toDto(ai.floedb.metacat.catalog.rpc.Snapshot snapshot) {
    return new SnapshotDto(
        snapshot.hasTableId() ? snapshot.getTableId().getId() : null,
        snapshot.getSnapshotId(),
        toIso(snapshot.getUpstreamCreatedAt()),
        toIso(snapshot.getIngestedAt()),
        snapshot.getParentSnapshotId() == 0 ? null : snapshot.getParentSnapshotId(),
        snapshot.getSchemaJson(),
        PartitionSpecDto.fromProto(snapshot.getPartitionSpec()));
  }

  private String toIso(Timestamp ts) {
    if (ts == null) {
      return null;
    }
    return Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos()).toString();
  }

  private Timestamp fromIso(String iso) {
    Instant inst = Instant.parse(iso);
    return Timestamp.newBuilder()
        .setSeconds(inst.getEpochSecond())
        .setNanos(inst.getNano())
        .build();
  }

  private PageDto toDto(PageResponse page) {
    return new PageDto(page.getNextPageToken(), page.getTotalSize());
  }
}
