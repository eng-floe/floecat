package ai.floedb.metacat.gateway.iceberg.rest.resources.snapshot;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.*;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.*;
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
import java.util.Map;
import java.util.Optional;
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);

    ListSnapshotsRequest.Builder req = ListSnapshotsRequest.newBuilder().setTableId(tableId);
    if (pageToken != null || pageSize != null) {
      PageRequest.Builder page = PageRequest.newBuilder();
      if (pageToken != null) {
        page.setPageToken(pageToken);
      }
      if (pageSize != null) {
        page.setPageSize(pageSize);
      }
      req.setPage(page);
    }

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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);

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
    String catalogName = resolveCatalog(prefix);
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
    // Underlying API does not yet expose rollback; return 501.
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            new IcebergErrorResponse(
                new IcebergError("rollback not implemented", "UnsupportedOperationException", 501)))
        .build();
  }

  private String resolveCatalog(String prefix) {
    Map<String, String> mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
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
