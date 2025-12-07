package ai.floedb.floecat.gateway.iceberg.rest.resources.support;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;

public final class SnapshotSupport {
  private SnapshotSupport() {}

  public static ResourceId resolveTableId(
      GrpcWithHeaders grpc,
      IcebergGatewayConfig config,
      String prefix,
      String namespace,
      String table) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    return NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
  }

  public static ListSnapshotsRequest.Builder listSnapshots(
      ResourceId tableId, String pageToken, Integer pageSize) {
    ListSnapshotsRequest.Builder builder = ListSnapshotsRequest.newBuilder().setTableId(tableId);
    var page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
      builder.setPage(page);
    }
    return builder;
  }
}
