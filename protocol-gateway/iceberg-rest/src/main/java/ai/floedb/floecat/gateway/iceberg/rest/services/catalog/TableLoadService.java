package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TableLoadService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject SnapshotClient snapshotClient;

  public Response load(
      TableRequestContext tableContext,
      String tableName,
      String snapshots,
      String ifNoneMatch,
      TableGatewaySupport tableSupport) {
    Table tableRecord = tableLifecycleService.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(tableRecord);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(snapshotClient, tableContext.tableId(), snapshotMode, metadata);
    String etagValue = metadataLocation(tableRecord, metadata);
    if (etagMatches(etagValue, ifNoneMatch)) {
      return Response.status(Response.Status.NOT_MODIFIED).tag(etagValue).build();
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                tableRecord,
                metadata,
                snapshotList,
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private boolean etagMatches(String etagValue, String ifNoneMatch) {
    if (etagValue == null || ifNoneMatch == null) {
      return false;
    }
    String token = ifNoneMatch.trim();
    if (token.startsWith("\"") && token.endsWith("\"") && token.length() >= 2) {
      token = token.substring(1, token.length() - 1);
    }
    return token.equals(etagValue);
  }

  private String metadataLocation(Table table, IcebergMetadata metadata) {
    var props =
        table == null || table.getPropertiesMap() == null ? java.util.Map.<String, String>of() : table.getPropertiesMap();
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null
        && !propertyLocation.isBlank()
        && !MetadataLocationUtil.isPointer(propertyLocation)) {
      return propertyLocation;
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return table != null && table.hasResourceId() ? table.getResourceId().getId() : null;
  }
}
