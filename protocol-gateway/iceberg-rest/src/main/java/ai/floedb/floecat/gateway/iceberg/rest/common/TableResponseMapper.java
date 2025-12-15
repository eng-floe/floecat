package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

public final class TableResponseMapper {
  private static final Logger LOG = Logger.getLogger(TableResponseMapper.class);

  private TableResponseMapper() {}

  public static LoadTableResultDto toLoadResult(
      String tableName,
      Table table,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    LOG.debugf(
        "Mapping load result from catalog table=%s metadataLocation=%s configKeys=%s"
            + " requestProps=%s",
        tableName,
        metadata == null ? "<null>" : metadata.getMetadataLocation(),
        configOverrides.keySet(),
        table.getPropertiesMap());
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCatalog(tableName, table, props, metadata, snapshots);
    Map<String, String> effectiveConfig =
        augmentConfigWithMetadataPath(configOverrides, metadataView);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, effectiveConfig, storageCredentials);
  }

  public static CommitTableResponseDto toCommitResponse(
      String tableName, Table table, IcebergMetadata metadata, List<Snapshot> snapshots) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCatalog(tableName, table, props, metadata, snapshots);
    return new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
  }

  public static LoadTableResultDto toLoadResultFromCreate(
      String tableName,
      Table table,
      TableRequests.Create request,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    LOG.debugf(
        "Mapping load result from create table=%s metadataLocation=%s configKeys=%s",
        tableName,
        request == null ? "<null>" : MetadataLocationUtil.metadataLocation(request.properties()),
        configOverrides.keySet());
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCreateRequest(tableName, table, request);
    Map<String, String> effectiveConfig =
        augmentConfigWithMetadataPath(configOverrides, metadataView);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, effectiveConfig, storageCredentials);
  }

  private static Map<String, String> augmentConfigWithMetadataPath(
      Map<String, String> originalConfig, TableMetadataView metadataView) {
    String metadataLocation = metadataView.metadataLocation();
    if (metadataLocation == null || metadataLocation.isBlank()) {
      metadataLocation = MetadataLocationUtil.metadataLocation(metadataView.properties());
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return originalConfig;
    }
    String directory = MetadataLocationUtil.canonicalMetadataDirectory(metadataLocation);
    if (directory == null || directory.isBlank()) {
      return originalConfig;
    }
    Map<String, String> updated =
        originalConfig.isEmpty()
            ? new java.util.LinkedHashMap<>()
            : new java.util.LinkedHashMap<>(originalConfig);
    updated.put("write.metadata.path", directory);
    return Map.copyOf(updated);
  }
}
