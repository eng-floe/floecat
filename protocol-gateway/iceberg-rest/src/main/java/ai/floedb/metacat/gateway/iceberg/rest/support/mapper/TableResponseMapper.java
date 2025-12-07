package ai.floedb.metacat.gateway.iceberg.rest.support.mapper;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.table.TableMetadataBuilder;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableResponseMapper {
  private TableResponseMapper() {}

  public static LoadTableResultDto toLoadResult(
      String tableName,
      Table table,
      IcebergMetadata metadata,
      List<Snapshot> snapshots,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    TableMetadataView metadataView =
        ensureMetadataPath(
            TableMetadataBuilder.fromCatalog(tableName, table, props, metadata, snapshots));
    Map<String, String> effectiveConfig =
        augmentConfigWithMetadataPath(configOverrides, metadataView.metadataLocation());
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, effectiveConfig, storageCredentials);
  }

  public static CommitTableResponseDto toCommitResponse(
      String tableName, Table table, IcebergMetadata metadata, List<Snapshot> snapshots) {
    Map<String, String> props = new LinkedHashMap<>(table.getPropertiesMap());
    TableMetadataView metadataView =
        ensureMetadataPath(
            TableMetadataBuilder.fromCatalog(tableName, table, props, metadata, snapshots));
    return new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
  }

  public static LoadTableResultDto toLoadResultFromCreate(
      String tableName,
      Table table,
      TableRequests.Create request,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    TableMetadataView metadataView =
        ensureMetadataPath(TableMetadataBuilder.fromCreateRequest(tableName, table, request));
    Map<String, String> effectiveConfig =
        augmentConfigWithMetadataPath(configOverrides, metadataView.metadataLocation());
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, effectiveConfig, storageCredentials);
  }

  private static Map<String, String> augmentConfigWithMetadataPath(
      Map<String, String> originalConfig, String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return originalConfig;
    }
    String directory = metadataDirectory(metadataLocation);
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

  private static String metadataDirectory(String metadataLocation) {
    int slash = metadataLocation.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    String file = metadataLocation.substring(slash + 1);
    if ("metadata.json".equalsIgnoreCase(file)) {
      return null;
    }
    return metadataLocation.substring(0, slash);
  }

  private static TableMetadataView ensureMetadataPath(TableMetadataView metadataView) {
    if (metadataView == null || metadataView.metadataLocation() == null) {
      return metadataView;
    }
    Map<String, String> props = metadataView.properties();
    if (props != null && props.containsKey("write.metadata.path")) {
      return metadataView;
    }
    return metadataView.withMetadataLocation(metadataView.metadataLocation());
  }
}
