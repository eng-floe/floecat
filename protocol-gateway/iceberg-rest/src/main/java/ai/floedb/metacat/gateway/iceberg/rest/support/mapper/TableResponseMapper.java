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
        TableMetadataBuilder.fromCatalog(tableName, table, props, metadata, snapshots);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, configOverrides, storageCredentials);
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
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCreateRequest(tableName, table, request);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, configOverrides, storageCredentials);
  }
}
