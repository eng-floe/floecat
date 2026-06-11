/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.services.table.load;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.resolver.DeltaSchemaNormalizer;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageRequest;
import ai.floedb.floecat.storage.rpc.ResolveSnapshotCompatStorageResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableLoadService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableLoadSupport loadSupport;
  @Inject GrpcServiceFacade grpcClient;
  @Inject IcebergGatewayConfig config;

  public Response load(
      TableRef tableContext,
      String tableName,
      String snapshots,
      String accessDelegationMode,
      String ifNoneMatch,
      TableGatewaySupport tableSupport) {
    Table tableRecord = tableLifecycleService.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = loadSupport.parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    TableLoadSupport.LoadData loadData =
        loadSupport.loadData(tableRecord, snapshotMode, tableSupport);
    String etagValue = loadSupport.etagValue(loadData.metadataLocation(), snapshotMode);
    if (loadSupport.hasWildcardIfNoneMatch(ifNoneMatch)) {
      return IcebergErrorResponses.validation("If-None-Match may not take the value of '*'");
    }
    if (loadSupport.etagMatches(etagValue, ifNoneMatch)) {
      return Response.notModified().build();
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(tableRecord, accessDelegationMode);
      credentials = appendCompatStorageCredentials(tableRecord, loadData.snapshots(), credentials);
    } catch (IllegalArgumentException e) {
      if (shouldFallbackToCompatOnly(tableRecord, accessDelegationMode, tableSupport, e)) {
        credentials = appendCompatStorageCredentials(tableRecord, loadData.snapshots(), null);
      } else {
        return IcebergErrorResponses.validation(e.getMessage());
      }
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                rewriteDeltaVariantSchemas(tableRecord),
                rewriteDeltaVariantSchemas(tableRecord, loadData.snapshots()),
                loadData.metadataLocation(),
                tableSupport.defaultTableConfig(tableRecord),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }

  public Response loadResolvedTable(
      String tableName,
      Table tableRecord,
      String accessDelegationMode,
      TableGatewaySupport tableSupport) {
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(tableRecord, accessDelegationMode);
      TableLoadSupport.LoadData loadData =
          loadSupport.loadData(tableRecord, SnapshotLister.Mode.ALL, tableSupport);
      credentials = appendCompatStorageCredentials(tableRecord, loadData.snapshots(), credentials);
      return buildLoadResponse(
          tableName,
          tableRecord,
          SnapshotLister.Mode.ALL,
          credentials,
          null,
          tableSupport,
          loadData);
    } catch (IllegalArgumentException e) {
      if (!shouldFallbackToCompatOnly(tableRecord, accessDelegationMode, tableSupport, e)) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
      TableLoadSupport.LoadData loadData =
          loadSupport.loadData(tableRecord, SnapshotLister.Mode.ALL, tableSupport);
      return buildLoadResponse(
          tableName,
          tableRecord,
          SnapshotLister.Mode.ALL,
          appendCompatStorageCredentials(tableRecord, loadData.snapshots(), null),
          null,
          tableSupport,
          loadData);
    }
  }

  public Response loadResolvedTable(
      String tableName,
      Table tableRecord,
      List<StorageCredentialDto> credentials,
      TableGatewaySupport tableSupport) {
    TableLoadSupport.LoadData loadData =
        loadSupport.loadData(tableRecord, SnapshotLister.Mode.ALL, tableSupport);
    return buildLoadResponse(
        tableName,
        tableRecord,
        SnapshotLister.Mode.ALL,
        appendCompatStorageCredentials(tableRecord, loadData.snapshots(), credentials),
        null,
        tableSupport,
        loadData);
  }

  private Response buildLoadResponse(
      String tableName,
      Table tableRecord,
      SnapshotLister.Mode snapshotMode,
      List<StorageCredentialDto> credentials,
      String etagValue,
      TableGatewaySupport tableSupport) {
    TableLoadSupport.LoadData loadData =
        loadSupport.loadData(tableRecord, snapshotMode, tableSupport);
    return buildLoadResponse(
        tableName, tableRecord, snapshotMode, credentials, etagValue, tableSupport, loadData);
  }

  private Response buildLoadResponse(
      String tableName,
      Table tableRecord,
      SnapshotLister.Mode snapshotMode,
      List<StorageCredentialDto> credentials,
      String etagValue,
      TableGatewaySupport tableSupport,
      TableLoadSupport.LoadData loadData) {
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                rewriteDeltaVariantSchemas(tableRecord),
                rewriteDeltaVariantSchemas(tableRecord, loadData.snapshots()),
                loadData.metadataLocation(),
                tableSupport.defaultTableConfig(tableRecord),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }

  private List<StorageCredentialDto> appendCompatStorageCredentials(
      Table tableRecord,
      List<ai.floedb.floecat.catalog.rpc.Snapshot> snapshots,
      List<StorageCredentialDto> base) {
    if (!loadSupport.deltaCompatEnabled(tableRecord)
        || !hasPersistedTableId(tableRecord)
        || snapshots == null) {
      return base;
    }
    Map<String, StorageCredentialDto> merged = new LinkedHashMap<>();
    if (base != null) {
      for (StorageCredentialDto credential : base) {
        if (credential != null && credential.prefix() != null && !credential.prefix().isBlank()) {
          merged.put(credential.prefix(), credential);
        }
      }
    }
    for (ai.floedb.floecat.catalog.rpc.Snapshot snapshot : snapshots) {
      if (snapshot == null || snapshot.getSnapshotId() < 0) {
        continue;
      }
      ResolveSnapshotCompatStorageResponse response =
          grpcClient.resolveSnapshotCompatStorage(
              ResolveSnapshotCompatStorageRequest.newBuilder()
                  .setTableId(tableRecord.getResourceId())
                  .setSnapshotId(snapshot.getSnapshotId())
                  .setIncludeCredentials(true)
                  .build());
      if (response == null
          || !response.hasStorage()
          || response.getStorage().getStorageCredentialsCount() == 0) {
        continue;
      }
      response
          .getStorage()
          .getStorageCredentialsList()
          .forEach(
              credential ->
                  merged.put(
                      credential.getPrefix(),
                      new StorageCredentialDto(
                          credential.getPrefix(),
                          credential.getConfigMap(),
                          credential.hasExpiresAt()
                              ? java.time.Instant.ofEpochSecond(
                                  credential.getExpiresAt().getSeconds(),
                                  credential.getExpiresAt().getNanos())
                              : null)));
    }
    return merged.isEmpty() ? null : List.copyOf(merged.values());
  }

  private static boolean hasPersistedTableId(Table table) {
    return table != null
        && table.hasResourceId()
        && table.getResourceId().getKind() == ResourceKind.RK_TABLE;
  }

  private boolean shouldFallbackToCompatOnly(
      Table tableRecord,
      String accessDelegationMode,
      TableGatewaySupport tableSupport,
      IllegalArgumentException error) {
    if (error == null
        || tableSupport == null
        || !loadSupport.deltaCompatEnabled(tableRecord)
        || !tableSupport.usesVendedCredentials(accessDelegationMode)) {
      return false;
    }
    String message = error.getMessage();
    if (message == null) {
      return false;
    }
    return message.contains("Credential vending");
  }

  private Table rewriteDeltaVariantSchemas(Table tableRecord) {
    if (!shouldRewriteDeltaVariantSchemas(tableRecord)
        || tableRecord == null
        || tableRecord.getSchemaJson() == null
        || tableRecord.getSchemaJson().isBlank()) {
      return tableRecord;
    }
    return tableRecord.toBuilder()
        .setSchemaJson(
            DeltaSchemaNormalizer.normalizeSchemaJson(tableRecord.getSchemaJson(), 0, true))
        .build();
  }

  private List<Snapshot> rewriteDeltaVariantSchemas(Table tableRecord, List<Snapshot> snapshots) {
    if (!shouldRewriteDeltaVariantSchemas(tableRecord)
        || snapshots == null
        || snapshots.isEmpty()) {
      return snapshots;
    }
    List<Snapshot> rewritten = new java.util.ArrayList<>(snapshots.size());
    for (Snapshot snapshot : snapshots) {
      if (snapshot == null
          || snapshot.getSchemaJson() == null
          || snapshot.getSchemaJson().isBlank()) {
        rewritten.add(snapshot);
        continue;
      }
      rewritten.add(
          snapshot.toBuilder()
              .setSchemaJson(
                  DeltaSchemaNormalizer.normalizeSchemaJson(
                      snapshot.getSchemaJson(), snapshot.getSchemaId(), true))
              .build());
    }
    return List.copyOf(rewritten);
  }

  private boolean shouldRewriteDeltaVariantSchemas(Table tableRecord) {
    if (tableRecord == null || !loadSupport.deltaCompatEnabled(tableRecord)) {
      return false;
    }
    if (config == null || config.deltaCompat().isEmpty()) {
      return false;
    }
    return config.deltaCompat().get().rewriteVariantAsStruct();
  }
}
