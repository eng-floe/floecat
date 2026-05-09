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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.load.TableLoadService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TableRegisterService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject GrpcServiceFacade snapshotClient;
  @Inject TransactionCommitService transactionCommitService;
  @Inject TableLoadService tableLoadService;
  @Inject TableRegisterRequestBuilder tableRegisterRequestBuilder;

  public Response register(
      NamespaceRef namespaceContext,
      String idempotencyKey,
      TableRequests.Register req,
      TableGatewaySupport tableSupport) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String tableName = req.name().trim();

    Map<String, String> ioProperties =
        tableSupport.resolveRegisterFileIoProperties(req.properties());
    ImportedMetadata importedMetadata;
    try {
      importedMetadata = tableMetadataImportService.importMetadata(metadataLocation, ioProperties);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    boolean overwrite = Boolean.TRUE.equals(req.overwrite());
    boolean tableExists = false;
    if (overwrite) {
      try {
        ResourceId existingTableId =
            tableLifecycleService.resolveTableId(
                namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
        tableLifecycleService.getTable(existingTableId);
        tableExists = true;
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
      }
    }
    Response commitResponse =
        overwrite && tableExists
            ? overwriteRegisteredTable(
                namespaceContext,
                tableName,
                metadataLocation,
                idempotencyKey,
                ioProperties,
                importedMetadata,
                tableSupport)
            : createRegisteredTable(
                namespaceContext,
                tableName,
                metadataLocation,
                idempotencyKey,
                ioProperties,
                importedMetadata,
                tableSupport);
    if (commitResponse != null) {
      return commitResponse;
    }

    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    Table created = tableLifecycleService.getTable(tableId);
    return tableLoadService.loadResolvedTable(tableName, created, (String) null, tableSupport);
  }

  private Response createRegisteredTable(
      NamespaceRef namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      TableGatewaySupport tableSupport) {
    Response response =
        transactionCommitService.commitRegister(
            namespaceContext.prefix(),
            idempotencyKey,
            tableRegisterRequestBuilder.buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                tableRegisterRequestBuilder.mergeImportedProperties(
                    null, importedMetadata, metadataLocation, ioProperties),
                importedMetadata,
                List.of(),
                true),
            tableSupport);
    if (response != null && response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
      return IcebergErrorResponses.conflict("Table already exists");
    }
    return response != null && response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()
        ? response
        : null;
  }

  private Response overwriteRegisteredTable(
      NamespaceRef namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      TableGatewaySupport tableSupport) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    Table existing;
    try {
      existing = tableLifecycleService.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return IcebergErrorResponses.noSuchTable(
            "Table "
                + String.join(".", namespaceContext.namespacePath())
                + "."
                + tableName
                + " not found");
      }
      throw e;
    }
    List<Long> existingSnapshotIds = listSnapshotIds(tableId);
    Map<String, String> props =
        tableRegisterRequestBuilder.mergeImportedProperties(
            existing.getPropertiesMap(), importedMetadata, metadataLocation, ioProperties);
    Response response =
        transactionCommitService.commitRegister(
            namespaceContext.prefix(),
            idempotencyKey,
            tableRegisterRequestBuilder.buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                props,
                importedMetadata,
                existingSnapshotIds,
                false),
            tableSupport);
    if (response != null && response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return response;
    }
    Table updated = tableLifecycleService.getTable(tableId);

    return tableLoadService.loadResolvedTable(tableName, updated, (String) null, tableSupport);
  }

  private List<Long> listSnapshotIds(ResourceId tableId) {
    if (tableId == null) {
      return List.of();
    }
    try {
      return snapshotClient
          .listSnapshots(
              ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest.newBuilder()
                  .setTableId(tableId)
                  .build())
          .getSnapshotsList()
          .stream()
          .map(ai.floedb.floecat.catalog.rpc.Snapshot::getSnapshotId)
          .toList();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return List.of();
      }
      throw e;
    }
  }
}
