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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.NamespaceRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableRegisterService {
  private static final Logger LOG = Logger.getLogger(TableRegisterService.class);

  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject SnapshotClient snapshotClient;
  @Inject TransactionCommitService transactionCommitService;

  public Response register(
      NamespaceRequestContext namespaceContext,
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

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(created);
    List<Snapshot> snapshots =
        SnapshotLister.fetchSnapshots(
            snapshotClient, created.getResourceId(), SnapshotLister.Mode.ALL, metadata);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                created,
                metadata,
                snapshots,
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(created, metadata);
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, IcebergHttpUtil.etagForMetadataLocation(etagValue));
    }
    return builder.build();
  }

  private Response createRegisteredTable(
      NamespaceRequestContext namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      TableGatewaySupport tableSupport) {
    Response response =
        transactionCommitService.commit(
            namespaceContext.prefix(),
            idempotencyKey,
            buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                mergeImportedProperties(null, importedMetadata, metadataLocation, ioProperties),
                metadataLocation,
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
      NamespaceRequestContext namespaceContext,
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
        mergeImportedProperties(
            existing.getPropertiesMap(), importedMetadata, metadataLocation, ioProperties);
    Response response =
        transactionCommitService.commit(
            namespaceContext.prefix(),
            idempotencyKey,
            buildRegisterTransactionRequest(
                namespaceContext.namespacePath(),
                tableName,
                props,
                metadataLocation,
                importedMetadata,
                existingSnapshotIds,
                false),
            tableSupport);
    if (response != null && response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return response;
    }
    Table updated = tableLifecycleService.getTable(tableId);

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(updated);
    List<Snapshot> snapshots =
        SnapshotLister.fetchSnapshots(snapshotClient, tableId, SnapshotLister.Mode.ALL, metadata);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                updated,
                metadata,
                snapshots,
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(updated, metadata);
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, IcebergHttpUtil.etagForMetadataLocation(etagValue));
    }
    return builder.build();
  }

  private TransactionCommitRequest buildRegisterTransactionRequest(
      List<String> namespacePath,
      String tableName,
      Map<String, String> mergedProps,
      String metadataLocation,
      ImportedMetadata importedMetadata,
      List<Long> existingSnapshotIds,
      boolean assertCreate) {
    List<Map<String, Object>> requirements = new ArrayList<>();
    if (assertCreate) {
      requirements.add(Map.of("type", "assert-create"));
    }
    List<Map<String, Object>> updates = new ArrayList<>();
    Map<String, String> props = mergedProps == null ? Map.of() : new LinkedHashMap<>(mergedProps);
    if (!props.isEmpty()) {
      updates.add(Map.of("action", "set-properties", "updates", props));
    }
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      updates.add(Map.of("action", "set-location", "location", location));
    }
    List<Long> importedSnapshotIds = new ArrayList<>();
    if (importedMetadata != null && importedMetadata.snapshots() != null) {
      for (ImportedSnapshot snapshot : importedMetadata.snapshots()) {
        Map<String, Object> snapshotMap = new LinkedHashMap<>();
        if (snapshot.snapshotId() != null) {
          snapshotMap.put("snapshot-id", snapshot.snapshotId());
          importedSnapshotIds.add(snapshot.snapshotId());
        }
        if (snapshot.parentSnapshotId() != null) {
          snapshotMap.put("parent-snapshot-id", snapshot.parentSnapshotId());
        }
        if (snapshot.sequenceNumber() != null) {
          snapshotMap.put("sequence-number", snapshot.sequenceNumber());
        }
        if (snapshot.timestampMs() != null) {
          snapshotMap.put("timestamp-ms", snapshot.timestampMs());
        }
        if (snapshot.manifestList() != null && !snapshot.manifestList().isBlank()) {
          snapshotMap.put("manifest-list", snapshot.manifestList());
        }
        if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
          snapshotMap.put("summary", snapshot.summary());
        }
        if (snapshot.schemaId() != null) {
          snapshotMap.put("schema-id", snapshot.schemaId());
        }
        if (importedMetadata.schemaJson() != null && !importedMetadata.schemaJson().isBlank()) {
          snapshotMap.put("schema-json", importedMetadata.schemaJson());
        }
        updates.add(Map.of("action", "add-snapshot", "snapshot", snapshotMap));
      }
    } else if (importedMetadata != null && importedMetadata.currentSnapshot() != null) {
      ImportedSnapshot snapshot = importedMetadata.currentSnapshot();
      Map<String, Object> snapshotMap = new LinkedHashMap<>();
      if (snapshot.snapshotId() != null) {
        snapshotMap.put("snapshot-id", snapshot.snapshotId());
        importedSnapshotIds.add(snapshot.snapshotId());
      }
      if (snapshot.parentSnapshotId() != null) {
        snapshotMap.put("parent-snapshot-id", snapshot.parentSnapshotId());
      }
      if (snapshot.sequenceNumber() != null) {
        snapshotMap.put("sequence-number", snapshot.sequenceNumber());
      }
      if (snapshot.timestampMs() != null) {
        snapshotMap.put("timestamp-ms", snapshot.timestampMs());
      }
      if (snapshot.manifestList() != null && !snapshot.manifestList().isBlank()) {
        snapshotMap.put("manifest-list", snapshot.manifestList());
      }
      if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
        snapshotMap.put("summary", snapshot.summary());
      }
      if (snapshot.schemaId() != null) {
        snapshotMap.put("schema-id", snapshot.schemaId());
      }
      if (importedMetadata.schemaJson() != null && !importedMetadata.schemaJson().isBlank()) {
        snapshotMap.put("schema-json", importedMetadata.schemaJson());
      }
      updates.add(Map.of("action", "add-snapshot", "snapshot", snapshotMap));
    }
    if (existingSnapshotIds != null
        && !existingSnapshotIds.isEmpty()
        && !importedSnapshotIds.isEmpty()) {
      List<Long> removals =
          existingSnapshotIds.stream().filter(id -> !importedSnapshotIds.contains(id)).toList();
      if (!removals.isEmpty()) {
        updates.add(Map.of("action", "remove-snapshots", "snapshot-ids", removals));
      }
    }
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName), requirements, updates)));
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

  private Map<String, String> mergeImportedProperties(
      Map<String, String> existing,
      ImportedMetadata importedMetadata,
      String metadataLocation,
      Map<String, String> registerIoProperties) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existing != null && !existing.isEmpty()) {
      merged.putAll(existing);
    }
    if (importedMetadata != null && importedMetadata.properties() != null) {
      merged.putAll(importedMetadata.properties());
    }
    if (importedMetadata != null
        && importedMetadata.tableLocation() != null
        && !importedMetadata.tableLocation().isBlank()) {
      merged.put("location", importedMetadata.tableLocation());
    }
    if (registerIoProperties != null && !registerIoProperties.isEmpty()) {
      merged.putAll(registerIoProperties);
    }
    MetadataLocationUtil.setMetadataLocation(merged, metadataLocation);
    return merged;
  }

  private String metadataLocation(Table table, IcebergMetadata metadata) {
    Map<String, String> props =
        table == null || table.getPropertiesMap() == null ? Map.of() : table.getPropertiesMap();
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null && !propertyLocation.isBlank()) {
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
