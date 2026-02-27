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
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableLoadService {
  private static final Logger LOG = Logger.getLogger(TableLoadService.class);
  @Inject IcebergGatewayConfig config;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject SnapshotClient snapshotClient;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject DeltaIcebergMetadataService deltaMetadataService;
  @Inject TableMetadataImportService tableMetadataImportService;

  public Response load(
      TableRequestContext tableContext,
      String tableName,
      String snapshots,
      String accessDelegationMode,
      String ifNoneMatch,
      TableGatewaySupport tableSupport) {
    Table tableRecord = tableLifecycleService.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata;
    List<Snapshot> snapshotList;
    if (deltaCompatEnabled(tableRecord)) {
      DeltaIcebergMetadataService.DeltaLoadResult delta =
          deltaMetadataService.load(tableContext.tableId(), tableRecord, snapshotMode);
      metadata = delta.metadata();
      snapshotList = delta.snapshots();
    } else {
      metadata = tableSupport.loadCurrentMetadata(tableRecord);
      metadata = hydrateMetadataIfIncomplete(tableRecord, tableSupport, metadata);
      snapshotList =
          SnapshotLister.fetchSnapshots(
              snapshotClient, tableContext.tableId(), snapshotMode, metadata);
    }
    String etagValue = etagSource(metadata, snapshotMode);
    if (etagValue != null) {
      etagValue = IcebergHttpUtil.etagForMetadataLocation(etagValue);
    }
    if (ifNoneMatch != null && ifNoneMatch.trim().equals("*")) {
      return IcebergErrorResponses.validation("If-None-Match may not take the value of '*'");
    }
    if (etagMatches(etagValue, ifNoneMatch)) {
      return Response.notModified().build();
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                tableRecord,
                metadata,
                snapshotList,
                tableSupport.defaultTableConfig(),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
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
    String expected = normalizeEtag(etagValue);
    for (String raw : ifNoneMatch.split(",")) {
      String token = normalizeEtag(raw);
      if (!token.isEmpty() && token.equals(expected)) {
        return true;
      }
    }
    return false;
  }

  private String normalizeEtag(String token) {
    if (token == null) {
      return "";
    }
    String value = token.trim();
    if (value.startsWith("W/")) {
      value = value.substring(2).trim();
    }
    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  private String metadataLocation(IcebergMetadata metadata, Table tableRecord) {
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    if (tableRecord != null && tableRecord.getPropertiesCount() > 0) {
      String propertyLocation =
          MetadataLocationUtil.metadataLocation(tableRecord.getPropertiesMap());
      if (propertyLocation != null && !propertyLocation.isBlank()) {
        return propertyLocation;
      }
    }
    return null;
  }

  private String etagSource(IcebergMetadata metadata, SnapshotLister.Mode snapshotMode) {
    String metadataLocation = metadataLocation(metadata, null);
    if (metadataLocation == null) {
      return null;
    }
    String mode;
    if (snapshotMode == null) {
      mode = SnapshotLister.Mode.ALL.name().toLowerCase();
    } else {
      mode = snapshotMode.name().toLowerCase();
    }
    return metadataLocation + "|snapshots=" + mode;
  }

  private boolean deltaCompatEnabled(Table table) {
    if (config == null || tableFormatSupport == null || table == null) {
      return false;
    }
    var deltaCompat = config.deltaCompat();
    return deltaCompat.isPresent()
        && deltaCompat.get().enabled()
        && tableFormatSupport.isDelta(table);
  }

  private IcebergMetadata hydrateMetadataIfIncomplete(
      Table tableRecord, TableGatewaySupport tableSupport, IcebergMetadata metadata) {
    if (metadata != null && metadata.getSchemasCount() > 0) {
      return metadata;
    }
    String metadataLocation = metadataLocation(metadata, tableRecord);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      LOG.warn("Load metadata had no schemas/metadata and no metadata-location; cannot hydrate");
      return metadata;
    }
    LOG.infof("Hydrating load metadata from metadata-location=%s", metadataLocation);
    try {
      Map<String, String> ioProps = new LinkedHashMap<>(tableSupport.defaultFileIoProperties());
      if (tableRecord != null && tableRecord.getPropertiesCount() > 0) {
        ioProps.putAll(FileIoFactory.filterIoProperties(tableRecord.getPropertiesMap()));
      }
      IcebergMetadata imported =
          tableMetadataImportService.importMetadata(metadataLocation, ioProps).icebergMetadata();
      LOG.infof(
          "Hydrated load metadata schemas=%d partitionSpecs=%d sortOrders=%d",
          imported.getSchemasCount(),
          imported.getPartitionSpecsCount(),
          imported.getSortOrdersCount());
      return imported;
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Failed to hydrate table metadata from %s; continuing with catalog metadata",
          metadataLocation);
      return metadata;
    }
  }
}
