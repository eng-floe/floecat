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
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.DeltaIcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableLoadSupport {
  private static final Logger LOG = Logger.getLogger(TableLoadSupport.class);

  @Inject IcebergGatewayConfig config;
  @Inject GrpcServiceFacade snapshotClient;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject DeltaIcebergMetadataService deltaMetadataService;
  @Inject TableMetadataImportService tableMetadataImportService;

  LoadData loadData(
      Table tableRecord, SnapshotLister.Mode snapshotMode, TableGatewaySupport tableSupport) {
    IcebergMetadata metadata;
    List<Snapshot> snapshotList;
    if (deltaCompatEnabled(tableRecord)) {
      DeltaIcebergMetadataService.DeltaLoadResult delta =
          deltaMetadataService.load(tableRecord.getResourceId(), tableRecord, snapshotMode);
      metadata = delta.metadata();
      snapshotList = delta.snapshots();
    } else {
      metadata = tableSupport.loadCurrentMetadata(tableRecord);
      metadata = hydrateMetadataIfNeeded(tableRecord, tableSupport, metadata);
      snapshotList =
          SnapshotLister.fetchSnapshots(
              snapshotClient, tableRecord.getResourceId(), snapshotMode, metadata);
    }
    return new LoadData(metadata, snapshotList);
  }

  SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  String etagValue(IcebergMetadata metadata, SnapshotLister.Mode snapshotMode) {
    String source = etagSource(metadata, snapshotMode);
    return source == null ? null : IcebergHttpUtil.etagForMetadataLocation(source);
  }

  boolean hasWildcardIfNoneMatch(String ifNoneMatch) {
    return ifNoneMatch != null && ifNoneMatch.trim().equals("*");
  }

  boolean etagMatches(String etagValue, String ifNoneMatch) {
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
    if (tableRecord != null && tableRecord.getPropertiesCount() > 0) {
      String propertyLocation =
          MetadataLocationUtil.metadataLocation(tableRecord.getPropertiesMap());
      if (propertyLocation != null && !propertyLocation.isBlank()) {
        return propertyLocation;
      }
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return null;
  }

  private String etagSource(IcebergMetadata metadata, SnapshotLister.Mode snapshotMode) {
    String metadataLocation = metadataLocation(metadata, null);
    if (metadataLocation == null) {
      return null;
    }
    String mode =
        snapshotMode == null
            ? SnapshotLister.Mode.ALL.name().toLowerCase()
            : snapshotMode.name().toLowerCase();
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

  private IcebergMetadata hydrateMetadataIfNeeded(
      Table tableRecord, TableGatewaySupport tableSupport, IcebergMetadata metadata) {
    String tableMetadataLocation = metadataLocation(null, tableRecord);
    String snapshotMetadataLocation = metadataLocation(metadata, null);
    boolean pointerAdvanced =
        tableMetadataLocation != null
            && !tableMetadataLocation.isBlank()
            && !tableMetadataLocation.equals(snapshotMetadataLocation);
    if (!pointerAdvanced && metadata != null && metadata.getSchemasCount() > 0) {
      return metadata;
    }
    String metadataLocation =
        pointerAdvanced ? tableMetadataLocation : metadataLocation(metadata, tableRecord);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      LOG.warn("Load metadata had no schemas/metadata and no metadata-location; cannot hydrate");
      return metadata;
    }
    LOG.infof("Hydrating load metadata from metadata-location=%s", metadataLocation);
    try {
      Map<String, String> ioProps =
          new LinkedHashMap<>(
              tableSupport.serverSideFileIoPropertiesForLocation(tableRecord, metadataLocation));
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

  record LoadData(IcebergMetadata metadata, List<Snapshot> snapshots) {}
}
