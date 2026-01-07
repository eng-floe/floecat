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
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableDropCleanupService {
  private static final Logger LOG = Logger.getLogger(TableDropCleanupService.class);

  @Inject IcebergGatewayConfig config;

  public void purgeTableData(String catalogName, String namespace, String tableName, Table table) {
    String tableId =
        table != null && table.hasResourceId() && table.getResourceId().getId() != null
            ? table.getResourceId().getId()
            : "<missing>";
    LOG.infof(
        "Purging request received namespace=%s table=%s catalog=%s tableId=%s",
        namespace, tableName, catalogName, tableId);
    if (table == null) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because table metadata was unavailable",
          namespace, tableName, catalogName);
      return;
    }
    Map<String, String> props =
        table.getPropertiesMap() == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(table.getPropertiesMap());
    if (props == null || props.isEmpty()) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because table properties were empty",
          namespace, tableName, catalogName);
      return;
    }
    String metadataLocation = MetadataLocationUtil.metadataLocation(props);
    if (metadataLocation == null || metadataLocation.isBlank()) {
      LOG.debugf(
          "Skipping purge for %s.%s in catalog %s because metadata-location was missing",
          namespace, tableName, catalogName);
      return;
    }
    Map<String, String> ioProps = FileIoFactory.filterIoProperties(props);
    LOG.infof(
        "Purging Iceberg data namespace=%s table=%s metadata=%s ioProps=%s",
        namespace, tableName, metadataLocation, ioProps);
    FileIO fileIO = null;
    try {
      fileIO = FileIoFactory.createFileIo(props, config, true);
      TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
      CatalogUtil.dropTableData(fileIO, metadata);
      LOG.infof(
          "Purged Iceberg metadata and data for %s.%s in catalog %s (tableId=%s metadata=%s)",
          namespace, tableName, catalogName, tableId, metadataLocation);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Failed to purge Iceberg metadata/data for %s.%s in catalog %s (metadata=%s)",
          namespace,
          tableName,
          catalogName,
          metadataLocation);
    } finally {
      closeQuietly(fileIO);
    }
  }

  private void closeQuietly(FileIO fileIO) {
    if (fileIO instanceof AutoCloseable closable) {
      try {
        closable.close();
      } catch (Exception e) {
        LOG.debugf(e, "Failed to close FileIO %s", fileIO.getClass().getName());
      }
    }
  }
}
