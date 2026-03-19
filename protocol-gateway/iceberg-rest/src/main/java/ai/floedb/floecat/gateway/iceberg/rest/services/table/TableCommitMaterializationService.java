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
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.CanonicalTableMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataException;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.Locale;
import org.apache.iceberg.TableMetadata;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitMaterializationService {
  private static final Logger LOG = Logger.getLogger(TableCommitMaterializationService.class);

  @Inject MaterializeMetadataService materializeMetadataService;
  @Inject IcebergGatewayConfig config;
  @Inject CanonicalTableMetadataService canonicalTableMetadataService;

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      Table tableRecord,
      TableMetadata metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.validation("metadata is required for materialization"));
    }

    String requestedLocation = firstNonBlank(metadataLocation, metadata.metadataFileLocation());
    if (!hasText(requestedLocation)) {
      requestedLocation = deriveMetadataLocation(tableRecord, metadata);
    }
    if (!hasText(requestedLocation)) {
      requestedLocation = deriveDefaultMetadataLocation(namespace, table);
    }

    TableMetadata canonicalMetadata =
        ensureTableLocation(namespace, table, tableRecord, metadata, requestedLocation);

    try {
      MaterializeMetadataService.MaterializeResult materializeResult =
          materializeMetadataService.materialize(
              namespace, table, canonicalMetadata, requestedLocation);
      String resolvedLocation = materializeResult.metadataLocation();
      TableMetadata resolvedMetadata =
          materializeResult.tableMetadata() == null
              ? canonicalMetadata
              : materializeResult.tableMetadata();
      TableMetadataView resolvedView =
          canonicalTableMetadataService.toTableMetadataView(resolvedMetadata, resolvedLocation);
      return MaterializeMetadataResult.success(resolvedView, resolvedLocation, resolvedMetadata);
    } catch (MaterializeMetadataException e) {
      LOG.warnf(
          e,
          "Failed to materialize Iceberg metadata for %s.%s to %s",
          namespace,
          table,
          requestedLocation);
      return MaterializeMetadataResult.failure(
          IcebergErrorResponses.failure(
              "metadata materialization failed",
              "MaterializeMetadataException",
              Response.Status.INTERNAL_SERVER_ERROR));
    }
  }

  private TableMetadata ensureTableLocation(
      String namespace,
      String tableName,
      Table tableRecord,
      TableMetadata metadata,
      String metadataLocation) {
    if (metadata == null || hasText(metadata.location())) {
      return metadata;
    }
    String resolvedLocation =
        firstNonBlank(
            tableLocation(tableRecord),
            metadata.properties() == null ? null : metadata.properties().get("location"),
            deriveTableLocationFromMetadataLocation(metadataLocation),
            deriveDefaultTableLocation(namespace, tableName));
    if (!hasText(resolvedLocation)) {
      return metadata;
    }
    return TableMetadata.buildFrom(metadata).discardChanges().setLocation(resolvedLocation).build();
  }

  private String deriveMetadataLocation(Table tableRecord, TableMetadata metadata) {
    String location = tableLocation(tableRecord);
    if (!hasText(location) && metadata != null) {
      location = metadata.location();
    }
    if (!hasText(location)) {
      return null;
    }
    String base = stripTrailingSlash(location);
    if (base.toLowerCase(Locale.ROOT).endsWith("/metadata")) {
      return base + "/";
    }
    return base + "/metadata/";
  }

  private String deriveDefaultMetadataLocation(String namespace, String tableName) {
    String tableLocation = deriveDefaultTableLocation(namespace, tableName);
    return hasText(tableLocation) ? tableLocation + "/metadata/" : null;
  }

  private String deriveDefaultTableLocation(String namespace, String tableName) {
    if (config == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (warehouse == null || warehouse.isBlank()) {
      return null;
    }
    StringBuilder path = new StringBuilder(stripTrailingSlash(warehouse));
    if (namespace != null && !namespace.isBlank()) {
      for (String segment : namespace.split("\\.")) {
        if (segment != null && !segment.isBlank()) {
          path.append('/').append(segment);
        }
      }
    }
    path.append('/').append(tableName);
    return path.toString();
  }

  private String deriveTableLocationFromMetadataLocation(String metadataLocation) {
    if (!hasText(metadataLocation)) {
      return null;
    }
    String trimmed = stripTrailingSlash(metadataLocation);
    if (!hasText(trimmed)) {
      return null;
    }
    if (trimmed.endsWith(".metadata.json")) {
      int slash = trimmed.lastIndexOf('/');
      if (slash > 0) {
        trimmed = trimmed.substring(0, slash);
      }
    }
    if (trimmed.endsWith("/metadata")) {
      return trimmed.substring(0, trimmed.length() - "/metadata".length());
    }
    return null;
  }

  private String tableLocation(Table tableRecord) {
    if (tableRecord == null) {
      return null;
    }
    if (tableRecord.hasUpstream()) {
      String uri = tableRecord.getUpstream().getUri();
      if (hasText(uri)) {
        return uri;
      }
    }
    String location = tableRecord.getPropertiesMap().get("location");
    return hasText(location) ? location : null;
  }

  private static boolean hasText(String value) {
    return value != null && !value.isBlank();
  }

  private String stripTrailingSlash(String value) {
    if (value == null || value.length() <= 1) {
      return value;
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  private String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (hasText(value)) {
        return value;
      }
    }
    return null;
  }
}
