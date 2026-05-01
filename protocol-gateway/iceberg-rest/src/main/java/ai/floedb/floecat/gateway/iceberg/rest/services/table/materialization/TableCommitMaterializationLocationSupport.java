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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.materialization;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableMetadataViewSupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Locale;

@ApplicationScoped
public class TableCommitMaterializationLocationSupport {

  @Inject IcebergGatewayConfig config;

  String resolveOutputMetadataLocation(
      String namespace,
      String tableName,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    if (!hasText(metadataLocation)
        && !hasText(metadata == null ? null : metadata.metadataLocation())) {
      metadataLocation = deriveMetadataLocation(tableRecord, metadata);
    }
    if (!hasText(metadataLocation)
        && !hasText(metadata == null ? null : metadata.metadataLocation())) {
      metadataLocation = deriveDefaultMetadataLocation(namespace, tableName);
    }
    return metadataLocation;
  }

  TableMetadataView normalizeTableLocation(
      String namespace,
      String tableName,
      Table tableRecord,
      TableMetadataView metadata,
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
    return TableMetadataViewSupport.copyMetadata(metadata).location(resolvedLocation).build();
  }

  private String deriveMetadataLocation(Table tableRecord, TableMetadataView metadata) {
    String location = tableLocation(tableRecord);
    if (location == null && metadata != null) {
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
    if (config == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (!hasText(warehouse)) {
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
    path.append('/').append(tableName).append("/metadata/");
    return path.toString();
  }

  private String deriveDefaultTableLocation(String namespace, String tableName) {
    if (config == null || tableName == null || tableName.isBlank()) {
      return null;
    }
    String warehouse = config.defaultWarehousePath().orElse(null);
    if (!hasText(warehouse)) {
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
    String location = tableRecord.getPropertiesMap().get("location");
    if (hasText(location)) {
      return location;
    }
    if (tableRecord.hasUpstream() && hasText(tableRecord.getUpstream().getUri())) {
      return tableRecord.getUpstream().getUri();
    }
    return null;
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

  private String stripTrailingSlash(String value) {
    if (value == null || value.length() <= 1) {
      return value;
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  private boolean hasText(String value) {
    return value != null && !value.isBlank();
  }
}
