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
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.metadata.TableMetadataViewSupport;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TableCommitMaterializationLocationSupport {

  String resolveOutputMetadataLocation(
      String namespace,
      String tableName,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    return firstNonBlank(metadataLocation, metadata == null ? null : metadata.metadataLocation());
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
            deriveTableLocationFromMetadataLocation(metadataLocation));
    if (!hasText(resolvedLocation)) {
      return metadata;
    }
    return TableMetadataViewSupport.copyMetadata(metadata).location(resolvedLocation).build();
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
