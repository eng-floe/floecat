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

package ai.floedb.floecat.gateway.iceberg.rest.support;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.iceberg.TableMetadata;

public final class MetadataLocationUtil {

  public static final String PRIMARY_KEY = "metadata-location";

  private MetadataLocationUtil() {}

  public static String metadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String location = props.get(PRIMARY_KEY);
    return (location == null || location.isBlank()) ? null : location;
  }

  public static String metadataLocation(TableMetadata metadata) {
    return metadata == null
        ? null
        : ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank(
            metadata.metadataFileLocation(), metadata.properties().get(PRIMARY_KEY));
  }

  public static String resolveCurrentMetadataLocation(
      Map<String, String> props, String fallbackMetadataLocation) {
    String propertyLocation = metadataLocation(props);
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return (fallbackMetadataLocation == null || fallbackMetadataLocation.isBlank())
        ? null
        : fallbackMetadataLocation;
  }

  public static String resolveCurrentMetadataLocation(Table table, IcebergMetadata metadata) {
    String fallback =
        metadata == null
                || metadata.getMetadataLocation() == null
                || metadata.getMetadataLocation().isBlank()
            ? null
            : metadata.getMetadataLocation();
    return resolveCurrentMetadataLocation(
        table == null ? null : table.getPropertiesMap(), fallback);
  }

  public static String resolveOrBootstrapMetadataLocation(
      Map<String, String> props,
      String fallbackMetadataLocation,
      String tableLocation,
      String tableUuid) {
    String metadataLocation = resolveCurrentMetadataLocation(props, fallbackMetadataLocation);
    if ((metadataLocation == null || metadataLocation.isBlank())
        && tableLocation != null
        && !tableLocation.isBlank()
        && tableUuid != null
        && !tableUuid.isBlank()) {
      metadataLocation = bootstrapMetadataLocation(tableLocation, tableUuid);
      setMetadataLocation(props, metadataLocation);
    }
    return metadataLocation;
  }

  public static void setMetadataLocation(Map<String, String> props, String metadataLocation) {
    if (props == null) {
      return;
    }
    setMetadataLocation(props::put, metadataLocation);
  }

  public static void setMetadataLocation(
      BiConsumer<String, String> setter, String metadataLocation) {
    if (setter == null || metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    setter.accept(PRIMARY_KEY, metadataLocation);
  }

  public static String metadataDirectory(String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return null;
    }
    String trimmed = metadataLocation;
    while (trimmed.endsWith("/") && trimmed.length() > 1) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int slash = trimmed.lastIndexOf('/');
    if (slash < 0) {
      return null;
    }
    return trimmed.substring(0, slash);
  }

  public static String bootstrapMetadataLocation(String tableLocation, String tableUuid) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }
    if (tableUuid == null || tableUuid.isBlank()) {
      return null;
    }
    String trimmed = tableLocation;
    while (trimmed.endsWith("/") && trimmed.length() > 1) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed + "/metadata/00000-" + tableUuid + ".metadata.json";
  }
}
