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

package ai.floedb.floecat.gateway.iceberg.rest.services.storage;

import ai.floedb.floecat.catalog.rpc.Table;
import java.util.Locale;

public final class StorageLocationResolver {
  private StorageLocationResolver() {}

  public static String resolveLocationPrefix(Table table) {
    String propertyLocation = table != null ? table.getPropertiesMap().get("location") : null;
    if (isNonBlank(propertyLocation)) {
      return propertyLocation.trim();
    }
    String storageLocation =
        table != null ? table.getPropertiesMap().get("storage_location") : null;
    if (isStorageUri(storageLocation)) {
      return storageLocation.trim();
    }
    String deltaTableRoot = table != null ? table.getPropertiesMap().get("delta.table-root") : null;
    if (isStorageUri(deltaTableRoot)) {
      return deltaTableRoot.trim();
    }
    String externalLocation =
        table != null ? table.getPropertiesMap().get("external.location") : null;
    if (isStorageUri(externalLocation)) {
      return externalLocation.trim();
    }
    String upstreamUri = table != null && table.hasUpstream() ? table.getUpstream().getUri() : null;
    if (isStorageUri(upstreamUri)) {
      return upstreamUri.trim();
    }
    return null;
  }

  public static String resolveLocationPrefix(String location) {
    return isStorageUri(location) ? location.trim() : null;
  }

  public static boolean isStorageUri(String value) {
    if (!isNonBlank(value)) {
      return false;
    }
    String lower = value.trim().toLowerCase(Locale.ROOT);
    return lower.startsWith("s3://")
        || lower.startsWith("s3a://")
        || lower.startsWith("s3n://")
        || lower.startsWith("abfs://")
        || lower.startsWith("abfss://")
        || lower.startsWith("gs://")
        || lower.startsWith("gcs://")
        || lower.startsWith("wasb://")
        || lower.startsWith("wasbs://")
        || lower.startsWith("adl://")
        || lower.startsWith("oss://")
        || lower.startsWith("cos://")
        || lower.startsWith("file://");
  }

  private static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }
}
