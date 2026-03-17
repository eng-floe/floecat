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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.services.metadata.FileIoFactory;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;

@ApplicationScoped
public class TableStorageCleanupService {
  private final MinimalGatewayConfig config;

  @Inject
  public TableStorageCleanupService(MinimalGatewayConfig config) {
    this.config = config;
  }

  public void purgeTableData(Table tableRecord) {
    String location = tableLocation(tableRecord);
    if (location == null || location.isBlank()) {
      return;
    }
    FileIO fileIo = null;
    try {
      fileIo = newFileIo(tableRecord.getPropertiesMap());
      if (!(fileIo instanceof SupportsPrefixOperations prefixOps)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("Configured FileIO does not support prefix deletes")
            .asRuntimeException();
      }
      prefixOps.deletePrefix(normalizePrefix(location));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("failed to purge table data")
          .withCause(e)
          .asRuntimeException();
    } finally {
      if (fileIo != null) {
        try {
          fileIo.close();
        } catch (Exception ignored) {
        }
      }
    }
  }

  protected FileIO newFileIo(Map<String, String> properties) {
    return FileIoFactory.createFileIo(FileIoFactory.filterIoProperties(properties), config);
  }

  static String tableLocation(Table tableRecord) {
    if (tableRecord == null) {
      return null;
    }
    String location = blankToNull(tableRecord.getPropertiesMap().get("location"));
    if (location != null) {
      return location;
    }
    location = blankToNull(tableRecord.getPropertiesMap().get("storage_location"));
    if (location != null) {
      return location;
    }
    String metadataLocation = blankToNull(tableRecord.getPropertiesMap().get("metadata-location"));
    if (metadataLocation == null) {
      return null;
    }
    int metadataDir = metadataLocation.indexOf("/metadata/");
    if (metadataDir > 0) {
      return metadataLocation.substring(0, metadataDir);
    }
    int slash = metadataLocation.lastIndexOf('/');
    return slash > 0 ? metadataLocation.substring(0, slash) : metadataLocation;
  }

  private static String normalizePrefix(String location) {
    return location.endsWith("/") ? location : location + "/";
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }
}
