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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableResponseMapper {
  private TableResponseMapper() {}

  public static LoadTableResultDto toLoadResult(
      TableMetadataView metadataView,
      Map<String, String> configOverrides,
      List<StorageCredentialDto> storageCredentials) {
    Map<String, String> effectiveConfig =
        augmentConfigWithMetadataPath(configOverrides, metadataView);
    return new LoadTableResultDto(
        metadataView.metadataLocation(), metadataView, effectiveConfig, storageCredentials);
  }

  public static CommitTableResponseDto toCommitResponse(TableMetadataView metadataView) {
    return new CommitTableResponseDto(metadataView.metadataLocation(), metadataView);
  }

  private static Map<String, String> augmentConfigWithMetadataPath(
      Map<String, String> originalConfig, TableMetadataView metadataView) {
    String metadataLocation = metadataView.metadataLocation();
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return originalConfig;
    }
    String directory = MetadataLocationUtil.canonicalMetadataDirectory(metadataLocation);
    if (directory == null || directory.isBlank()) {
      return originalConfig;
    }
    Map<String, String> updated =
        originalConfig.isEmpty() ? new LinkedHashMap<>() : new LinkedHashMap<>(originalConfig);
    updated.put("write.metadata.path", directory);
    return Map.copyOf(updated);
  }
}
