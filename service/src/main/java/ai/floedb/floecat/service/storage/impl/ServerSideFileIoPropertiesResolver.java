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

package ai.floedb.floecat.service.storage.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ServerSideFileIoPropertiesResolver {
  private static final List<String> FILE_IO_PROPERTY_KEYS =
      List.of(
          "s3.region",
          "s3.endpoint",
          "s3.path-style-access",
          "s3.access-key-id",
          "s3.secret-access-key",
          "s3.session-token");

  @Inject StorageAuthorityRepository repo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StorageAuthorityResolver resolver;

  public Map<String, String> resolve(Table table, String location) {
    String locationPrefix =
        TableStorageLocationResolver.resolveRequestedOrTableLocation(table, location, snapshotRepo);
    ResourceId tableId = resolvePersistedTableId(table);
    if (locationPrefix == null || tableId == null) {
      return Map.of();
    }

    var authorities = repo.list(tableId.getAccountId(), Integer.MAX_VALUE, "", new StringBuilder());
    var authority = StorageAuthorityResolver.resolveBest(authorities, locationPrefix).orElse(null);
    ResolveStorageAuthorityResponse response =
        resolver.buildResponse(
            authority,
            locationPrefix,
            locationPrefix == null ? java.util.List.of() : java.util.List.of(locationPrefix),
            tableId.getAccountId(),
            true);
    return mergeStorageAuthorityFileIoConfig(response);
  }

  public Map<String, String> applyToTableProperties(
      Table table, String location, Map<String, String> properties) {
    Map<String, String> resolved = resolve(table, location);
    if (resolved.isEmpty()) {
      return properties == null || properties.isEmpty() ? Map.of() : Map.copyOf(properties);
    }

    LinkedHashMap<String, String> merged = new LinkedHashMap<>();
    if (properties != null && !properties.isEmpty()) {
      merged.putAll(properties);
      FILE_IO_PROPERTY_KEYS.forEach(merged::remove);
    }
    merged.putAll(resolved);
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  private static Map<String, String> mergeStorageAuthorityFileIoConfig(
      ResolveStorageAuthorityResponse response) {
    if (response == null) {
      return Map.of();
    }
    LinkedHashMap<String, String> merged = new LinkedHashMap<>();
    if (response.getClientSafeConfigCount() > 0) {
      merged.putAll(response.getClientSafeConfigMap());
    }
    if (response.getStorageCredentialsCount() > 0) {
      merged.putAll(response.getStorageCredentials(0).getConfigMap());
    }
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
  }

  private static ResourceId resolvePersistedTableId(Table table) {
    if (table != null
        && table.hasResourceId()
        && table.getResourceId().getKind() == ResourceKind.RK_TABLE) {
      return table.getResourceId();
    }
    return null;
  }
}
