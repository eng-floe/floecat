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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityForLocationRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityRequest;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GrpcStorageCredentialAuthority implements StorageCredentialAuthority {
  private final GrpcServiceFacade grpcClient;
  private final AccountContext accountContext;

  @Inject
  public GrpcStorageCredentialAuthority(
      GrpcServiceFacade grpcClient, AccountContext accountContext) {
    this.grpcClient = grpcClient;
    this.accountContext = accountContext;
  }

  @Override
  public List<StorageCredentialDto> resolveForTable(Table table, boolean required) {
    String locationPrefix = resolveRequiredLocationPrefix(table, required);
    if (locationPrefix == null) {
      return null;
    }
    ResourceId tableId = resolvePersistedTableId(table);
    if (tableId == null) {
      if (required) {
        throw new IllegalArgumentException(
            "Credential vending requires a persisted table resource");
      }
      return null;
    }
    var response = resolveStorageAuthority(tableId, locationPrefix, true, required, false);
    if (response == null || response.getStorageCredentialsCount() == 0) {
      return null;
    }
    return response.getStorageCredentialsList().stream()
        .map(
            credential ->
                new StorageCredentialDto(
                    credential.getPrefix(),
                    credential.getConfigMap(),
                    credential.hasExpiresAt()
                        ? java.time.Instant.ofEpochSecond(
                            credential.getExpiresAt().getSeconds(),
                            credential.getExpiresAt().getNanos())
                        : null))
        .toList();
  }

  @Override
  public Map<String, String> clientSafeConfig(Table table) {
    String locationPrefix = resolveRequiredLocationPrefix(table, false);
    if (locationPrefix == null) {
      return Map.of();
    }
    ResourceId tableId = resolvePersistedTableId(table);
    if (tableId == null) {
      return Map.of();
    }
    try {
      var response = resolveStorageAuthority(tableId, locationPrefix, false, false, false);
      if (response == null || response.getClientSafeConfigCount() == 0) {
        return Map.of();
      }
      return response.getClientSafeConfigMap();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    var response = resolveStorageAuthorityForLocation(locationPrefix, false, false);
    if (response == null || response.getClientSafeConfigCount() == 0) {
      return Map.of();
    }
    return response.getClientSafeConfigMap();
  }

  @Override
  public Map<String, String> resolveServerSideFileIoConfig(Table table, boolean required) {
    String locationPrefix = resolveRequiredLocationPrefix(table, required);
    if (locationPrefix == null) {
      return Map.of();
    }
    ResourceId tableId = resolvePersistedTableId(table);
    if (tableId != null) {
      try {
        return serverSideFileIoConfig(
            resolveStorageAuthority(tableId, locationPrefix, true, required, true));
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
      }
    }
    return serverSideFileIoConfig(
        resolveStorageAuthorityForLocation(locationPrefix, true, required));
  }

  @Override
  public Map<String, String> resolveFileIoConfigForLocation(String location, boolean required) {
    String locationPrefix = resolveRequiredLocationPrefix(location, required);
    if (locationPrefix == null) {
      return Map.of();
    }
    return serverSideFileIoConfig(
        resolveStorageAuthorityForLocation(locationPrefix, true, required));
  }

  @Override
  public Map<String, String> resolveFileIoConfigForLocation(
      ResourceId tableId, String location, boolean required) {
    String locationPrefix = resolveRequiredLocationPrefix(location, required);
    if (locationPrefix == null || tableId == null) {
      return Map.of();
    }
    return serverSideFileIoConfig(
        resolveStorageAuthorityForLocation(locationPrefix, true, required));
  }

  private ResolveStorageAuthorityResponse resolveStorageAuthority(
      ResourceId tableId,
      String locationPrefix,
      boolean includeCredentials,
      boolean required,
      boolean serverSide) {
    return grpcClient.resolveStorageAuthority(
        ResolveStorageAuthorityRequest.newBuilder()
            .setTableId(tableId)
            .setLocationPrefix(locationPrefix)
            .setIncludeCredentials(includeCredentials)
            .setRequired(required)
            .setServerSide(serverSide)
            .build());
  }

  private ResolveStorageAuthorityResponse resolveStorageAuthorityForLocation(
      String locationPrefix, boolean includeCredentials, boolean required) {
    String accountId = accountContext != null ? accountContext.getAccountId() : null;
    if (!isNonBlank(accountId)) {
      if (required) {
        throw new IllegalArgumentException(
            "Storage authority resolution was requested but no account context is available");
      }
      return null;
    }
    return grpcClient.resolveStorageAuthorityForLocation(
        ResolveStorageAuthorityForLocationRequest.newBuilder()
            .setLocationPrefix(locationPrefix)
            .setIncludeCredentials(includeCredentials)
            .setRequired(required)
            .build());
  }

  private static String resolveRequiredLocationPrefix(Table table, boolean required) {
    String location = resolveLocationPrefix(table);
    if (location != null) {
      return location;
    }
    if (required) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no concrete storage location is available for this table");
    }
    return null;
  }

  private static String resolveRequiredLocationPrefix(String location, boolean required) {
    String resolved = resolveLocationPrefix(location);
    if (resolved != null) {
      return resolved;
    }
    if (required) {
      throw new IllegalArgumentException(
          "Credential vending was requested but no concrete storage location is available for this table");
    }
    return null;
  }

  static String resolveLocationPrefix(Table table) {
    return StorageLocationResolver.resolveLocationPrefix(table);
  }

  static String resolveLocationPrefix(String location) {
    return StorageLocationResolver.resolveLocationPrefix(location);
  }

  private static Map<String, String> serverSideFileIoConfig(
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

  private static boolean isNonBlank(String value) {
    return value != null && !value.isBlank();
  }

  private ResourceId resolvePersistedTableId(Table table) {
    if (table != null
        && table.hasResourceId()
        && table.getResourceId().getKind() == ResourceKind.RK_TABLE) {
      return table.getResourceId();
    }
    return null;
  }

  static boolean isStorageUri(String value) {
    return StorageLocationResolver.isStorageUri(value);
  }
}
