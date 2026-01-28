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

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.common.IcebergHttpUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TableLoadService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject SnapshotClient snapshotClient;

  public Response load(
      TableRequestContext tableContext,
      String tableName,
      String snapshots,
      String accessDelegationMode,
      String ifNoneMatch,
      TableGatewaySupport tableSupport) {
    Table tableRecord = tableLifecycleService.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(tableRecord);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(
            snapshotClient, tableContext.tableId(), snapshotMode, metadata);
    String etagValue = metadataLocation(metadata);
    if (etagValue != null) {
      etagValue = IcebergHttpUtil.etagForMetadataLocation(etagValue);
    }
    if (ifNoneMatch != null && ifNoneMatch.trim().equals("*")) {
      return IcebergErrorResponses.validation("If-None-Match may not take the value of '*'");
    }
    if (etagMatches(etagValue, ifNoneMatch)) {
      return Response.notModified().build();
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                tableRecord,
                metadata,
                snapshotList,
                tableSupport.defaultTableConfig(),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }

  private SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private boolean etagMatches(String etagValue, String ifNoneMatch) {
    if (etagValue == null || ifNoneMatch == null) {
      return false;
    }
    String expected = normalizeEtag(etagValue);
    for (String raw : ifNoneMatch.split(",")) {
      String token = normalizeEtag(raw);
      if (!token.isEmpty() && token.equals(expected)) {
        return true;
      }
    }
    return false;
  }

  private String normalizeEtag(String token) {
    if (token == null) {
      return "";
    }
    String value = token.trim();
    if (value.startsWith("W/")) {
      value = value.substring(2).trim();
    }
    if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
      value = value.substring(1, value.length() - 1);
    }
    return value;
  }

  private String metadataLocation(IcebergMetadata metadata) {
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    return null;
  }
}
