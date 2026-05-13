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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.load;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableRef;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TableLoadService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableLoadSupport loadSupport;

  public Response load(
      TableRef tableContext,
      String tableName,
      String snapshots,
      String accessDelegationMode,
      String ifNoneMatch,
      TableGatewaySupport tableSupport) {
    Table tableRecord = tableLifecycleService.getTable(tableContext.tableId());
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = loadSupport.parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    TableLoadSupport.LoadData loadData =
        loadSupport.loadData(tableRecord, snapshotMode, tableSupport);
    String etagValue = loadSupport.etagValue(loadData.metadataLocation(), snapshotMode);
    if (loadSupport.hasWildcardIfNoneMatch(ifNoneMatch)) {
      return IcebergErrorResponses.validation("If-None-Match may not take the value of '*'");
    }
    if (loadSupport.etagMatches(etagValue, ifNoneMatch)) {
      return Response.notModified().build();
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(tableRecord, accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                tableRecord,
                loadData.snapshots(),
                loadData.metadataLocation(),
                tableSupport.defaultTableConfig(tableRecord),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }

  public Response loadResolvedTable(
      String tableName,
      Table tableRecord,
      String accessDelegationMode,
      TableGatewaySupport tableSupport) {
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(tableRecord, accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    return buildLoadResponse(
        tableName, tableRecord, SnapshotLister.Mode.ALL, credentials, null, tableSupport);
  }

  public Response loadResolvedTable(
      String tableName,
      Table tableRecord,
      List<StorageCredentialDto> credentials,
      TableGatewaySupport tableSupport) {
    return buildLoadResponse(
        tableName, tableRecord, SnapshotLister.Mode.ALL, credentials, null, tableSupport);
  }

  private Response buildLoadResponse(
      String tableName,
      Table tableRecord,
      SnapshotLister.Mode snapshotMode,
      List<StorageCredentialDto> credentials,
      String etagValue,
      TableGatewaySupport tableSupport) {
    TableLoadSupport.LoadData loadData =
        loadSupport.loadData(tableRecord, snapshotMode, tableSupport);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                tableRecord,
                loadData.snapshots(),
                loadData.metadataLocation(),
                tableSupport.defaultTableConfig(tableRecord),
                credentials));
    if (etagValue != null) {
      builder.header(HttpHeaders.ETAG, etagValue);
    }
    return builder.build();
  }
}
