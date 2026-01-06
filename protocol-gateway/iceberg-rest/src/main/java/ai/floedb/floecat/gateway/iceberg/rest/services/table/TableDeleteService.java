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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.TableRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TableDeleteService {
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableDropCleanupService tableDropCleanupService;

  public Response delete(
      TableRequestContext tableContext,
      String tableName,
      Boolean purgeRequested,
      TableGatewaySupport tableSupport) {
    ResourceId tableId = tableContext.tableId();
    ResourceId connectorId = null;
    Table existing = null;
    try {
      existing = tableLifecycleService.getTable(tableId);
      if (existing.hasUpstream() && existing.getUpstream().hasConnectorId()) {
        connectorId = existing.getUpstream().getConnectorId();
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    boolean purge = Boolean.TRUE.equals(purgeRequested);
    if (purge) {
      tableDropCleanupService.purgeTableData(
          tableContext.catalog().catalogName(), tableContext.namespaceName(), tableName, existing);
    }
    if (connectorId != null) {
      tableSupport.deleteConnector(connectorId);
    }
    tableLifecycleService.deleteTable(tableId, purge);
    return Response.noContent().build();
  }
}
