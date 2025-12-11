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
    tableLifecycleService.deleteTable(tableId, purge);
    if (connectorId != null) {
      tableSupport.deleteConnector(connectorId);
    }
    return Response.noContent().build();
  }
}
