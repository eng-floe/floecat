package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TransactionCommitService {
  @Inject AccountContext accountContext;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;

  public Response commit(
      String prefix,
      String idempotencyKey,
      String transactionId,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes = request.resolvedTableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();
    for (TransactionCommitRequest.TableChange change : changes) {
      var identifier = change.identifier();
      List<String> namespacePath =
          identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
      String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
      ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
      TableRequests.Commit commitReq =
          new TableRequests.Commit(
              null,
              namespacePath,
              null,
              null,
              change.stageId(),
              change.requirements(),
              change.updates());
      Response tableResponse =
          tableCommitService.commit(
              new TableCommitService.CommitCommand(
                  prefix,
                  namespace,
                  namespacePath,
                  identifier.name(),
                  catalogName,
                  catalogId,
                  namespaceId,
                  idempotencyKey,
                  transactionId,
                  commitReq,
                  tableSupport));
      if (tableResponse.getStatus() >= 400) {
        return tableResponse;
      }
    }
    return Response.noContent().build();
  }
}
