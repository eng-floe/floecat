package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableAdminResourceUnitTest {
  private final TableAdminResource resource = new TableAdminResource();
  private final GrpcWithHeaders grpc = mock(GrpcWithHeaders.class);
  private final GrpcClients grpcClients = mock(GrpcClients.class);
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory =
      mock(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class);
  private final IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
  private final AccountContext accountContext = mock(AccountContext.class);
  private final TableLifecycleService tableLifecycleService = mock(TableLifecycleService.class);
  private final TableCommitService tableCommitService = mock(TableCommitService.class);
  private final Config mpConfig = mock(Config.class);

  @BeforeEach
  void setUp() {
    resource.grpc = grpc;
    resource.config = config;
    resource.accountContext = accountContext;
    resource.tableLifecycleService = tableLifecycleService;
    resource.tableCommitService = tableCommitService;
    resource.mapper = new ObjectMapper();
    resource.mpConfig = mpConfig;
    when(grpc.raw()).thenReturn(grpcClients);
    when(grpc.withHeaders(any(DirectoryServiceGrpc.DirectoryServiceBlockingStub.class)))
        .thenAnswer(invocation -> invocation.getArgument(0));
    when(grpcClients.directory()).thenReturn(directory);
    when(directory.resolveCatalog(any()))
        .thenReturn(
            ResolveCatalogResponse.newBuilder()
                .setResourceId(ResourceId.newBuilder().setId("cat:foo").build())
                .build());
    when(mpConfig.getOptionalValue(any(), any())).thenReturn(Optional.empty());
    when(config.catalogMapping()).thenReturn(Map.of());
    when(config.storageCredential()).thenReturn(Optional.empty());
    when(config.defaultRegion()).thenReturn(Optional.empty());
    when(accountContext.getAccountId()).thenReturn("account1");
    resource.initSupport();
  }

  @Test
  void commitTransactionDelegatesToTableCommitService() {
    ResourceId namespaceId = ResourceId.newBuilder().setId("cat:db").build();
    when(tableLifecycleService.resolveNamespaceId(anyString(), anyList())).thenReturn(namespaceId);
    CommitTableResponseDto commitResponse =
        new CommitTableResponseDto("s3://bucket/orders/metadata.json", null);
    when(tableCommitService.commit(any())).thenReturn(Response.ok(commitResponse).build());

    TransactionCommitRequest request =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(List.of("db"), "orders"), "stage-commit", null, null)),
            List.of(
                new TransactionCommitRequest.StagedRefUpdate(
                    new TableIdentifierDto(List.of("db"), "orders"), "stage-commit", null, null)),
            null,
            null);

    var response = resource.commitTransaction("foo", null, null, request);
    assertEquals(200, response.getStatus());
  }
}
