package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableResponse;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class TableClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public TableClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListTablesResponse listTables(ListTablesRequest request) {
    return stub().listTables(request);
  }

  public GetTableResponse getTable(GetTableRequest request) {
    return stub().getTable(request);
  }

  public CreateTableResponse createTable(CreateTableRequest request) {
    return stub().createTable(request);
  }

  public UpdateTableResponse updateTable(UpdateTableRequest request) {
    return stub().updateTable(request);
  }

  public DeleteTableResponse deleteTable(DeleteTableRequest request) {
    return stub().deleteTable(request);
  }

  private TableServiceGrpc.TableServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().table());
  }
}
