package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QuerySchemaClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public QuerySchemaClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public DescribeInputsResponse describeInputs(DescribeInputsRequest request) {
    return stub().describeInputs(request);
  }

  private QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().querySchema());
  }
}
