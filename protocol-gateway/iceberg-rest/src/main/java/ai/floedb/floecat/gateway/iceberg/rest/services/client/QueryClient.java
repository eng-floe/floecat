package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.FetchScanBundleResponse;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class QueryClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public QueryClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public BeginQueryResponse beginQuery(BeginQueryRequest request) {
    return queryStub().beginQuery(request);
  }

  public GetQueryResponse getQuery(GetQueryRequest request) {
    return queryStub().getQuery(request);
  }

  public void endQuery(EndQueryRequest request) {
    queryStub().endQuery(request);
  }

  public FetchScanBundleResponse fetchScanBundle(FetchScanBundleRequest request) {
    return queryScanStub().fetchScanBundle(request);
  }

  public QueryServiceGrpc.QueryServiceBlockingStub queryStub() {
    return grpc.withHeaders(grpc.raw().query());
  }

  public QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScanStub() {
    return grpc.withHeaders(grpc.raw().queryScan());
  }
}
