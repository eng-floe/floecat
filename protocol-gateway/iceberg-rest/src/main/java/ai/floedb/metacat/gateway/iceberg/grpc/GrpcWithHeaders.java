package ai.floedb.metacat.gateway.iceberg.grpc;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import io.grpc.Metadata;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.MetadataUtils;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;

@RequestScoped
public class GrpcWithHeaders {
  private final GrpcClients clients;
  private final Metadata metadata;

  @Inject
  public GrpcWithHeaders(GrpcClients clients, IcebergGatewayConfig config, HttpHeaders headers) {
    this.clients = clients;
    this.metadata = AuthMetadata.fromHeaders(config, headers);
  }

  public <T extends AbstractBlockingStub<T>> T withHeaders(T stub) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }

  public GrpcClients raw() {
    return clients;
  }
}
