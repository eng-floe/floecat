package ai.floedb.metacat.service.context.impl;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.grpc.*;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@GlobalInterceptor
public class PrincipalContextInterceptor implements ServerInterceptor {
  private static final Metadata.Key<byte[]> PRINCIPAL_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    PrincipalContext ctx;
    byte[] bin = headers.get(PRINCIPAL_BIN);
    if (bin != null) {
      try { ctx = PrincipalContext.parseFrom(bin); } catch (Exception e) { ctx = devContext(); }
    } else {
      ctx = devContext();
    }
    Context newCtx = Context.current().withValue(PrincipalProvider.KEY, ctx);
    return Contexts.interceptCall(newCtx, call, headers, next);
  }

  private static PrincipalContext devContext() {
    return PrincipalContext.newBuilder()
        .setTenantId("t-0001")
        .setSubject("dev-user")
        .addPermissions("catalog.read")
        .build();
  }
}