package ai.floedb.metacat.service.context.impl;

import java.util.UUID;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.MDC;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@ApplicationScoped
@GlobalInterceptor
public class CorrelationInterceptor implements ServerInterceptor {
  private static final Metadata.Key<String> CORR_ID =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
    ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String incoming = headers.get(CORR_ID);
    String corr = (incoming == null || incoming.isBlank()) ? UUID.randomUUID().toString() : incoming;

    MDC.put("correlation_id", corr);

    PrincipalContext pc = PrincipalProvider.KEY.get();
    if (pc != null && !pc.getCorrelationId().equals(corr)) {
      pc = pc.toBuilder().setCorrelationId(corr).build();
    } else if (pc == null) {
      pc = PrincipalContext.newBuilder().setCorrelationId(corr).build();
    }
    Context ctx = Context.current().withValue(PrincipalProvider.KEY, pc);

    ServerCall<ReqT, RespT> forwarding = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
      @Override public void sendHeaders(Metadata responseHeaders) {
        responseHeaders.put(CORR_ID, corr);
        super.sendHeaders(responseHeaders);
      }
      @Override public void close(Status status, Metadata trailers) {
        trailers.put(CORR_ID, corr);
        super.close(status, trailers);
      }
    };

    return Contexts.interceptCall(ctx, forwarding, headers, next);
  }
}