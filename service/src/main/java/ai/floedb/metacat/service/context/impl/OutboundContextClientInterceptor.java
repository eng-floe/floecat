package ai.floedb.metacat.service.context.impl;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.opentelemetry.api.baggage.Baggage;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

@ApplicationScoped
@GlobalInterceptor
public class OutboundContextClientInterceptor implements io.grpc.ClientInterceptor {
  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> PLAN_ID =
      Metadata.Key.of("x-plan-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(
        next.newCall(method, callOptions)) {

      @Override
      public void start(Listener<RespT> reponseListener, Metadata headers) {
        var principalContext = InboundContextInterceptor.PC_KEY.get();
        var planId =
            Optional.ofNullable(InboundContextInterceptor.PLAN_KEY.get())
                .orElseGet(() -> Baggage.current().getEntryValue("plan_id"));
        var correlationId =
            Optional.ofNullable(InboundContextInterceptor.CORR_KEY.get())
                .orElseGet(() -> Baggage.current().getEntryValue("correlation_id"));

        if (principalContext != null) {
          headers.put(PRINC_BIN, principalContext.toByteArray());
        }

        if (planId != null && !planId.isBlank()) {
          headers.put(PLAN_ID, planId);
        }

        if (correlationId != null && !correlationId.isBlank()) {
          headers.put(CORR, correlationId);
        }
        super.start(reponseListener, headers);
      }
    };
  }
}
