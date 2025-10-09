package ai.floedb.metacat.service.context.impl;

import java.util.Optional;

import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.opentelemetry.api.baggage.Baggage;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.enterprise.context.ApplicationScoped;

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
    io.grpc.MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, Channel next) {

    return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
      @Override public void start(Listener<RespT> rl, Metadata headers) {
        var pc    = InboundContextInterceptor.PC_KEY.get();
        var plan  = Optional.ofNullable(InboundContextInterceptor.PLAN_KEY.get())
                            .orElseGet(() -> Baggage.current().getEntryValue("plan_id"));
        var corr  = Optional.ofNullable(InboundContextInterceptor.CORR_KEY.get())
                            .orElseGet(() -> Baggage.current().getEntryValue("correlation_id"));

        if (pc != null) headers.put(PRINC_BIN, pc.toByteArray());
        if (plan != null && !plan.isBlank()) headers.put(PLAN_ID, plan);
        if (corr != null && !corr.isBlank()) headers.put(CORR, corr);

        super.start(rl, headers);
      }
    };
  }
}