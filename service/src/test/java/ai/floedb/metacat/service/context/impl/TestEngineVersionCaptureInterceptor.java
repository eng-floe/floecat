package ai.floedb.metacat.service.context.impl;

import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test-only server interceptor that captures the raw `x-engine-version` header seen by inbound gRPC
 * calls so integration tests can assert propagation.
 */
@ApplicationScoped
@GlobalInterceptor
@Priority(1000)
public class TestEngineVersionCaptureInterceptor implements ServerInterceptor {
  private static final AtomicReference<String> CAPTURED = new AtomicReference<>();

  public static void reset() {
    CAPTURED.set(null);
  }

  public static String captured() {
    return CAPTURED.get();
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    Metadata.Key<String> ENGINE_VERSION =
        Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
    CAPTURED.set(headers.get(ENGINE_VERSION));
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
        next.startCall(
            new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
              @Override
              public void sendHeaders(Metadata responseHeaders) {
                super.sendHeaders(responseHeaders);
              }
            },
            headers)) {};
  }
}
