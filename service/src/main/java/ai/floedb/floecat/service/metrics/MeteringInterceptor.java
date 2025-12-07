package ai.floedb.floecat.service.metrics;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.context.impl.InboundContextInterceptor;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
@GlobalInterceptor
@Priority(2)
public class MeteringInterceptor implements ServerInterceptor {

  @Inject MeterRegistry registry;

  private final Map<String, LongTaskTimer> activeTimers = new ConcurrentHashMap<>();
  private final Map<String, Timer> latencyTimers = new ConcurrentHashMap<>();
  private final Map<String, Counter> requestCounters = new ConcurrentHashMap<>();

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    final String op = simplifyOp(call.getMethodDescriptor().getFullMethodName());
    final Timer.Sample latencySample = Timer.start(registry);

    ServerCall<ReqT, RespT> forwarding =
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          LongTaskTimer.Sample inFlightSample;

          @Override
          public void sendHeaders(Metadata h) {
            final PrincipalContext pc = InboundContextInterceptor.PC_KEY.get();
            final String account = nullToDash(pc == null ? null : pc.getAccountId());

            LongTaskTimer ltt =
                activeTimers.computeIfAbsent(
                    key("rpc.active", account, op, "-"),
                    k ->
                        LongTaskTimer.builder("rpc_active_seconds")
                            .description("In-flight RPCs")
                            .tags("account", account, "op", op)
                            .register(registry));
            inFlightSample = ltt.start();

            super.sendHeaders(h);
          }

          @Override
          public void close(Status status, Metadata trailers) {
            final PrincipalContext pc = InboundContextInterceptor.PC_KEY.get();
            final String account = nullToDash(pc == null ? null : pc.getAccountId());
            final String statusStr = status.getCode().name();

            try {
              Counter reqs =
                  requestCounters.computeIfAbsent(
                      key("rpc.requests", account, op, statusStr),
                      k ->
                          Counter.builder("rpc_requests")
                              .description("Total gRPC requests")
                              .tags("account", account, "op", op, "status", statusStr)
                              .register(registry));
              reqs.increment();

              Timer latency =
                  latencyTimers.computeIfAbsent(
                      key("rpc.latency", account, op, statusStr),
                      k ->
                          Timer.builder("rpc_latency_seconds")
                              .description("RPC latency")
                              .tags("account", account, "op", op, "status", statusStr)
                              .publishPercentileHistogram(true)
                              .register(registry));
              latencySample.stop(latency);
            } finally {
              if (inFlightSample != null) inFlightSample.stop();
            }
            super.close(status, trailers);
          }
        };

    return next.startCall(forwarding, headers);
  }

  private static String simplifyOp(String fullMethod) {
    int slash = fullMethod.indexOf('/');
    if (slash <= 0) {
      return fullMethod;
    }

    String svc = fullMethod.substring(0, slash);
    int lastDot = svc.lastIndexOf('.');
    if (lastDot >= 0) {
      svc = svc.substring(lastDot + 1);
    }

    String method = fullMethod.substring(slash + 1);
    return svc + "." + method;
  }

  private static String nullToDash(String s) {
    return (s == null || s.isBlank()) ? "-" : s;
  }

  private static String key(String metric, String account, String op, String status) {
    return metric + "|" + account + "|" + op + "|" + (status == null ? "-" : status);
  }
}
