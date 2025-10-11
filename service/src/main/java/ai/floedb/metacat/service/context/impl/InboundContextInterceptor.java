package ai.floedb.metacat.service.context.impl;

import java.time.Clock;
import java.util.Optional;
import java.util.UUID;

import org.jboss.logging.MDC;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.service.planning.PlanContextStore;
import ai.floedb.metacat.service.planning.impl.PlanContext;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;

@ApplicationScoped
@GlobalInterceptor
@Priority(1)
public class InboundContextInterceptor implements ServerInterceptor {
  private static final Metadata.Key<byte[]> PRINC_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> PLAN_ID =
      Metadata.Key.of("x-plan-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  public static final Context.Key<PrincipalContext> PC_KEY = PrincipalProvider.KEY;
  public static final Context.Key<String> PLAN_KEY = Context.key("plan_id");
  public static final Context.Key<String> CORR_KEY = Context.key("correlation_id");

  private Clock clock = Clock.systemUTC();

  @Inject PlanContextStore planStore;

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String corr = Optional.ofNullable(headers.get(CORR))
      .filter(s -> !s.isBlank())
      .orElse(UUID.randomUUID().toString());

    String planIdHdr = Optional.ofNullable(headers.get(PLAN_ID)).orElse("");

    ResolvedContext rc = resolvePrincipalAndPlan(headers, planIdHdr);

    PrincipalContext pc = rc.pc();
    String planId = rc.planId();

    Context ctx = Context.current()
      .withValue(PC_KEY, pc)
      .withValue(PLAN_KEY, planId)
      .withValue(CORR_KEY, corr);

    MDC.put("plan_id", planId);
    MDC.put("correlation_id", corr);

    var baggage = Baggage.current().toBuilder()
      .put("plan_id", planId)
      .put("correlation_id", corr)
      .build();
    var otelCtx = baggage.storeInContext(io.opentelemetry.context.Context.current());
    var span = Span.fromContext(otelCtx);
    if (span.getSpanContext().isValid()) {
      span.setAttribute("plan_id", planId);
      span.setAttribute("correlation_id", corr);
      span.setAttribute("tenant_id", pc.getTenantId());
      span.setAttribute("subject", pc.getSubject());
    }

    var forwarding = new SimpleForwardingServerCall<ReqT, RespT>(call) {
      @Override public void sendHeaders(Metadata h) { h.put(CORR, corr); super.sendHeaders(h); }
      @Override public void close(Status s, Metadata t) {
        try { t.put(CORR, corr); }
        finally { MDC.remove("plan_id"); MDC.remove("correlation_id"); }
        super.close(s, t);
      }
    };

    try (var scope = otelCtx.makeCurrent()) {
      return Contexts.interceptCall(ctx, forwarding, headers, next);
    }
  }

  private ResolvedContext resolvePrincipalAndPlan(Metadata headers, String planIdHdr) {
    byte[] pcBytes = headers.get(PRINC_BIN);

    if (pcBytes != null) {
      PrincipalContext pc = parsePrincipal(pcBytes);

      if (!isBlank(planIdHdr) && !isBlank(pc.getPlanId()) && !pc.getPlanId().equals(planIdHdr)) {
        throw Status.FAILED_PRECONDITION
          .withDescription("plan_id mismatch between header and principal")
          .asRuntimeException();
      }

      String canonicalPlan = !isBlank(pc.getPlanId()) ? pc.getPlanId() : planIdHdr;
      return new ResolvedContext(pc, canonicalPlan);
    }

    if (!isBlank(planIdHdr)) {
      PlanContext ctx = planStore.get(planIdHdr)
        .orElseThrow(() -> Status.UNAUTHENTICATED
          .withDescription("unknown x-plan-id")
          .asRuntimeException());

      long now = clock.millis();
      if (ctx.getState() != PlanContext.State.ACTIVE) {
        throw Status.FAILED_PRECONDITION
          .withDescription("plan not active")
          .asRuntimeException();
      }
      if (ctx.getExpiresAtMs() < now) {
        throw Status.FAILED_PRECONDITION
          .withDescription("plan lease expired")
          .asRuntimeException();
      }

      PrincipalContext pc = ctx.getPrincipal();
      if (!isBlank(pc.getPlanId()) && !pc.getPlanId().equals(planIdHdr)) {
        throw Status.FAILED_PRECONDITION
          .withDescription("plan_id mismatch (store vs principal)")
          .asRuntimeException();
      }

      if (!ctx.getTenantId().equals(pc.getTenantId())) {
        throw Status.FAILED_PRECONDITION
          .withDescription("tenant mismatch (store vs principal)")
          .asRuntimeException();
      }

      return new ResolvedContext(pc, planIdHdr);
    }

    return new ResolvedContext(devContext(), "");
  }

  private static PrincipalContext parsePrincipal(byte[] bin) {
    try {
      return PrincipalContext.parseFrom(bin);
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED
        .withDescription("invalid x-principal-bin")
        .withCause(e)
        .asRuntimeException();
    }
  }

  private static boolean isBlank(String s) { return s == null || s.isBlank(); }

  private static PrincipalContext devContext() {
    return PrincipalContext.newBuilder()
      .setTenantId("t-0001")
      .setSubject("dev-user")
      .setLocale("en")
      .addPermissions("catalog.read")
      .addPermissions("catalog.write")
      .addPermissions("namespace.read")
      .addPermissions("namespace.write")
      .addPermissions("table.read")
      .addPermissions("table.write")
      .build();
  }

  private record ResolvedContext(PrincipalContext pc, String planId) {}
}
