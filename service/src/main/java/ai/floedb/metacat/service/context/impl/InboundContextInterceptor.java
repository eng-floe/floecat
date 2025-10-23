package ai.floedb.metacat.service.context.impl;

import ai.floedb.metacat.common.rpc.PrincipalContext;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.planning.PlanContextStore;
import ai.floedb.metacat.service.planning.impl.PlanContext;
import ai.floedb.metacat.service.repo.impl.TenantRepository;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.trace.Span;
import io.quarkus.grpc.GlobalInterceptor;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.MDC;

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
  @Inject TenantRepository tenantRepository;

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String correlationId =
        Optional.ofNullable(headers.get(CORR))
            .filter(s -> !s.isBlank())
            .orElse(UUID.randomUUID().toString());

    String planIdHeader = Optional.ofNullable(headers.get(PLAN_ID)).orElse("");

    ResolvedContext resolvedContext = resolvePrincipalAndPlan(headers, planIdHeader);

    PrincipalContext principalContext = resolvedContext.pc();
    String planId = resolvedContext.planId();

    Context context =
        Context.current()
            .withValue(PC_KEY, principalContext)
            .withValue(PLAN_KEY, planId)
            .withValue(CORR_KEY, correlationId);

    MDC.put("plan_id", planId);
    MDC.put("correlation_id", correlationId);

    var baggage =
        Baggage.current().toBuilder()
            .put("plan_id", planId)
            .put("correlation_id", correlationId)
            .build();

    var otelCtx = baggage.storeInContext(io.opentelemetry.context.Context.current());

    var span = Span.fromContext(otelCtx);
    if (span.getSpanContext().isValid()) {
      span.setAttribute("plan_id", planId);
      span.setAttribute("correlation_id", correlationId);
      span.setAttribute("tenant_id", principalContext.getTenantId().getId());
      span.setAttribute("subject", principalContext.getSubject());
    }

    var forwarding =
        new SimpleForwardingServerCall<ReqT, RespT>(call) {
          @Override
          public void sendHeaders(Metadata h) {
            h.put(CORR, correlationId);
            super.sendHeaders(h);
          }

          @Override
          public void close(Status status, Metadata metadata) {
            try {
              metadata.put(CORR, correlationId);
            } finally {
              MDC.remove("plan_id");
              MDC.remove("correlation_id");
            }
            super.close(status, metadata);
          }
        };

    var listener = Contexts.interceptCall(context, forwarding, headers, next);

    span = io.opentelemetry.api.trace.Span.current();
    if (span.getSpanContext().isValid()) {
      span.setAttribute("plan_id", planId);
      span.setAttribute("correlation_id", correlationId);
      span.setAttribute("tenant_id", principalContext.getTenantId().getId());
      span.setAttribute("subject", principalContext.getSubject());
    }

    return listener;
  }

  private ResolvedContext resolvePrincipalAndPlan(Metadata headers, String planIdHeader) {
    byte[] pcBytes = headers.get(PRINC_BIN);

    if (pcBytes != null) {
      PrincipalContext pc = parsePrincipal(pcBytes);

      validateTenant(pc.getTenantId());

      if (!isBlank(planIdHeader)
          && !isBlank(pc.getPlanId())
          && !pc.getPlanId().equals(planIdHeader)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("plan_id mismatch between header and principal")
            .asRuntimeException();
      }

      String canonicalPlan = !isBlank(pc.getPlanId()) ? pc.getPlanId() : planIdHeader;
      return new ResolvedContext(pc, canonicalPlan);
    }

    if (!isBlank(planIdHeader)) {
      PlanContext ctx =
          planStore
              .get(planIdHeader)
              .orElseThrow(
                  () ->
                      Status.UNAUTHENTICATED
                          .withDescription("unknown x-plan-id")
                          .asRuntimeException());

      long now = clock.millis();
      if (ctx.getState() != PlanContext.State.ACTIVE) {
        throw Status.FAILED_PRECONDITION.withDescription("plan not active").asRuntimeException();
      }
      if (ctx.getExpiresAtMs() < now) {
        throw Status.FAILED_PRECONDITION.withDescription("plan lease expired").asRuntimeException();
      }

      PrincipalContext principalContext = ctx.getPrincipal();
      if (!isBlank(principalContext.getPlanId())
          && !principalContext.getPlanId().equals(planIdHeader)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("plan_id mismatch (store vs principal)")
            .asRuntimeException();
      }

      return new ResolvedContext(principalContext, planIdHeader);
    }

    return new ResolvedContext(devContext(), "");
  }

  private static PrincipalContext parsePrincipal(byte[] encoded) {
    try {
      return PrincipalContext.parseFrom(encoded);
    } catch (Exception e) {
      throw Status.UNAUTHENTICATED
          .withDescription("invalid x-principal-bin")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private static boolean isBlank(String inputString) {
    return inputString == null || inputString.isBlank();
  }

  private static PrincipalContext devContext() {
    var id = UUID.nameUUIDFromBytes("/tenant:t-0001".getBytes()).toString();
    var rid =
        ResourceId.newBuilder().setTenantId(id).setId(id).setKind(ResourceKind.RK_TENANT).build();
    return PrincipalContext.newBuilder()
        .setTenantId(rid)
        .setSubject("dev-user")
        .setLocale("en")
        .addPermissions("tenant.read")
        .addPermissions("tenant.write")
        .addPermissions("catalog.read")
        .addPermissions("catalog.write")
        .addPermissions("namespace.read")
        .addPermissions("namespace.write")
        .addPermissions("table.read")
        .addPermissions("table.write")
        .addPermissions("connector.manage")
        .build();
  }

  private void validateTenant(ResourceId tenantId) {
    if (tenantId == null
        || isBlank(tenantId.getId())
        || tenantRepository.getById(tenantId).isEmpty()) {
      throw Status.UNAUTHENTICATED
          .withDescription("invalid or unknown tenant: " + tenantId)
          .asRuntimeException();
    }
  }

  private record ResolvedContext(PrincipalContext pc, String planId) {}
}
