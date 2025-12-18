package ai.floedb.floecat.service.context.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
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
  private static final Metadata.Key<String> QUERY_ID_HEADER =
      Metadata.Key.of("x-query-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_VERSION_HEADER =
      Metadata.Key.of("x-engine-version", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> ENGINE_KIND_HEADER =
      Metadata.Key.of("x-engine-kind", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);

  public static final Context.Key<PrincipalContext> PC_KEY = PrincipalProvider.KEY;
  public static final Context.Key<String> QUERY_KEY = Context.key("query_id");
  public static final Context.Key<String> ENGINE_VERSION_KEY = Context.key("engine_version");
  public static final Context.Key<String> ENGINE_KIND_KEY = Context.key("engine_kind");
  public static final Context.Key<String> CORR_KEY = Context.key("correlation_id");

  private Clock clock = Clock.systemUTC();

  @Inject QueryContextStore queryStore;
  @Inject AccountRepository accountRepository;

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String correlationId =
        Optional.ofNullable(headers.get(CORR))
            .filter(s -> !s.isBlank())
            .orElse(UUID.randomUUID().toString());

    String queryIdHeader = Optional.ofNullable(headers.get(QUERY_ID_HEADER)).orElse("");
    String engineVersion = Optional.ofNullable(headers.get(ENGINE_VERSION_HEADER)).orElse("");
    String engineKind = Optional.ofNullable(headers.get(ENGINE_KIND_HEADER)).orElse("");

    ResolvedContext resolvedContext = resolvePrincipalAndQuery(headers, queryIdHeader);

    PrincipalContext principalContext = resolvedContext.pc();
    String queryId = resolvedContext.queryId();

    Context context =
        Context.current()
            .withValue(PC_KEY, principalContext)
            .withValue(QUERY_KEY, queryId)
            .withValue(ENGINE_VERSION_KEY, engineVersion)
            .withValue(ENGINE_KIND_KEY, engineKind)
            .withValue(CORR_KEY, correlationId);

    MDC.put("query_id", queryId);
    MDC.put("correlation_id", correlationId);
    MDC.put("account_id", principalContext.getAccountId());
    MDC.put("subject", principalContext.getSubject());

    var span = Span.current();
    if (span.getSpanContext().isValid()) {
      span.setAttribute("query_id", queryId);
      span.setAttribute("correlation_id", correlationId);
      span.setAttribute("account_id", principalContext.getAccountId());
      span.setAttribute("subject", principalContext.getSubject());
      if (engineVersion != null && !engineVersion.isBlank()) {
        span.setAttribute("engine_version", engineVersion);
      }
      if (engineKind != null && !engineKind.isBlank()) {
        span.setAttribute("engine_kind", engineKind);
      }
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
              MDC.remove("query_id");
              MDC.remove("correlation_id");
              MDC.remove("account_id");
              MDC.remove("subject");
            }
            super.close(status, metadata);
          }
        };

    var listener = Contexts.interceptCall(context, forwarding, headers, next);

    span = io.opentelemetry.api.trace.Span.current();
    if (span.getSpanContext().isValid()) {
      span.setAttribute("query_id", queryId);
      span.setAttribute("correlation_id", correlationId);
      span.setAttribute("account_id", principalContext.getAccountId());
      span.setAttribute("subject", principalContext.getSubject());
      String ev = ENGINE_VERSION_KEY.get();
      if (ev != null && !ev.isBlank()) {
        span.setAttribute("engine_version", ev);
      }
      String ek = ENGINE_KIND_KEY.get();
      if (ek != null && !ek.isBlank()) {
        span.setAttribute("engine_kind", ek);
      }
    }

    return listener;
  }

  private ResolvedContext resolvePrincipalAndQuery(Metadata headers, String queryIdHeader) {
    byte[] pcBytes = headers.get(PRINC_BIN);

    if (pcBytes != null) {
      PrincipalContext pc = parsePrincipal(pcBytes);

      validateAccount(pc.getAccountId());

      if (!isBlank(queryIdHeader)
          && !isBlank(pc.getQueryId())
          && !pc.getQueryId().equals(queryIdHeader)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("query_id mismatch between header and principal")
            .asRuntimeException();
      }

      String canonicalQueryId = !isBlank(pc.getQueryId()) ? pc.getQueryId() : queryIdHeader;
      return new ResolvedContext(pc, canonicalQueryId);
    }

    if (!isBlank(queryIdHeader)) {
      QueryContext ctx =
          queryStore
              .get(queryIdHeader)
              .orElseThrow(
                  () ->
                      Status.UNAUTHENTICATED
                          .withDescription("unknown x-query-id")
                          .asRuntimeException());

      long now = clock.millis();
      if (ctx.getState() != QueryContext.State.ACTIVE) {
        throw Status.FAILED_PRECONDITION.withDescription("query not active").asRuntimeException();
      }
      if (ctx.getExpiresAtMs() < now) {
        throw Status.FAILED_PRECONDITION
            .withDescription("query lease expired")
            .asRuntimeException();
      }

      PrincipalContext principalContext = ctx.getPrincipal();
      if (!isBlank(principalContext.getQueryId())
          && !principalContext.getQueryId().equals(queryIdHeader)) {
        throw Status.FAILED_PRECONDITION
            .withDescription("query_id mismatch (store vs principal)")
            .asRuntimeException();
      }

      return new ResolvedContext(principalContext, queryIdHeader);
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
    var id = AccountIds.deterministicAccountId("/account:t-0001");
    var rid =
        ResourceId.newBuilder().setAccountId(id).setId(id).setKind(ResourceKind.RK_ACCOUNT).build();
    return PrincipalContext.newBuilder()
        .setAccountId(rid.getId())
        .setSubject("dev-user")
        .setLocale("en")
        .addPermissions("account.read")
        .addPermissions("account.write")
        .addPermissions("catalog.read")
        .addPermissions("catalog.write")
        .addPermissions("namespace.read")
        .addPermissions("namespace.write")
        .addPermissions("table.read")
        .addPermissions("table.write")
        .addPermissions("view.read")
        .addPermissions("view.write")
        .addPermissions("connector.manage")
        .build();
  }

  private void validateAccount(String accountId) {
    ResourceId accountRid =
        ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build();
    if (accountId == null
        || isBlank(accountId)
        || accountRepository.getById(accountRid).isEmpty()) {
      throw Status.UNAUTHENTICATED
          .withDescription("invalid or unknown account: " + accountId)
          .asRuntimeException();
    }
  }

  private record ResolvedContext(PrincipalContext pc, String queryId) {}
}
