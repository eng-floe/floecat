/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.context.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.context.impl.InboundCallContextHelper.ResolvedCallContext;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.quarkus.oidc.TenantIdentityProvider;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Inbound gRPC context logic (plain helper).
 *
 * <p>Intentionally does NOT implement io.grpc.ServerInterceptor to avoid Quarkus build-time "unused
 * gRPC interceptor" warnings. It is invoked by {@link BlockingInboundContextInterceptor} via a
 * runtime-generated proxy, and executed off the Vert.x event-loop using {@code
 * BlockingServerInterceptor.wrap(...)}.
 *
 * <p>All auth/principal/OIDC logic is now delegated to {@link InboundCallContextHelper}, which is
 * shared with the Arrow Flight path ({@code InboundContextFlightMiddleware}). This class is
 * responsible only for the gRPC-specific wiring: populating {@link io.grpc.Context} keys, managing
 * MDC entries, and echoing the correlation-id in response headers.
 */
public class InboundContextInterceptor {

  private static final Logger LOG = Logger.getLogger(InboundContextInterceptor.class);

  private static final Metadata.Key<String> QUERY_ID_HEADER =
      Metadata.Key.of(InboundCallContextHelper.HEADER_QUERY_ID, Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORR =
      Metadata.Key.of(
          InboundCallContextHelper.HEADER_CORRELATION_ID, Metadata.ASCII_STRING_MARSHALLER);
  private static final ConcurrentHashMap<String, Metadata.Key<String>> HEADER_KEYS =
      new ConcurrentHashMap<>();

  // -------------------------------------------------------------------------
  //  gRPC Context keys — read by EngineContextProvider, PrincipalProvider, etc.
  // -------------------------------------------------------------------------

  public static final Context.Key<PrincipalContext> PC_KEY = PrincipalProvider.KEY;
  public static final Context.Key<String> QUERY_KEY = Context.key("query_id");
  public static final Context.Key<String> ENGINE_VERSION_KEY = Context.key("engine_version");
  public static final Context.Key<String> ENGINE_KIND_KEY = Context.key("engine_kind");
  public static final Context.Key<EngineContext> ENGINE_CONTEXT_KEY = Context.key("engine_context");
  public static final Context.Key<String> CORR_KEY = Context.key("correlation_id");
  public static final Context.Key<String> SESSION_HEADER_VALUE_KEY =
      Context.key("session_header_value");
  public static final Context.Key<String> AUTHORIZATION_HEADER_VALUE_KEY =
      Context.key("authorization_header_value");

  // -------------------------------------------------------------------------

  private final InboundCallContextHelper helper;

  public InboundContextInterceptor(
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      boolean validateAccount,
      Optional<String> sessionHeader,
      Optional<String> authorizationHeader,
      String authMode,
      String accountClaimName,
      String roleClaimName) {
    this.helper =
        new InboundCallContextHelper(
            accountRepository,
            identityProvider,
            validateAccount,
            sessionHeader,
            authorizationHeader,
            authMode,
            accountClaimName,
            roleClaimName);

    LOG.infof(
        "InboundContextInterceptor ready: sessionHeader=%s authorizationHeader=%s"
            + " authMode=%s validateAccount=%s",
        sessionHeader.filter(h -> !h.isBlank()).orElse("<disabled>"),
        authorizationHeader.filter(h -> !h.isBlank()).orElse("<disabled>"),
        authMode,
        validateAccount);
  }

  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    ResolvedCallContext resolved =
        helper.resolve(name -> headers.get(metadataKey(name)), isCallAllowed(call));

    PrincipalContext principalContext = resolved.principalContext();
    String queryId = resolved.queryId();
    String correlationId = resolved.correlationId();
    EngineContext engineContext = resolved.engineContext();
    String engineKind = engineContext.engineKind();
    String engineVersion = engineContext.engineVersion();
    String sessionHeaderValue = resolved.sessionHeaderValue();
    String authorizationHeaderValue = resolved.authorizationHeaderValue();

    Context context =
        Context.current()
            .withValue(PC_KEY, principalContext)
            .withValue(QUERY_KEY, queryId)
            .withValue(ENGINE_VERSION_KEY, engineVersion)
            .withValue(ENGINE_KIND_KEY, engineKind)
            .withValue(ENGINE_CONTEXT_KEY, engineContext)
            .withValue(CORR_KEY, correlationId);
    if (sessionHeaderValue != null) {
      context = context.withValue(SESSION_HEADER_VALUE_KEY, sessionHeaderValue);
    }
    if (authorizationHeaderValue != null) {
      context = context.withValue(AUTHORIZATION_HEADER_VALUE_KEY, authorizationHeaderValue);
    }

    MDC.put("query_id", queryId);
    MDC.put("correlation_id", correlationId);
    MDC.put("floecat_account_id", principalContext.getAccountId());
    MDC.put("floecat_subject", principalContext.getSubject());
    MDC.put("floecat_engine_kind", engineKind);
    MDC.put("floecat_engine_version", engineVersion);

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
              MDC.remove("floecat_account_id");
              MDC.remove("floecat_subject");
              MDC.remove("floecat_engine_kind");
              MDC.remove("floecat_engine_version");
            }
            super.close(status, metadata);
          }
        };

    return Contexts.interceptCall(context, forwarding, headers, next);
  }

  private static boolean isCallAllowed(ServerCall<?, ?> call) {
    String method = call.getMethodDescriptor().getFullMethodName();
    return method != null
        && (method.startsWith("grpc.health.v1.Health/")
            || method.startsWith("grpc.reflection.v1.ServerReflection/"));
  }

  private static Metadata.Key<String> metadataKey(String name) {
    return HEADER_KEYS.computeIfAbsent(
        name, h -> Metadata.Key.of(h, Metadata.ASCII_STRING_MARSHALLER));
  }
}
