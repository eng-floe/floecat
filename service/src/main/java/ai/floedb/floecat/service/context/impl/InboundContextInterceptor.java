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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.AccountIds;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.security.RolePermissions;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.trace.Span;
import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

@ApplicationScoped
public class InboundContextInterceptor implements ServerInterceptor {
  private static final Logger LOG = Logger.getLogger(InboundContextInterceptor.class);

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
  public static final Context.Key<EngineContext> ENGINE_CONTEXT_KEY = Context.key("engine_context");
  public static final Context.Key<String> CORR_KEY = Context.key("correlation_id");

  @Inject QueryContextStore queryStore;
  @Inject AccountRepository accountRepository;
  @Inject TenantIdentityProvider identityProvider;

  @ConfigProperty(name = "floecat.interceptor.validate.account", defaultValue = "true")
  boolean validateAccount;

  @ConfigProperty(name = "floecat.interceptor.session.header")
  Optional<String> sessionHeader;

  @ConfigProperty(name = "floecat.interceptor.session.account-claim", defaultValue = "account_id")
  String accountClaimName;

  @ConfigProperty(name = "floecat.interceptor.session.role-claim", defaultValue = "roles")
  String roleClaimName;

  @ConfigProperty(name = "floecat.interceptor.allow.principal-header", defaultValue = "false")
  boolean allowPrincipalHeader;

  @ConfigProperty(name = "floecat.interceptor.allow.dev-context", defaultValue = "true")
  boolean allowDevContext;

  @PostConstruct
  void logConfig() {
    String header = sessionHeader.orElse("");
    LOG.infof(
        "InboundContextInterceptor ready: sessionHeader=%s allowPrincipalHeader=%s"
            + " allowDevContext=%s validateAccount=%s",
        header.isBlank() ? "<disabled>" : header,
        allowPrincipalHeader,
        allowDevContext,
        validateAccount);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

    String correlationId =
        Optional.ofNullable(headers.get(CORR))
            .filter(s -> !s.isBlank())
            .orElse(UUID.randomUUID().toString());

    String queryIdHeader = Optional.ofNullable(headers.get(QUERY_ID_HEADER)).orElse("");
    String engineVersion = readHeader(headers, ENGINE_VERSION_HEADER);
    String engineKind = readHeader(headers, ENGINE_KIND_HEADER);

    ResolvedContext resolvedContext = resolvePrincipalAndQuery(headers, queryIdHeader);

    PrincipalContext principalContext = resolvedContext.pc();
    String queryId = resolvedContext.queryId();
    EngineContext engineContext = EngineContext.of(engineKind, engineVersion);

    Context context =
        Context.current()
            .withValue(PC_KEY, principalContext)
            .withValue(QUERY_KEY, queryId)
            .withValue(ENGINE_VERSION_KEY, engineVersion)
            .withValue(ENGINE_KIND_KEY, engineKind)
            .withValue(ENGINE_CONTEXT_KEY, engineContext)
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
      if (!engineVersion.isBlank()) {
        span.setAttribute("engine_version", engineVersion);
      }
      if (!engineKind.isBlank()) {
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
      if (!engineVersion.isBlank()) {
        span.setAttribute("engine_version", engineVersion);
      }
      if (!engineKind.isBlank()) {
        span.setAttribute("engine_kind", engineKind);
      }
    }

    return listener;
  }

  private ResolvedContext resolvePrincipalAndQuery(Metadata headers, String queryIdHeader) {

    if (this.sessionHeader.isPresent()) {
      SecurityIdentity identity = validateSessionHeader(headers);
      PrincipalContext principal = buildPrincipalFromIdentity(identity, queryIdHeader);
      return new ResolvedContext(principal, queryIdHeader);
    }

    if (allowPrincipalHeader && headers.containsKey(PRINC_BIN)) {
      byte[] pcBytes = headers.get(PRINC_BIN);
      PrincipalContext pc = parsePrincipal(pcBytes);

      if (this.validateAccount) {
        validateAccount(pc.getAccountId());
      }

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

    if (allowDevContext) {
      return new ResolvedContext(devContext(), "");
    }

    String message =
        allowPrincipalHeader
            ? "missing x-principal-bin"
            : "missing session header and x-principal-bin disabled";
    throw Status.UNAUTHENTICATED.withDescription(message).asRuntimeException();
  }

  private SecurityIdentity validateSessionHeader(Metadata headers) {
    var config = ConfigProvider.getConfig();
    boolean tenantEnabled =
        config.getOptionalValue("quarkus.oidc.tenant-enabled", Boolean.class).orElse(true);
    boolean hasPublicKey =
        config
            .getOptionalValue("quarkus.oidc.public-key", String.class)
            .filter(value -> !value.isBlank())
            .isPresent();
    boolean hasAuthServerUrl =
        config
            .getOptionalValue("quarkus.oidc.auth-server-url", String.class)
            .filter(value -> !value.isBlank())
            .isPresent();
    if (!tenantEnabled) {
      throw Status.UNAUTHENTICATED
          .withDescription("session header configured but OIDC tenant is disabled")
          .asRuntimeException();
    }
    if (!hasPublicKey && !hasAuthServerUrl) {
      throw Status.UNAUTHENTICATED
          .withDescription(
              "session header configured but no OIDC public key or auth server URL configured")
          .asRuntimeException();
    }
    var headerName = this.sessionHeader.orElseThrow();
    var key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    String token = Optional.ofNullable(headers.get(key)).map(String::trim).orElse("");
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      token = token.substring(7).trim();
    }
    if (token.isBlank()) {
      throw Status.UNAUTHENTICATED.withDescription("missing session token").asRuntimeException();
    }
    try {
      AccessTokenCredential credential = new AccessTokenCredential(token);
      SecurityIdentity identity = identityProvider.authenticate(credential).await().indefinitely();
      if (identity == null || identity.isAnonymous()) {
        LOG.warn("Session token inactive");
        throw Status.UNAUTHENTICATED.withDescription("inactive session token").asRuntimeException();
      }
      return identity;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Session token validation failed: %s", e.getMessage());
      throw Status.UNAUTHENTICATED
          .withDescription("invalid session token")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private String requireAccountIdClaim(SecurityIdentity identity) {
    JsonWebToken jwt = requireJwt(identity);
    Object claim = jwt.getClaim(accountClaimName);
    if (claim instanceof String value && !value.isBlank()) {
      return value;
    }
    if (claim instanceof Number value) {
      return value.toString();
    }
    if (claim instanceof Iterable<?> items) {
      for (Object item : items) {
        if (item instanceof String value && !value.isBlank()) {
          return value;
        }
      }
    }
    throw Status.UNAUTHENTICATED
        .withDescription("missing " + accountClaimName + " claim")
        .asRuntimeException();
  }

  private String requireSubjectClaim(SecurityIdentity identity) {
    JsonWebToken jwt = requireJwt(identity);
    String subject = jwt.getSubject();
    if (isBlank(subject)) {
      subject = identity.getPrincipal().getName();
    }
    if (isBlank(subject)) {
      throw Status.UNAUTHENTICATED.withDescription("missing subject").asRuntimeException();
    }
    return subject;
  }

  private JsonWebToken requireJwt(SecurityIdentity identity) {
    if (identity.getPrincipal() instanceof JsonWebToken jwt) {
      return jwt;
    }
    throw Status.UNAUTHENTICATED.withDescription("missing jwt principal").asRuntimeException();
  }

  private List<String> extractRoles(SecurityIdentity identity) {
    JsonWebToken jwt = requireJwt(identity);
    Object claim = jwt.getClaim(roleClaimName);
    if (claim instanceof String value) {
      return List.of(value);
    }
    if (claim instanceof Iterable<?> items) {
      List<String> roles = new ArrayList<>();
      for (Object item : items) {
        if (item instanceof String value) {
          roles.add(value);
        }
      }
      return roles;
    }
    return List.of();
  }

  private PrincipalContext buildPrincipalFromIdentity(
      SecurityIdentity identity, String queryIdHeader) {
    String accountId = requireAccountIdClaim(identity);
    if (validateAccount) {
      validateAccount(accountId);
    }
    String subject = requireSubjectClaim(identity);
    var roles = extractRoles(identity);
    PrincipalContext.Builder builder =
        PrincipalContext.newBuilder().setAccountId(accountId).setSubject(subject);
    if (!isBlank(queryIdHeader)) {
      builder.setQueryId(queryIdHeader);
    }
    RolePermissions.permissionsForRoles(roles, allowDevContext).forEach(builder::addPermissions);
    return builder.build();
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

  private static String readHeader(Metadata headers, Metadata.Key<String> key) {
    return Optional.ofNullable(headers.get(key)).map(String::trim).orElse("");
  }

  private PrincipalContext devContext() {
    var id = AccountIds.deterministicAccountId("/account:t-0001");
    var rid =
        ResourceId.newBuilder().setAccountId(id).setId(id).setKind(ResourceKind.RK_ACCOUNT).build();
    PrincipalContext.Builder builder =
        PrincipalContext.newBuilder()
            .setAccountId(rid.getId())
            .setSubject("dev-user")
            .setLocale("en");
    RolePermissions.permissionsForRoles(List.of(), true).forEach(builder::addPermissions);
    return builder.build();
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
