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
import io.grpc.Status;
import io.opentelemetry.api.trace.Span;
import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * Inbound gRPC context logic (plain helper).
 *
 * <p>Intentionally does NOT implement io.grpc.ServerInterceptor to avoid Quarkus build-time "unused
 * gRPC interceptor" warnings. It is invoked by BlockingInboundContextInterceptor via a
 * runtime-generated proxy, and executed off the Vert.x event-loop using
 * BlockingServerInterceptor.wrap(...).
 */
public class InboundContextInterceptor {
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

  private final AccountRepository accountRepository;
  private final TenantIdentityProvider identityProvider;

  private final boolean validateAccount;
  private final Optional<String> sessionHeader;
  private final Optional<String> authorizationHeader;
  private final Optional<String> accountHeader;
  private final AuthMode authMode;
  private final boolean allowDevContext;
  private final String accountClaimName;
  private final String roleClaimName;
  private final boolean allowPrincipalHeader;

  public InboundContextInterceptor(
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      boolean validateAccount,
      Optional<String> sessionHeader,
      Optional<String> authorizationHeader,
      Optional<String> accountHeader,
      String authMode,
      boolean allowDevContext,
      String accountClaimName,
      String roleClaimName,
      boolean allowPrincipalHeader) {
    this.accountRepository = accountRepository;
    this.identityProvider = identityProvider;
    this.validateAccount = validateAccount;
    this.sessionHeader = sessionHeader.filter(header -> !header.isBlank());
    this.authorizationHeader = authorizationHeader.filter(header -> !header.isBlank());
    this.accountHeader = accountHeader.filter(header -> !header.isBlank());
    this.authMode = AuthMode.fromString(authMode);
    this.allowDevContext = allowDevContext;
    this.accountClaimName = accountClaimName;
    this.roleClaimName = roleClaimName;
    this.allowPrincipalHeader = allowPrincipalHeader;

    String header = this.sessionHeader.orElse("");
    String authHeader = this.authorizationHeader.orElse("");
    String acctHeader = this.accountHeader.orElse("");
    LOG.infof(
        "InboundContextInterceptor ready: sessionHeader=%s authorizationHeader=%s"
            + " accountHeader=%s authMode=%s allowPrincipalHeader=%s allowDevContext=%s"
            + " validateAccount=%s",
        header.isBlank() ? "<disabled>" : header,
        authHeader.isBlank() ? "<disabled>" : authHeader,
        acctHeader.isBlank() ? "<disabled>" : acctHeader,
        this.authMode.name().toLowerCase(),
        allowPrincipalHeader,
        allowDevContext,
        validateAccount);
  }

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

    return Contexts.interceptCall(context, forwarding, headers, next);
  }

  private ResolvedContext resolvePrincipalAndQuery(Metadata headers, String queryIdHeader) {
    Optional<String> accountHeaderValue = readHeaderValue(headers, accountHeader);

    switch (authMode) {
      case DEV -> {
        if (!allowDevContext) {
          throw Status.UNAUTHENTICATED
              .withDescription("dev auth mode enabled but dev context is disabled")
              .asRuntimeException();
        }
        PrincipalContext dev = devContext();
        if (accountHeaderValue.isPresent()
            && !accountHeaderValue.orElseThrow().equals(dev.getAccountId())) {
          throw Status.UNAUTHENTICATED
              .withDescription("account header does not match principal")
              .asRuntimeException();
        }
        return new ResolvedContext(dev, "");
      }
      case PRINCIPAL_HEADER -> {
        if (!allowPrincipalHeader) {
          throw Status.UNAUTHENTICATED
              .withDescription("principal header auth mode enabled but header is disabled")
              .asRuntimeException();
        }
        if (!headers.containsKey(PRINC_BIN)) {
          throw Status.UNAUTHENTICATED
              .withDescription("missing x-principal-bin")
              .asRuntimeException();
        }
        byte[] pcBytes = headers.get(PRINC_BIN);
        PrincipalContext pc = parsePrincipal(pcBytes);
        if (accountHeaderValue.isPresent()
            && !accountHeaderValue.orElseThrow().equals(pc.getAccountId())) {
          throw Status.UNAUTHENTICATED
              .withDescription("account header does not match principal")
              .asRuntimeException();
        }
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
      case OIDC -> {
        PrincipalContext principal =
            resolveOidcPrincipal(headers, queryIdHeader, accountHeaderValue);
        return new ResolvedContext(principal, queryIdHeader);
      }
      case OIDC_OR_PRINCIPAL -> {
        Optional<PrincipalContext> oidc =
            resolveOidcPrincipalOptional(headers, queryIdHeader, accountHeaderValue);
        if (oidc.isPresent()) {
          return new ResolvedContext(oidc.get(), queryIdHeader);
        }
        if (!allowPrincipalHeader) {
          throw Status.UNAUTHENTICATED
              .withDescription("missing session token")
              .asRuntimeException();
        }
        if (!headers.containsKey(PRINC_BIN)) {
          throw Status.UNAUTHENTICATED
              .withDescription("missing session token")
              .asRuntimeException();
        }
        byte[] pcBytes = headers.get(PRINC_BIN);
        PrincipalContext pc = parsePrincipal(pcBytes);
        if (accountHeaderValue.isPresent()
            && !accountHeaderValue.orElseThrow().equals(pc.getAccountId())) {
          throw Status.UNAUTHENTICATED
              .withDescription("account header does not match principal")
              .asRuntimeException();
        }
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
      case AUTO -> {}
    }

    if (authMode == AuthMode.AUTO && this.sessionHeader.isPresent()) {
      validateSessionHeaderConfig();
    }

    if (this.sessionHeader.isPresent() && hasHeader(headers, this.sessionHeader.orElseThrow())) {
      String headerName = this.sessionHeader.orElseThrow();
      SecurityIdentity identity = validateOidcHeader(headers, headerName, "session");
      PrincipalContext principal =
          buildPrincipalFromIdentity(identity, queryIdHeader, accountHeaderValue);
      return new ResolvedContext(principal, queryIdHeader);
    }

    if (this.authorizationHeader.isPresent()
        && hasHeader(headers, this.authorizationHeader.orElseThrow())) {
      String headerName = this.authorizationHeader.orElseThrow();
      SecurityIdentity identity = validateOidcHeader(headers, headerName, "authorization");
      PrincipalContext principal =
          buildPrincipalFromIdentity(identity, queryIdHeader, accountHeaderValue);
      return new ResolvedContext(principal, queryIdHeader);
    }

    if (this.sessionHeader.isPresent()) {
      throw Status.UNAUTHENTICATED.withDescription("missing session token").asRuntimeException();
    }

    if (allowPrincipalHeader && headers.containsKey(PRINC_BIN)) {
      byte[] pcBytes = headers.get(PRINC_BIN);
      PrincipalContext pc = parsePrincipal(pcBytes);
      if (accountHeaderValue.isPresent()
          && !accountHeaderValue.orElseThrow().equals(pc.getAccountId())) {
        throw Status.UNAUTHENTICATED
            .withDescription("account header does not match principal")
            .asRuntimeException();
      }

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
      PrincipalContext dev = devContext();
      if (accountHeaderValue.isPresent()
          && !accountHeaderValue.orElseThrow().equals(dev.getAccountId())) {
        throw Status.UNAUTHENTICATED
            .withDescription("account header does not match principal")
            .asRuntimeException();
      }
      return new ResolvedContext(dev, "");
    }

    String message =
        allowPrincipalHeader
            ? "missing x-principal-bin"
            : "missing session header and x-principal-bin disabled";
    throw Status.UNAUTHENTICATED.withDescription(message).asRuntimeException();
  }

  private void validateSessionHeaderConfig() {
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
  }

  private PrincipalContext resolveOidcPrincipal(
      Metadata headers, String queryIdHeader, Optional<String> accountHeaderValue) {
    Optional<PrincipalContext> principal =
        resolveOidcPrincipalOptional(headers, queryIdHeader, accountHeaderValue);
    if (principal.isPresent()) {
      return principal.get();
    }
    if (this.sessionHeader.isPresent()) {
      throw Status.UNAUTHENTICATED.withDescription("missing session token").asRuntimeException();
    }
    throw Status.UNAUTHENTICATED
        .withDescription("missing authorization token")
        .asRuntimeException();
  }

  private Optional<PrincipalContext> resolveOidcPrincipalOptional(
      Metadata headers, String queryIdHeader, Optional<String> accountHeaderValue) {
    if (this.sessionHeader.isPresent() && hasHeader(headers, this.sessionHeader.orElseThrow())) {
      String headerName = this.sessionHeader.orElseThrow();
      SecurityIdentity identity = validateOidcHeader(headers, headerName, "session");
      return Optional.of(buildPrincipalFromIdentity(identity, queryIdHeader, accountHeaderValue));
    }
    if (this.authorizationHeader.isPresent()
        && hasHeader(headers, this.authorizationHeader.orElseThrow())) {
      String headerName = this.authorizationHeader.orElseThrow();
      SecurityIdentity identity = validateOidcHeader(headers, headerName, "authorization");
      return Optional.of(buildPrincipalFromIdentity(identity, queryIdHeader, accountHeaderValue));
    }
    return Optional.empty();
  }

  private SecurityIdentity validateOidcHeader(
      Metadata headers, String headerName, String headerLabel) {
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
          .withDescription(headerLabel + " header configured but OIDC tenant is disabled")
          .asRuntimeException();
    }
    if (!hasPublicKey && !hasAuthServerUrl) {
      throw Status.UNAUTHENTICATED
          .withDescription(
              headerLabel
                  + " header configured but no OIDC public key or auth server URL configured")
          .asRuntimeException();
    }
    var key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    String token = Optional.ofNullable(headers.get(key)).map(String::trim).orElse("");
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      token = token.substring(7).trim();
    }
    if (token.isBlank()) {
      throw Status.UNAUTHENTICATED
          .withDescription("missing " + headerLabel + " token")
          .asRuntimeException();
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

  private static boolean hasHeader(Metadata headers, String headerName) {
    Metadata.Key<String> key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    return headers.containsKey(key);
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
    List<String> roles = extractRolesFromClaim(claim);
    if (!roles.isEmpty()) {
      return roles;
    }
    Object realmAccess = jwt.getClaim("realm_access");
    roles = extractRolesFromClaim(realmAccess);
    if (!roles.isEmpty()) {
      return roles;
    }
    return List.of();
  }

  private static List<String> extractRolesFromClaim(Object claim) {
    if (claim instanceof String value) {
      String trimmed = value.trim();
      if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
        try {
          io.vertx.core.json.JsonArray jsonArray = new io.vertx.core.json.JsonArray(trimmed);
          return extractRolesFromIterable(jsonArray.getList());
        } catch (RuntimeException ignored) {
          // Fall through to treat as a single role string.
        }
      }
      return List.of(value);
    }
    if (claim instanceof io.vertx.core.json.JsonArray jsonArray) {
      return extractRolesFromIterable(jsonArray.getList());
    }
    if (claim instanceof io.vertx.core.json.JsonObject jsonObject) {
      Object rolesValue = jsonObject.getValue("roles");
      return extractRolesFromClaim(rolesValue);
    }
    if (claim instanceof jakarta.json.JsonArray jsonArray) {
      List<String> roles = new ArrayList<>();
      for (jakarta.json.JsonValue value : jsonArray) {
        if (value.getValueType() == jakarta.json.JsonValue.ValueType.STRING) {
          roles.add(((jakarta.json.JsonString) value).getString());
        }
      }
      return roles;
    }
    if (claim instanceof Iterable<?> items) {
      return extractRolesFromIterable(items);
    }
    if (claim instanceof java.util.Map<?, ?> map) {
      Object rolesValue = map.get("roles");
      return extractRolesFromClaim(rolesValue);
    }
    return List.of();
  }

  private static List<String> extractRolesFromIterable(Iterable<?> items) {
    List<String> roles = new ArrayList<>();
    for (Object item : items) {
      if (item instanceof String value) {
        roles.add(value);
      }
    }
    return roles;
  }

  private PrincipalContext buildPrincipalFromIdentity(
      SecurityIdentity identity, String queryIdHeader, Optional<String> accountHeaderValue) {
    String accountId = requireAccountIdClaim(identity);
    if (accountHeaderValue.isPresent() && !accountHeaderValue.orElseThrow().equals(accountId)) {
      throw Status.UNAUTHENTICATED
          .withDescription("account header does not match principal")
          .asRuntimeException();
    }
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

  private static Optional<String> readHeaderValue(Metadata headers, Optional<String> headerName) {
    if (headerName.isEmpty()) {
      return Optional.empty();
    }
    Metadata.Key<String> key =
        Metadata.Key.of(headerName.orElseThrow(), Metadata.ASCII_STRING_MARSHALLER);
    return Optional.ofNullable(headers.get(key))
        .map(String::trim)
        .filter(value -> !value.isBlank());
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

  private enum AuthMode {
    AUTO,
    DEV,
    PRINCIPAL_HEADER,
    OIDC,
    OIDC_OR_PRINCIPAL;

    static AuthMode fromString(String value) {
      if (value == null || value.isBlank()) {
        return AUTO;
      }
      return switch (value.trim().toLowerCase()) {
        case "dev" -> DEV;
        case "principal-header", "principal" -> PRINCIPAL_HEADER;
        case "oidc" -> OIDC;
        case "oidc-or-principal", "oidc_or_principal" -> OIDC_OR_PRINCIPAL;
        case "auto", "legacy" -> AUTO;
        default ->
            throw new IllegalArgumentException(
                "unknown floecat.auth.mode: "
                    + value
                    + " (expected auto|dev|principal-header|oidc|oidc-or-principal)");
      };
    }
  }
}
