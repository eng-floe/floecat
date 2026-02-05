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
  public static final Context.Key<String> SESSION_HEADER_VALUE_KEY =
      Context.key("session_header_value");
  public static final Context.Key<String> AUTHORIZATION_HEADER_VALUE_KEY =
      Context.key("authorization_header_value");

  private final AccountRepository accountRepository;
  private final TenantIdentityProvider identityProvider;

  private final boolean validateAccount;
  private final Optional<String> sessionHeader;
  private final Optional<String> authorizationHeader;
  private final Optional<String> accountHeader;
  private final AuthMode authMode;
  private final String accountClaimName;
  private final String roleClaimName;

  public InboundContextInterceptor(
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      boolean validateAccount,
      Optional<String> sessionHeader,
      Optional<String> authorizationHeader,
      Optional<String> accountHeader,
      String authMode,
      String accountClaimName,
      String roleClaimName) {
    this.accountRepository = accountRepository;
    this.identityProvider = identityProvider;
    this.validateAccount = validateAccount;
    this.sessionHeader = sessionHeader.filter(header -> !header.isBlank());
    this.authorizationHeader = authorizationHeader.filter(header -> !header.isBlank());
    this.accountHeader = accountHeader.filter(header -> !header.isBlank());
    this.authMode = AuthMode.fromString(authMode);
    this.accountClaimName = accountClaimName;
    this.roleClaimName = roleClaimName;

    String header = this.sessionHeader.orElse("");
    String authHeader = this.authorizationHeader.orElse("");
    String acctHeader = this.accountHeader.orElse("");
    LOG.infof(
        "InboundContextInterceptor ready: sessionHeader=%s authorizationHeader=%s"
            + " accountHeader=%s authMode=%s validateAccount=%s",
        header.isBlank() ? "<disabled>" : header,
        authHeader.isBlank() ? "<disabled>" : authHeader,
        acctHeader.isBlank() ? "<disabled>" : acctHeader,
        this.authMode.name().toLowerCase(),
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
    String sessionHeaderValue = readHeaderValue(headers, sessionHeader).orElse(null);
    String authorizationHeaderValue = readHeaderValue(headers, authorizationHeader).orElse(null);

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
        PrincipalContext dev = devContext();
        if (accountHeaderValue.isPresent()
            && !accountHeaderValue.orElseThrow().equals(dev.getAccountId())) {
          throw Status.UNAUTHENTICATED
              .withDescription("account header does not match principal")
              .asRuntimeException();
        }
        return new ResolvedContext(dev, "");
      }
      case OIDC -> {
        if (this.sessionHeader.isPresent()) {
          ensureOidcConfigured("session");
        }
        if (this.authorizationHeader.isPresent()) {
          ensureOidcConfigured("authorization");
        }
        PrincipalContext principal =
            resolveOidcPrincipal(headers, queryIdHeader, accountHeaderValue);
        return new ResolvedContext(principal, queryIdHeader);
      }
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

    throw Status.UNAUTHENTICATED
        .withDescription("missing session or authorization token")
        .asRuntimeException();
  }

  private PrincipalContext resolveOidcPrincipal(
      Metadata headers, String queryIdHeader, Optional<String> accountHeaderValue) {
    Optional<PrincipalContext> principal =
        resolveOidcPrincipalOptional(headers, queryIdHeader, accountHeaderValue);
    if (principal.isPresent()) {
      return principal.get();
    }
    boolean hasSessionHeader = this.sessionHeader.isPresent();
    boolean hasAuthorizationHeader = this.authorizationHeader.isPresent();
    if (hasSessionHeader && hasAuthorizationHeader) {
      throw Status.UNAUTHENTICATED
          .withDescription("missing session or authorization token")
          .asRuntimeException();
    }
    if (hasSessionHeader) {
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
    ensureOidcConfigured(headerLabel);
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
        LOG.warnf("%s token inactive", headerLabel);
        throw Status.UNAUTHENTICATED
            .withDescription("inactive " + headerLabel + " token")
            .asRuntimeException();
      }
      return identity;
    } catch (RuntimeException e) {
      LOG.warnf(e, "%s token validation failed: %s", headerLabel, e.getMessage());
      throw Status.UNAUTHENTICATED
          .withDescription("invalid " + headerLabel + " token")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private void ensureOidcConfigured(String headerLabel) {
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
  }

  private static boolean hasHeader(Metadata headers, String headerName) {
    Metadata.Key<String> key = Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
    return headers.containsKey(key);
  }

  private String requireAccountIdClaim(SecurityIdentity identity) {
    return requireAccountIdClaim(identity, List.of());
  }

  private String requireAccountIdClaim(SecurityIdentity identity, List<String> roles) {
    JsonWebToken jwt = requireJwt(identity);
    Object claim = jwt.getClaim(accountClaimName);
    String accountId = extractAccountIdClaim(claim);
    if (!isBlank(accountId)) {
      return accountId;
    }
    if (hasPlatformAdminRole(roles)) {
      return "";
    }
    throw Status.UNAUTHENTICATED
        .withDescription("missing " + accountClaimName + " claim")
        .asRuntimeException();
  }

  private String extractAccountIdClaim(Object claim) {
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
    return null;
  }

  private static boolean hasPlatformAdminRole(List<String> roles) {
    if (roles == null || roles.isEmpty()) {
      return false;
    }
    String platformRole = RolePermissions.platformAdminRole();
    return roles.stream().anyMatch(role -> role.equalsIgnoreCase(platformRole));
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
    String subject = requireSubjectClaim(identity);
    var roles = extractRoles(identity);
    String accountId = requireAccountIdClaim(identity, roles);
    if (accountHeaderValue.isPresent()) {
      String headerValue = accountHeaderValue.orElseThrow();
      if (accountId.isBlank()) {
        throw Status.UNAUTHENTICATED
            .withDescription("account header provided without account_id claim")
            .asRuntimeException();
      }
      if (!headerValue.equals(accountId)) {
        throw Status.UNAUTHENTICATED
            .withDescription("account header does not match principal")
            .asRuntimeException();
      }
    }
    if (validateAccount && !accountId.isBlank()) {
      validateAccount(accountId);
    }
    PrincipalContext.Builder builder =
        PrincipalContext.newBuilder().setAccountId(accountId).setSubject(subject);
    if (!isBlank(queryIdHeader)) {
      builder.setQueryId(queryIdHeader);
    }
    List<String> effectiveRoles = roles;
    if (accountId.isBlank()) {
      String platformRole = RolePermissions.platformAdminRole();
      effectiveRoles = roles.stream().filter(role -> role.equalsIgnoreCase(platformRole)).toList();
    }
    RolePermissions.permissionsForRoles(effectiveRoles, authMode == AuthMode.DEV)
        .forEach(builder::addPermissions);
    return builder.build();
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
    String displayName = "t-0001";
    String id =
        accountRepository
            .getByName(displayName)
            .map(account -> account.getResourceId().getId())
            .orElseGet(AccountIds::randomAccountId);
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
    DEV,
    OIDC;

    static AuthMode fromString(String value) {
      if (value == null || value.isBlank()) {
        return OIDC;
      }
      return switch (value.trim().toLowerCase()) {
        case "dev" -> DEV;
        case "oidc" -> OIDC;
        default ->
            throw new IllegalArgumentException(
                "unknown floecat.auth.mode: " + value + " (expected dev|oidc)");
      };
    }
  }
}
