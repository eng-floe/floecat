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
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import io.grpc.Status;
import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.logging.Logger;

/**
 * Protocol-agnostic helper that resolves the full inbound call context (principal, engine context,
 * correlation/query IDs, session/auth token values) from a generic header reader.
 *
 * <p>This class contains all auth and principal-building logic that was previously embedded in
 * {@link InboundContextInterceptor}. It is shared by:
 *
 * <ul>
 *   <li>The gRPC path: {@link InboundContextInterceptor} adapts {@code io.grpc.Metadata} into a
 *       {@code Function<String, String>} and delegates here.
 *   <li>The Arrow Flight path: {@code InboundContextFlightMiddleware} passes {@code
 *       CallHeaders::get} directly.
 * </ul>
 *
 * <p>The class is constructed with the same configuration as {@link InboundContextInterceptor} and
 * is intentionally not a CDI bean — it is instantiated by its callers.
 */
public final class InboundCallContextHelper {

  private static final Logger LOG = Logger.getLogger(InboundCallContextHelper.class);

  // -------------------------------------------------------------------------
  //  Well-known header names (shared constants for both protocols)
  // -------------------------------------------------------------------------

  public static final String HEADER_QUERY_ID = "x-query-id";
  public static final String HEADER_CORRELATION_ID = "x-correlation-id";
  // x-engine-kind and x-engine-version are owned by EngineContext.fromHeaders()

  // -------------------------------------------------------------------------
  //  Configuration
  // -------------------------------------------------------------------------

  private final AccountRepository accountRepository;
  private final TenantIdentityProvider identityProvider;
  private final boolean validateAccount;
  private final Optional<String> sessionHeader;
  private final Optional<String> authorizationHeader;
  private final AuthMode authMode;
  private final String accountClaimName;
  private final String roleClaimName;
  private final boolean oidcTenantEnabled;
  private final boolean oidcHasPublicKeyOrAuthServer;

  public InboundCallContextHelper(
      AccountRepository accountRepository,
      TenantIdentityProvider identityProvider,
      boolean validateAccount,
      Optional<String> sessionHeader,
      Optional<String> authorizationHeader,
      String authMode,
      String accountClaimName,
      String roleClaimName) {
    this.accountRepository = accountRepository;
    this.identityProvider = identityProvider;
    this.validateAccount = validateAccount;
    this.sessionHeader = sessionHeader.filter(h -> !h.isBlank());
    this.authorizationHeader = authorizationHeader.filter(h -> !h.isBlank());
    this.authMode = AuthMode.fromString(authMode);
    this.accountClaimName = accountClaimName;
    this.roleClaimName = roleClaimName;
    var config = ConfigProvider.getConfig();
    this.oidcTenantEnabled =
        config.getOptionalValue("quarkus.oidc.tenant-enabled", Boolean.class).orElse(true);
    boolean hasPublicKey =
        config
            .getOptionalValue("quarkus.oidc.public-key", String.class)
            .filter(v -> !v.isBlank())
            .isPresent();
    boolean hasAuthServerUrl =
        config
            .getOptionalValue("quarkus.oidc.auth-server-url", String.class)
            .filter(v -> !v.isBlank())
            .isPresent();
    this.oidcHasPublicKeyOrAuthServer = hasPublicKey || hasAuthServerUrl;
  }

  // -------------------------------------------------------------------------
  //  Public API
  // -------------------------------------------------------------------------

  /**
   * Resolves the full inbound call context from a protocol-agnostic header reader.
   *
   * @param headerReader reads a raw header value by plain name (e.g. {@code "x-engine-kind"});
   *     returns {@code null} when the header is absent
   * @param allowUnauthenticated when {@code true} (e.g. gRPC health/reflection endpoints) the
   *     principal resolution is skipped and a default {@link PrincipalContext} is returned
   * @return fully resolved call context; never {@code null}
   * @throws io.grpc.StatusRuntimeException with {@code UNAUTHENTICATED} when auth fails; callers on
   *     non-gRPC transports must convert this to the appropriate transport-level error
   */
  public ResolvedCallContext resolve(
      Function<String, String> headerReader, boolean allowUnauthenticated) {
    String correlationId =
        Optional.ofNullable(headerReader.apply(HEADER_CORRELATION_ID))
            .filter(s -> !s.isBlank())
            .orElse(UUID.randomUUID().toString());

    String queryId =
        Optional.ofNullable(headerReader.apply(HEADER_QUERY_ID)).map(String::trim).orElse("");

    EngineContext engineContext = EngineContext.fromHeaders(headerReader);

    ResolvedPrincipal resolvedPrincipal =
        allowUnauthenticated
            ? new ResolvedPrincipal(PrincipalContext.getDefaultInstance(), "")
            : resolvePrincipal(headerReader, queryId);

    PrincipalContext principalContext = resolvedPrincipal.pc();
    // queryId from header takes precedence; fall back to what the auth path extracted
    String effectiveQueryId = queryId.isBlank() ? resolvedPrincipal.queryId() : queryId;

    String sessionHeaderValue = readHeaderValue(headerReader, sessionHeader).orElse(null);
    String authorizationHeaderValue =
        readHeaderValue(headerReader, authorizationHeader).orElse(null);

    return new ResolvedCallContext(
        principalContext,
        effectiveQueryId,
        correlationId,
        engineContext,
        sessionHeaderValue,
        authorizationHeaderValue);
  }

  // -------------------------------------------------------------------------
  //  Principal / auth resolution
  // -------------------------------------------------------------------------

  private ResolvedPrincipal resolvePrincipal(Function<String, String> h, String queryIdHeader) {
    return switch (authMode) {
      case DEV -> new ResolvedPrincipal(devContext(), "");
      case OIDC -> {
        if (this.sessionHeader.isPresent()) {
          ensureOidcConfigured("session");
        }
        if (this.authorizationHeader.isPresent()) {
          ensureOidcConfigured("authorization");
        }
        PrincipalContext principal = resolveOidcPrincipal(h, queryIdHeader);
        yield new ResolvedPrincipal(principal, queryIdHeader);
      }
      default -> throw new IllegalStateException("unsupported auth mode: " + authMode);
    };
  }

  private PrincipalContext resolveOidcPrincipal(Function<String, String> h, String queryIdHeader) {
    Optional<PrincipalContext> principal = resolveOidcPrincipalOptional(h, queryIdHeader);
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
      Function<String, String> h, String queryIdHeader) {
    if (this.sessionHeader.isPresent() && hasHeader(h, this.sessionHeader.orElseThrow())) {
      SecurityIdentity identity =
          validateOidcHeader(h, this.sessionHeader.orElseThrow(), "session");
      return Optional.of(buildPrincipalFromIdentity(identity, queryIdHeader));
    }
    if (this.authorizationHeader.isPresent()
        && hasHeader(h, this.authorizationHeader.orElseThrow())) {
      SecurityIdentity identity =
          validateOidcHeader(h, this.authorizationHeader.orElseThrow(), "authorization");
      return Optional.of(buildPrincipalFromIdentity(identity, queryIdHeader));
    }
    return Optional.empty();
  }

  private SecurityIdentity validateOidcHeader(
      Function<String, String> h, String headerName, String headerLabel) {
    ensureOidcConfigured(headerLabel);
    String token = Optional.ofNullable(h.apply(headerName)).map(String::trim).orElse("");
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
    if (!oidcTenantEnabled) {
      throw Status.UNAUTHENTICATED
          .withDescription(headerLabel + " header configured but OIDC tenant is disabled")
          .asRuntimeException();
    }
    if (!oidcHasPublicKeyOrAuthServer) {
      throw Status.UNAUTHENTICATED
          .withDescription(
              headerLabel
                  + " header configured but no OIDC public key or auth server URL configured")
          .asRuntimeException();
    }
  }

  // -------------------------------------------------------------------------
  //  Principal building from OIDC identity
  // -------------------------------------------------------------------------

  private PrincipalContext buildPrincipalFromIdentity(
      SecurityIdentity identity, String queryIdHeader) {
    String subject = requireSubjectClaim(identity);
    var roles = extractRoles(identity);
    String accountId = requireAccountIdClaim(identity, roles);
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
      effectiveRoles = roles.stream().filter(r -> r.equalsIgnoreCase(platformRole)).toList();
    }
    RolePermissions.permissionsForRoles(effectiveRoles, authMode == AuthMode.DEV)
        .forEach(builder::addPermissions);
    return builder.build();
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
    return roles.stream().anyMatch(r -> r.equalsIgnoreCase(platformRole));
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
          io.vertx.core.json.JsonArray arr = new io.vertx.core.json.JsonArray(trimmed);
          return extractRolesFromIterable(arr.getList());
        } catch (RuntimeException ignored) {
          // fall through to treat as a single role string
        }
      }
      return List.of(value);
    }
    if (claim instanceof io.vertx.core.json.JsonArray arr) {
      return extractRolesFromIterable(arr.getList());
    }
    if (claim instanceof io.vertx.core.json.JsonObject obj) {
      return extractRolesFromClaim(obj.getValue("roles"));
    }
    if (claim instanceof jakarta.json.JsonArray arr) {
      List<String> roles = new ArrayList<>();
      for (jakarta.json.JsonValue v : arr) {
        if (v.getValueType() == jakarta.json.JsonValue.ValueType.STRING) {
          roles.add(((jakarta.json.JsonString) v).getString());
        }
      }
      return roles;
    }
    if (claim instanceof Iterable<?> items) {
      return extractRolesFromIterable(items);
    }
    if (claim instanceof java.util.Map<?, ?> map) {
      return extractRolesFromClaim(map.get("roles"));
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

  // -------------------------------------------------------------------------
  //  Dev mode
  // -------------------------------------------------------------------------

  private PrincipalContext devContext() {
    String displayName = "t-0001";
    String id =
        accountRepository
            .getByName(displayName)
            .map(account -> account.getResourceId().getId())
            .orElseGet(this::firstAccountIdOrRandom);
    PrincipalContext.Builder builder =
        PrincipalContext.newBuilder().setAccountId(id).setSubject("dev-user").setLocale("en");
    RolePermissions.permissionsForRoles(List.of(), true).forEach(builder::addPermissions);
    return builder.build();
  }

  private String firstAccountIdOrRandom() {
    StringBuilder next = new StringBuilder();
    List<ai.floedb.floecat.account.rpc.Account> accounts = accountRepository.list(1, "", next);
    if (!accounts.isEmpty()) {
      return accounts.get(0).getResourceId().getId();
    }
    return AccountIds.randomAccountId();
  }

  private void validateAccount(String accountId) {
    ResourceId accountRid =
        ResourceId.newBuilder().setId(accountId).setKind(ResourceKind.RK_ACCOUNT).build();
    if (isBlank(accountId) || accountRepository.getById(accountRid).isEmpty()) {
      throw Status.UNAUTHENTICATED
          .withDescription("invalid or unknown account: " + accountId)
          .asRuntimeException();
    }
  }

  // -------------------------------------------------------------------------
  //  Header utilities
  // -------------------------------------------------------------------------

  private static boolean hasHeader(Function<String, String> h, String headerName) {
    String v = h.apply(headerName);
    return v != null && !v.isBlank();
  }

  private static Optional<String> readHeaderValue(
      Function<String, String> h, Optional<String> headerName) {
    if (headerName.isEmpty()) {
      return Optional.empty();
    }
    return Optional.ofNullable(h.apply(headerName.orElseThrow()))
        .map(String::trim)
        .filter(v -> !v.isBlank());
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  // -------------------------------------------------------------------------
  //  Auth mode
  // -------------------------------------------------------------------------

  public enum AuthMode {
    DEV,
    OIDC;

    public static AuthMode fromString(String value) {
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

  // -------------------------------------------------------------------------
  //  Result types
  // -------------------------------------------------------------------------

  /**
   * The fully resolved per-call context, returned by {@link #resolve}.
   *
   * <p>Both gRPC and Arrow Flight paths populate this record; each transport then adapts it into
   * its own context propagation mechanism (gRPC {@code Context} keys vs. Flight middleware).
   */
  public record ResolvedCallContext(
      PrincipalContext principalContext,
      String queryId,
      String correlationId,
      EngineContext engineContext,
      /** Raw session token value; {@code null} if no session header is configured. */
      String sessionHeaderValue,
      /** Raw authorization token value; {@code null} if no authorization header is configured. */
      String authorizationHeaderValue) {}

  private record ResolvedPrincipal(PrincipalContext pc, String queryId) {}
}
