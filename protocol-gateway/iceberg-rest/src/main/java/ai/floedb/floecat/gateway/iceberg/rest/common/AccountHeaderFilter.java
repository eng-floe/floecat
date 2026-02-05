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

package ai.floedb.floecat.gateway.iceberg.rest.common;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import io.quarkus.oidc.AccessTokenCredential;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;

@Provider
@PreMatching
public class AccountHeaderFilter implements ContainerRequestFilter {
  @Inject Instance<IcebergGatewayConfig> config;
  @Inject AccountContext accountContext;
  @Inject SecurityIdentity identity;
  @Inject JsonWebToken jwt;
  @Inject Instance<TenantIdentityProvider> identityProvider;

  public void setConfigInstance(Instance<IcebergGatewayConfig> config) {
    this.config = config;
  }

  public void setAccountContext(AccountContext accountContext) {
    this.accountContext = accountContext;
  }

  private static final String DEFAULT_ACCOUNT_HEADER = "x-account-id";
  private static final String DEFAULT_AUTH_HEADER = "authorization";

  @Override
  @ServerRequestFilter(priority = 10)
  public void filter(ContainerRequestContext requestContext) {
    rewriteDefaultPrefix(requestContext);
    if (requestContext.getUriInfo().getPath().equals("v1/config")) {
      return;
    }
    boolean devMode = isDevMode();
    String accountHeader = resolveAccountHeaderName();
    String account;
    if (devMode) {
      account = defaultAccount().orElse(null);
    } else {
      account = resolveAccountFromAuthClaim(requestContext, accountHeader);
    }
    if (requestContext.getProperty("authAbort") != null) {
      return;
    }
    if (account == null || account.isBlank()) {
      requestContext.abortWith(unauthorized("missing account header"));
      return;
    }
    requestContext.getHeaders().putSingle(accountHeader, account);
    accountContext.setAccountId(account.trim());

    String authHeader = resolveAuthHeaderName();
    String auth = headerValue(requestContext, authHeader);
    if (auth == null || auth.isBlank()) {
      auth = defaultAuthorization().orElse(null);
      if (auth != null) {
        requestContext.getHeaders().putSingle(authHeader, auth);
      }
    }
    if (auth == null || auth.isBlank()) {
      if (devMode && allowMissingAuthInDev()) {
        return;
      }
      requestContext.abortWith(unauthorized("missing authorization header"));
    }
  }

  private static final Set<String> PREFIXED_SEGMENTS =
      new HashSet<>(List.of("namespaces", "tables", "views", "transactions"));

  private void rewriteDefaultPrefix(ContainerRequestContext context) {
    String prefix = defaultPrefix();
    if (prefix.isEmpty()) {
      return;
    }
    UriInfo uriInfo = context.getUriInfo();
    List<PathSegment> segments = uriInfo.getPathSegments();
    if (segments.size() < 2) {
      return;
    }
    if (!"v1".equals(segments.get(0).getPath())) {
      return;
    }
    String second = segments.get(1).getPath();
    if (prefix.equals(second) || !PREFIXED_SEGMENTS.contains(second)) {
      return;
    }
    StringBuilder newPath = new StringBuilder("/v1/").append(prefix);
    for (int i = 1; i < segments.size(); i++) {
      newPath.append('/').append(segments.get(i).getPath());
    }
    URI newUri =
        UriBuilder.fromUri(uriInfo.getRequestUri()).replacePath(newPath.toString()).build();
    context.setRequestUri(newUri);
    context.setProperty("rewrittenPath", newPath.toString());
  }

  private String resolveAccountHeaderName() {
    if (config.isUnsatisfied()) {
      return DEFAULT_ACCOUNT_HEADER;
    }
    try {
      String header = config.get().accountHeader();
      return header == null || header.isBlank() ? DEFAULT_ACCOUNT_HEADER : header;
    } catch (Exception ignored) {
      return DEFAULT_ACCOUNT_HEADER;
    }
  }

  private String resolveAuthHeaderName() {
    if (config.isUnsatisfied()) {
      return DEFAULT_AUTH_HEADER;
    }
    try {
      String header = config.get().authHeader();
      return header == null || header.isBlank() ? DEFAULT_AUTH_HEADER : header;
    } catch (Exception ignored) {
      return DEFAULT_AUTH_HEADER;
    }
  }

  private Response unauthorized(String message) {
    return Response.status(Response.Status.UNAUTHORIZED)
        .type(MediaType.APPLICATION_JSON)
        .header("Content-Type", MediaType.APPLICATION_JSON)
        .entity(new IcebergErrorResponse(new IcebergError(message, "UnauthorizedException", 401)))
        .build();
  }

  private Optional<String> defaultAccount() {
    if (config.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      return config.get().defaultAccountId().map(String::trim).filter(v -> !v.isBlank());
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }

  private Optional<String> defaultAuthorization() {
    if (config.isUnsatisfied()) {
      return Optional.empty();
    }
    try {
      return config
          .get()
          .defaultAuthorization()
          .map(String::trim)
          .filter(value -> !value.isBlank())
          .filter(value -> !isPlaceholder(value));
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }

  private String resolveAccountFromAuthClaim(
      ContainerRequestContext requestContext, String accountHeader) {
    String claimName = resolveAccountClaimName();
    SecurityIdentity resolvedIdentity = identity;
    if (resolvedIdentity == null || resolvedIdentity.isAnonymous()) {
      String authHeader = resolveAuthHeaderName();
      String token = headerValue(requestContext, authHeader);
      if (token != null && !token.isBlank()) {
        String oidcError = ensureOidcConfigured(authHeader);
        if (oidcError != null) {
          requestContext.abortWith(unauthorized(oidcError));
          requestContext.setProperty("authAbort", Boolean.TRUE);
          return null;
        }
        resolvedIdentity = validateOidcHeader(token);
      } else {
        return null;
      }
    }
    Object claim = extractClaim(resolvedIdentity, claimName);
    String account = claim == null ? null : claim.toString();
    if (account != null && !account.isBlank()) {
      requestContext.getHeaders().putSingle(accountHeader, account);
      return account;
    }
    return null;
  }

  private SecurityIdentity validateOidcHeader(String headerValue) {
    if (identityProvider == null || identityProvider.isUnsatisfied()) {
      return null;
    }
    String token = headerValue.trim();
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      token = token.substring(7).trim();
    }
    if (token.isBlank()) {
      return null;
    }
    try {
      AccessTokenCredential credential = new AccessTokenCredential(token);
      SecurityIdentity validated =
          identityProvider.get().authenticate(credential).await().indefinitely();
      if (validated == null || validated.isAnonymous()) {
        return null;
      }
      return validated;
    } catch (RuntimeException e) {
      return null;
    }
  }

  private String ensureOidcConfigured(String headerLabel) {
    var cfg = ConfigProvider.getConfig();
    boolean tenantEnabled =
        cfg.getOptionalValue("quarkus.oidc.tenant-enabled", Boolean.class).orElse(true);
    boolean hasPublicKey =
        cfg.getOptionalValue("quarkus.oidc.public-key", String.class)
            .filter(value -> !value.isBlank())
            .isPresent();
    boolean hasAuthServerUrl =
        cfg.getOptionalValue("quarkus.oidc.auth-server-url", String.class)
            .filter(value -> !value.isBlank())
            .isPresent();
    if (!tenantEnabled) {
      return headerLabel + " header configured but OIDC tenant is disabled";
    }
    if (!hasPublicKey && !hasAuthServerUrl) {
      return headerLabel
          + " header configured but no OIDC public key or auth server URL configured";
    }
    return null;
  }

  private Object extractClaim(SecurityIdentity resolvedIdentity, String claimName) {
    if (resolvedIdentity == null || resolvedIdentity.isAnonymous()) {
      return null;
    }
    if (resolvedIdentity.getPrincipal() instanceof JsonWebToken jwtPrincipal) {
      return jwtPrincipal.getClaim(claimName);
    }
    return jwt != null ? jwt.getClaim(claimName) : null;
  }

  private String resolveAccountClaimName() {
    if (config.isUnsatisfied()) {
      return "account_id";
    }
    try {
      String claim = config.get().accountClaim();
      return claim == null || claim.isBlank() ? "account_id" : claim.trim();
    } catch (Exception ignored) {
      return "account_id";
    }
  }

  private boolean allowMissingAuthInDev() {
    if (config.isUnsatisfied()) {
      return false;
    }
    try {
      return config.get().devAllowMissingAuth();
    } catch (Exception ignored) {
      return false;
    }
  }

  private boolean isDevMode() {
    if (config.isUnsatisfied()) {
      return false;
    }
    try {
      String mode = config.get().authMode();
      return mode != null && !mode.isBlank() && mode.trim().equalsIgnoreCase("dev");
    } catch (Exception ignored) {
      return false;
    }
  }

  private String defaultPrefix() {
    if (config.isUnsatisfied()) {
      return "";
    }
    try {
      var cfg = config.get();
      String value = cfg.defaultPrefix().map(String::trim).orElse("");
      if (value.isEmpty()) {
        String systemValue = System.getProperty("floecat.gateway.default-prefix", "");
        String envValue = System.getenv("FLOECAT_GATEWAY_DEFAULT_PREFIX");
        if (!systemValue.isBlank()) {
          value = systemValue.trim();
        } else if (envValue != null && !envValue.isBlank()) {
          value = envValue.trim();
        }
      }
      return value;
    } catch (Exception ex) {
      return "";
    }
  }

  private boolean isPlaceholder(String value) {
    String normalized = value.trim();
    return normalized.equalsIgnoreCase("undefined") || normalized.equalsIgnoreCase("null");
  }

  private String headerValue(ContainerRequestContext context, String headerName) {
    String value = context.getHeaderString(headerName);
    return value == null ? null : value.trim();
  }
}
