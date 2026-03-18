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

package ai.floedb.floecat.gateway.iceberg.minimal.resources.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.minimal.services.account.AccountContext;
import io.quarkus.oidc.TenantIdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AccountHeaderFilterTest {
  @Test
  void populatesDefaultHeadersWhenMissing() {
    setSystemProps("dev", "authorization", "account-default", "Bearer default", null);
    AccountHeaderFilter filter = new AccountHeaderFilter();
    AccountContext accountContext = mock(AccountContext.class);
    filter.accountContext = accountContext;

    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn("v1/foo/namespaces/db/tables");
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(ctx.getHeaderString("authorization")).thenReturn(null);
    MultivaluedHashMap<String, String> headers = new MultivaluedHashMap<>();
    when(ctx.getHeaders()).thenReturn(headers);
    AtomicBoolean aborted = new AtomicBoolean(false);
    doAnswer(
            inv -> {
              aborted.set(true);
              return null;
            })
        .when(ctx)
        .abortWith(any());

    filter.filter(ctx);

    assertFalse(aborted.get());
    assertEquals("Bearer default", headers.getFirst("authorization"));
    verify(accountContext).setAccountId("account-default");
    clearSystemProps();
  }

  @Test
  void abortsWhenHeadersMissingAndNoDefaults() {
    setSystemProps("oidc", "authorization", null, null, null);
    AccountHeaderFilter filter = new AccountHeaderFilter();
    filter.accountContext = mock(AccountContext.class);

    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn("v1/foo");
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(ctx.getHeaderString(Mockito.anyString())).thenReturn(null);
    when(ctx.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    AtomicBoolean aborted = new AtomicBoolean(false);
    doAnswer(
            inv -> {
              aborted.set(true);
              return null;
            })
        .when(ctx)
        .abortWith(any());

    filter.filter(ctx);

    assertTrue(aborted.get());
    clearSystemProps();
  }

  @Test
  void placeholderAuthorizationValuesAreIgnored() {
    setSystemProps("dev", "authorization", "account-default", "undefined", null);
    AccountHeaderFilter filter = new AccountHeaderFilter();
    filter.accountContext = mock(AccountContext.class);

    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn("v1/foo");
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(ctx.getHeaderString("authorization")).thenReturn(null);
    when(ctx.getHeaders()).thenReturn(new MultivaluedHashMap<>());
    AtomicBoolean aborted = new AtomicBoolean(false);
    doAnswer(
            inv -> {
              aborted.set(true);
              return null;
            })
        .when(ctx)
        .abortWith(any());

    filter.filter(ctx);

    assertTrue(aborted.get());
    clearSystemProps();
  }

  @Test
  void rewritesLegacyPathsWhenDefaultPrefixConfigured() {
    setSystemProps("dev", "authorization", "account-default", "Bearer default", "sales");
    AccountHeaderFilter filter = new AccountHeaderFilter();
    AccountContext accountContext = mock(AccountContext.class);
    filter.accountContext = accountContext;

    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPath()).thenReturn("v1/namespaces/db/tables");
    when(uriInfo.getRequestUri())
        .thenReturn(URI.create("http://localhost:9200/v1/namespaces/db/tables"));
    when(uriInfo.getPathSegments())
        .thenReturn(
            List.of(
                pathSegment("v1"),
                pathSegment("namespaces"),
                pathSegment("db"),
                pathSegment("tables")));

    MultivaluedHashMap<String, String> headers = new MultivaluedHashMap<>();
    when(ctx.getHeaders()).thenReturn(headers);
    when(ctx.getHeaderString("authorization")).thenReturn("Bearer default");

    AtomicReference<URI> rewrittenUri = new AtomicReference<>();
    doAnswer(
            inv -> {
              rewrittenUri.set(inv.getArgument(0));
              return null;
            })
        .when(ctx)
        .setRequestUri(Mockito.any(URI.class));

    Map<String, Object> properties = new HashMap<>();
    doAnswer(
            inv -> {
              properties.put(inv.getArgument(0), inv.getArgument(1));
              return null;
            })
        .when(ctx)
        .setProperty(Mockito.anyString(), Mockito.any());
    when(ctx.getProperty(Mockito.anyString()))
        .thenAnswer(inv -> properties.get(inv.getArgument(0)));

    filter.filter(ctx);

    assertEquals(
        "http://localhost:9200/v1/sales/namespaces/db/tables", rewrittenUri.get().toString());
    assertEquals("/v1/sales/namespaces/db/tables", properties.get("rewrittenPath"));
    verify(ctx).setRequestUri(Mockito.any(URI.class));
    verify(accountContext).setAccountId("account-default");
    clearSystemProps();
  }

  @Test
  void oidcModeUsesCustomAuthHeaderForJwtClaim() throws Exception {
    setSystemProps("oidc", "x-floe-session", null, null, null);
    System.setProperty("quarkus.oidc.public-key", "test-key");
    System.setProperty("quarkus.oidc.auth-server-url", "http://issuer.example.com/realms/test");
    System.setProperty("quarkus.oidc.tenant-enabled", "true");
    try {
      AccountHeaderFilter filter = new AccountHeaderFilter();
      AccountContext accountContext = mock(AccountContext.class);
      filter.accountContext = accountContext;

      TenantIdentityProvider identityProvider = mock(TenantIdentityProvider.class);
      @SuppressWarnings("unchecked")
      jakarta.enterprise.inject.Instance<TenantIdentityProvider> identityProviderInstance =
          mock(jakarta.enterprise.inject.Instance.class);
      when(identityProviderInstance.isUnsatisfied()).thenReturn(false);
      when(identityProviderInstance.get()).thenReturn(identityProvider);
      SecurityIdentity identity = mock(SecurityIdentity.class);
      when(identity.isAnonymous()).thenReturn(true);
      JsonWebToken jwt = mock(JsonWebToken.class);
      when(jwt.getClaim("account_id")).thenReturn("acct-123");
      SecurityIdentity validated = mock(SecurityIdentity.class);
      when(validated.isAnonymous()).thenReturn(false);
      when(validated.getPrincipal()).thenReturn(jwt);
      when(identityProvider.authenticate(any())).thenReturn(Uni.createFrom().item(validated));

      setField(filter, "identityProvider", identityProviderInstance);
      setField(filter, "identity", identity);
      setField(filter, "jwt", null);

      ContainerRequestContext ctx = mock(ContainerRequestContext.class);
      UriInfo uriInfo = mock(UriInfo.class);
      when(uriInfo.getPath()).thenReturn("v1/foo/namespaces/db/tables");
      when(ctx.getUriInfo()).thenReturn(uriInfo);
      when(ctx.getHeaderString("x-floe-session")).thenReturn("Bearer token");
      MultivaluedHashMap<String, String> headers = new MultivaluedHashMap<>();
      when(ctx.getHeaders()).thenReturn(headers);
      AtomicBoolean aborted = new AtomicBoolean(false);
      doAnswer(
              inv -> {
                aborted.set(true);
                return null;
              })
          .when(ctx)
          .abortWith(any());

      filter.filter(ctx);

      assertFalse(aborted.get());
      verify(accountContext).setAccountId("acct-123");
    } finally {
      clearSystemProps();
      System.clearProperty("quarkus.oidc.public-key");
      System.clearProperty("quarkus.oidc.auth-server-url");
      System.clearProperty("quarkus.oidc.tenant-enabled");
    }
  }

  private PathSegment pathSegment(String path) {
    return new SimplePathSegment(path);
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    var field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static void setSystemProps(
      String authMode,
      String authHeader,
      String accountId,
      String defaultAuth,
      String defaultPrefix) {
    System.setProperty("floecat.gateway.minimal.auth-mode", authMode);
    System.setProperty("floecat.gateway.minimal.auth-header", authHeader);
    if (accountId == null) {
      System.clearProperty("floecat.gateway.minimal.default-account-id");
    } else {
      System.setProperty("floecat.gateway.minimal.default-account-id", accountId);
    }
    if (defaultAuth == null) {
      System.clearProperty("floecat.gateway.minimal.default-authorization");
    } else {
      System.setProperty("floecat.gateway.minimal.default-authorization", defaultAuth);
    }
    if (defaultPrefix == null) {
      System.clearProperty("floecat.gateway.minimal.default-prefix");
    } else {
      System.setProperty("floecat.gateway.minimal.default-prefix", defaultPrefix);
    }
  }

  private static void clearSystemProps() {
    System.clearProperty("floecat.gateway.minimal.auth-mode");
    System.clearProperty("floecat.gateway.minimal.auth-header");
    System.clearProperty("floecat.gateway.minimal.default-account-id");
    System.clearProperty("floecat.gateway.minimal.default-authorization");
    System.clearProperty("floecat.gateway.minimal.default-prefix");
  }

  private static final class SimplePathSegment implements PathSegment {
    private final String value;

    private SimplePathSegment(String value) {
      this.value = value;
    }

    @Override
    public String getPath() {
      return value;
    }

    @Override
    public MultivaluedMap<String, String> getMatrixParameters() {
      return new MultivaluedHashMap<>();
    }
  }
}
