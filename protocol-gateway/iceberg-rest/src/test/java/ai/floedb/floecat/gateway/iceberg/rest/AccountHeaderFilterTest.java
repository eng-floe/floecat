package ai.floedb.floecat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.filter.AccountHeaderFilter;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AccountHeaderFilterTest {
  @Test
  void populatesDefaultHeadersWhenMissing() {
    AccountHeaderFilter filter = new AccountHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
    when(configInstance.isUnsatisfied()).thenReturn(false);
    when(configInstance.get()).thenReturn(config);
    when(config.accountHeader()).thenReturn("x-tenant-id");
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAccountId()).thenReturn("account-default");
    when(config.defaultAuthorization()).thenReturn("Bearer default");

    filter.setConfigInstance(configInstance);
    AccountContext accountContext = mock(AccountContext.class);
    filter.setAccountContext(accountContext);

    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    UriInfo uriInfo = mock(UriInfo.class);
    when(uriInfo.getPath()).thenReturn("v1/foo/namespaces/db/tables");
    when(ctx.getUriInfo()).thenReturn(uriInfo);
    when(ctx.getHeaderString("x-tenant-id")).thenReturn(null);
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
    assertEquals("account-default", headers.getFirst("x-tenant-id"));
    assertEquals("Bearer default", headers.getFirst("authorization"));
    verify(accountContext).setAccountId("account-default");
  }

  @Test
  void abortsWhenHeadersMissingAndNoDefaults() {
    AccountHeaderFilter filter = new AccountHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    when(configInstance.isUnsatisfied()).thenReturn(true);
    filter.setConfigInstance(configInstance);
    filter.setAccountContext(mock(AccountContext.class));

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
  }

  @Test
  void placeholderAuthorizationValuesAreIgnored() {
    AccountHeaderFilter filter = new AccountHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
    when(configInstance.isUnsatisfied()).thenReturn(false);
    when(configInstance.get()).thenReturn(config);
    when(config.accountHeader()).thenReturn("x-tenant-id");
    when(config.defaultAccountId()).thenReturn("account-default");
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAuthorization()).thenReturn("undefined");
    filter.setConfigInstance(configInstance);
    filter.setAccountContext(mock(AccountContext.class));

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
  }

  @Test
  void rewritesLegacyPathsWhenDefaultPrefixConfigured() {
    AccountHeaderFilter filter = new AccountHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
    when(configInstance.isUnsatisfied()).thenReturn(false);
    when(configInstance.get()).thenReturn(config);
    when(config.accountHeader()).thenReturn("x-tenant-id");
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAccountId()).thenReturn("account-default");
    when(config.defaultAuthorization()).thenReturn("Bearer default");
    when(config.defaultPrefix()).thenReturn(Optional.of("sales"));
    filter.setConfigInstance(configInstance);
    AccountContext accountContext = mock(AccountContext.class);
    filter.setAccountContext(accountContext);

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
    when(ctx.getHeaderString("x-tenant-id")).thenReturn("account-default");
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
  }

  private PathSegment pathSegment(String path) {
    return new SimplePathSegment(path);
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
