package ai.floedb.metacat.gateway.iceberg.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import ai.floedb.metacat.gateway.iceberg.rest.support.filter.TenantHeaderFilter;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TenantHeaderFilterTest {
  @Test
  void populatesDefaultHeadersWhenMissing() {
    TenantHeaderFilter filter = new TenantHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
    when(configInstance.isUnsatisfied()).thenReturn(false);
    when(configInstance.get()).thenReturn(config);
    when(config.tenantHeader()).thenReturn("x-tenant-id");
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultTenantId()).thenReturn("tenant-default");
    when(config.defaultAuthorization()).thenReturn("Bearer default");

    filter.setConfigInstance(configInstance);
    TenantContext tenantContext = mock(TenantContext.class);
    filter.setTenantContext(tenantContext);

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
    assertEquals("tenant-default", headers.getFirst("x-tenant-id"));
    assertEquals("Bearer default", headers.getFirst("authorization"));
    verify(tenantContext).setTenantId("tenant-default");
  }

  @Test
  void abortsWhenHeadersMissingAndNoDefaults() {
    TenantHeaderFilter filter = new TenantHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    when(configInstance.isUnsatisfied()).thenReturn(true);
    filter.setConfigInstance(configInstance);
    filter.setTenantContext(mock(TenantContext.class));

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
    TenantHeaderFilter filter = new TenantHeaderFilter();
    Instance<IcebergGatewayConfig> configInstance = mock(Instance.class);
    IcebergGatewayConfig config = mock(IcebergGatewayConfig.class);
    when(configInstance.isUnsatisfied()).thenReturn(false);
    when(configInstance.get()).thenReturn(config);
    when(config.tenantHeader()).thenReturn("x-tenant-id");
    when(config.defaultTenantId()).thenReturn("tenant-default");
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAuthorization()).thenReturn("undefined");
    filter.setConfigInstance(configInstance);
    filter.setTenantContext(mock(TenantContext.class));

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
}
