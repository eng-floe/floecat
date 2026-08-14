/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogIntegrationDiscoveryTest {
  private CatalogIntegrationDiscovery discovery;
  private CatalogIntegrationAccess access;
  private CatalogClient client;
  private final CatalogIntegration integration = CatalogIntegration.getDefaultInstance();

  @BeforeEach
  void setUp() {
    discovery = new CatalogIntegrationDiscovery();
    access = mock(CatalogIntegrationAccess.class);
    client = mock(CatalogClient.class);
    discovery.access = access;
    when(access.open(integration)).thenReturn(client);
  }

  @Test
  void validationPassesOnlyAfterCatalogVendingAndStorageChecks() {
    NamespacePath sales = NamespacePath.of("production", "sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    when(client.capabilities())
        .thenReturn(
            CatalogCapabilities.of(
                CatalogCapability.VALIDATE,
                CatalogCapability.LIST_NAMESPACES,
                CatalogCapability.LIST_TABLES,
                CatalogCapability.VEND_STORAGE_CREDENTIALS,
                CatalogCapability.VALIDATE_STORAGE_ACCESS));
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(sales));
    when(client.listTables(sales)).thenReturn(List.of(orders));
    when(client.vendStorageCredentials(orders))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty())));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    assertEquals(4, result.checks().size());
    assertTrue(
        result.checks().stream()
            .allMatch(
                check -> check.getStatus() == CatalogIntegrationValidationStatus.CIVS_PASSED));
    verify(client).validateStorageAccess(orders);
  }

  @Test
  void validationDoesNotClaimSuccessWhenCatalogVendsNoCredentials() {
    CatalogObjectName orders =
        new CatalogObjectName(NamespacePath.of("production", "sales"), "orders");
    when(client.capabilities())
        .thenReturn(
            CatalogCapabilities.of(
                CatalogCapability.VALIDATE,
                CatalogCapability.LIST_NAMESPACES,
                CatalogCapability.LIST_TABLES,
                CatalogCapability.VEND_STORAGE_CREDENTIALS,
                CatalogCapability.VALIDATE_STORAGE_ACCESS));
    when(client.listTables(NamespacePath.root())).thenReturn(List.of(orders));
    when(client.vendStorageCredentials(orders)).thenReturn(Optional.empty());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_FAILED, result.checks().get(2).getStatus());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_NOT_RUN, result.checks().get(3).getStatus());
    verify(client, never()).validateStorageAccess(orders);
  }

  @Test
  void listsCasePreservingNamespacesAndLightweightObjectsWithoutLoadingThem() {
    NamespacePath parent = NamespacePath.of("Production");
    NamespacePath finance = NamespacePath.of("Production", "Finance");
    NamespacePath sales = NamespacePath.of("Production", "Sales");
    when(client.capabilities())
        .thenReturn(
            CatalogCapabilities.of(
                CatalogCapability.LIST_NAMESPACES,
                CatalogCapability.LIST_TABLES,
                CatalogCapability.LIST_VIEWS));
    when(client.listNamespaces(parent)).thenReturn(List.of(sales, finance, sales));
    when(client.listTables(sales)).thenReturn(List.of(new CatalogObjectName(sales, "orders")));
    when(client.listViews(sales))
        .thenReturn(List.of(new CatalogObjectName(sales, "monthly_sales")));

    assertEquals(List.of(finance, sales), discovery.listNamespaces(integration, parent));
    var objects = discovery.listObjects(integration, sales, Set.of());
    assertEquals(
        List.of("monthly_sales", "orders"),
        objects.stream().map(value -> value.name().name()).toList());
    verify(client, never()).loadTable(org.mockito.ArgumentMatchers.any());
    verify(client, never()).loadView(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void explicitUnsupportedViewListingFailsInsteadOfFallingBack() {
    when(client.capabilities()).thenReturn(CatalogCapabilities.of(CatalogCapability.LIST_TABLES));

    CatalogAccessException error =
        assertThrows(
            CatalogAccessException.class,
            () ->
                discovery.listObjects(
                    integration,
                    NamespacePath.of("sales"),
                    Set.of(CatalogIntegrationDiscovery.ObjectKind.VIEW)));

    assertEquals(CatalogAccessException.Code.UNSUPPORTED, error.code());
  }
}
