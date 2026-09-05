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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
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
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationCheckType;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationIssue;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationValidationStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jboss.logging.MDC;
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
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName orders = setUpSuccessfulValidation(vended);

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    assertEquals(5, result.checks().size());
    assertTrue(
        result.checks().stream()
            .allMatch(
                check -> check.getStatus() == CatalogIntegrationValidationStatus.CIVS_PASSED));
    verify(client).validateStorageAccess(orders, vended);
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
        CatalogIntegrationValidationStatus.CIVS_FAILED, result.checks().get(3).getStatus());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_NOT_RUN, result.checks().get(4).getStatus());
    verify(client, never()).validateStorageAccess(any(), any());
  }

  @Test
  void validationSkipsTablesWithoutVendedCredentials() {
    NamespacePath commerce = NamespacePath.of("unity", "commerce");
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName ecomm = new CatalogObjectName(commerce, "ecomm");
    CatalogObjectName callCenter = new CatalogObjectName(defaults, "call_center");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults, commerce));
    when(client.listTables(commerce)).thenReturn(List.of(ecomm));
    when(client.listTables(defaults)).thenReturn(List.of(callCenter));
    when(client.vendStorageCredentials(ecomm)).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(callCenter)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).vendStorageCredentials(ecomm);
    verify(client).vendStorageCredentials(callCenter);
    verify(client).validateStorageAccess(callCenter, vended);
  }

  /**
   * A per-table vending failure means the same as an empty vend -- not this table -- so the search
   * steps over it. Aborting instead let the alphabetically first table decide validation for a
   * catalog whose very next table vends fine.
   */
  @Test
  void validationSkipsTablesWhoseVendFailsForThatTableAlone() {
    NamespacePath commerce = NamespacePath.of("unity", "commerce");
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName ecomm = new CatalogObjectName(commerce, "ecomm");
    CatalogObjectName callCenter = new CatalogObjectName(defaults, "call_center");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults, commerce));
    when(client.listTables(commerce)).thenReturn(List.of(ecomm));
    when(client.listTables(defaults)).thenReturn(List.of(callCenter));
    when(client.vendStorageCredentials(ecomm))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Unity Catalog table does not expose a stable table ID"));
    when(client.vendStorageCredentials(callCenter)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).validateStorageAccess(callCenter, vended);
  }

  /**
   * A failure that will answer identically for every remaining table stops the search where it
   * stands: walking the rest only spends the budget to rediscover what is already known.
   */
  @Test
  void validationStopsAtAVendFailureThatDescribesTheIntegration() {
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName callCenter = new CatalogObjectName(defaults, "call_center");
    CatalogObjectName orders = new CatalogObjectName(defaults, "orders");
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults));
    when(client.listTables(defaults)).thenReturn(List.of(callCenter, orders));
    // Not PERMISSION_DENIED: a Unity vend grant is per schema, so a refusal there describes one
    // branch and is stepped over. A configuration the integration cannot serve is what answers
    // identically for every remaining table.
    when(client.vendStorageCredentials(callCenter))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.INVALID_CONFIGURATION,
                "stored namespace path is not catalog.schema"));

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    verify(client, never()).vendStorageCredentials(orders);
  }

  /**
   * And the converse, which is why PERMISSION_DENIED moved: Unity gates
   * generateTemporaryTableCredentials on EXTERNAL USE SCHEMA, granted per schema. A principal can
   * be refused in one schema and succeed in the next, so a refusal must not decide the integration.
   */
  @Test
  void aVendRefusalInOneSchemaDoesNotDecideTheIntegration() {
    NamespacePath denied = NamespacePath.of("unity", "denied");
    NamespacePath granted = NamespacePath.of("unity", "granted");
    CatalogObjectName blocked = new CatalogObjectName(denied, "audit");
    CatalogObjectName allowed = new CatalogObjectName(granted, "orders");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(denied, granted));
    when(client.listTables(denied)).thenReturn(List.of(blocked));
    when(client.listTables(granted)).thenReturn(List.of(allowed));
    when(client.vendStorageCredentials(blocked))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "no EXTERNAL USE SCHEMA on denied"));
    when(client.vendStorageCredentials(allowed)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).validateStorageAccess(allowed, vended);
  }

  /**
   * A search that steps over every table still reports why. The skipped reason is the diagnosis --
   * losing it to a blanker "nothing vended" would be the price of skipping at all.
   */
  @Test
  void validationReportsTheSkippedReasonWhenNothingVends() {
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName callCenter = new CatalogObjectName(defaults, "call_center");
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults));
    when(client.listTables(defaults)).thenReturn(List.of(callCenter));
    when(client.vendStorageCredentials(callCenter))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID, "scope misses the table"));

    var result = discovery.validate(integration);

    assertFalse(result.valid());
  }

  /**
   * Each attempt is a table load plus a credential mint upstream. A catalog where nothing vends
   * must not have validation walk the whole inventory at that price and report a timeout in place
   * of the answer it had after the first few.
   */
  @Test
  void validationCapsHowManyTablesItAsksToVend() {
    NamespacePath defaults = NamespacePath.of("unity", "default");
    List<CatalogObjectName> many =
        java.util.stream.IntStream.range(0, 60)
            .mapToObj(i -> new CatalogObjectName(defaults, String.format("t%03d", i)))
            .toList();
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults));
    when(client.listTables(defaults)).thenReturn(many);
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    verify(client, atMost(25)).vendStorageCredentials(any());
  }

  /**
   * A namespace gets a share of the attempts, not all of them. Checked against the whole walk, the
   * cap let the first few tables in the catalog decide validation -- so an alphabetically-first
   * schema full of browsable-but-not-vendable tables reported the integration invalid even though a
   * later schema vended. Unity grants EXTERNAL USE SCHEMA per schema, so sampling more than one
   * schema is the point.
   */
  @Test
  void aNamespaceThatCannotVendDoesNotConsumeTheWholeAttemptBudget() {
    NamespacePath crowded = NamespacePath.of("unity", "aaa_crowded");
    NamespacePath granted = NamespacePath.of("unity", "zzz_granted");
    List<CatalogObjectName> many =
        java.util.stream.IntStream.range(0, 40)
            .mapToObj(i -> new CatalogObjectName(crowded, String.format("t%03d", i)))
            .toList();
    CatalogObjectName vendable = new CatalogObjectName(granted, "orders");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(crowded, granted));
    when(client.listTables(crowded)).thenReturn(many);
    when(client.listTables(granted)).thenReturn(List.of(vendable));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(vendable)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "a later schema that vends must still be reached");
    verify(client).validateStorageAccess(vendable, vended);
    // Two from the crowded schema, not forty and not the whole budget.
    verify(client, atMost(3)).vendStorageCredentials(any());
  }

  /**
   * The total still bounds the walk: each attempt is a table load plus a real credential mint, and
   * they share the validation budget with the namespace walk and the storage check.
   */
  @Test
  void theTotalAttemptBudgetEndsTheWalk() {
    List<NamespacePath> schemas =
        java.util.stream.IntStream.range(0, 20)
            .mapToObj(i -> NamespacePath.of("unity", String.format("s%02d", i)))
            .toList();
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(schemas);
    for (NamespacePath schema : schemas) {
      when(client.listTables(schema))
          .thenReturn(
              List.of(
                  new CatalogObjectName(schema, "a"),
                  new CatalogObjectName(schema, "b"),
                  new CatalogObjectName(schema, "c")));
    }
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    // Twenty schemas at two apiece would be forty; the total cap stops it well before that.
    verify(client, atMost(10)).vendStorageCredentials(any());
  }

  /**
   * A Unity workspace almost always exposes a system catalog whose schemas the integration
   * principal cannot list. Both listTables calls already tolerate that on a single namespace; the
   * listNamespaces call did not, and since the search now continues until a table vends rather than
   * stopping at the first one it finds, that refusal became reachable and aborted everything.
   */
  @Test
  void aNamespaceListingRefusalOnOneParentDoesNotAbortTheSearch() {
    NamespacePath system = NamespacePath.of("system");
    NamespacePath main = NamespacePath.of("main");
    NamespacePath sales = NamespacePath.of("main", "sales");
    CatalogObjectName systemTable = new CatalogObjectName(system, "audit");
    CatalogObjectName mainTable = new CatalogObjectName(main, "staging");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(main, system));
    when(client.listTables(system)).thenReturn(List.of(systemTable));
    when(client.listTables(main)).thenReturn(List.of(mainTable));
    when(client.vendStorageCredentials(systemTable)).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(mainTable)).thenReturn(Optional.empty());
    // The refusal that used to end everything.
    when(client.listNamespaces(system))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "cannot list system schemas"));
    when(client.listNamespaces(main)).thenReturn(List.of(sales));
    when(client.listTables(sales)).thenReturn(List.of(orders));
    when(client.vendStorageCredentials(orders)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).validateStorageAccess(orders, vended);
  }

  /**
   * An already-expired tuple is a per-table failure, not the catalog's answer. Returning the first
   * non-empty credential and judging its expiry outside the loop meant the first table decided
   * validation while the next would have vended a live one -- the table-order dependence the
   * bounded search exists to remove, and the same condition vendFailureSkippable already skips when
   * a provider throws CREDENTIAL_EXPIRED rather than returning it.
   */
  @Test
  void anExpiredVendIsSkippedSoALaterTableCanStillValidate() {
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName stale = new CatalogObjectName(defaults, "aaa_stale");
    CatalogObjectName live = new CatalogObjectName(defaults, "zzz_live");
    var expired =
        new VendedStorageCredentials(
            Map.of("key", "value"),
            "",
            Optional.of(java.time.Instant.parse("2020-01-01T00:00:00Z")));
    var usable =
        new VendedStorageCredentials(
            Map.of("key", "value"),
            "",
            Optional.of(java.time.Instant.parse("2999-01-01T00:00:00Z")));
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults));
    when(client.listTables(defaults)).thenReturn(List.of(stale, live));
    when(client.vendStorageCredentials(stale)).thenReturn(Optional.of(expired));
    when(client.vendStorageCredentials(live)).thenReturn(Optional.of(usable));

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "the live credential from the next table must be reached");
    verify(client).validateStorageAccess(live, usable);
  }

  /** But if every attempt is expired, that is still the reported reason. */
  @Test
  void anAllExpiredSearchStillReportsTheExpiry() {
    NamespacePath defaults = NamespacePath.of("unity", "default");
    CatalogObjectName only = new CatalogObjectName(defaults, "orders");
    var expired =
        new VendedStorageCredentials(
            Map.of("key", "value"),
            "",
            Optional.of(java.time.Instant.parse("2020-01-01T00:00:00Z")));
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(defaults));
    when(client.listTables(defaults)).thenReturn(List.of(only));
    when(client.vendStorageCredentials(only)).thenReturn(Optional.of(expired));

    var result = discovery.validate(integration);

    assertFalse(result.valid());
  }

  /**
   * One catalog must not answer for the workspace. The walk is breadth-first and a Unity catalog is
   * a one-segment namespace whose table listing is empty without an RPC, so every catalog is
   * enqueued before any schema is visited -- and the first catalog popped then spent the whole
   * total on its own schemas. On Databricks that is hive_metastore, which sorts ahead of main and
   * reports DELTA tables that cannot mint credentials.
   */
  @Test
  void vendAttemptsReachASecondCatalogWhenTheFirstCannotVend() {
    NamespacePath hive = NamespacePath.of("hive_metastore");
    NamespacePath main = NamespacePath.of("main");
    List<NamespacePath> hiveSchemas =
        java.util.stream.IntStream.range(0, 6)
            .mapToObj(i -> NamespacePath.of("hive_metastore", String.format("s%d", i)))
            .toList();
    NamespacePath mainSales = NamespacePath.of("main", "sales");
    CatalogObjectName vendable = new CatalogObjectName(mainSales, "orders");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(hive, main));
    when(client.listNamespaces(hive)).thenReturn(hiveSchemas);
    when(client.listNamespaces(main)).thenReturn(List.of(mainSales));
    for (NamespacePath schema : hiveSchemas) {
      when(client.listTables(schema))
          .thenReturn(
              List.of(new CatalogObjectName(schema, "a"), new CatalogObjectName(schema, "b")));
    }
    when(client.listTables(mainSales)).thenReturn(List.of(vendable));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(vendable)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "the second catalog must still be sampled");
    verify(client).validateStorageAccess(vendable, vended);
  }

  /**
   * A catalog that has spent its vend budget stops being enumerated. Returning from the table batch
   * alone stopped the attempts but not the walk: the traversal kept listing every remaining schema
   * under the same catalog and enqueueing their descendants, spending the validation budget on an
   * inventory it had already stopped sampling.
   *
   * <p>Two catalogs, because the per-catalog cap only applies above one -- with a single catalog
   * there is nothing for it to protect and it is deliberately stood down.
   */
  @Test
  void aCatalogThatHasSpentItsVendBudgetIsNotEnumeratedFurther() {
    NamespacePath crowded = NamespacePath.of("crowded");
    NamespacePath other = NamespacePath.of("other");
    List<NamespacePath> schemas =
        java.util.stream.IntStream.range(0, 30)
            .mapToObj(i -> NamespacePath.of("crowded", String.format("s%02d", i)))
            .toList();
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(crowded, other));
    when(client.listNamespaces(crowded)).thenReturn(schemas);
    when(client.listNamespaces(other)).thenReturn(List.of());
    when(client.listTables(other)).thenReturn(List.of());
    for (NamespacePath schema : schemas) {
      when(client.listTables(schema))
          .thenReturn(
              List.of(new CatalogObjectName(schema, "a"), new CatalogObjectName(schema, "b")));
    }
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    // Four attempts is the crowded catalog's share; after that its remaining schemas are not even
    // listed. The two root listings are the two catalogs themselves.
    verify(client, atMost(4)).vendStorageCredentials(any());
    verify(client, atMost(6)).listTables(any());
  }

  @Test
  void closeFailureDoesNotRewriteCompletedValidationChecks() {
    setUpSuccessfulValidation(
        new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty()));
    doThrow(new IllegalStateException("close failed")).when(client).close();

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    assertEquals(5, result.checks().size());
  }

  @Test
  void providerCallsRetainPropagatedContext() {
    setUpSuccessfulValidation(
        new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty()));
    MDC.put("catalog-test-correlation", "corr-123");
    doAnswer(
            invocation -> {
              assertEquals("corr-123", MDC.get("catalog-test-correlation"));
              return null;
            })
        .when(client)
        .validate();

    try {
      assertTrue(discovery.validate(integration).valid());
    } finally {
      MDC.remove("catalog-test-correlation");
    }
  }

  @Test
  void strictProviderRootTableRejectionFallsThroughToNamespaceWalk() {
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName orders = setUpSuccessfulValidation(vended);
    when(client.listTables(NamespacePath.root()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.INVALID_CONFIGURATION,
                "Root is not a table namespace"));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).validateStorageAccess(orders, vended);
  }

  @Test
  void rootTablePermissionFailureFallsThroughToNamespaceWalk() {
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName orders = setUpSuccessfulValidation(vended);
    when(client.listTables(NamespacePath.root()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "Root tables are not visible"));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).validateStorageAccess(orders, vended);
  }

  @Test
  void namespaceTablePermissionFailureDoesNotAbortValidationWalk() {
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName orders = setUpSuccessfulValidation(vended);
    NamespacePath restricted = NamespacePath.of("production", "restricted");
    NamespacePath sales = orders.namespace();
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(restricted, sales));
    when(client.listTables(restricted))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "Namespace tables are not visible"));

    var result = discovery.validate(integration);

    assertTrue(result.valid());
    verify(client).listTables(restricted);
    verify(client).validateStorageAccess(orders, vended);
  }

  /**
   * A refusal on one parent is a fact about that branch; a refusal on the root is a fact about the
   * whole workspace, and {@code CatalogOverlayReconciler.tolerateBranchFailure} refuses to tolerate
   * it for exactly that reason. Tolerating it here drained the walk at once and reported "no
   * upstream table is available to validate credential vending" -- a statement about the workspace,
   * for a principal that simply could not enumerate it, which would then fail reconcile on the same
   * call it had just validated.
   */
  @Test
  void aRootNamespaceListingRefusalIsNotToleratedTheWayABranchIs() {
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "cannot list catalogs"));

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_DISCOVERY_FAILED, result.checks().get(2).getIssue());
  }

  /**
   * The ordering dependence the vend sampling exists to remove, one step later. A vend is gated on
   * EXTERNAL USE SCHEMA per schema while the bucket grant behind it belongs to a per-external-
   * location storage credential, so a table can vend and then be refused at the object store while
   * a sibling reads fine. Deciding the whole Integration on the first table that vended reported a
   * healthy workspace invalid.
   */
  @Test
  void aTableThatVendsButCannotBeReadIsSteppedOverLikeOneThatCannotVend() {
    NamespacePath sales = NamespacePath.of("main", "sales");
    CatalogObjectName unreadable = new CatalogObjectName(sales, "a_unreadable");
    CatalogObjectName readable = new CatalogObjectName(sales, "b_readable");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(NamespacePath.of("main")));
    when(client.listNamespaces(NamespacePath.of("main"))).thenReturn(List.of(sales));
    when(client.listTables(sales)).thenReturn(List.of(unreadable, readable));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.of(vended));
    doThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "403 from the object store"))
        .when(client)
        .validateStorageAccess(unreadable, vended);

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "the sibling that reads must still be reached");
    verify(client).validateStorageAccess(readable, vended);
  }

  /**
   * Two more reachable shapes from this branch's own validator: a bucket outside the integration's
   * configured region answers PermanentRedirect, mapped to INVALID_CONFIGURATION, and an EXTERNAL
   * table registered before its Delta log was written raises UNSUPPORTED. Neither says anything
   * about the next table.
   */
  @Test
  void perTableStorageFailureClassesDoNotDecideTheIntegration() {
    for (CatalogAccessException.Code code :
        List.of(
            CatalogAccessException.Code.INVALID_CONFIGURATION,
            CatalogAccessException.Code.UNSUPPORTED,
            CatalogAccessException.Code.NOT_FOUND,
            CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID)) {
      reset(client);
      NamespacePath sales = NamespacePath.of("main", "sales");
      CatalogObjectName first = new CatalogObjectName(sales, "a_first");
      CatalogObjectName second = new CatalogObjectName(sales, "b_second");
      var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
      when(client.capabilities()).thenReturn(validationCapabilities());
      when(client.listTables(NamespacePath.root())).thenReturn(List.of());
      when(client.listNamespaces(NamespacePath.root()))
          .thenReturn(List.of(NamespacePath.of("main")));
      when(client.listNamespaces(NamespacePath.of("main"))).thenReturn(List.of(sales));
      when(client.listTables(sales)).thenReturn(List.of(first, second));
      when(client.vendStorageCredentials(any())).thenReturn(Optional.of(vended));
      doThrow(new CatalogAccessException(code, "per-table: " + code))
          .when(client)
          .validateStorageAccess(first, vended);

      assertTrue(discovery.validate(integration).valid(), code.toString());
    }
  }

  /**
   * And the converse. Skipping is only sound because validation still fails when no sampled table
   * can be read -- reporting the refusal it stepped over, and reporting vending as passed, because
   * reaching a storage probe at all proves a credential was vended.
   */
  @Test
  void whenNoSampledTableCanBeReadTheStorageRefusalIsWhatIsReported() {
    NamespacePath sales = NamespacePath.of("main", "sales");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(NamespacePath.of("main")));
    when(client.listNamespaces(NamespacePath.of("main"))).thenReturn(List.of(sales));
    when(client.listTables(sales))
        .thenReturn(List.of(new CatalogObjectName(sales, "a"), new CatalogObjectName(sales, "b")));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.of(vended));
    doThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "403 from the object store"))
        .when(client)
        .validateStorageAccess(any(), any());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_PASSED, result.checks().get(3).getStatus());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_STORAGE_ACCESS_FAILED,
        result.checks().get(4).getIssue());
  }

  /**
   * A storage failure that will answer the same for every table stops the search where it stands,
   * rather than paying for a probe per sampled table to collect it again.
   */
  @Test
  void anUnreachableStoreStopsTheSearchRatherThanBeingSteppedOver() {
    NamespacePath sales = NamespacePath.of("main", "sales");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(NamespacePath.of("main")));
    when(client.listNamespaces(NamespacePath.of("main"))).thenReturn(List.of(sales));
    when(client.listTables(sales))
        .thenReturn(List.of(new CatalogObjectName(sales, "a"), new CatalogObjectName(sales, "b")));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.of(vended));
    doThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.UNAVAILABLE, "bucket unreachable"))
        .when(client)
        .validateStorageAccess(any(), any());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    verify(client, times(1)).validateStorageAccess(any(), any());
  }

  /**
   * A single-catalog workspace gets the whole total. The per-catalog cap is keyed on the top-level
   * segment, so with one catalog it capped the walk at two schemas -- and if those two happened to
   * hold browsable-but-not-vendable tables, validation reported the Integration invalid even though
   * a later schema vended. That is the ordering dependence the per-namespace budget removed,
   * reappearing one level up, and it is the common shape: Unity OSS ships one catalog.
   */
  @Test
  void aSingleCatalogWorkspaceSamplesBeyondThePerCatalogCap() {
    NamespacePath unity = NamespacePath.of("unity");
    List<NamespacePath> schemas =
        List.of(
            NamespacePath.of("unity", "a"),
            NamespacePath.of("unity", "b"),
            NamespacePath.of("unity", "c"),
            NamespacePath.of("unity", "d"));
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName late = new CatalogObjectName(NamespacePath.of("unity", "d"), "orders");
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(unity));
    when(client.listNamespaces(unity)).thenReturn(schemas);
    for (NamespacePath schema : schemas) {
      when(client.listTables(schema))
          .thenReturn(
              List.of(
                  new CatalogObjectName(schema, "orders"),
                  new CatalogObjectName(schema, "returns")));
    }
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(late)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "the fourth schema must still be reached");
    verify(client).validateStorageAccess(late, vended);
  }

  /**
   * And the cap still does its job where it has one. On a Databricks workspace hive_metastore sorts
   * ahead of main and cannot mint credentials for any of its tables, so without a per-catalog share
   * it spent the whole total there and reported a workspace invalid whose Unity-managed catalog
   * would have vended.
   */
  @Test
  void withSeveralCatalogsOneCatalogStillCannotSpendTheWholeTotal() {
    NamespacePath hive = NamespacePath.of("hive_metastore");
    NamespacePath main = NamespacePath.of("main");
    NamespacePath mainSales = NamespacePath.of("main", "sales");
    CatalogObjectName good = new CatalogObjectName(mainSales, "orders");
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    List<NamespacePath> hiveSchemas =
        java.util.stream.IntStream.range(0, 12)
            .mapToObj(i -> NamespacePath.of("hive_metastore", String.format("s%02d", i)))
            .toList();
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(hive, main));
    when(client.listNamespaces(hive)).thenReturn(hiveSchemas);
    when(client.listNamespaces(main)).thenReturn(List.of(mainSales));
    for (NamespacePath schema : hiveSchemas) {
      when(client.listTables(schema))
          .thenReturn(
              List.of(new CatalogObjectName(schema, "a"), new CatalogObjectName(schema, "b")));
    }
    when(client.listTables(mainSales)).thenReturn(List.of(good));
    when(client.vendStorageCredentials(any())).thenReturn(Optional.empty());
    when(client.vendStorageCredentials(good)).thenReturn(Optional.of(vended));

    var result = discovery.validate(integration);

    assertTrue(result.valid(), "the second catalog must still be sampled");
    verify(client).validateStorageAccess(good, vended);
  }

  /**
   * A budget expiry during sampling is a vending timeout, not a discovery failure. The per-table
   * budget check sits outside the try that classifies a vending failure, so this unwound into the
   * generic handler and reported CIVI_DISCOVERY_FAILED with vending marked not run -- while
   * discovery had demonstrably succeeded and vending was underway, pointing an operator at their
   * namespace listing rather than at the vend that was actually slow.
   *
   * <p>The clock is moved by the vend itself rather than by a read count, so the expiry lands
   * inside sampling by construction and not by arithmetic about how often the budget is consulted.
   */
  @Test
  void aBudgetExpiryWhileSamplingIsReportedAgainstVendingNotDiscovery() {
    NamespacePath sales = NamespacePath.of("main", "sales");
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(NamespacePath.of("main")));
    when(client.listNamespaces(NamespacePath.of("main"))).thenReturn(List.of(sales));
    when(client.listTables(sales))
        .thenReturn(List.of(new CatalogObjectName(sales, "a"), new CatalogObjectName(sales, "b")));

    AtomicBoolean spent = new AtomicBoolean();
    discovery.validationTimeout = Duration.ofSeconds(1);
    discovery.nanoTime = () -> spent.get() ? Duration.ofSeconds(1).toNanos() : 0L;
    // The first vend consumes the budget; the walk is well past discovery by then.
    when(client.vendStorageCredentials(any()))
        .thenAnswer(
            invocation -> {
              spent.set(true);
              return Optional.empty();
            });

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_PASSED,
        result.checks().get(2).getStatus(),
        "discovery enumerated the workspace, so it must not be reported as the failure");
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_VENDING_FAILED,
        result.checks().get(3).getIssue());
  }

  @Test
  void validationWalkStopsAtItsWallClockBudget() {
    discovery.validationTimeout = Duration.ofSeconds(1);
    AtomicInteger clockReads = new AtomicInteger();
    discovery.nanoTime =
        () -> clockReads.incrementAndGet() < 4 ? 0L : Duration.ofSeconds(1).toNanos();
    when(client.capabilities()).thenReturn(validationCapabilities());

    var result = discovery.validate(integration);

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_DISCOVERY_FAILED, result.checks().get(2).getIssue());
    verify(client, never()).listTables(any());
  }

  @Test
  void validationDeadlineInterruptsHungProviderCall() {
    discovery.validationTimeout = Duration.ofMillis(50);
    when(client.capabilities()).thenReturn(validationCapabilities());
    doAnswer(
            invocation -> {
              new CountDownLatch(1).await();
              return null;
            })
        .when(client)
        .validate();

    var result =
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> discovery.validate(integration));

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
        result.checks().getFirst().getIssue());
  }

  @Test
  void clientReturnedAfterOpenTimeoutIsClosed() throws InterruptedException {
    discovery.validationTimeout = Duration.ofMillis(50);
    CountDownLatch closed = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              try {
                new CountDownLatch(1).await();
              } catch (InterruptedException ignored) {
                // Simulate an HTTP layer that consumes interruption before returning its client.
              }
              return client;
            })
        .when(access)
        .open(integration);
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();

    var result =
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> discovery.validate(integration));

    assertFalse(result.valid());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_CONNECTION_FAILED,
        result.checks().getFirst().getIssue());
    assertTrue(closed.await(2, TimeUnit.SECONDS));
  }

  @Test
  void listDeadlineInterruptsHungProviderCall() {
    discovery.listTimeout = Duration.ofMillis(50);
    NamespacePath parent = NamespacePath.of("production");
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.LIST_NAMESPACES));
    when(client.listNamespaces(parent))
        .thenAnswer(
            invocation -> {
              new CountDownLatch(1).await();
              return List.of();
            });

    CatalogAccessException error =
        assertTimeoutPreemptively(
            Duration.ofSeconds(2),
            () ->
                assertThrows(
                    CatalogAccessException.class,
                    () -> discovery.listNamespaces(integration, parent)));

    assertEquals(CatalogAccessException.Code.TIMEOUT, error.code());
    verify(client).close();
  }

  @Test
  void authenticationFailureIsSeparateFromConnectionFailure() {
    when(client.capabilities()).thenReturn(validationCapabilities());
    doThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.UNAUTHENTICATED, "Authentication rejected"))
        .when(client)
        .validate();

    var result = discovery.validate(integration);

    assertEquals(
        CatalogIntegrationValidationStatus.CIVS_PASSED, result.checks().get(0).getStatus());
    assertEquals(
        CatalogIntegrationValidationCheckType.CIVCT_CATALOG_AUTHENTICATION,
        result.checks().get(1).getType());
    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_AUTHENTICATION_FAILED,
        result.checks().get(1).getIssue());
  }

  @Test
  void expiredVendedCredentialsAreReportedSeparately() {
    discovery.clock = Clock.fixed(Instant.parse("2026-08-14T12:00:00Z"), ZoneOffset.UTC);
    CatalogObjectName orders =
        setUpSuccessfulValidation(
            new VendedStorageCredentials(
                Map.of("key", "value"), "", Optional.of(Instant.parse("2026-08-14T11:59:59Z"))));

    var result = discovery.validate(integration);

    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_EXPIRED,
        result.checks().get(3).getIssue());
    verify(client, never()).validateStorageAccess(any(), any());
  }

  @Test
  void invalidCredentialScopeIsReportedSeparately() {
    var vended = new VendedStorageCredentials(Map.of("key", "value"), "", Optional.empty());
    CatalogObjectName orders = setUpSuccessfulValidation(vended);
    when(client.vendStorageCredentials(orders))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
                "Credential scope does not cover the table"));

    var result = discovery.validate(integration);

    assertEquals(
        CatalogIntegrationValidationIssue.CIVI_CREDENTIAL_SCOPE_INVALID,
        result.checks().get(3).getIssue());
    verify(client, never()).validateStorageAccess(any(), any());
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

  private CatalogObjectName setUpSuccessfulValidation(VendedStorageCredentials vended) {
    NamespacePath sales = NamespacePath.of("production", "sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    when(client.capabilities()).thenReturn(validationCapabilities());
    when(client.listTables(NamespacePath.root())).thenReturn(List.of());
    when(client.listNamespaces(NamespacePath.root())).thenReturn(List.of(sales));
    when(client.listTables(sales)).thenReturn(List.of(orders));
    when(client.vendStorageCredentials(orders)).thenReturn(Optional.of(vended));
    return orders;
  }

  private static CatalogCapabilities validationCapabilities() {
    return CatalogCapabilities.of(
        CatalogCapability.VALIDATE,
        CatalogCapability.LIST_NAMESPACES,
        CatalogCapability.LIST_TABLES,
        CatalogCapability.VEND_STORAGE_CREDENTIALS,
        CatalogCapability.VALIDATE_STORAGE_ACCESS);
  }
}
