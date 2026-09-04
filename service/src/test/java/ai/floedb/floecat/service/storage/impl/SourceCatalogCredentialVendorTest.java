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

package ai.floedb.floecat.service.storage.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.spi.DatabricksAccessDelegation;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.service.integration.CatalogIntegrationAccess;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SourceCatalogCredentialVendorTest {

  private static final Connector CONNECTOR =
      Connector.newBuilder().setResourceId(ResourceId.newBuilder().setId("c1")).build();
  private static final ResourceId INTEGRATION_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
          .setId("integration-1")
          .build();
  private static final Instant EXPIRY = Instant.parse("2026-09-01T15:00:00Z");

  private SourceCatalogCredentialVendor vendor;
  private CatalogIntegrationRepository integrations;
  private CatalogIntegrationAccess access;
  private CatalogClient client;
  private CatalogIntegration integration;

  @BeforeEach
  void setUp() {
    vendor = new SourceCatalogCredentialVendor();
    integrations = mock(CatalogIntegrationRepository.class);
    access = mock(CatalogIntegrationAccess.class);
    client = mock(CatalogClient.class);
    integration =
        CatalogIntegration.newBuilder()
            .setResourceId(INTEGRATION_ID)
            .setDisplayName("warehouse")
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("https://glue.us-east-1.amazonaws.com/iceberg")
            .putProperties("s3.region", "us-west-2")
            .build();

    // Opening an integration requires catalog-integration.use; a real Authorizer over a principal
    // that carries it keeps these tests exercising the same admission the service does.
    vendor.principal = mock(ai.floedb.floecat.service.security.impl.PrincipalProvider.class);
    when(vendor.principal.get())
        .thenReturn(
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions(
                    ai.floedb.floecat.service.security.RolePermissions.CATALOG_INTEGRATION_USE)
                .build());
    vendor.authz = new ai.floedb.floecat.service.security.impl.Authorizer();
    vendor.catalogIntegrationRepo = integrations;
    vendor.catalogIntegrationAccess = access;
    vendor.connectorRepo = mock(ConnectorRepository.class);
    vendor.defaultRegion = "us-east-1";
    vendor.upstreamTimeout = Duration.ofSeconds(30);
    vendor.clock = Clock.fixed(Instant.parse("2026-09-01T14:00:00Z"), ZoneOffset.UTC);
    when(integrations.getById(INTEGRATION_ID)).thenReturn(Optional.of(integration));
    when(access.open(integration)).thenReturn(client);
  }

  @Test
  void vendsForAnIcebergCatalogIntegrationOrigin() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(new CatalogObjectName(NamespacePath.of("sales"), "orders")))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders",
                    Optional.of(EXPIRY))));

    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders/metadata/v1.metadata.json",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertEquals(1, response.getStorageCredentialsCount());
    var credential = response.getStorageCredentials(0);
    // The already-authorized requested location, never the catalog's broader table prefix:
    // widening it here would widen the caller's grant from one object to the whole table.
    assertEquals("s3://warehouse/orders/metadata/v1.metadata.json", credential.getPrefix());
    assertEquals("ASIA-VENDED", credential.getConfigMap().get("s3.access-key-id"));
    assertEquals("secret-vended", credential.getConfigMap().get("s3.secret-access-key"));
    assertEquals("session-vended", credential.getConfigMap().get("s3.session-token"));
    assertTrue(credential.hasExpiresAt());
    assertEquals("us-west-2", response.getClientSafeConfigMap().get("s3.region"));
    assertFalse(response.getClientSafeConfigMap().containsKey("s3.secret-access-key"));
    verify(client).close();
  }

  /**
   * A Unity integration vend carries the whole session triple. The fixture used to omit the session
   * token while still supplying an expiry, which is not a shape Unity produces -- {@code
   * generateTemporaryTableCredentials} mints an AWS temporary credential and those always carry a
   * token -- and it stopped compiling as a valid case once an integration was required to hold the
   * triple on the query path too, not only on reconcile.
   */
  @Test
  void vendsForAUnityCatalogIntegrationOrigin() {
    integration = integration.toBuilder().setType(CatalogIntegrationType.CIT_UNITY).build();
    when(integrations.getById(INTEGRATION_ID)).thenReturn(Optional.of(integration));
    when(access.open(integration)).thenReturn(client);
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(new CatalogObjectName(NamespacePath.of("sales"), "orders")))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-UNITY",
                        "s3.secret-access-key", "secret-unity",
                        "s3.session-token", "session-unity"),
                    "s3://warehouse/orders",
                    Optional.of(EXPIRY))));

    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders/data.parquet",
            SourceCatalogCredentialVendor.CredentialUse.QUERY);

    assertEquals(1, response.getStorageCredentialsCount());
    var credential = response.getStorageCredentials(0);
    assertEquals("ASIA-UNITY", credential.getConfigMap().get("s3.access-key-id"));
    assertEquals("session-unity", credential.getConfigMap().get("s3.session-token"));
    verify(client).close();
  }

  @Test
  void refusesWhenTheCatalogProviderDoesNotVend() {
    // Not a fall-back. A storage authority is not a second way for an Integration-backed table to
    // reach its data -- it is the split-brain this feature replaces -- and the Integration model
    // has no way to express one anyway. Naming the cause beats relabelling it as a missing
    // authority the operator was never asked to configure.
    when(client.capabilities()).thenReturn(CatalogCapabilities.of(CatalogCapability.LOAD_TABLE));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
    assertTrue(
        failure.getStatus().getDescription().contains("vending"),
        failure.getStatus().getDescription());
    verify(client, never()).vendStorageCredentials(any());
    verify(client).close();
  }

  @Test
  void aMissingIntegrationIsRefusedRatherThanBlamedOnTheCaller() {
    // The permission gates opening the integration. A record that is not there was never going to
    // be opened, so answering PERMISSION_DENIED would blame the caller for the configuration. The
    // refusal names the record instead.
    when(vendor.principal.get())
        .thenReturn(
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("table.read")
                .build());
    when(integrations.getById(INTEGRATION_ID)).thenReturn(Optional.empty());

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    assertEquals(io.grpc.Status.Code.FAILED_PRECONDITION, failure.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  @Test
  void openingAnIntegrationStillRequiresTheVendPermission() {
    when(vendor.principal.get())
        .thenReturn(
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("table.read")
                .build());

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    verify(access, never()).open(any());
  }

  @Test
  void scopesTheIntegrationLookupToTheTableAccount() {
    ResourceId foreignIntegrationId = INTEGRATION_ID.toBuilder().setAccountId("foreign").build();
    when(client.capabilities()).thenReturn(CatalogCapabilities.of(CatalogCapability.LOAD_TABLE));

    assertThrows(
        StatusRuntimeException.class,
        () ->
            vendor.vendForTable(
                integrationTable(foreignIntegrationId),
                "s3://warehouse/orders",
                SourceCatalogCredentialVendor.CredentialUse.QUERY));

    verify(integrations).getById(INTEGRATION_ID);
    verify(integrations, never()).getById(foreignIntegrationId);
  }

  @Test
  void mapsTypedCatalogAuthorizationFailures() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.PERMISSION_DENIED, "Catalog access denied"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    assertEquals(io.grpc.Status.Code.PERMISSION_DENIED, failure.getStatus().getCode());
    assertTrue(failure.getStatus().getDescription().contains("integration-1"));
    // The code alone cannot be told apart from floecat's own auth failing; the reason names the
    // source catalog as the refuser.
    assertTrue(
        ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(
            failure));
    verify(client).close();
  }

  @Test
  void mapsDeterministicCatalogFailuresToATerminalVendingReason() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.NOT_FOUND, "Upstream table does not exist"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  @Test
  void aProviderThatHoldsNothingForTheTableIsRefused() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID,
                "Vended storage credentials do not cover the upstream table location"));

    // Not the disjoint-scope case responsePrefix handles: this is the provider reporting that what
    // it holds does not cover the table at all, so there is no scope to narrow and no credential to
    // stamp. Deterministic, and there is no authority to fall back to for an Integration, so it is
    // refused with the reason rather than relabelled as a missing authority.
    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  @Test
  void aWrongConfigurationIsTerminalRatherThanRetriedForever() {
    when(access.open(integration))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.INVALID_CONFIGURATION,
                "Catalog Integration authentication is not recognized"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    // An unrecognised auth configuration, an upstream 400 and a malformed catalog URI all arrive as
    // this code, and no number of retries changes any of them.
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  /**
   * A just-expired expiry is a race, not an answer: transit time between the catalog issuing the
   * credential and us reading it, or floecat's clock sitting seconds from the catalog's. The next
   * vend mints a new one, so it must not carry a terminal reason -- the reconciler would stop
   * retrying a job that self-heals on the first credential refresh.
   */
  @Test
  void aJustExpiredVendStaysRetryableForReconcile() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(Optional.of(vendedAt(Instant.parse("2026-09-01T13:59:59Z"))));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    // Structured, so it is not mistaken for floecat's own storage service being unreachable.
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(failure));
    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
    assertFalse(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure));
  }

  /**
   * A blank upstream display name on the Integration path names its cause instead of returning
   * null. Null means "no source-catalog answer here", which sends the caller to a storage authority
   * -- and an Integration is configured without one, so the operator would read a missing-authority
   * error for a table whose record is simply incomplete. Reachable: validateUpstreamRef never
   * requires the field, and records written before it was populated carry it empty.
   */
  @Test
  void anUnclassifiedVendFailureWarnsRatherThanVanishingAtDebug() {
    // INTERNAL is the one answer here that carries no floecat Error detail, so BaseServiceImpl
    // rebuilds it and GrpcErrors.shouldHideMessage replaces the description with the bare
    // correlation id. If it also logged at debug on the reconcile path, a provider that broke its
    // contract -- or any exception IcebergRestCatalogErrors.translate returned unwrapped -- would
    // fail the job with nothing anywhere to say why.
    record Case(
        String name,
        StatusRuntimeException status,
        SourceCatalogCredentialVendor.CredentialUse use,
        boolean warns) {}
    StatusRuntimeException refusal = SourceCatalogVendingGrpcStatus.sourceCatalogVendRefused("no");
    StatusRuntimeException retryable =
        SourceCatalogVendingGrpcStatus.sourceCatalogVendUnavailable("later", null);
    StatusRuntimeException internal =
        io.grpc.Status.INTERNAL.withDescription("broke its contract").asRuntimeException();

    for (var c :
        java.util.List.of(
            new Case(
                "a refusal on reconcile",
                refusal,
                SourceCatalogCredentialVendor.CredentialUse.RECONCILE,
                true),
            new Case(
                "a refusal on query",
                refusal,
                SourceCatalogCredentialVendor.CredentialUse.QUERY,
                true),
            // The flood this exists to damp: one per file group per attempt, all the same outage,
            // and the reconciler reports the classified failure anyway.
            new Case(
                "a retryable outage on reconcile",
                retryable,
                SourceCatalogCredentialVendor.CredentialUse.RECONCILE,
                false),
            new Case(
                "a retryable outage on query",
                retryable,
                SourceCatalogCredentialVendor.CredentialUse.QUERY,
                true),
            new Case(
                "an unclassified failure on reconcile",
                internal,
                SourceCatalogCredentialVendor.CredentialUse.RECONCILE,
                true),
            new Case(
                "an unclassified failure on query",
                internal,
                SourceCatalogCredentialVendor.CredentialUse.QUERY,
                true))) {
      assertEquals(
          c.warns(), SourceCatalogCredentialVendor.warrantsWarn(c.status(), c.use()), c.name());
    }
  }

  @Test
  void aBlankUpstreamNameOnTheIntegrationPathIsRefusedRatherThanSkipped() {
    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTableNamed("  "),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
    assertThat(failure.getStatus().getDescription()).contains("no upstream table reference");
    // Refused before any upstream call, so no client is opened for a request that cannot be built.
    verifyNoInteractions(client);
  }

  /**
   * Far enough in the past and it is no longer a race but an answer, and the same answer every
   * time. RefreshingAwsCredentialsProviderRegistry reads a past expiry as "refresh now" on every
   * resolveCredentials call, so a catalog that keeps reporting it has the job re-vend once per
   * resolution and still hand S3 credentials it refuses. Retryable even so, because that loop needs
   * a registered provider to exist: the first vend has no snapshot behind it, and its failure is
   * bounded by the reconciler's attempt budget. Terminal here would permanently fail the job on one
   * stale response, and the next vend may well mint a live credential.
   */
  @Test
  void aStaleVendExpiryIsRetryableForReconcile() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(Optional.of(vendedAt(Instant.parse("2026-09-01T13:00:00Z"))));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertFalse(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure));
    assertThat(failure.getStatus().getDescription()).contains("s3.session-token-expires-at-ms");
  }

  /**
   * Inside the tolerance the query path reads the credential rather than refusing it. It registers
   * no refresh provider, the tuple is almost certainly live, and a second of clock disagreement is
   * not a reason to fail a scan that would have worked.
   */
  @Test
  void theQueryPathReadsAJustExpiredVend() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(Optional.of(vendedAt(Instant.parse("2026-09-01T13:59:59Z"))));

    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders",
            SourceCatalogCredentialVendor.CredentialUse.QUERY);

    assertEquals(1, response.getStorageCredentialsCount());
    assertEquals(
        "ASIA-VENDED", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
  }

  /**
   * Past the tolerance the query path fails here rather than at S3. There is no fall-back to
   * preserve -- ServerSideFileIoPropertiesResolver consults the vendor only once no storage
   * authority covers the location, so its null branch raises the no-matching-authority error
   * instead of producing credentials -- and handing the scan engine a credential known to be dead
   * only turns a diagnosable failure into an opaque 403 partway through a file group.
   */
  @Test
  void theQueryPathRefusesAStaleVendRatherThanFailingAtS3() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(Optional.of(vendedAt(Instant.parse("2026-09-01T13:00:00Z"))));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    // Retryable, not terminal: nothing retries on this path, and the caller is being told the
    // catalog's answer was stale rather than that its table is unreadable.
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(failure));
    assertFalse(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure));
    assertThat(failure.getStatus().getDescription()).contains("s3.session-token-expires-at-ms");
  }

  /** A complete session tuple scoped to the requested location, expiring when the caller says. */
  private static VendedStorageCredentials vendedAt(Instant expiresAt) {
    return new VendedStorageCredentials(
        Map.of(
            "s3.access-key-id", "ASIA-VENDED",
            "s3.secret-access-key", "secret-vended",
            "s3.session-token", "session-vended"),
        "s3://warehouse/orders",
        Optional.of(expiresAt));
  }

  @Test
  void retriesRatherThanRefusingWhileIntegrationCredentialsAreUnresolvable() {
    when(access.open(integration))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.CREDENTIAL_UNAVAILABLE,
                "Catalog Integration credentials are not currently resolvable"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    // The window while a stored secret is superseded looks exactly like this, and it closes on its
    // own -- a terminal refusal here permanently fails every job on the integration.
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(failure));
    assertFalse(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  @Test
  void aFailingClientCloseDoesNotDiscardCredentialsAlreadyVended() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders",
                    Optional.of(EXPIRY))));
    // An HTTP pool already shut down, an interrupted keep-alive. The credentials are in hand by
    // then, so letting the close decide the answer throws away work that succeeded.
    doThrow(new IllegalStateException("connection pool already closed")).when(client).close();

    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders/metadata/v1.metadata.json",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertEquals(1, response.getStorageCredentialsCount());
    assertEquals(
        "ASIA-VENDED", response.getStorageCredentials(0).getConfigMap().get("s3.access-key-id"));
  }

  @Test
  void refusesWhenTheIntegrationAuthModeCannotVend() {
    when(access.open(integration))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.UNSUPPORTED,
                "Ambient and assumed AWS Catalog Integration credentials are not supported"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.QUERY));

    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(failure));
  }

  @Test
  void aDisjointVendedScopeStampsTheAuthorizedPrefixRatherThanRefusing() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders",
                    Optional.of(EXPIRY))));

    // The scope and the requested location have no intersection. Refusing would guarantee failure:
    // this path is reached only once no storage authority matched, so there is nothing to fall back
    // to. The requested prefix is the only bound the caller was authorized for, so it travels, the
    // read is attempted, and S3 -- which knows the real grant -- decides. Same policy as the
    // connector path, which is now the same code.
    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://external-data/orders/part-0.parquet",
            SourceCatalogCredentialVendor.CredentialUse.QUERY);

    assertEquals(
        "s3://external-data/orders/part-0.parquet", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void aScopeNarrowerThanTheRequestIsStampedAsItself() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders/data",
                    Optional.of(EXPIRY))));

    // A catalog that scoped to a subtree of what floecat asked for is a legitimately narrowed
    // credential, not a fault. The earlier inline check admitted only a scope containing the
    // request, so this ended as a missing-authority error.
    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertEquals("s3://warehouse/orders/data", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void acceptsAScopeCoveringTheRequestedLocationAcrossS3SchemeAliases() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3a://warehouse/orders",
                    Optional.of(EXPIRY))));

    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders/data/part-0.parquet",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertEquals(1, response.getStorageCredentialsCount());
    // Spelled the way the caller spells it. A client vend hands this prefix to the Iceberg reader,
    // and S3FileIO matches it against a storage path with a raw startsWith -- an "s3a://" prefix is
    // simply invisible to an "s3://" path, and the reader falls through to the root client.
    assertEquals(
        "s3://warehouse/orders/data/part-0.parquet", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void aNarrowerScopeIsStampedInTheRequestedSchemeSpelling() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3a://warehouse/orders/data",
                    Optional.of(EXPIRY))));

    // The narrower scope is the one returned, so this is the case where the catalog's own spelling
    // would otherwise travel back and be unmatchable against the reader's s3:// paths.
    var response =
        vendor.vendForTable(
            integrationTable(),
            "s3://warehouse/orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertEquals("s3://warehouse/orders/data", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void boundsTheWholeUpstreamConversationWithOneDeadline() {
    vendor.upstreamTimeout = Duration.ofMillis(50);
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenAnswer(
            invocation -> {
              new CountDownLatch(1).await();
              return Optional.empty();
            });

    StatusRuntimeException failure =
        assertTimeoutPreemptively(
            Duration.ofSeconds(2),
            () ->
                assertThrows(
                    StatusRuntimeException.class,
                    () ->
                        vendor.vendForTable(
                            integrationTable(),
                            "s3://warehouse/orders",
                            SourceCatalogCredentialVendor.CredentialUse.QUERY)));

    // A stalled upstream is a come-back-later condition, so it carries the retryable reason rather
    // than a bare INTERNAL whose description the gateway would hide.
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, failure.getStatus().getCode());
    assertTrue(SourceCatalogVendingGrpcStatus.isSourceCatalogVendUnavailable(failure));
    assertTrue(failure.getStatus().getDescription().contains("TIMEOUT"));
    verify(client).close();
  }

  @Test
  void closesAClientReturnedAfterTheOpenDeadline() throws InterruptedException {
    vendor.upstreamTimeout = Duration.ofMillis(50);
    CountDownLatch closed = new CountDownLatch(1);
    when(access.open(integration))
        .thenAnswer(
            invocation -> {
              try {
                new CountDownLatch(1).await();
              } catch (InterruptedException ignored) {
                // An HTTP layer that consumes interruption before handing back its client.
              }
              return client;
            });
    doAnswer(
            invocation -> {
              closed.countDown();
              return null;
            })
        .when(client)
        .close();

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertThrows(
                StatusRuntimeException.class,
                () ->
                    vendor.vendForTable(
                        integrationTable(),
                        "s3://warehouse/orders",
                        SourceCatalogCredentialVendor.CredentialUse.QUERY)));

    assertTrue(closed.await(2, java.util.concurrent.TimeUnit.SECONDS));
  }

  @Test
  void aCallerWithoutIntegrationUsePermissionCannotOpenTheIntegration() {
    // Table authorization admits a caller to this vend; it does not admit them to the integration's
    // own credential. Opening one resolves its stored secret and spends an OAuth exchange upstream,
    // so it takes the same permission every other site that opens an integration requires -- and
    // the read role carries table.read without it.
    when(vendor.principal.get())
        .thenReturn(
            ai.floedb.floecat.common.rpc.PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setCorrelationId("corr")
                .addPermissions("table.read")
                .addPermissions("catalog.read")
                .build());

    assertThrows(
        StatusRuntimeException.class,
        () ->
            vendor.vendForTable(
                integrationTable(),
                "s3://warehouse/orders",
                SourceCatalogCredentialVendor.CredentialUse.QUERY));

    verifyNoInteractions(access);
  }

  @Test
  void rejectsNonRefreshableIntegrationCredentialsForReconcile() {
    // Not merely "unusual shape": an integration vends only when the credential it holds is itself
    // a temporary session, and an AWS temporary credential always carries a session token. A pair
    // without one means the integration is not holding what it should, so this is a fault to
    // report rather than a long-lived credential to pass along -- and passing it along would
    // publish something floecat cannot bound, which the authority path refuses to do for its own
    // credentials. The connector path accepts this shape deliberately; see VendSource.
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended"),
                    "s3://warehouse/orders",
                    Optional.empty())));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertTrue(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure));
    assertThat(failure.getStatus().getDescription()).contains("s3.session-token-expires-at-ms");
  }

  /**
   * The other tuple that cannot be renewed, and the one the exemption does not cover: a session
   * token present but no expiry. The case above is refused because an integration should not be
   * holding a static key at all; this one because the reconciler registers a refresh provider only
   * when it can see an expiry, so it would embed a token that lapses mid-capture with no recovery.
   */
  @Test
  void reconcileRefusesASessionTokenWithNoExpiry() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders",
                    Optional.empty())));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTable(),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    assertTrue(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure));
    assertThat(failure.getStatus().getDescription()).contains("s3.session-token-expires-at-ms");
  }

  @Test
  void routingPropertiesKeepConnectorEndpointAndRegionAlias() {
    // A Unity vend response carries only the credential tuple, so endpoint and path-style can come
    // from nowhere but the connector: dropping them sends the reader at standard AWS S3 instead of
    // the configured S3-compatible endpoint. aws.region is a documented alias, so a connector
    // configured with it must not be overwritten by the deployment default either.
    SourceCatalogCredentialVendor vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    Map<String, String> routing =
        vendor.routingProperties(
            Map.of(),
            Map.of(
                "s3.endpoint",
                "https://minio.internal:9000",
                "s3.path-style-access",
                "true",
                "aws.region",
                "us-west-2"));

    assertThat(routing)
        .containsEntry("s3.endpoint", "https://minio.internal:9000")
        .containsEntry("s3.path-style-access", "true")
        .containsEntry("s3.region", "us-west-2")
        .containsEntry("region", "us-west-2")
        .containsEntry("client.region", "us-west-2");
  }

  @Test
  void routingPropertiesPreferVendedValuesOverConnectorConfiguration() {
    SourceCatalogCredentialVendor vendor = new SourceCatalogCredentialVendor();
    vendor.defaultRegion = "us-east-1";

    Map<String, String> routing =
        vendor.routingProperties(
            Map.of("s3.endpoint", "https://vended:9000", "s3.region", "eu-west-1"),
            Map.of("s3.endpoint", "https://connector:9000", "aws.region", "us-west-2"));

    assertThat(routing)
        .containsEntry("s3.endpoint", "https://vended:9000")
        .containsEntry("s3.region", "eu-west-1");
  }

  @Test
  void permissionDeniedAccessExceptionIsClassifiedTerminal() {
    // Regression guard for the misclassification bug: a non-Iceberg connector (e.g. Unity Catalog)
    // raises SourceCatalogAccessException, which must map to a terminal PERMISSION_DENIED rather
    // than a retryable INTERNAL that loops the reconcile job forever.
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new SourceCatalogAccessException(
                SourceCatalogAccessException.Denial.PERMISSION_DENIED, "HTTP 403"),
            CONNECTOR,
            "cat.schema",
            "orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void unauthenticatedAccessExceptionIsClassifiedTerminal() {
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new SourceCatalogAccessException(
                SourceCatalogAccessException.Denial.UNAUTHENTICATED, "HTTP 401"),
            CONNECTOR,
            "cat.schema",
            "orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.UNAUTHENTICATED);
  }

  @Test
  void accessExceptionWrappedInCauseChainIsStillClassifiedTerminal() {
    // The classifier walks the cause chain, so a wrapped access exception is still terminal.
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new RuntimeException(
                "vend failed",
                new SourceCatalogAccessException(
                    SourceCatalogAccessException.Denial.PERMISSION_DENIED, "HTTP 403")),
            CONNECTOR,
            "cat.schema",
            "orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void catalogSuppliedTextCannotForgeALogLineOrOverrunTheStatus() {
    // Both identifiers come off a persisted UpstreamRef and the cause message is whatever the
    // catalog returned, so all three are attacker-influenced -- and since these refusals gained a
    // keyed template whose body is the detail parameter, the description is what a client reads.
    String forged = "orders\nWARN  [floecat] credentials vended successfully";
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new RuntimeException("<html>\r\n<body>" + "x".repeat(20_000) + "</body>\r\n</html>"),
            CONNECTOR,
            "cat.schema",
            forged,
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    String description = status.getStatus().getDescription();
    // The table name still reaches it, so flattening and bounding still matter: an unflattened
    // newline would travel into trailer metadata and forge a server log entry.
    assertThat(description).doesNotContain("\n").doesNotContain("\r");
    assertThat(description.length()).isLessThan(1_024);
    // The throwable's own text does not reach it at all. It is upstream-controlled -- a proxy's
    // HTML, an internal host name, somebody else's stack -- and it used to be dropped because these
    // statuses had no keyed template; now that they render from the detail, excluding it is the
    // only
    // thing keeping it off the wire. The full cause still goes to the log.
    assertThat(description).doesNotContain("<html>").doesNotContain("xxxxxxxx");
    // The class name stays, which is as much of the throwable as a caller needs to tell one
    // failure from another.
    assertThat(description).contains("RuntimeException");
  }

  @Test
  void catalogSuppliedTextIsBoundedOnTheIntegrationPathToo() {
    // Same attacker-influenced inputs as the connector path: the table name comes off a persisted
    // UpstreamRef, and the status description becomes percent-encoded grpc-message trailer metadata
    // that withReason copies into two packed details. The Integration path had neither the flatten
    // nor the bound.
    String forged = "orders\nWARN  [floecat] credentials vended successfully" + "x".repeat(20_000);
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenThrow(
            new CatalogAccessException(
                CatalogAccessException.Code.NOT_FOUND, "Upstream table does not exist"));

    StatusRuntimeException failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                vendor.vendForTable(
                    integrationTableNamed(forged),
                    "s3://warehouse/orders",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE));

    String description = failure.getStatus().getDescription();
    assertThat(description).doesNotContain("\n").doesNotContain("\r");
    assertThat(description.length()).isLessThan(1_024);
  }

  @Test
  void aBlankRequestedPrefixIsStampedWithTheVendedScopeRatherThanUnrestricted() {
    when(client.capabilities())
        .thenReturn(CatalogCapabilities.of(CatalogCapability.VEND_STORAGE_CREDENTIALS));
    when(client.vendStorageCredentials(any()))
        .thenReturn(
            Optional.of(
                new VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "ASIA-VENDED",
                        "s3.secret-access-key", "secret-vended",
                        "s3.session-token", "session-vended"),
                    "s3://warehouse/orders/",
                    Optional.of(EXPIRY))));

    var response =
        vendor.vendForTable(
            integrationTable(), "", SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    // An empty prefix is what every consumer reads as unrestricted, so passing the blank through
    // would advertise more than the catalog granted. The trailing slash is normalized away because
    // consumers key a FileIO cache on this value.
    assertEquals("s3://warehouse/orders", response.getStorageCredentials(0).getPrefix());
  }

  @Test
  void unrecognizedFailureStaysRetryableInternal() {
    // A plain RuntimeException (a 5xx, a timeout) is genuinely transient and must stay retryable.
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new RuntimeException("UC temporary-table-credentials returned HTTP 503"),
            CONNECTOR,
            "cat.schema",
            "orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  void catalogIssuedScopeWinsWhenItNarrowsTheRequest() {
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            "  s3://warehouse/tpch/region/data  ",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                vended, "s3://warehouse/tpch/region", "cat.schema", "orders"))
        .isEqualTo("s3://warehouse/tpch/region/data");
  }

  @Test
  void catalogIssuedScopeCannotWidenBeyondTheAuthorizedPrefix() {
    // The requested prefix is the authorized location -- the same value that raises
    // PERMISSION_DENIED for an out-of-scope request. A catalog credential covering a broader tree
    // (Iceberg vends one scoped to "s3://warehouse/tpch" for a table under
    // "s3://warehouse/tpch_10") must not come back stamped with that broader prefix, or consumers
    // would apply the credential to sibling prefixes like "s3://warehouse/tpch_other".
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            "s3://warehouse/tpch",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                vended, "s3://warehouse/tpch_10/customer", "cat.schema", "orders"))
        .isEqualTo("s3://warehouse/tpch_10/customer");
  }

  @Test
  void aSiblingScopeSharingATextualPrefixDoesNotCount() {
    // Containment is by path boundary, not by string prefix: "s3://warehouse/tpch_other" starts
    // with "s3://warehouse/tpch" but is a different tree.
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            "s3://warehouse/tpch_other",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                vended, "s3://warehouse/tpch", "cat.schema", "orders"))
        .isEqualTo("s3://warehouse/tpch");
  }

  @Test
  void aDisjointVendedScopeStillReturnsTheAuthorizedPrefix() {
    // Neither contains the other -- reachable when the lease authorizes an absolute path in another
    // bucket, which a Delta log may carry. There is no intersection to return, so the requested
    // prefix stands: it is the only bound the caller was authorized for. A warning names the
    // mismatch, because the read then fails at storage with nothing pointing back at the catalog.
    var disjoint =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            "s3://table-bucket/table",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                disjoint, "s3://other-bucket/file.parquet", "cat.schema", "orders"))
        .isEqualTo("s3://other-bucket/file.parquet");
  }

  @Test
  void aBroaderVendedScopeIsNarrowedToTheRequestWithoutComplaint() {
    // The ordinary case the method exists for, kept distinct from disjoint: a catalog that scoped
    // to the bucket gets narrowed to the table, and that is not worth a warning.
    var broader =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            "s3://warehouse",
            Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                broader, "s3://warehouse/tpch/orders", "cat.schema", "orders"))
        .isEqualTo("s3://warehouse/tpch/orders");
  }

  @Test
  void requestedPrefixIsFallbackForLegacyConnector() {
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"), null, Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(
                vended, "s3://requested/table", "cat.schema", "orders"))
        .isEqualTo("s3://requested/table");
  }

  @Test
  void anEmptyCredentialObjectFallsBackRatherThanFailingTerminally() {
    // "The catalog handed back a credential object with no credentials" is the same answer as
    // "the catalog does not delegate": the caller must be able to fall back to a storage authority.
    // Letting it reach requireUsableCredentials would fail the reconcile job permanently.
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("catalog-1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setState(ConnectorState.CS_ACTIVE)
            .putProperties("iceberg.source", "rest")
            .putProperties("header.X-Iceberg-Access-Delegation", "vended-credentials")
            .build();
    ConnectorRepository connectorRepo = mock(ConnectorRepository.class);
    when(connectorRepo.getById(connectorId)).thenReturn(Optional.of(connector));

    FloecatConnector source = mock(FloecatConnector.class);
    when(source.vendStorageCredentials("cat.schema", "orders"))
        .thenReturn(
            Optional.of(
                new FloecatConnector.VendedStorageCredentials(
                    Map.of(), "s3://warehouse/orders", Instant.parse("2030-01-01T00:00:00Z"))));

    SourceCatalogCredentialVendor vendor = new SourceCatalogCredentialVendor();
    vendor.connectorRepo = connectorRepo;
    vendor.connectorFactory = ignored -> source;
    vendor.defaultRegion = "us-east-1";

    assertThat(
            vendor.vendForTable(
                tableFor(connectorId),
                "s3://warehouse/orders/data.parquet",
                SourceCatalogCredentialVendor.CredentialUse.RECONCILE))
        .isNull();
  }

  @Test
  void anUnsupportedRefusalTravelsAsTheStructuredVendRefusedReason() {
    // Terminal, but not a denial: reporting PERMISSION_DENIED would say "you may not read this
    // table" for a catalog that simply cannot vend for it.
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new SourceCatalogAccessException(
                SourceCatalogAccessException.Denial.UNSUPPORTED, "HTTP 400 external access off"),
            CONNECTOR,
            "cat.schema",
            "orders",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
    assertThat(SourceCatalogVendingGrpcStatus.isSourceCatalogVendRefused(status)).isTrue();
  }

  @Test
  void incompleteConnectorTupleIsAServiceLevelTerminalFailure() {
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("catalog-1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setState(ConnectorState.CS_ACTIVE)
            .putProperties("iceberg.source", "rest")
            .putProperties("header.X-Iceberg-Access-Delegation", "vended-credentials")
            .build();
    ConnectorRepository connectorRepo = mock(ConnectorRepository.class);
    when(connectorRepo.getById(connectorId)).thenReturn(Optional.of(connector));

    FloecatConnector source = mock(FloecatConnector.class);
    when(source.vendStorageCredentials("cat.schema", "orders"))
        .thenReturn(
            Optional.of(
                new FloecatConnector.VendedStorageCredentials(
                    Map.of("s3.access-key-id", "key", "s3.secret-access-key", "secret"),
                    "s3://warehouse/orders",
                    Instant.parse("2030-01-01T00:00:00Z"))));

    SourceCatalogCredentialVendor vendor = new SourceCatalogCredentialVendor();
    vendor.connectorRepo = connectorRepo;
    vendor.connectorFactory = ignored -> source;
    vendor.defaultRegion = "us-east-1";
    Table table = tableFor(connectorId);

    assertThatThrownBy(
            () ->
                vendor.vendForTable(
                    table,
                    "s3://warehouse/orders/data.parquet",
                    SourceCatalogCredentialVendor.CredentialUse.RECONCILE))
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            failure -> {
              assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
              assertThat(SourceCatalogVendingGrpcStatus.isVendedCredentialsNotRefreshable(failure))
                  .isTrue();
            });
  }

  @Test
  void anAccessPointScopedVendIsUsedAgainstTheBucketRatherThanRefused() {
    // Unity returns access_point for any external location that has one configured, and the grant
    // behind it commonly still permits addressing the bucket. Refusing on the field's presence
    // alone would fail closed on a tuple that reads fine, so the ARN is dropped and the rest used.
    // It must also not travel onward: nothing downstream addresses an access point.
    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CONNECTOR)
            .setId("catalog-1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setKind(ConnectorKind.CK_DELTA)
            .setState(ConnectorState.CS_ACTIVE)
            .putProperties("delta.source", "unity")
            .putProperties(DatabricksAccessDelegation.VEND_OPTION, "vended-credentials")
            .build();
    ConnectorRepository connectorRepo = mock(ConnectorRepository.class);
    when(connectorRepo.getById(connectorId)).thenReturn(Optional.of(connector));

    FloecatConnector source = mock(FloecatConnector.class);
    when(source.vendStorageCredentials("cat.schema", "orders"))
        .thenReturn(
            Optional.of(
                new FloecatConnector.VendedStorageCredentials(
                    Map.of(
                        "s3.access-key-id", "key",
                        "s3.secret-access-key", "secret",
                        "s3.session-token", "token",
                        "s3.access-point", "arn:aws:s3:eu-west-1:123:accesspoint/orders"),
                    "s3://warehouse/orders",
                    Instant.now().plusSeconds(3600))));

    SourceCatalogCredentialVendor vendor = new SourceCatalogCredentialVendor();
    vendor.connectorRepo = connectorRepo;
    vendor.connectorFactory = ignored -> source;
    vendor.defaultRegion = "us-east-1";

    var response =
        vendor.vendForTable(
            tableFor(connectorId),
            "s3://warehouse/orders/data.parquet",
            SourceCatalogCredentialVendor.CredentialUse.RECONCILE);

    assertThat(response).isNotNull();
    assertThat(response.getStorageCredentialsList()).hasSize(1);
    assertThat(response.getStorageCredentialsList().get(0).getConfigMap())
        .containsEntry("s3.access-key-id", "key")
        .doesNotContainKey("s3.access-point");
  }

  private static Table tableFor(ResourceId connectorId) {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1"))
        .setUpstream(
            UpstreamRef.newBuilder()
                .setConnectorId(connectorId)
                .addAllNamespacePath(List.of("cat", "schema"))
                .setTableDisplayName("orders"))
        .build();
  }

  private static Table integrationTable() {
    return integrationTable(INTEGRATION_ID);
  }

  /** A table whose upstream display name is whatever the catalog called it. */
  private static Table integrationTableNamed(String tableDisplayName) {
    Table base = integrationTable();
    return base.toBuilder()
        .setUpstream(base.getUpstream().toBuilder().setTableDisplayName(tableDisplayName))
        .build();
  }

  private static Table integrationTable(ResourceId integrationId) {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("table-1"))
        .setUpstream(
            UpstreamRef.newBuilder()
                .setCatalogIntegrationId(integrationId)
                .addNamespacePath("sales")
                .setTableDisplayName("orders"))
        .build();
  }
}
