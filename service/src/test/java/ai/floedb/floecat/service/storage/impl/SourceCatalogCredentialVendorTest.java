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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.connector.spi.SourceCatalogAccessException;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SourceCatalogCredentialVendorTest {

  private static final Connector CONNECTOR =
      Connector.newBuilder().setResourceId(ResourceId.newBuilder().setId("c1")).build();

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
            "orders");

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
            "orders");

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
            "orders");

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  @Test
  void unrecognizedFailureStaysRetryableInternal() {
    // A plain RuntimeException (a 5xx, a timeout) is genuinely transient and must stay retryable.
    StatusRuntimeException status =
        SourceCatalogCredentialVendor.catalogFailureStatus(
            new RuntimeException("UC temporary-table-credentials returned HTTP 503"),
            CONNECTOR,
            "cat.schema",
            "orders");

    assertThat(status.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
  }

  @Test
  void catalogIssuedScopeWinsWhenItNarrowsTheRequest() {
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            Instant.parse("2030-01-01T00:00:00Z"),
            "  s3://warehouse/tpch/region/data  ");

    assertThat(SourceCatalogCredentialVendor.responsePrefix(vended, "s3://warehouse/tpch/region"))
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
            Instant.parse("2030-01-01T00:00:00Z"),
            "s3://warehouse/tpch");

    assertThat(
            SourceCatalogCredentialVendor.responsePrefix(vended, "s3://warehouse/tpch_10/customer"))
        .isEqualTo("s3://warehouse/tpch_10/customer");
  }

  @Test
  void aSiblingScopeSharingATextualPrefixDoesNotCount() {
    // Containment is by path boundary, not by string prefix: "s3://warehouse/tpch_other" starts
    // with "s3://warehouse/tpch" but is a different tree.
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"),
            Instant.parse("2030-01-01T00:00:00Z"),
            "s3://warehouse/tpch_other");

    assertThat(SourceCatalogCredentialVendor.responsePrefix(vended, "s3://warehouse/tpch"))
        .isEqualTo("s3://warehouse/tpch");
  }

  @Test
  void requestedPrefixIsFallbackForLegacyConnector() {
    var vended =
        new FloecatConnector.VendedStorageCredentials(
            Map.of("s3.access-key-id", "key"), Instant.parse("2030-01-01T00:00:00Z"));

    assertThat(SourceCatalogCredentialVendor.responsePrefix(vended, "s3://requested/table"))
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
                    Map.of(), Instant.parse("2030-01-01T00:00:00Z"), "s3://warehouse/orders")));

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
            "orders");

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
                    Instant.parse("2030-01-01T00:00:00Z"),
                    "s3://warehouse/orders")));

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
}
