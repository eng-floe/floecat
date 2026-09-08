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
package ai.floedb.floecat.catalog.sharing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.access.CatalogAccessException;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogTraversalFailures;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.catalog.delta.DeltaLogStorageProbe;
import ai.floedb.floecat.client.sharing.DeltaSharingClient;
import ai.floedb.floecat.client.sharing.DeltaSharingException;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.AccessMode;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.CredentialCloud;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Protocol;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Schema;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Share;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.Table;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableDescription;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TableMetadata;
import ai.floedb.floecat.client.sharing.DeltaSharingModel.TemporaryCredentials;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class DeltaSharingCatalogClientTest {

  private static final String LOCATION = "s3://sharing/gold/events";

  @Test
  void capabilitiesCoverDiscoveryAndVending() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThat(client.capabilities().supports(CatalogCapability.LIST_NAMESPACES)).isTrue();
      assertThat(client.capabilities().supports(CatalogCapability.VEND_STORAGE_CREDENTIALS))
          .isTrue();
      assertThat(client.capabilities().supports(CatalogCapability.STABLE_OBJECT_IDS)).isTrue();
    }
  }

  @Test
  void rootListsSharesAndOneSegmentListsSchemas() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.listNamespaces(new NamespacePath(List.of())))
          .containsExactly(new NamespacePath(List.of("prod")));
      assertThat(client.listNamespaces(new NamespacePath(List.of("prod"))))
          .containsExactly(new NamespacePath(List.of("prod", "gold")));
    }
  }

  @Test
  void belowSchemaIsEmptyRatherThanAnError() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThat(client.listNamespaces(new NamespacePath(List.of("prod", "gold", "events"))))
          .isEmpty();
    }
  }

  /**
   * A share holds schemas, not tables. The reconciler lists tables at every selected namespace
   * before descending into it, so raising here abandoned any overlay whose include named a whole
   * share rather than a schema within it.
   */
  @Test
  void listingTablesAtAShareIsEmptyRatherThanAnError() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.listTables(new NamespacePath(List.of("prod")))).isEmpty();
      assertThat(client.listTables(NamespacePath.root())).isEmpty();
      assertThat(client.listTables(new NamespacePath(List.of("prod", "gold", "deeper")))).isEmpty();
    }
    // Empty because the level holds none, not because the upstream was asked and said so.
    assertThat(fake.listTablesCalls).isZero();
  }

  /** Addressing a table still has to name one, so the load path keeps its shape check. */
  @Test
  void loadingATableOutsideShareDotSchemaIsAConfigurationError() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThatThrownBy(
              () ->
                  client.loadTable(new CatalogObjectName(new NamespacePath(List.of("prod")), "t")))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.INVALID_CONFIGURATION));
    }
  }

  /**
   * A table id is unique within its share, not across the server, so an identity built from it
   * alone would collide between two shares carrying the same id -- which the reconciler treats as a
   * duplicate and abandons the overlay over.
   */
  @Test
  void loadTableScopesTheStableIdentityToItsShare() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      CatalogTable table = client.loadTable(events());
      assertThat(table.identity().value()).isEqualTo("share-uuid:tbl-1");
      assertThat(table.identity().stable()).isTrue();
      assertThat(table.storageLocation()).contains(LOCATION);
      assertThat(table.properties()).containsEntry("delta.sharing.access-modes", "url,dir");
      assertThat(table.properties()).containsEntry("delta.sharing.version", "12");
      // The reconciler accepts ICEBERG or DELTA and nothing else. The metaData action's
      // format.provider names the data file format underneath the table, which is a different
      // question, and is kept as a property rather than reported as the table's format.
      assertThat(table.format()).isEqualTo("DELTA");
      assertThat(table.properties()).containsEntry("delta.sharing.file-format", "parquet");
    }
  }

  @Test
  void aShareWithoutAServerIdIsNamedButNotClaimedStable() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.shareId = Optional.empty();
    try (DeltaSharingCatalogClient client = client(fake)) {
      CatalogTable table = client.loadTable(events());
      // Unambiguous, because the share name still separates two shares. Not stable, because the
      // protocol promises nothing about a share name surviving.
      assertThat(table.identity().value()).isEqualTo("prod:tbl-1");
      assertThat(table.identity().stable()).isFalse();
    }
  }

  /**
   * The metaData action's id is the underlying Delta table's, not the share entry's. A share
   * exposing one physical table under two names reports the same value for both, and the reconciler
   * abandons the whole overlay on a duplicate stable identity -- so the listing's id or nothing.
   */
  @Test
  void aListingWithoutATableIdDoesNotBorrowThePhysicalTableId() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.tableId = Optional.empty();
    fake.metadataId = Optional.of("physical-table-uuid");
    try (DeltaSharingCatalogClient client = client(fake)) {
      CatalogTable table = client.loadTable(events());
      assertThat(table.identity().value()).isEqualTo("prod.gold.events");
      assertThat(table.identity().stable()).isFalse();
    }
  }

  /** Two entries on one physical table must not collide, which is what dropped the overlay. */
  @Test
  void twoEntriesSharingOnePhysicalTableGetDistinctIdentities() {
    FakeSharingClient first = new FakeSharingClient();
    first.tableId = Optional.empty();
    first.metadataId = Optional.of("physical-table-uuid");
    FakeSharingClient second = new FakeSharingClient();
    second.tableId = Optional.empty();
    second.metadataId = Optional.of("physical-table-uuid");
    second.schemaName = "silver";
    try (DeltaSharingCatalogClient one = client(first);
        DeltaSharingCatalogClient two = client(second)) {
      assertThat(one.loadTable(events()).identity().value())
          .isNotEqualTo(
              two.loadTable(
                      new CatalogObjectName(new NamespacePath(List.of("prod", "silver")), "events"))
                  .identity()
                  .value());
    }
  }

  @Test
  void aTableWithNoIdAnywhereFallsBackToItsNameAndReportsThatAsUnstable() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.tableId = Optional.empty();
    fake.metadataId = Optional.empty();
    try (DeltaSharingCatalogClient client = client(fake)) {
      CatalogTable table = client.loadTable(events());
      assertThat(table.identity().value()).isEqualTo("prod.gold.events");
      assertThat(table.identity().stable()).isFalse();
    }
  }

  /**
   * The property is a report of what the share said, so it must not say url when the share said
   * nothing -- that is the reading the vend deliberately declines to apply.
   */
  @Test
  void aServerStatingNoModesIsReportedAsUnstatedRatherThanUrl() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.loadTable(events()).properties())
          .containsEntry("delta.sharing.access-modes", "unstated");
    }
  }

  /** In the delta response format the metaData action states the modes, not only the listing. */
  @Test
  void modesStatedOnlyByTheMetadataActionAreStillReported() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.metadataAccessModes = List.of(AccessMode.DIR);
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.loadTable(events()).properties())
          .containsEntry("delta.sharing.access-modes", "dir");
    }
  }

  /**
   * The protocol wants a credential per location, root and auxiliary alike, and tells a client that
   * cannot read from several to fall back to url access or raise. One scoped credential cannot
   * carry the rest, so the table is refused before it can reconcile.
   */
  @Test
  void aTableWithAuxiliaryLocationsIsRefusedBeforeItCanReconcile() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.auxiliaryLocations = List.of("s3://sharing/gold/events-aux");
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage())
                    .contains("prod.gold.events", "auxiliary storage locations");
              });
    }
  }

  @Test
  void aTableTheShareDoesNotCarryIsNotFound() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThatThrownBy(() -> client.loadTable(new CatalogObjectName(schemaPath(), "not_shared")))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.NOT_FOUND));
    }
  }

  @Test
  void vendingPublishesTheSessionTriadScopedToTheServersLocation() {
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(new FakeSharingClient(), Map.of("s3.region", "us-east-1"))) {
      VendedStorageCredentials vended = client.vendStorageCredentials(events()).orElseThrow();
      assertThat(vended.properties())
          .containsEntry("s3.access-key-id", "AKIA")
          .containsEntry("s3.secret-access-key", "secret")
          .containsEntry("s3.session-token", "session")
          .containsEntry("s3.region", "us-east-1");
      assertThat(vended.scopePrefix()).isEqualTo(LOCATION);
      assertThat(vended.expiresAt()).isPresent();
    }
  }

  @Test
  void aUrlOnlyTableIsRefusedByNameRatherThanVendingNothing() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.URL);
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("prod.gold.events", "url access only");
              });
    }
  }

  /**
   * A server that states nothing is asked rather than refused.
   *
   * <p>The protocol reads an absent field as url only. The reference implementation vends through
   * {@code temporary-table-credentials} while never sending the field at all, so holding to that
   * reading refuses every table on a server that would have answered.
   */
  @Test
  void aServerThatStatesNoAccessModesIsAskedRatherThanRefused() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    try (DeltaSharingCatalogClient client = client(fake)) {
      VendedStorageCredentials vended = client.vendStorageCredentials(events()).orElseThrow();
      assertThat(vended.scopePrefix()).isEqualTo(LOCATION);
    }
  }

  @Test
  void theStrictSettingReadsAnUnstatedTableAsUrlOnlyWithoutAsking() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.credentialFailure = DeltaSharingException.Failure.SERVER_ERROR;
    try (DeltaSharingCatalogClient client = new DeltaSharingCatalogClient(fake, Map.of(), true)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage())
                    .contains("delta.sharing.strict-access-modes", "prod.gold.events");
              });
    }
  }

  @Test
  void theStrictSettingStillVendsForATableThatStatesDirectoryAccess() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = new DeltaSharingCatalogClient(fake, Map.of(), true)) {
      assertThat(client.vendStorageCredentials(events())).isPresent();
    }
  }

  @Test
  void aServerWithoutTheCredentialEndpointIsReportedAsUnsupportedNotMissing() {
    for (DeltaSharingException.Failure refusal :
        List.of(
            DeltaSharingException.Failure.NOT_FOUND,
            DeltaSharingException.Failure.INVALID_REQUEST)) {
      FakeSharingClient fake = new FakeSharingClient();
      fake.accessModes = List.of();
      fake.credentialFailure = refusal;
      // The status the protocol defines for each: 404 for a server without the endpoint, 400 for
      // its refusal of a table that does not offer directory access.
      fake.credentialStatus = refusal == DeltaSharingException.Failure.NOT_FOUND ? 404 : 400;
      try (DeltaSharingCatalogClient client = client(fake)) {
        assertThatThrownBy(() -> client.vendStorageCredentials(events()))
            .describedAs("%s", refusal)
            .isInstanceOfSatisfying(
                CatalogAccessException.class,
                failure -> {
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                  assertThat(failure.getMessage()).contains("prod.gold.events");
                });
      }
    }
  }

  @Test
  void askingDoesNotTurnARejectedTokenIntoAnUnsupportedTable() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.credentialFailure = DeltaSharingException.Failure.UNAUTHENTICATED;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.UNAUTHENTICATED));
    }
  }

  @Test
  void aTableThatStatesUrlOnlyIsRefusedEvenWithoutTheStrictSetting() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.URL);
    fake.credentialFailure = DeltaSharingException.Failure.SERVER_ERROR;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  @Test
  void aListingWithoutALocationFallsBackToTheMetadataEndpoint() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.empty();
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, probe)) {
      client.validateStorageAccess(
          events(),
          new VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIA"), "s3://sharing/gold", Optional.empty()));
    }
    assertThat(probe.location).isEqualTo(LOCATION);
  }

  @Test
  void aNonAwsCredentialIsRefusedByCloud() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AZURE,
            LOCATION,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("AZURE");
              });
    }
  }

  @Test
  void validateStorageAccessProbesTheTableItWasAskedAbout() {
    RecordingProbe probe = new RecordingProbe();
    VendedStorageCredentials credentials =
        new VendedStorageCredentials(
            Map.of("s3.access-key-id", "AKIA"), "s3://sharing/gold", Optional.empty());
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(new FakeSharingClient(), Map.of(), false, probe)) {
      client.validateStorageAccess(events(), credentials);
    }
    assertThat(probe.location).isEqualTo(LOCATION);
    assertThat(probe.credentials).isSameAs(credentials);
  }

  /** Scope answers without a network call, so a credential scoped elsewhere never reaches it. */
  @Test
  void aCredentialScopedElsewhereIsRefusedBeforeTheProbeRuns() {
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(new FakeSharingClient(), Map.of(), false, probe)) {
      assertThatThrownBy(
              () ->
                  client.validateStorageAccess(
                      events(),
                      new VendedStorageCredentials(
                          Map.of("s3.access-key-id", "AKIA"), "s3://other/gold", Optional.empty())))
          .isInstanceOf(CatalogAccessException.class);
    }
    assertThat(probe.location).isNull();
  }

  @Test
  void validateStorageAccessRejectsACredentialScopedElsewhere() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThatThrownBy(
              () ->
                  client.validateStorageAccess(
                      events(),
                      new VendedStorageCredentials(
                          Map.of("s3.access-key-id", "AKIA"), "s3://other/gold", Optional.empty())))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID));
    }
  }

  @Test
  void validateProvesTheEndpointAndTokenByListingShares() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      client.validate();
      assertThat(fake.listSharesCalls).isEqualTo(1);
    }
  }

  @Test
  void aRejectedTokenStaysTerminalAndAServerErrorStaysRetryable() {
    FakeSharingClient rejected = new FakeSharingClient();
    rejected.failure = DeltaSharingException.Failure.UNAUTHENTICATED;
    try (DeltaSharingCatalogClient client = client(rejected)) {
      assertThatThrownBy(client::validate)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.UNAUTHENTICATED));
    }

    FakeSharingClient down = new FakeSharingClient();
    down.failure = DeltaSharingException.Failure.SERVER_ERROR;
    try (DeltaSharingCatalogClient client = client(down)) {
      assertThatThrownBy(client::validate)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNAVAILABLE));
    }
  }

  @Test
  void viewsAreEmptyAndLoadingOneIsUnsupported() {
    try (DeltaSharingCatalogClient client = client(new FakeSharingClient())) {
      assertThat(client.listViews(schemaPath())).isEmpty();
      assertThatThrownBy(() -> client.loadView(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  @Test
  void closingTheCatalogClientClosesTheRecipient() {
    FakeSharingClient fake = new FakeSharingClient();
    client(fake).close();
    assertThat(fake.closed).isTrue();
  }

  private static DeltaSharingCatalogClient client(DeltaSharingClient sharing) {
    return new DeltaSharingCatalogClient(sharing, Map.of());
  }

  private static NamespacePath schemaPath() {
    return new NamespacePath(List.of("prod", "gold"));
  }

  private static CatalogObjectName events() {
    return new CatalogObjectName(schemaPath(), "events");
  }

  /** Records what the storage probe was asked, instead of reaching an object store. */
  private static final class RecordingProbe implements DeltaLogStorageProbe {
    private String location;
    private VendedStorageCredentials credentials;

    @Override
    public void validate(String tableLocation, VendedStorageCredentials vended) {
      this.location = tableLocation;
      this.credentials = vended;
    }
  }

  /**
   * A credential is vended on every storage-authority resolve, so listing the schema to find one
   * table paged the whole schema before every credential POST -- unbounded in the schema's table
   * count, and quadratic across a reconcile.
   */
  @Test
  void vendingDoesNotListTheSchema() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      client.vendStorageCredentials(events());
    }
    assertThat(fake.listTablesCalls).isZero();
  }

  @Test
  void validatingStorageAccessDoesNotListTheSchema() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      client.validateStorageAccess(
          events(),
          new VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIA"), "s3://sharing/gold", Optional.empty()));
    }
    assertThat(fake.listTablesCalls).isZero();
  }

  /**
   * UNSUPPORTED now means the provider will never do this, which the service acts on by skipping
   * the table and reporting a vending-unsupported issue. An unclassified upstream status is a real
   * problem instead, and must not be reported as a capability boundary.
   */
  @Test
  void anUnclassifiedUpstreamStatusIsNotReportedAsUnsupported() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.credentialFailure = DeltaSharingException.Failure.OTHER;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNAVAILABLE));
    }
  }

  /**
   * The reconciler lists a schema once and then loads every table in it. Looking each one up by
   * paging the listing again made that pass quadratic in the schema's table count.
   */
  @Test
  void loadingSeveralTablesListsTheSchemaOnce() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      client.loadTable(events());
      client.loadTable(events());
      client.loadTable(events());
    }
    assertThat(fake.listTablesCalls).isEqualTo(1);
  }

  /**
   * Either place may state the modes and neither is required to. Strict mode refuses without asking
   * the server, so refusing on the metadata action alone would reject a table the share had marked
   * directory-accessible in its listing.
   */
  @Test
  void theStrictSettingConsultsTheListingBeforeRefusing() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataAccessModes = List.of();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), true, new RecordingProbe())) {
      assertThat(client.vendStorageCredentials(events())).isPresent();
    }
  }

  /**
   * The runtime path never reaches validateStorageAccess, so a credential scoped somewhere
   * unrelated would otherwise be published on every storage-authority resolve.
   */
  @Test
  void aCredentialScopedOutsideTheTableIsRefusedByTheVend() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://somewhere-else/entirely",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID));
    }
  }

  /** The listing states a location too, so a share using only that is not unvalidatable. */
  @Test
  void storageValidationFallsBackToTheListingLocation() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.metadataLocation = Optional.empty();
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, probe)) {
      client.validateStorageAccess(
          events(),
          new VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIA"), "s3://sharing/gold", Optional.empty()));
    }
    assertThat(probe.location).isEqualTo(LOCATION);
  }

  /**
   * A body this client cannot read describes the catalog, not one table in it.
   * INVALID_CONFIGURATION is per-branch, so a reconcile would record every table as unobserved and
   * leave a broken share looking partially healthy.
   */
  @Test
  void anUnreadableResponseIsNotAPerBranchFailure() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.failure = DeltaSharingException.Failure.INVALID_RESPONSE;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(client::validate)
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.INTERNAL);
                assertThat(CatalogTraversalFailures.describesOneBranch(failure)).isFalse();
              });
    }
  }

  /**
   * A share root, a bucket, an external-location prefix: broader than the table and left alone.
   * StorageLocations states the rule -- never be stricter than the component that will use the
   * credential, because this path is reached only once no storage authority matched, so refusing
   * fails a read the catalog's own client would have attempted.
   */
  @Test
  void aCredentialScopedAboveTheTableIsPublished() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://sharing",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.vendStorageCredentials(events()).orElseThrow().scopePrefix())
          .isEqualTo("s3://sharing");
    }
  }

  /** An id carrying the delimiter must not collide with a different pair of components. */
  @Test
  void identityComponentsAreEscapedSoThePairCannotCollide() {
    FakeSharingClient first = new FakeSharingClient();
    first.shareId = Optional.of("a:b");
    first.tableId = Optional.of("c");
    first.metadataId = Optional.of("c");
    FakeSharingClient second = new FakeSharingClient();
    second.shareId = Optional.of("a");
    second.tableId = Optional.of("b:c");
    second.metadataId = Optional.of("b:c");
    try (DeltaSharingCatalogClient one = client(first);
        DeltaSharingCatalogClient two = client(second)) {
      assertThat(one.loadTable(events()).identity().value())
          .isNotEqualTo(two.loadTable(events()).identity().value());
    }
  }

  /**
   * The reconciler lists a schema and then loads every table in it; that is one listing, not two.
   */
  @Test
  void listingThenLoadingPagesTheSchemaOnce() {
    FakeSharingClient fake = new FakeSharingClient();
    try (DeltaSharingCatalogClient client = client(fake)) {
      client.listTables(schemaPath());
      client.loadTable(events());
    }
    assertThat(fake.listTablesCalls).isEqualTo(1);
  }

  /**
   * The protocol states auxiliary locations on the listing as well as the metadata action, and says
   * a client that cannot read one should fall back to url access or fail the request.
   */
  @Test
  void auxiliaryLocationsStatedOnlyByTheListingAlsoRefuseTheLoad() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedAuxiliaryLocations = List.of("s3://sharing/gold/events-aux");
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("auxiliary storage locations");
              });
    }
  }

  /**
   * The reference implementation names a location on neither its listing nor its metadata action,
   * only on the credential response. Reconciling without one stores no storage location and falls
   * back to the Integration's HTTPS catalog URI, so the table materializes and cannot be opened.
   */
  @Test
  void aLocationStatedOnlyByTheCredentialResponseStillReachesTheTable() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    fake.accessModes = List.of();
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
  }

  /**
   * A url-only table is refused at the load, not left to fail at read time. Materializing it stores
   * no storage location and leaves the share's own HTTPS endpoint as the upstream reference, which
   * is the unreadable shape the location work exists to prevent -- so the two cases are refused the
   * same way rather than one of each.
   */
  @Test
  void aUrlOnlyTableIsRefusedAtTheLoadWithoutBeingAsked() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.URL);
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("url access only");
              });
    }
    assertThat(fake.credentialCalls).isZero();
  }

  /**
   * The load and the vend answer the same question the same way. The strict refusal sat below the
   * early return on a stated location, so a table with no stated modes whose listing named a
   * location loaded, and then the vend refused it on the rule the load had skipped: it reconciled
   * and every read of it failed. Knowing where a table lives says nothing about whether it may be
   * read that way.
   */
  @Test
  void strictModeRefusesAnUnstatedTableAtBothPathsEvenWhenTheListingNamesALocation() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    fake.metadataLocation = Optional.empty();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), true, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .as("load")
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage())
                    .contains(DeltaSharingCatalogClient.STRICT_ACCESS_MODES);
              });
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .as("vend")
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
    assertThat(fake.credentialCalls).isZero();
  }

  /**
   * One location, whichever path asks. The load preferred the listing while validation and the vend
   * read the metaData action, so a server stating a location on both surfaces that differ had the
   * table reconciling at one, being probed at another, and having the credential's scope compared
   * against that other -- a passing storage-access check saying nothing about where a read would
   * go. All three agree on the metaData action, because the vend cannot see a listing without
   * paging the schema on every read.
   */
  @Test
  void allThreePathsResolveTheSameLocationWhenTheSurfacesDisagree() {
    String listingLocation = "s3://sharing/gold/from-listing";
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.listedLocation = Optional.of(listingLocation);
    fake.metadataLocation = Optional.of(LOCATION);
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, probe)) {
      assertThat(client.loadTable(events()).storageLocation()).as("load").contains(LOCATION);
      assertThat(client.vendStorageCredentials(events()))
          .as("vend")
          .get()
          .extracting(VendedStorageCredentials::scopePrefix)
          .isEqualTo(LOCATION);
      client.validateStorageAccess(
          events(),
          new VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIA"), LOCATION, Optional.empty()));
    }
    assertThat(probe.location).as("probed").isEqualTo(LOCATION);
  }

  /**
   * The last surface pairing where the two paths disagreed, and it ran the wrong way. statedModes
   * prefers the listing, so a table stating dir only there took the load's early return without
   * asking, while the vend -- which reads the metaData action alone -- saw nothing stated, treated
   * the table as ambiguous and asked. A server advertising dir on its listing and then refusing the
   * credential endpoint reconciled and failed every read.
   */
  @Test
  void aDirStatedOnlyOnTheListingIsStillAskedAboutAtTheLoad() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    fake.metadataLocation = Optional.of(LOCATION);
    fake.credentialFailure = DeltaSharingException.Failure.INVALID_REQUEST;
    fake.credentialStatus = 400;
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
    assertThat(fake.credentialCalls).isPositive();
  }

  /**
   * A dir the metaData action does state is trusted, and no request is spent on it. Both paths
   * agree on that, which is the property that matters; a server contradicting its own metaData is a
   * different problem from one that left the question open.
   */
  @Test
  void aDirStatedOnTheMetadataActionCostsNoCredentialCallAtTheLoad() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataAccessModes = List.of(AccessMode.DIR);
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
    assertThat(fake.credentialCalls).isZero();
  }

  /**
   * The location that reconciles has to be one the read path can parse. Encoding the space only
   * inside the probe was half a fix: the raw string was still what got published, so the table
   * materialised as {@code s3://.../my table} and every scan threw on {@code URI.create} after
   * validation had passed. The read path derives its key with {@code URI.getPath}, which decodes,
   * so the encoded form asks S3 for the key that was written.
   */
  @Test
  void aLocationWithASpaceIsPublishedInAFormTheReadPathCanParse() {
    String raw = "s3://sharing/gold/my table";
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.listedLocation = Optional.of(raw);
    fake.metadataLocation = Optional.of(raw);
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            raw,
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, probe)) {
      String published = client.loadTable(events()).storageLocation().orElseThrow();
      assertThat(published).isEqualTo("s3://sharing/gold/my%20table");
      assertThatCode(() -> URI.create(published)).doesNotThrowAnyException();
      assertThat(URI.create(published).getPath()).isEqualTo("/gold/my table");

      // And the vend publishes the same form, so covers() compares like with like.
      assertThat(client.vendStorageCredentials(events()))
          .get()
          .extracting(VendedStorageCredentials::scopePrefix)
          .isEqualTo("s3://sharing/gold/my%20table");
    }
  }

  /**
   * Only 400 is the protocol's own refusal. The transport folds 400, 405 and 422 into one failure,
   * so a GET-only proxy answering 405 was read as every table lacking directory access while the
   * real fault went unnamed. Both stay a per-table skip; what changes is what the operator is told.
   */
  @Test
  void aMethodNotAllowedIsNotReadAsMissingDirectoryAccess() {
    for (int status : List.of(405, 422)) {
      FakeSharingClient fake = new FakeSharingClient();
      fake.accessModes = List.of(AccessMode.DIR);
      fake.credentialFailure = DeltaSharingException.Failure.INVALID_REQUEST;
      fake.credentialStatus = status;
      try (DeltaSharingCatalogClient client =
          new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
        assertThatThrownBy(() -> client.vendStorageCredentials(events()))
            .describedAs("%s", status)
            .isInstanceOfSatisfying(
                CatalogAccessException.class,
                failure ->
                    assertThat(failure.code())
                        .isNotEqualTo(CatalogAccessException.Code.UNSUPPORTED));
      }
    }
  }

  /**
   * Under the strict setting the vend falls back to the listing, so the load consults it too. Both
   * surfaces are already in hand, so this costs no request -- it stops the knob from spending a
   * credential POST per table against a server stating its modes where the protocol defines them.
   */
  @Test
  void strictModeTrustsADirStatedOnTheListingWithoutAsking() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), true, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
    assertThat(fake.credentialCalls).isZero();
  }

  /**
   * The scope the vend publishes is checked the way the load checks a location. requireServable ran
   * only at the load, so on the reference-server shape -- no location on the metaData action, so
   * the coverage check is skipped -- whatever the credential endpoint returned was published
   * unexamined, and a table reconciled from an earlier good credential could start receiving a
   * tuple scoped somewhere this provider cannot read.
   */
  @Test
  void theVendRefusesACredentialScopedSomewhereItCannotRead() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataLocation = Optional.empty();
    fake.listedLocation = Optional.empty();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "gs://elsewhere/table",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("gs");
              });
    }
  }

  /**
   * The vend says the same thing the load does. Validation's sampler sends names straight from
   * listTables to vendStorageCredentials without loading them, so for a table stating only modes
   * this client does not know, this is the message an operator running a validation actually sees.
   */
  @Test
  void theVendAlsoNamesUnrecognisedModesRatherThanClaimingUrlAccess() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.OTHER);
    fake.metadataAccessModes = List.of(AccessMode.OTHER);
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("does not recognise");
                assertThat(failure.getMessage()).doesNotContain("url access only");
              });
    }
  }

  /**
   * And a table stating only modes this client does not recognise is refused for that, not told it
   * offers url access.
   */
  @Test
  void aTableStatingOnlyUnrecognisedModesSaysSo() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.OTHER);
    fake.metadataAccessModes = List.of(AccessMode.OTHER);
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("does not recognise");
              });
    }
  }

  /**
   * A redirect is a wrong base URI, not a table without directory access. The transport folds a
   * refused 3xx into INVALID_REQUEST, which this path read as the protocol's per-table refusal, so
   * an auth proxy in front of the server reported every table as offering no directory access -- a
   * configuration error arriving table by table as a capability limit, leaving the integration
   * looking partly healthy instead of misconfigured.
   */
  @Test
  void aRedirectOnTheCredentialEndpointIsNotReadAsMissingDirectoryAccess() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.credentialFailure = DeltaSharingException.Failure.INVALID_REQUEST;
    fake.credentialStatus = 302;
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isNotEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  /** A 400 is still the protocol's own per-table refusal, which is what UNSUPPORTED is for. */
  @Test
  void aBadRequestOnTheCredentialEndpointIsStillATableRefusal() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.credentialFailure = DeltaSharingException.Failure.INVALID_REQUEST;
    fake.credentialStatus = 400;
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  /**
   * The two surfaces disagreeing is refused, not resolved in the load's favour. statedModes prefers
   * the listing and the vend reads the metaData action alone, so a server saying dir on one and url
   * on the other reconciled here and was refused on every read for the life of the table. Whichever
   * surface refuses, the load has to be no more permissive than the vend.
   */
  @Test
  void surfacesDisagreeingAboutAccessModesAreRefusedAtBothPaths() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.metadataAccessModes = List.of(AccessMode.URL);
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .as("load")
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage()).contains("url access only");
              });
      assertThatThrownBy(() -> client.vendStorageCredentials(events()))
          .as("vend")
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  /**
   * A location does not establish directory access. The protocol lets a url-only server state one,
   * so a legacy server that omits its access modes, names a location and refuses the credential
   * endpoint has to be refused at the load -- the early return on a stated location was skipping
   * the request the default setting exists to make, and the table reconciled with every read of it
   * failing.
   */
  @Test
  void anUnstatedTableIsAskedAboutEvenWhenTheListingNamesALocation() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    fake.credentialFailure = DeltaSharingException.Failure.INVALID_REQUEST;
    fake.credentialStatus = 400;
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
    assertThat(fake.credentialCalls).isPositive();
  }

  /**
   * The same shape with a server that does vend: the ask is what establishes directory access, and
   * the location a surface stated is still what the table reconciles with rather than the scope the
   * credential happened to name.
   */
  @Test
  void anUnstatedTableThatVendsKeepsTheLocationItsListingStated() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    fake.metadataLocation = Optional.empty();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
    assertThat(fake.credentialCalls).isPositive();
  }

  /**
   * Strict mode refuses without asking, which is the point of it. Asking for a location would be
   * the round trip the knob turns off, and answering would reconcile a table whose every vend then
   * fails -- a configuration choice turned into a runtime failure.
   */
  @Test
  void strictModeRefusesAnUnstatedTableAtTheLoadWithoutAsking() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), true, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                assertThat(failure.getMessage())
                    .contains(DeltaSharingCatalogClient.STRICT_ACCESS_MODES);
              });
    }
    assertThat(fake.credentialCalls).isZero();
  }

  /**
   * The reference-server shape again: neither surface states a location, so validation has only the
   * scope the credential itself named. Without that it refused every table before attempting a
   * read, on exactly the servers the ask default exists to support.
   */
  @Test
  void storageValidationFallsBackToTheVendedScopeWhenNoSurfaceStatesALocation() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    RecordingProbe probe = new RecordingProbe();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, probe)) {
      client.validateStorageAccess(
          events(),
          new VendedStorageCredentials(
              Map.of("s3.access-key-id", "AKIA"), LOCATION, Optional.empty()));
    }
    assertThat(probe.location).isEqualTo(LOCATION);
  }

  /** Better a named refusal than a table that materializes and cannot be opened. */
  @Test
  void aTableWhoseLocationNoSurfaceStatesIsRefused() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    fake.accessModes = List.of();
    fake.credentialNamesNoLocation = true;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOf(CatalogAccessException.class);
    }
  }

  /** The vend runs per resolve with a fresh client, so it must not page a schema to find one. */
  @Test
  void vendingDoesNotPageTheSchemaLookingForALocation() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.metadataLocation = Optional.empty();
    try (DeltaSharingCatalogClient client = client(fake)) {
      client.vendStorageCredentials(events());
    }
    assertThat(fake.listTablesCalls).isZero();
  }

  /** The knob governs the ambiguous case; a table stating dir is not ambiguous. */
  @Test
  void strictModeStillLoadsATableThatStatesDirectoryAccess() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), true, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
  }

  /**
   * The probe addresses S3 and the vend publishes an AWS session, so a table elsewhere would load,
   * reconcile, and fail at every scan -- the unreadable materialized table by another route.
   */
  @Test
  void aTableOnACloudThisProviderCannotReadIsRefused() {
    for (String elsewhere :
        List.of("abfss://c@a.dfs.core.windows.net/gold/events", "gs://sharing/gold/events")) {
      FakeSharingClient fake = new FakeSharingClient();
      fake.listedLocation = Optional.of(elsewhere);
      fake.metadataLocation = Optional.of(elsewhere);
      try (DeltaSharingCatalogClient client = client(fake)) {
        assertThatThrownBy(() -> client.loadTable(events()))
            .describedAs("%s", elsewhere)
            .isInstanceOfSatisfying(
                CatalogAccessException.class,
                failure -> {
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED);
                  assertThat(failure.getMessage()).contains("cannot read");
                });
      }
    }
  }

  /**
   * The refusal reaches operator logs through the throwable the reconciler records, and the
   * location is the server's: a query is a signature or a SAS token, userinfo is a password, and a
   * newline forges a log line. The scheme answers which cloud and can hold none of them.
   */
  @Test
  void refusingACloudNamesTheSchemeAndNotTheLocation() {
    String withSecrets = "abfss://user:pa55word@a.dfs.core.windows.net/gold/events?sig=SECRET-SAS";
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.of(withSecrets);
    fake.metadataLocation = Optional.of(withSecrets);
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure -> {
                assertThat(failure.getMessage()).contains("abfss");
                assertThat(failure.getMessage())
                    .doesNotContain("SECRET-SAS", "pa55word", "a.dfs.core.windows.net");
              });
    }
  }

  /**
   * The load's speculative ask is the same question the vend asks, so it is classified the same
   * way: a server with no credential endpoint is an unsupported capability, not a missing table.
   */
  @Test
  void aServerWithoutTheCredentialEndpointIsUnsupportedOnTheLoadPathToo() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    fake.credentialFailure = DeltaSharingException.Failure.NOT_FOUND;
    try (DeltaSharingCatalogClient client = client(fake)) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
    }
  }

  /**
   * The same answer from the same server, classified the same way whichever path asked. Gating this
   * on whether the table stated its modes would make one refusal a capability boundary on one path
   * and a configuration error on the other.
   */
  @Test
  void theSameCredentialRefusalIsClassifiedAlikeOnBothPaths() {
    for (DeltaSharingException.Failure refusal :
        List.of(
            DeltaSharingException.Failure.NOT_FOUND,
            DeltaSharingException.Failure.INVALID_REQUEST)) {
      FakeSharingClient onVend = new FakeSharingClient();
      onVend.accessModes = List.of(AccessMode.DIR);
      onVend.credentialFailure = refusal;
      onVend.credentialStatus = refusal == DeltaSharingException.Failure.NOT_FOUND ? 404 : 400;
      FakeSharingClient onLoad = new FakeSharingClient();
      onLoad.accessModes = List.of(AccessMode.DIR);
      onLoad.listedLocation = Optional.empty();
      onLoad.metadataLocation = Optional.empty();
      onLoad.credentialFailure = refusal;
      onLoad.credentialStatus = refusal == DeltaSharingException.Failure.NOT_FOUND ? 404 : 400;
      try (DeltaSharingCatalogClient vending = client(onVend);
          DeltaSharingCatalogClient loading = client(onLoad)) {
        assertThatThrownBy(() -> vending.vendStorageCredentials(events()))
            .describedAs("vend %s", refusal)
            .isInstanceOfSatisfying(
                CatalogAccessException.class,
                failure ->
                    assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
        assertThatThrownBy(() -> loading.loadTable(events()))
            .describedAs("load %s", refusal)
            .isInstanceOfSatisfying(
                CatalogAccessException.class,
                failure ->
                    assertThat(failure.code()).isEqualTo(CatalogAccessException.Code.UNSUPPORTED));
      }
    }
  }

  /**
   * A stated location has to be reachable with the credential the server answered with.
   *
   * <p>Where both are known and disjoint, reconciling the stated one materialises a table every
   * read of which is scoped elsewhere, and validation is optional so nothing else need catch it.
   * The vend makes this comparison for a location the metaData action states; this covers the
   * surface the vend cannot see.
   */
  @Test
  void aStatedLocationTheCredentialDoesNotReachIsRefused() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of("s3://sharing/gold/events");
    fake.metadataLocation = Optional.empty();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://sharing/silver/elsewhere",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThatThrownBy(() -> client.loadTable(events()))
          .isInstanceOfSatisfying(
              CatalogAccessException.class,
              failure ->
                  assertThat(failure.code())
                      .isEqualTo(CatalogAccessException.Code.CREDENTIAL_SCOPE_INVALID));
    }
  }

  /** A credential scoped above the stated location still reaches it, and is left alone. */
  @Test
  void aStatedLocationUnderABroaderCredentialScopeIsAccepted() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of();
    fake.metadataAccessModes = List.of();
    fake.listedLocation = Optional.of(LOCATION);
    fake.metadataLocation = Optional.empty();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://sharing/gold",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains(LOCATION);
    }
  }

  /**
   * A credential scoped to a bucket is the table's root.
   *
   * <p>The protocol answers a request naming no location with the table's main location, and a
   * Delta table may sit directly at a bucket root -- {@code deltaLogPrefix("")} answers {@code
   * _delta_log/} for that case. Whether the scope matches where the table lives is settled by
   * reading the Delta log under it, which validation does; the shape of the path cannot settle it,
   * and a load that refused on shape would skip tables validation reports as readable.
   */
  @Test
  void aCredentialScopedToAWholeBucketIsTakenAsTheTableRoot() {
    FakeSharingClient fake = new FakeSharingClient();
    fake.accessModes = List.of(AccessMode.DIR);
    fake.listedLocation = Optional.empty();
    fake.metadataLocation = Optional.empty();
    fake.credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            "s3://sharing-bucket/",
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    try (DeltaSharingCatalogClient client =
        new DeltaSharingCatalogClient(fake, Map.of(), false, new RecordingProbe())) {
      assertThat(client.loadTable(events()).storageLocation()).contains("s3://sharing-bucket/");
    }
  }

  /** A sharing server whose answers each test bends to the one shape it is about. */
  private static final class FakeSharingClient implements DeltaSharingClient {

    private Optional<String> tableId = Optional.of("tbl-1");
    private Optional<String> metadataId = Optional.of("tbl-1");
    private String schemaName = "gold";
    private Optional<String> shareId = Optional.of("share-uuid");
    private List<String> auxiliaryLocations = List.of();
    private List<String> listedAuxiliaryLocations = List.of();
    // Null means "same as the listing", which is what a delta-format server does: it states the
    // modes in both places. A test sets this only to make the two disagree.
    private List<AccessMode> metadataAccessModes;
    private List<AccessMode> accessModes = List.of(AccessMode.URL, AccessMode.DIR);
    private Optional<String> listedLocation = Optional.of(LOCATION);
    private Optional<String> metadataLocation = Optional.of(LOCATION);
    private DeltaSharingException.Failure failure;
    private DeltaSharingException.Failure credentialFailure;
    private int credentialStatus;
    private int credentialCalls;
    private boolean credentialNamesNoLocation;
    private TemporaryCredentials credentials =
        new TemporaryCredentials(
            CredentialCloud.AWS,
            LOCATION,
            Optional.of("AKIA"),
            Optional.of("secret"),
            Optional.of("session"),
            Optional.of(Instant.parse("2026-01-01T00:00:00Z")));
    private int listSharesCalls;
    private int listTablesCalls;
    private boolean closed;

    @Override
    public List<Share> listShares() {
      listSharesCalls++;
      raiseIfConfigured();
      return List.of(new Share("prod", Optional.of("share-1")));
    }

    @Override
    public List<Schema> listSchemas(String share) {
      raiseIfConfigured();
      return List.of(new Schema(share, schemaName));
    }

    @Override
    public List<Table> listTables(String share, String schema) {
      listTablesCalls++;
      raiseIfConfigured();
      List<Table> tables = new ArrayList<>();
      tables.add(
          new Table(
              share,
              schema,
              "events",
              tableId,
              shareId,
              listedLocation,
              listedAuxiliaryLocations,
              accessModes));
      return tables;
    }

    @Override
    public TableDescription describeTable(String share, String schema, String table) {
      raiseIfConfigured();
      return new TableDescription(
          new Protocol(1, List.of()),
          new TableMetadata(
              metadataId,
              Optional.of(table),
              "parquet",
              "{\"type\":\"struct\",\"fields\":[]}",
              List.of("day"),
              Map.of(),
              Optional.of(12L),
              metadataLocation,
              auxiliaryLocations,
              metadataAccessModes == null ? accessModes : metadataAccessModes),
          Optional.of(12L));
    }

    @Override
    public TemporaryCredentials temporaryTableCredentials(
        String share, String schema, String table, String location) {
      credentialCalls++;
      raiseIfConfigured();
      if (credentialFailure != null) {
        throw new DeltaSharingException(
            credentialFailure, credentialStatus, "configured for " + credentialFailure);
      }
      if (credentialNamesNoLocation) {
        // The record forbids a blank location, so this stands in for a server naming none by
        // answering somewhere the client's own filter will not accept as the table's.
        throw new DeltaSharingException(
            DeltaSharingException.Failure.INVALID_RESPONSE, 200, "named no location");
      }
      return credentials;
    }

    @Override
    public void close() {
      closed = true;
    }

    private void raiseIfConfigured() {
      if (failure != null) {
        throw new DeltaSharingException(failure, 0, "configured for " + failure);
      }
    }
  }
}
