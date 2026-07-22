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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.testsupport.SnapshotTestSupport;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests of {@link PossessionGate}: the pin-identity minting and the identity-only DECISION
 * the {@link UserObjectBundleService} driver delegates to. Exercises {@code scopedIdentity}
 * (deterministic and schema-scoped) and {@code identityOnly} (slim only when the client proved
 * possession) with a real {@link RelationBundleBuilder} over the shared fakes.
 */
class PossessionGateTest {

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("catalog")
          .setKind(ResourceKind.RK_CATALOG)
          .build();

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_X")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final EngineContext ENGINE = EngineContext.of("pg", "16.0");

  private final FakeCatalogOverlay overlay = new FakeCatalogOverlay();

  // A PinValidator that fails loudly if reached: these tests read schema/pins straight from the
  // fakes and never reach per-read pin validation. Mirrors the test-only service constructor.
  private final PinValidator throwingPinValidator =
      new PinValidator(null, RootRepairRequests.disabled()) {
        @Override
        public void validate(String correlationId, TablePin pin) {
          throw new IllegalStateException("pin validation not expected in this test");
        }
      };

  private PossessionGate gate;

  @BeforeEach
  void setUp() {
    overlay.clear();
    overlay.registerCatalog(CATALOG, "cat");
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    RelationBundleBuilder builder =
        new RelationBundleBuilder(
            overlay,
            ctxIgnored -> Optional.empty(),
            false,
            FlightEndpointRef.newBuilder().setHost("floecat-flight").setPort(80).build(),
            throwingPinValidator);
    gate = new PossessionGate(builder, "1");
  }

  private UserObjectBundleService.ResolvedRelation resolved() {
    RelationNode node = (RelationNode) overlay.resolve(TABLE).orElseThrow();
    return new UserObjectBundleService.ResolvedRelation(
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE))
            .build(),
        TABLE,
        node,
        QueryInput.newBuilder().setTableId(TABLE).build());
  }

  private static QueryContext pinnedWith(TablePin pin) {
    return QueryContext.builder()
        .queryId("q-1")
        .principal(
            PrincipalContext.newBuilder()
                .setAccountId("acct")
                .setSubject("tester")
                .setCorrelationId("cid")
                .build())
        .relationPins(SnapshotTestSupport.relationPins(pin).toByteArray())
        .createdAtMs(1)
        .expiresAtMs(1000)
        .state(QueryContext.State.ACTIVE)
        .version(1)
        .queryDefaultCatalogId(CATALOG)
        .build();
  }

  @Test
  void scopedIdentityIsDeterministicForSameRelationAndContext() {
    QueryContext ctx = pinnedWith(SnapshotTestSupport.blobBackedPin(TABLE, 1L, "fp-1"));

    Optional<RelationPinIdentity> first = gate.scopedIdentity("cid", resolved(), ctx, ENGINE);
    Optional<RelationPinIdentity> second = gate.scopedIdentity("cid", resolved(), ctx, ENGINE);

    assertThat(first).isPresent();
    assertThat(first.get().getTableBlobVersion()).isNotEmpty();
    // Same relation + same context → byte-identical identity and possession token.
    assertThat(second).isPresent();
    assertThat(second.get()).isEqualTo(first.get());
    assertThat(second.get().getTableBlobVersion()).isEqualTo(first.get().getTableBlobVersion());
  }

  @Test
  void scopedIdentityTokenIsSchemaScoped() {
    // Same table, same definition ref (blobBackedPin fixes table_blob_version), but two different
    // read-schema fingerprints — the shape of a snapshot-backed schema change. The possession token
    // must move with the schema scope, or a client would be served identity-only for a new schema.
    QueryContext ctxFp1 = pinnedWith(SnapshotTestSupport.blobBackedPin(TABLE, 1L, "fp-1"));
    QueryContext ctxFp2 = pinnedWith(SnapshotTestSupport.blobBackedPin(TABLE, 1L, "fp-2"));

    String token1 =
        gate.scopedIdentity("cid", resolved(), ctxFp1, ENGINE).get().getTableBlobVersion();
    String token2 =
        gate.scopedIdentity("cid", resolved(), ctxFp2, ENGINE).get().getTableBlobVersion();

    assertThat(token1).isNotEmpty();
    assertThat(token2).isNotEmpty();
    assertThat(token2)
        .as("a different read-schema fingerprint must move the possession token")
        .isNotEqualTo(token1);
  }

  @Test
  void identityOnlyServesSlimWhenClientHoldsTheToken() {
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder().setTableBlobVersion("v-token").build();
    UserObjectBundleService.TimingAccumulator timings =
        new UserObjectBundleService.TimingAccumulator();

    RelationInfo slim =
        gate.identityOnly(
            resolved(), Optional.of(identity), StatsProvider.NONE, Set.of("v-token"), timings);

    assertThat(slim).isNotNull();
    assertThat(slim.getRelationId()).isEqualTo(TABLE);
    assertThat(slim.getName().getName()).isEqualTo("x");
    assertThat(slim.getColumnsCount()).isZero();
    assertThat(slim.getPinIdentity().getTableBlobVersion()).isEqualTo("v-token");
  }

  @Test
  void identityOnlyIsNullWhenNoKnownVersions() {
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder().setTableBlobVersion("v-token").build();

    assertThat(
            gate.identityOnly(
                resolved(),
                Optional.of(identity),
                StatsProvider.NONE,
                Set.of(),
                new UserObjectBundleService.TimingAccumulator()))
        .isNull();
  }

  @Test
  void identityOnlyIsNullWhenTokenIsBlank() {
    // A blank token can never prove possession, even if the client advertises the empty string.
    RelationPinIdentity blank = RelationPinIdentity.newBuilder().setTableBlobVersion("").build();

    assertThat(
            gate.identityOnly(
                resolved(),
                Optional.of(blank),
                StatsProvider.NONE,
                Set.of(""),
                new UserObjectBundleService.TimingAccumulator()))
        .isNull();
  }

  @Test
  void identityOnlyIsNullWhenTokenNotAdvertised() {
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder().setTableBlobVersion("v-token").build();

    assertThat(
            gate.identityOnly(
                resolved(),
                Optional.of(identity),
                StatsProvider.NONE,
                Set.of("some-other-token"),
                new UserObjectBundleService.TimingAccumulator()))
        .isNull();
  }

  @Test
  void identityOnlyIsNullWhenScopedIdentityAbsent() {
    assertThat(
            gate.identityOnly(
                resolved(),
                Optional.empty(),
                StatsProvider.NONE,
                Set.of("v-token"),
                new UserObjectBundleService.TimingAccumulator()))
        .isNull();
  }
}
