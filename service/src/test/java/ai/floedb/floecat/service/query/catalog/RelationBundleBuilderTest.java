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
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.RelationPinSet;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableReferenceCandidate;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.catalog.impl.RootRepairRequests;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.ViewDecoration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests of {@link RelationBundleBuilder}: the "ResolvedRelation + config → RelationInfo"
 * assembly the {@link UserObjectBundleService} driver delegates to. Exercises what the driver-level
 * characterization suite could only reach indirectly — the column-failure taxonomy, the build-error
 * mapping, and the possession-token stamp — without a full stream.
 */
class RelationBundleBuilderTest {

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

  private final QueryContext ctx =
      QueryContext.builder()
          .queryId("q-1")
          .principal(
              PrincipalContext.newBuilder()
                  .setAccountId("acct")
                  .setSubject("tester")
                  .setCorrelationId("cid")
                  .build())
          .relationPins(RelationPinSet.getDefaultInstance().toByteArray())
          .createdAtMs(1)
          .expiresAtMs(1000)
          .state(QueryContext.State.ACTIVE)
          .version(1)
          .queryDefaultCatalogId(CATALOG)
          .build();

  // A PinValidator that fails loudly if reached: these tests use TABLE-kind graph nodes (not
  // UserTableNode), so buildRelation reads schema straight from the overlay and never validates a
  // pin. Mirrors the test-only UserObjectBundleService constructor.
  private final PinValidator throwingPinValidator =
      new PinValidator(null, RootRepairRequests.disabled()) {
        @Override
        public void validate(String correlationId, ai.floedb.floecat.query.rpc.TablePin pin) {
          throw new IllegalStateException("pin validation not expected in this test");
        }
      };

  @BeforeEach
  void setUp() {
    overlay.clear();
    overlay.registerCatalog(CATALOG, "cat");
  }

  private RelationBundleBuilder builder(
      EngineMetadataDecoratorProvider provider, boolean engineSpecificEnabled) {
    return new RelationBundleBuilder(
        overlay,
        provider,
        engineSpecificEnabled,
        FlightEndpointRef.newBuilder().setHost("floecat-flight").setPort(80).build(),
        throwingPinValidator);
  }

  private MetadataResolutionContext resolutionContext(StatsProvider stats) {
    return MetadataResolutionContext.of(overlay, CATALOG, ENGINE, stats);
  }

  private UserObjectBundleService.ResolvedRelation resolved(
      ResourceId id, TableReferenceCandidate candidate) {
    RelationNode node = (RelationNode) overlay.resolve(id).orElseThrow();
    return new UserObjectBundleService.ResolvedRelation(
        candidate,
        id,
        node,
        QueryInput.newBuilder().setTableId(id).build(),
        overlay.tableName(id).orElse(NameRef.newBuilder().setName(node.displayName()).build()));
  }

  private static TableReferenceCandidate fullCandidate() {
    return TableReferenceCandidate.newBuilder()
        .addCandidates(QueryInput.newBuilder().setTableId(TABLE))
        .build();
  }

  @Test
  void buildAssemblesRelationInfoWithColumnsStatsKindAndOrigin() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    StatsProvider stats =
        new StatsProvider() {
          @Override
          public Optional<TableStatsView> tableStats(ResourceId tableId) {
            return Optional.of(
                new TableStatsView() {
                  @Override
                  public ResourceId tableId() {
                    return tableId;
                  }

                  @Override
                  public long snapshotId() {
                    return 7L;
                  }

                  @Override
                  public OptionalLong rowCountValue() {
                    return OptionalLong.of(42L);
                  }

                  @Override
                  public OptionalLong totalSizeBytesValue() {
                    return OptionalLong.of(4096L);
                  }
                });
          }
        };

    RelationBundleBuilder builder = builder(ctxIgnored -> Optional.empty(), false);
    RelationBundleBuilder.BuildResult result =
        builder.build(
            "cid",
            resolved(TABLE, fullCandidate()),
            ctx,
            resolutionContext(stats),
            stats,
            Optional.empty());

    assertThat(result.isSuccess()).isTrue();
    RelationInfo info = result.info();
    assertThat(info.getRelationId()).isEqualTo(TABLE);
    assertThat(info.getName().getName()).isEqualTo("x");
    assertThat(info.getKind()).isEqualTo(RelationKind.RELATION_KIND_TABLE);
    assertThat(info.getOrigin()).isEqualTo(Origin.ORIGIN_USER);
    assertThat(info.getColumnsCount()).isEqualTo(1);
    assertThat(info.getColumns(0).getStatus()).isEqualTo(ColumnStatus.COLUMN_STATUS_OK);
    assertThat(info.hasStats()).isTrue();
    assertThat(info.getStats().getRowCount()).isEqualTo(42L);
    assertThat(info.getStats().getTotalSizeBytes()).isEqualTo(4096L);
  }

  @Test
  void buildStampsSystemTableFlightEndpointAndBackendKind() {
    ResourceId sysId =
        ResourceId.newBuilder()
            .setAccountId("sys")
            .setId("SYS_STORAGE")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    FlightEndpointRef endpoint =
        FlightEndpointRef.newBuilder().setHost("node-declared").setPort(4111).setTls(true).build();
    SystemTableNode.StorageSystemTableNode node =
        new SystemTableNode.StorageSystemTableNode(
            sysId,
            1L,
            "",
            sysId.getId(),
            ResourceId.getDefaultInstance(),
            List.of(),
            Map.of(),
            Map.of(),
            "sys://path",
            "",
            endpoint);
    overlay.registerRelation(
        sysId,
        node,
        UserObjectBundleTestSupport.schemaFor("sys_col"),
        NameRef.newBuilder().setCatalog("sys").setName("storage").build());

    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(sysId))
            .build();

    RelationBundleBuilder builder = builder(ctxIgnored -> Optional.empty(), false);
    RelationBundleBuilder.BuildResult result =
        builder.build(
            "cid",
            resolved(sysId, candidate),
            ctx,
            resolutionContext(StatsProvider.NONE),
            StatsProvider.NONE,
            Optional.empty());

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.info().getOrigin()).isEqualTo(Origin.ORIGIN_BUILTIN);
    assertThat(result.info().hasFlightEndpoint()).isTrue();
    assertThat(result.info().getFlightEndpoint().getHost()).isEqualTo("node-declared");
    assertThat(result.info().getFlightEndpoint().getPort()).isEqualTo(4111);
  }

  @Test
  void buildEmitsViewDefinitionDecoration() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("VIEW_X")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    List<SchemaColumn> schema =
        List.of(SchemaColumn.newBuilder().setId(1).setName("answer").setOrdinal(1).build());
    ViewNode view =
        new ViewNode(
            viewId,
            "blob://test/view-x",
            CATALOG,
            ResourceId.getDefaultInstance(),
            "view_x",
            "SELECT 42 AS answer",
            "pg",
            schema,
            List.of(),
            List.of(),
            GraphNodeOrigin.USER,
            Map.of(),
            Optional.empty(),
            Map.of(),
            Map.of());
    overlay.registerRelation(
        viewId, view, schema, NameRef.newBuilder().setCatalog("cat").setName("view_x").build());

    EngineMetadataDecoratorProvider provider =
        ignored ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateView(EngineContext ctx, ViewDecoration decoration) {
                    decoration
                        .viewBuilder()
                        .addEngineSpecific(
                            EngineSpecific.newBuilder().setPayloadType("test.view-decoration"));
                  }
                });
    TableReferenceCandidate candidate =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setViewId(viewId))
            .build();

    RelationInfo info =
        builder(provider, true)
            .build(
                "cid",
                resolved(viewId, candidate),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty())
            .info();

    assertThat(info.getViewDefinition().getEngineSpecificList())
        .extracting(EngineSpecific::getPayloadType)
        .containsExactly("test.view-decoration");
  }

  @Test
  void decorateColumnsMarksSchemaMismatchColumnsFailed() {
    // pruned smaller than the served columns ⇒ every column is failed as a schema mismatch.
    RelationBundleBuilder builder = builder(ctxIgnored -> Optional.empty(), false);
    List<ColumnInfo> columns =
        List.of(
            ColumnInfo.newBuilder().setId(11).setName("c1").setOrdinal(1).build(),
            ColumnInfo.newBuilder().setId(12).setName("c2").setOrdinal(2).build());
    List<SchemaColumn> pruned =
        List.of(SchemaColumn.newBuilder().setId(11).setName("c1").setOrdinal(1).build());

    List<ColumnResult> results =
        builder.decorateColumns(
            columns, pruned, null, Optional.empty(), EngineContext.of("pg", "16.0"), true, TABLE);

    assertThat(results).hasSize(2);
    assertThat(results)
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.hasFailure()
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_SCHEMA_MISMATCH
                    && c.getFailure().getDetailsMap().get("relation_id").equals(TABLE.getId())
                    && c.getFailure().getMessage().contains("Column/schema mismatch"));
  }

  @Test
  void columnFailureTaxonomyPayloadRequiredMissing() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());

    // Decorates without emitting the required engine payload → PAYLOAD_REQUIRED_MISSING.
    RelationBundleBuilder builder =
        builder(ctxIgnored -> Optional.of(new NoPayloadDecorator()), true);
    RelationInfo info =
        builder
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty())
            .info();

    assertThat(info.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING);
  }

  @Test
  void columnFailureTaxonomyTypeNotSupported() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());

    EngineMetadataDecoratorProvider provider =
        ctxIgnored ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    throw new DecorationException(
                        ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED,
                        "raw internal message");
                  }
                });
    RelationInfo info =
        builder(provider, true)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty())
            .info();

    assertThat(info.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED);
    // The raw decorator message is replaced by the user-facing text (not leaked).
    assertThat(info.getColumns(0).getFailure().getMessage())
        .isEqualTo("This column type is not supported by the engine metadata decorator.");
  }

  @Test
  void columnFailureTaxonomyEngineExtensionCode() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());

    EngineMetadataDecoratorProvider provider =
        ctxIgnored ->
            Optional.of(
                new EngineMetadataDecorator() {
                  @Override
                  public void decorateColumn(EngineContext ec, ColumnDecoration columnDecoration) {
                    throw new DecorationException(1201, "engine extension failure");
                  }
                });
    RelationInfo info =
        builder(provider, true)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty())
            .info();

    assertThat(info.getColumnsList())
        .allMatch(
            c ->
                c.getStatus() == ColumnStatus.COLUMN_STATUS_FAILED
                    && c.getFailure().getCode()
                        == ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION
                    && c.getFailure().getExtensionCodeValue() == 1201
                    && c.getFailure().getMessage().equals("engine extension failure"));
  }

  @Test
  void buildFailureMapsToBuildErrorWithResourceId() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    overlay.failSchemaFor(TABLE);

    RelationBundleBuilder.BuildResult result =
        builder(ctxIgnored -> Optional.empty(), false)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty());

    assertThat(result.isSuccess()).isFalse();
    RelationBundleBuilder.BuildError error = result.error();
    assertThat(error.code()).isEqualTo("catalog_bundle.build_failed");
    assertThat(error.resourceId()).isEqualTo(TABLE.getId());
    assertThat(error.message()).contains("schema unavailable");
    assertThat(result.timings()).isNotNull();
  }

  @Test
  void nullOverlaySchemaBuildsAnEmptyColumnList() {
    // Generic RelationNode implementations use the overlay schema path. A connector that has no
    // schema must produce a valid empty payload rather than a build-error resolution.
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    overlay.returnNullSchemaFor(TABLE);

    RelationBundleBuilder.BuildResult result =
        builder(ctxIgnored -> Optional.empty(), false)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.empty());

    assertThat(result.isSuccess()).isTrue();
    assertThat(result.info().getColumnsList()).isEmpty();
  }

  @Test
  void possessionTokenStampedForFullCacheablePayload() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder()
            .setTableBlobVersion("v-token")
            .setPinFingerprint("fp")
            .build();

    RelationInfo info =
        builder(ctxIgnored -> Optional.empty(), false)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.of(identity))
            .info();

    assertThat(info.hasPinIdentity()).isTrue();
    assertThat(info.getPinIdentity().getPinFingerprint()).isEqualTo("fp");
    assertThat(info.getPinIdentity().getTableBlobVersion())
        .as("a full, fully-decorated payload keeps the possession token")
        .isEqualTo("v-token");
  }

  @Test
  void possessionTokenBlankedForProjectedPayloadButIdentityPreserved() {
    overlay.registerTable(
        TABLE,
        List.of(
            SchemaColumn.newBuilder().setName("id_x").setNullable(true).build(),
            SchemaColumn.newBuilder().setName("payload_x").setNullable(true).build()),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder()
            .setTableBlobVersion("v-token")
            .setPinFingerprint("fp")
            .build();
    TableReferenceCandidate projected =
        TableReferenceCandidate.newBuilder()
            .addCandidates(QueryInput.newBuilder().setTableId(TABLE))
            .addInitialColumns("id_x")
            .build();

    RelationInfo info =
        builder(ctxIgnored -> Optional.empty(), false)
            .build(
                "cid",
                resolved(TABLE, projected),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.of(identity))
            .info();

    assertThat(info.getColumnsCount()).isEqualTo(1);
    // The data identity survives on a projected reply; only the payload-scoped token is withheld.
    assertThat(info.hasPinIdentity()).isTrue();
    assertThat(info.getPinIdentity().getPinFingerprint()).isEqualTo("fp");
    assertThat(info.getPinIdentity().getTableBlobVersion())
        .as("a projected payload must not advertise the full-schema possession token")
        .isEmpty();
  }

  @Test
  void possessionTokenBlankedWhenColumnDecorationFails() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder()
            .setTableBlobVersion("v-token")
            .setPinFingerprint("fp")
            .build();

    // Full schema, but a FAILED column makes the payload non-cacheable → token blanked, data kept.
    RelationInfo info =
        builder(ctxIgnored -> Optional.of(new NoPayloadDecorator()), true)
            .build(
                "cid",
                resolved(TABLE, fullCandidate()),
                ctx,
                resolutionContext(StatsProvider.NONE),
                StatsProvider.NONE,
                Optional.of(identity))
            .info();

    assertThat(info.getColumns(0).getStatus()).isEqualTo(ColumnStatus.COLUMN_STATUS_FAILED);
    assertThat(info.hasPinIdentity()).isTrue();
    assertThat(info.getPinIdentity().getPinFingerprint()).isEqualTo("fp");
    assertThat(info.getPinIdentity().getTableBlobVersion()).isEmpty();
  }

  @Test
  void buildIdentityOnlyAssemblesSlimPayloadWithoutColumns() {
    overlay.registerTable(
        TABLE,
        UserObjectBundleTestSupport.schemaFor("id_x"),
        NameRef.newBuilder().setCatalog("cat").setName("x").build());
    RelationPinIdentity identity =
        RelationPinIdentity.newBuilder().setTableBlobVersion("v-token").build();
    UserObjectBundleService.TimingAccumulator timings =
        new UserObjectBundleService.TimingAccumulator();

    RelationInfo info =
        builder(ctxIgnored -> Optional.empty(), false)
            .buildIdentityOnly(
                resolved(TABLE, fullCandidate()),
                Optional.of(identity),
                StatsProvider.NONE,
                timings);

    assertThat(info.getRelationId()).isEqualTo(TABLE);
    assertThat(info.getName().getName()).isEqualTo("x");
    assertThat(info.getColumnsCount()).isZero();
    assertThat(info.getPinIdentity().getTableBlobVersion()).isEqualTo("v-token");
  }

  /** Runs column decoration but never emits the required engine payload. */
  private static final class NoPayloadDecorator implements EngineMetadataDecorator {
    @Override
    public void decorateColumn(EngineContext ctx, ColumnDecoration columnDecoration) {
      // Emit a payload for a DIFFERENT engine kind so hasRequiredEnginePayload stays false for the
      // requesting engine, marking the column PAYLOAD_REQUIRED_MISSING.
      columnDecoration
          .builder()
          .addEngineSpecific(
              EngineSpecific.newBuilder().setEngineKind("other-engine").setPayloadType("").build());
    }
  }
}
