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

package ai.floedb.floecat.service.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ObjectCacheTest {

  @Test
  void mappedSchemaIdentityIncludesEveryMappingInput() {
    UserTableNode base = table("account", "table", TableFormat.TF_ICEBERG, List.of("part"));
    String schema = "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[]}";

    String identity = ObjectCache.schemaIdentity(base, schema);

    assertThat(ObjectCache.schemaIdentity(base, schema)).isEqualTo(identity);
    assertThat(ObjectCache.schemaIdentity(base, schema + " ")).isNotEqualTo(identity);
    assertThat(
            ObjectCache.schemaIdentity(
                table("account", "table", TableFormat.TF_DELTA, List.of("part")), schema))
        .isNotEqualTo(identity);
    assertThat(
            ObjectCache.schemaIdentity(
                table("account", "table", TableFormat.TF_ICEBERG, List.of("other")), schema))
        .isNotEqualTo(identity);
    assertThat(
            ObjectCache.schemaIdentity(
                table(
                    "account",
                    "table",
                    TableFormat.TF_ICEBERG,
                    ColumnIdAlgorithm.CID_PATH_ORDINAL,
                    List.of("part")),
                schema))
        .isNotEqualTo(identity);
  }

  @Test
  void mappedSchemaIsSharedAcrossTablesWithEquivalentMappingInputs() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    String schema =
        "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[{\"id\":1,\"name\":\"id\","
            + "\"required\":true,\"type\":\"long\"}]}";

    SchemaDescriptor first =
        cache.mappedSchema(table("account", "first", TableFormat.TF_ICEBERG, List.of()), schema);
    SchemaDescriptor second =
        cache.mappedSchema(table("account", "second", TableFormat.TF_ICEBERG, List.of()), schema);

    assertThat(second).isSameAs(first);
  }

  @Test
  void pinnedSchemaChecksItsIdentityBeforeLoadingTheBackingObjects() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    TablePin pin = pin(table.id(), "definition-a", "constraints", "schema-a");
    AtomicInteger loads = new AtomicInteger();
    String schema =
        "{\"type\":\"struct\",\"schema-id\":1,\"fields\":[{\"id\":1,\"name\":\"id\","
            + "\"required\":true,\"type\":\"long\"}]}";
    CatalogGraphView graphView = schemaGraph(table, schema, loads);

    SchemaDescriptor first = cache.pinnedSchema("correlation", pin, graphView);
    SchemaDescriptor second = cache.pinnedSchema("correlation", pin, graphView);

    assertThat(second).isSameAs(first);
    assertThat(loads).hasValue(1);
  }

  @Test
  void pinnedSchemaIdentityIncludesDefinitionInputs() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    AtomicInteger loads = new AtomicInteger();
    CatalogGraphView graphView = schemaGraph(table, table.schemaJson(), loads);

    cache.pinnedSchema(
        "correlation", pin(table.id(), "definition-a", "constraints", "schema"), graphView);
    cache.pinnedSchema(
        "correlation", pin(table.id(), "definition-b", "constraints", "schema"), graphView);

    assertThat(loads).hasValue(2);
  }

  @Test
  void constraintsAreContentKeyedAndAbsenceIsNotCached() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    ResourceId tableId = tableId("account", "table");
    AtomicInteger loads = new AtomicInteger();
    SnapshotConstraints constraints =
        SnapshotConstraints.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(7)
            .addConstraints(ConstraintDefinition.newBuilder().setName("pk"))
            .build();

    assertThat(
            cache.constraints(
                tableId,
                "blob://constraints/one",
                () -> {
                  loads.incrementAndGet();
                  return Optional.of(constraints);
                }))
        .containsSame(constraints);
    assertThat(
            cache.constraints(
                tableId,
                "blob://constraints/one",
                () -> {
                  loads.incrementAndGet();
                  return Optional.empty();
                }))
        .containsSame(constraints);
    assertThat(loads).hasValue(1);

    assertThat(
            cache.constraints(
                tableId,
                "blob://constraints/missing",
                () -> {
                  loads.incrementAndGet();
                  return Optional.empty();
                }))
        .isEmpty();
    assertThat(
            cache.constraints(
                tableId,
                "blob://constraints/missing",
                () -> {
                  loads.incrementAndGet();
                  return Optional.empty();
                }))
        .isEmpty();
    assertThat(loads).hasValue(3);
  }

  @Test
  void accountEvictionDropsEveryObjectKindWithoutTouchingAnotherAccount() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    ResourceId first = tableId("first", "table");
    ResourceId second = tableId("second", "table");
    AtomicInteger firstLoads = new AtomicInteger();
    AtomicInteger secondLoads = new AtomicInteger();

    cache.snapshotFacts(
        first,
        1,
        "first-generation",
        () -> {
          firstLoads.incrementAndGet();
          return Optional.of(
              new ObjectCache.SnapshotFacts(OptionalLong.of(1), OptionalLong.empty()));
        });
    cache.snapshotFacts(
        second,
        1,
        "second-generation",
        () -> {
          secondLoads.incrementAndGet();
          return Optional.of(
              new ObjectCache.SnapshotFacts(OptionalLong.of(2), OptionalLong.empty()));
        });

    cache.evictAccount("first");

    cache.snapshotFacts(
        first,
        1,
        "first-generation",
        () -> {
          firstLoads.incrementAndGet();
          return Optional.of(
              new ObjectCache.SnapshotFacts(OptionalLong.of(3), OptionalLong.empty()));
        });
    cache.snapshotFacts(
        second,
        1,
        "second-generation",
        () -> {
          secondLoads.incrementAndGet();
          return Optional.of(
              new ObjectCache.SnapshotFacts(OptionalLong.of(4), OptionalLong.empty()));
        });

    assertThat(firstLoads).hasValue(2);
    assertThat(secondLoads).hasValue(1);
  }

  @Test
  void liveSnapshotFactsAreReadThroughAndFrozenFactsAreIdentityKeyed() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    ResourceId tableId = tableId("account", "table");
    AtomicInteger loads = new AtomicInteger();

    assertThat(
            cache.snapshotFacts(
                tableId,
                1,
                "",
                () ->
                    Optional.of(
                        new ObjectCache.SnapshotFacts(
                            OptionalLong.of(loads.incrementAndGet()), OptionalLong.empty()))))
        .get()
        .extracting(facts -> facts.rowCount().getAsLong())
        .isEqualTo(1L);

    assertThat(
            cache.snapshotFacts(
                tableId,
                1,
                "generation-1",
                () ->
                    Optional.of(
                        new ObjectCache.SnapshotFacts(OptionalLong.of(7L), OptionalLong.of(70L)))))
        .get()
        .extracting(facts -> facts.rowCount().getAsLong())
        .isEqualTo(7L);
    assertThat(loads).hasValue(1);

    assertThat(
            cache.snapshotFacts(
                tableId,
                1,
                "",
                () ->
                    Optional.of(
                        new ObjectCache.SnapshotFacts(
                            OptionalLong.of(loads.incrementAndGet()), OptionalLong.empty()))))
        .get()
        .extracting(facts -> facts.rowCount().getAsLong())
        .isEqualTo(2L);
  }

  @Test
  void snapshotFactsKeepPinnedGenerationsSeparateFromEachOtherAndLive() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    ResourceId tableId = tableId("account", "table");
    AtomicInteger loads = new AtomicInteger();

    assertThat(rowCount(cache.snapshotFacts(tableId, 1, "gen-a", () -> facts(loads, 10))))
        .isEqualTo(10L);
    assertThat(rowCount(cache.snapshotFacts(tableId, 1, "gen-b", () -> facts(loads, 20))))
        .isEqualTo(20L);
    assertThat(rowCount(cache.snapshotFacts(tableId, 1, "", () -> facts(loads, 30))))
        .isEqualTo(30L);

    assertThat(rowCount(cache.snapshotFacts(tableId, 1, "gen-a", () -> facts(loads, 40))))
        .isEqualTo(10L);
    assertThat(rowCount(cache.snapshotFacts(tableId, 1, "", () -> facts(loads, 50))))
        .isEqualTo(50L);
    assertThat(loads).hasValue(4);
  }

  @Test
  void snapshotFactsRequireExplicitAbsence() {
    assertThatNullPointerException()
        .isThrownBy(() -> new ObjectCache.SnapshotFacts(null, OptionalLong.empty()))
        .withMessage("rowCount");
    assertThatNullPointerException()
        .isThrownBy(() -> new ObjectCache.SnapshotFacts(OptionalLong.empty(), null))
        .withMessage("totalSizeBytes");
  }

  private static Optional<ObjectCache.SnapshotFacts> facts(AtomicInteger loads, long rowCount) {
    loads.incrementAndGet();
    return Optional.of(
        new ObjectCache.SnapshotFacts(OptionalLong.of(rowCount), OptionalLong.empty()));
  }

  private static long rowCount(Optional<ObjectCache.SnapshotFacts> facts) {
    return facts.orElseThrow().rowCount().orElseThrow();
  }

  @Test
  void relationTemplateIsSharedAcrossRequestSpecificUse() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    RelationInfo template = RelationInfo.newBuilder().setRelationId(table.id()).build();
    ObjectCache.RelationObject relation =
        new ObjectCache.RelationObject(template, SchemaDescriptor.getDefaultInstance());
    AtomicInteger loads = new AtomicInteger();

    assertThat(cache.tableRelation(table, Optional.empty(), () -> loaded(loads, relation)))
        .isSameAs(relation);
    assertThat(cache.tableRelation(table, Optional.empty(), () -> loaded(loads, relation)))
        .isSameAs(relation);
    assertThat(loads).hasValue(1);
  }

  @Test
  void relationIdentityTracksOnlyInputsThatChangeTheCachedPayload() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    ObjectCache.RelationObject relation =
        new ObjectCache.RelationObject(
            RelationInfo.newBuilder().setRelationId(table.id()).build(),
            SchemaDescriptor.getDefaultInstance());
    AtomicInteger loads = new AtomicInteger();

    cache.tableRelation(
        table,
        Optional.of(pin(table.id(), "definition-a", "constraints-a", "schema-a")),
        () -> loaded(loads, relation));
    cache.tableRelation(
        table,
        Optional.of(pin(table.id(), "definition-b", "constraints-a", "schema-a")),
        () -> loaded(loads, relation));
    cache.tableRelation(
        table,
        Optional.of(pin(table.id(), "definition-a", "constraints-b", "schema-a")),
        () -> loaded(loads, relation));
    cache.tableRelation(
        table,
        Optional.of(pin(table.id(), "definition-a", "constraints-a", "schema-b")),
        () -> loaded(loads, relation));

    // Constraints are served from their own content-keyed entry. They are not part of RelationInfo,
    // so changing only that ref must keep the expensive relation template hot.
    assertThat(loads).hasValue(3);
  }

  @Test
  void legacyPinnedRelationFallsBackToTheImmutableSnapshotIdentity() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    ObjectCache.RelationObject relation =
        new ObjectCache.RelationObject(
            RelationInfo.newBuilder().setRelationId(table.id()).build(),
            SchemaDescriptor.getDefaultInstance());
    AtomicInteger loads = new AtomicInteger();
    TablePin legacy =
        pin(table.id(), "definition", "constraints", "ignored").toBuilder()
            .clearSchemaFingerprint()
            .clearSnapshotBlobVersion()
            .setSnapshotBlobUri("snapshot-a")
            .build();

    cache.tableRelation(table, Optional.of(legacy), () -> loaded(loads, relation));
    cache.tableRelation(
        table,
        Optional.of(legacy.toBuilder().setSnapshotBlobUri("snapshot-b").build()),
        () -> loaded(loads, relation));

    assertThat(loads).hasValue(2);
  }

  private static <T> T loaded(AtomicInteger loads, T value) {
    loads.incrementAndGet();
    return value;
  }

  private static CatalogGraphView schemaGraph(
      UserTableNode table, String schemaJson, AtomicInteger loads) {
    CatalogGraphView graphView = mock(CatalogGraphView.class);
    when(graphView.schemaFor(anyString(), eq(table.id()), any(), anyString(), anyString()))
        .thenAnswer(
            ignored -> {
              loads.incrementAndGet();
              return new CatalogGraphView.SchemaResolution(table, schemaJson);
            });
    return graphView;
  }

  private static TablePin pin(
      ResourceId tableId, String definition, String constraints, String schema) {
    return TablePin.newBuilder()
        .setTableId(tableId)
        .setTableBlobUri(definition)
        .setConstraintsRefUri(constraints)
        .setSchemaFingerprint(schema)
        .build();
  }

  private static UserTableNode table(
      String accountId, String id, TableFormat format, List<String> partitionKeys) {
    return table(accountId, id, format, ColumnIdAlgorithm.CID_FIELD_ID, partitionKeys);
  }

  private static UserTableNode table(
      String accountId,
      String id,
      TableFormat format,
      ColumnIdAlgorithm columnIdAlgorithm,
      List<String> partitionKeys) {
    ResourceId tableId = tableId(accountId, id);
    return new UserTableNode(
        tableId,
        "blob://table/" + id,
        resource(accountId, "catalog", ResourceKind.RK_CATALOG),
        resource(accountId, "namespace", ResourceKind.RK_NAMESPACE),
        id,
        format,
        columnIdAlgorithm,
        "",
        Map.of(),
        partitionKeys,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.of());
  }

  private static ResourceId tableId(String accountId, String id) {
    return resource(accountId, id, ResourceKind.RK_TABLE);
  }

  private static ResourceId resource(String accountId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(accountId).setId(id).setKind(kind).build();
  }
}
