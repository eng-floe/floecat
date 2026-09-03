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

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationStats;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        "generation",
        () -> {
          firstLoads.incrementAndGet();
          return Optional.of(RelationStats.newBuilder().setRowCount(1).build());
        });
    cache.snapshotFacts(
        second,
        1,
        "generation",
        () -> {
          secondLoads.incrementAndGet();
          return Optional.of(RelationStats.newBuilder().setRowCount(2).build());
        });

    cache.evictAccount("first");

    cache.snapshotFacts(
        first,
        1,
        "generation",
        () -> {
          firstLoads.incrementAndGet();
          return Optional.of(RelationStats.newBuilder().setRowCount(3).build());
        });
    cache.snapshotFacts(
        second,
        1,
        "generation",
        () -> {
          secondLoads.incrementAndGet();
          return Optional.of(RelationStats.newBuilder().setRowCount(4).build());
        });

    assertThat(firstLoads).hasValue(2);
    assertThat(secondLoads).hasValue(1);
  }

  @Test
  void relationTemplateIsSharedAcrossRequestSpecificUse() {
    ObjectCache cache = new ObjectCache(1024 * 1024, CacheEvents.none(), true);
    UserTableNode table = table("account", "table", TableFormat.TF_ICEBERG, List.of());
    ObjectCache.MappedSchema schema =
        new ObjectCache.MappedSchema("schema", SchemaDescriptor.getDefaultInstance());
    ObjectCache.RelationTemplate template =
        new ObjectCache.RelationTemplate(
            RelationInfo.newBuilder().setRelationId(table.id()).build(), schema.descriptor());
    AtomicInteger loads = new AtomicInteger();

    assertThat(cache.tableRelation(table, schema, () -> loaded(loads, template)))
        .isSameAs(template);
    assertThat(cache.tableRelation(table, schema, () -> loaded(loads, template)))
        .isSameAs(template);
    assertThat(loads).hasValue(1);
  }

  private static <T> T loaded(AtomicInteger loads, T value) {
    loads.incrementAndGet();
    return value;
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
