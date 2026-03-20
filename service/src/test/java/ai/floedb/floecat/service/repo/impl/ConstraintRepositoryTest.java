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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConstraintRepositoryTest {

  private ConstraintRepository repo;
  private ResourceId tableId;

  @BeforeEach
  void setUp() {
    repo = new ConstraintRepository(new InMemoryPointerStore(), new InMemoryBlobStore());
    tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();
  }

  @Test
  void upsertGetDeleteRoundTrip() {
    SnapshotConstraints payload =
        constraintsForSnapshot(
            tableId, 101L, List.of(definition("pk_orders", ConstraintType.CT_PRIMARY_KEY)));

    assertTrue(repo.putSnapshotConstraints(tableId, 101L, payload));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 101L).orElseThrow();
    assertEquals(101L, fetched.getSnapshotId());
    assertEquals(1, fetched.getConstraintsCount());
    assertEquals("pk_orders", fetched.getConstraints(0).getName());

    assertTrue(repo.deleteSnapshotConstraints(tableId, 101L));
    assertTrue(repo.getSnapshotConstraints(tableId, 101L).isEmpty());
    assertEquals(0, repo.countSnapshotConstraints(tableId));
  }

  @Test
  void listAndCountBySnapshotSupportPagination() {
    for (long snapshotId : List.of(10L, 20L, 30L)) {
      repo.putSnapshotConstraints(
          tableId,
          snapshotId,
          constraintsForSnapshot(
              tableId,
              snapshotId,
              List.of(definition("c" + snapshotId, ConstraintType.CT_UNIQUE))));
    }

    assertEquals(3, repo.countSnapshotConstraints(tableId));

    List<Long> seen = new ArrayList<>();
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      List<SnapshotConstraints> page = repo.listSnapshotConstraints(tableId, 1, token, next);
      assertEquals(1, page.size());
      seen.add(page.get(0).getSnapshotId());
      token = next.toString();
    } while (!token.isEmpty());

    assertEquals(List.of(10L, 20L, 30L), seen);
  }

  @Test
  void upsertIsIdempotentAndOverwritesWhenPayloadChanges() {
    SnapshotConstraints first =
        constraintsForSnapshot(
            tableId,
            200L,
            List.of(definition("pk_t", ConstraintType.CT_PRIMARY_KEY)),
            List.of("k1", "k2"));
    SnapshotConstraints sameSemanticDifferentPropertyOrder =
        constraintsForSnapshot(
            tableId,
            200L,
            List.of(definition("pk_t", ConstraintType.CT_PRIMARY_KEY)),
            List.of("k2", "k1"));

    assertTrue(repo.putSnapshotConstraints(tableId, 200L, first));
    assertFalse(repo.putSnapshotConstraints(tableId, 200L, sameSemanticDifferentPropertyOrder));

    SnapshotConstraints changed =
        constraintsForSnapshot(
            tableId,
            200L,
            List.of(
                definition("pk_t", ConstraintType.CT_PRIMARY_KEY),
                definition("uq_t_name", ConstraintType.CT_UNIQUE)),
            List.of("k2", "k1"));

    assertTrue(repo.putSnapshotConstraints(tableId, 200L, changed));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 200L).orElseThrow();
    assertEquals(2, fetched.getConstraintsCount());
    assertEquals(1, repo.countSnapshotConstraints(tableId));
  }

  @Test
  void upsertIsIdempotentWhenConstraintOrderAndNestedPropertyOrderDiffer() {
    ConstraintDefinition firstPk =
        definitionWithProperties("pk_t", ConstraintType.CT_PRIMARY_KEY, List.of("a", "b"));
    ConstraintDefinition firstCheck =
        definitionWithProperties("ck_t", ConstraintType.CT_CHECK, List.of("x", "y")).toBuilder()
            .setCheckExpression("id > 0")
            .build();

    SnapshotConstraints first =
        constraintsForSnapshot(tableId, 210L, List.of(firstPk, firstCheck), List.of("k1", "k2"));

    ConstraintDefinition replayCheck =
        definitionWithProperties("ck_t", ConstraintType.CT_CHECK, List.of("y", "x")).toBuilder()
            .setCheckExpression("id > 0")
            .build();
    ConstraintDefinition replayPk =
        definitionWithProperties("pk_t", ConstraintType.CT_PRIMARY_KEY, List.of("b", "a"));

    SnapshotConstraints replay =
        constraintsForSnapshot(tableId, 210L, List.of(replayCheck, replayPk), List.of("k2", "k1"));

    assertTrue(repo.putSnapshotConstraints(tableId, 210L, first));
    assertFalse(repo.putSnapshotConstraints(tableId, 210L, replay));
    assertEquals(2, repo.getSnapshotConstraints(tableId, 210L).orElseThrow().getConstraintsCount());
  }

  @Test
  void upsertUsesRepositoryIdentityOverPayloadIdentity() {
    ResourceId wrongTableId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setId(UUID.randomUUID().toString())
            .setKind(ResourceKind.RK_TABLE)
            .build();

    SnapshotConstraints payload =
        constraintsForSnapshot(
            wrongTableId, 201L, List.of(definition("pk_t", ConstraintType.CT_PRIMARY_KEY)));

    assertTrue(repo.putSnapshotConstraints(tableId, 200L, payload));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 200L).orElseThrow();
    assertEquals(tableId, fetched.getTableId());
    assertEquals(200L, fetched.getSnapshotId());
    assertTrue(repo.getSnapshotConstraints(tableId, 201L).isEmpty());
  }

  @Test
  void upsertTreatsReferencedTableAsMaterialChange() {
    SnapshotConstraints first =
        constraintsForSnapshot(
            tableId,
            220L,
            List.of(
                definitionWithReferencedTable(
                    "fk_customer",
                    ConstraintType.CT_FOREIGN_KEY,
                    NameRef.newBuilder().setName("customer_v1").build())));
    SnapshotConstraints changed =
        constraintsForSnapshot(
            tableId,
            220L,
            List.of(
                definitionWithReferencedTable(
                    "fk_customer",
                    ConstraintType.CT_FOREIGN_KEY,
                    NameRef.newBuilder().setName("customer_v2").build())));

    assertTrue(repo.putSnapshotConstraints(tableId, 220L, first));
    assertTrue(repo.putSnapshotConstraints(tableId, 220L, changed));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 220L).orElseThrow();
    assertEquals("customer_v2", fetched.getConstraints(0).getReferencedTable().getName());
  }

  @Test
  void upsertTreatsForeignKeyReferencedColumnOrderAsMaterialChange() {
    ResourceId referencedTableId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setId("customers")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    SnapshotConstraints first =
        constraintsForSnapshot(
            tableId,
            230L,
            List.of(
                foreignKeyDefinition(
                    "fk_customer",
                    referencedTableId,
                    List.of("customer_id", "customer_region_id"),
                    List.of("id", "region_id"))));
    SnapshotConstraints changed =
        constraintsForSnapshot(
            tableId,
            230L,
            List.of(
                foreignKeyDefinition(
                    "fk_customer",
                    referencedTableId,
                    List.of("customer_id", "customer_region_id"),
                    List.of("region_id", "id"))));

    assertTrue(repo.putSnapshotConstraints(tableId, 230L, first));
    assertTrue(repo.putSnapshotConstraints(tableId, 230L, changed));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 230L).orElseThrow();
    ConstraintDefinition fk = fetched.getConstraints(0);
    assertEquals("region_id", fk.getReferencedColumns(0).getColumnName());
    assertEquals("id", fk.getReferencedColumns(1).getColumnName());
  }

  @Test
  void upsertTreatsForeignKeyBehaviorMetadataAsMaterialChange() {
    ResourceId referencedTableId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setId("customers")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    ConstraintDefinition firstFk =
        foreignKeyDefinition(
                "fk_customer", referencedTableId, List.of("customer_id"), List.of("id"))
            .toBuilder()
            .setReferencedConstraintName("pk_customers")
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_FULL)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_RESTRICT)
            .build();
    ConstraintDefinition changedFk =
        firstFk.toBuilder()
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_SET_NULL)
            .build();

    assertTrue(
        repo.putSnapshotConstraints(
            tableId, 240L, constraintsForSnapshot(tableId, 240L, List.of(firstFk))));
    assertTrue(
        repo.putSnapshotConstraints(
            tableId, 240L, constraintsForSnapshot(tableId, 240L, List.of(changedFk))));

    ConstraintDefinition fetched =
        repo.getSnapshotConstraints(tableId, 240L).orElseThrow().getConstraints(0);
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_CASCADE, fetched.getUpdateRule());
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_SET_NULL, fetched.getDeleteRule());
  }

  @Test
  void upsertTreatsForeignKeyUnspecifiedAndExplicitDefaultBehaviorAsEquivalent() {
    ResourceId referencedTableId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setId("customers")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    ConstraintDefinition fkUnspecified =
        foreignKeyDefinition(
            "fk_customer", referencedTableId, List.of("customer_id"), List.of("id"));
    ConstraintDefinition fkExplicitDefaults =
        fkUnspecified.toBuilder()
            .setMatchOption(ForeignKeyMatchOption.FK_MATCH_OPTION_NONE)
            .setUpdateRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION)
            .setDeleteRule(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION)
            .build();

    assertTrue(
        repo.putSnapshotConstraints(
            tableId, 250L, constraintsForSnapshot(tableId, 250L, List.of(fkUnspecified))));
    assertFalse(
        repo.putSnapshotConstraints(
            tableId, 250L, constraintsForSnapshot(tableId, 250L, List.of(fkExplicitDefaults))));

    ConstraintDefinition fetched =
        repo.getSnapshotConstraints(tableId, 250L).orElseThrow().getConstraints(0);
    assertEquals(ForeignKeyMatchOption.FK_MATCH_OPTION_NONE, fetched.getMatchOption());
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION, fetched.getUpdateRule());
    assertEquals(ForeignKeyActionRule.FK_ACTION_RULE_NO_ACTION, fetched.getDeleteRule());
  }

  @Test
  void createIfAbsentCreatesOnceAndReturnsFalseAfterward() {
    SnapshotConstraints payload =
        constraintsForSnapshot(
            tableId, 300L, List.of(definition("pk_orders", ConstraintType.CT_PRIMARY_KEY)));

    assertTrue(repo.createSnapshotConstraintsIfAbsent(tableId, 300L, payload));
    assertFalse(repo.createSnapshotConstraintsIfAbsent(tableId, 300L, payload));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 300L).orElseThrow();
    assertEquals(1, fetched.getConstraintsCount());
    assertEquals("pk_orders", fetched.getConstraints(0).getName());
  }

  @Test
  void createIfAbsentReturnsFalseWhenBundleAlreadyExistsWithDifferentPayload() {
    SnapshotConstraints first =
        constraintsForSnapshot(
            tableId, 310L, List.of(definition("pk_orders", ConstraintType.CT_PRIMARY_KEY)));
    SnapshotConstraints different =
        constraintsForSnapshot(
            tableId,
            310L,
            List.of(
                definition("pk_orders", ConstraintType.CT_PRIMARY_KEY),
                definition("uq_orders_customer", ConstraintType.CT_UNIQUE)));

    assertTrue(repo.createSnapshotConstraintsIfAbsent(tableId, 310L, first));
    assertFalse(repo.createSnapshotConstraintsIfAbsent(tableId, 310L, different));

    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 310L).orElseThrow();
    assertEquals(1, fetched.getConstraintsCount());
    assertEquals("pk_orders", fetched.getConstraints(0).getName());
  }

  @Test
  void createIfAbsentConcurrentWritersOnlyOneCreateSucceeds() throws Exception {
    SnapshotConstraints payload =
        constraintsForSnapshot(
            tableId, 320L, List.of(definition("pk_orders", ConstraintType.CT_PRIMARY_KEY)));
    int writers = 8;
    CountDownLatch ready = new CountDownLatch(writers);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger created = new AtomicInteger();
    AtomicInteger existing = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      for (int i = 0; i < writers; i++) {
        pool.submit(
            () -> {
              ready.countDown();
              try {
                assertTrue(start.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              if (repo.createSnapshotConstraintsIfAbsent(tableId, 320L, payload)) {
                created.incrementAndGet();
              } else {
                existing.incrementAndGet();
              }
            });
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
    } finally {
      pool.shutdown();
      assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertEquals(1, created.get());
    assertEquals(writers - 1, existing.get());
    SnapshotConstraints fetched = repo.getSnapshotConstraints(tableId, 320L).orElseThrow();
    assertEquals(1, fetched.getConstraintsCount());
    assertEquals("pk_orders", fetched.getConstraints(0).getName());
  }

  private static SnapshotConstraints constraintsForSnapshot(
      ResourceId tableId, long snapshotId, List<ConstraintDefinition> definitions) {
    return constraintsForSnapshot(tableId, snapshotId, definitions, List.of());
  }

  private static SnapshotConstraints constraintsForSnapshot(
      ResourceId tableId,
      long snapshotId,
      List<ConstraintDefinition> definitions,
      List<String> propertyInsertionOrder) {
    SnapshotConstraints.Builder builder =
        SnapshotConstraints.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId)
            .addAllConstraints(definitions);

    for (String property : propertyInsertionOrder) {
      builder.putProperties(property, "v-" + property);
    }
    return builder.build();
  }

  private static ConstraintDefinition definition(String name, ConstraintType type) {
    return definitionWithProperties(name, type, List.of("source"));
  }

  private static ConstraintDefinition definitionWithProperties(
      String name, ConstraintType type, List<String> propertyInsertionOrder) {
    ConstraintDefinition.Builder builder =
        ConstraintDefinition.newBuilder()
            .setName(name)
            .setType(type)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .addColumns(
                ConstraintColumnRef.newBuilder()
                    .setOrdinal(1)
                    .setColumnId(1)
                    .setColumnName("id")
                    .build());
    for (String property : propertyInsertionOrder) {
      builder.putProperties(property, "v-" + property);
    }
    return builder.build();
  }

  private static ConstraintDefinition definitionWithReferencedTable(
      String name, ConstraintType type, NameRef referencedTable) {
    return definition(name, type).toBuilder().setReferencedTable(referencedTable).build();
  }

  private static ConstraintDefinition foreignKeyDefinition(
      String name,
      ResourceId referencedTableId,
      List<String> localColumnNames,
      List<String> referencedColumnNames) {
    ConstraintDefinition.Builder builder =
        ConstraintDefinition.newBuilder()
            .setName(name)
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .setReferencedTableId(referencedTableId);
    for (int i = 0; i < localColumnNames.size(); i++) {
      builder.addColumns(
          ConstraintColumnRef.newBuilder()
              .setOrdinal(i + 1)
              .setColumnId(i + 1L)
              .setColumnName(localColumnNames.get(i))
              .build());
    }
    for (int i = 0; i < referencedColumnNames.size(); i++) {
      builder.addReferencedColumns(
          ConstraintColumnRef.newBuilder()
              .setOrdinal(i + 1)
              .setColumnId(100 + i)
              .setColumnName(referencedColumnNames.get(i))
              .build());
    }
    return builder.build();
  }
}
