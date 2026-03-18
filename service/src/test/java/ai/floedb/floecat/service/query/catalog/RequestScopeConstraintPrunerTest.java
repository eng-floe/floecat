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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RequestScopeConstraintPrunerTest {

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("users")
          .build();

  private static final ResourceId TABLE_TWO =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("orders")
          .build();

  @Test
  void pkPreservedWhenColumnNotRequested() {
    RequestScopeConstraintPruner pruner =
        new RequestScopeConstraintPruner(
            Set.of(RequestScopeConstraintPruner.relationKey(TABLE)),
            Map.of(RequestScopeConstraintPruner.relationKey(TABLE), Set.of(1L)));

    ConstraintDefinition pk =
        ConstraintDefinition.newBuilder()
            .setName("pk_users")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(column(99L, "hidden_pk_col", 1))
            .build();

    List<ConstraintDefinition> out = pruner.prune(TABLE, List.of(pk));
    assertEquals(1, out.size());
    assertEquals("pk_users", out.get(0).getName());
    assertEquals(99L, out.get(0).getColumns(0).getColumnId());
  }

  @Test
  void fkDroppedWhenReferencedTableNotRequested() {
    RequestScopeConstraintPruner pruner =
        new RequestScopeConstraintPruner(
            Set.of(RequestScopeConstraintPruner.relationKey(TABLE)),
            Map.of(RequestScopeConstraintPruner.relationKey(TABLE), Set.of(1L)));

    ConstraintDefinition fk =
        ConstraintDefinition.newBuilder()
            .setName("fk_users_orders")
            .setType(ConstraintType.CT_FOREIGN_KEY)
            .addColumns(column(1L, "user_id", 1))
            .setReferencedTableId(TABLE_TWO)
            .addReferencedColumns(column(2L, "order_id", 1))
            .build();

    List<ConstraintDefinition> out = pruner.prune(TABLE, List.of(fk));
    assertTrue(out.isEmpty());
  }

  @Test
  void checkExpressionBlankedWhenHiddenColumnExists() {
    RequestScopeConstraintPruner pruner =
        new RequestScopeConstraintPruner(
            Set.of(RequestScopeConstraintPruner.relationKey(TABLE)),
            Map.of(RequestScopeConstraintPruner.relationKey(TABLE), Set.of(1L)));

    ConstraintDefinition check =
        ConstraintDefinition.newBuilder()
            .setName("check_users_secret")
            .setType(ConstraintType.CT_CHECK)
            .setCheckExpression("secret_col > 0")
            .addColumns(column(99L, "secret_col", 1))
            .build();

    List<ConstraintDefinition> out = pruner.prune(TABLE, List.of(check));
    assertEquals(1, out.size());
    assertEquals("", out.get(0).getCheckExpression());
  }

  @Test
  void constraintsOnlyModePreservesPkWithoutRequestedColumnMap() {
    RequestScopeConstraintPruner pruner =
        RequestScopeConstraintPruner.forRequestedTablesOnly(
            Set.of(RequestScopeConstraintPruner.relationKey(TABLE)));

    ConstraintDefinition pk =
        ConstraintDefinition.newBuilder()
            .setName("pk_users_constraints_only")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .addColumns(column(99L, "hidden_pk_col", 1))
            .build();

    List<ConstraintDefinition> out = pruner.prune(TABLE, List.of(pk));
    assertEquals(1, out.size());
    assertEquals("pk_users_constraints_only", out.get(0).getName());
    assertEquals(99L, out.get(0).getColumns(0).getColumnId());
  }

  @Test
  void relationKeyIncludesKind() {
    ResourceId tableKind =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("x")
            .build();
    ResourceId unspecifiedKind =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_UNSPECIFIED)
            .setId("x")
            .build();

    String tableKey = RequestScopeConstraintPruner.relationKey(tableKind);
    String unspecifiedKey = RequestScopeConstraintPruner.relationKey(unspecifiedKind);

    assertNotEquals(tableKey, unspecifiedKey);
    assertTrue(tableKey.contains("/" + ResourceKind.RK_TABLE_VALUE + "/"));
  }

  private static ConstraintColumnRef column(long id, String name, int ordinal) {
    return ConstraintColumnRef.newBuilder()
        .setColumnId(id)
        .setColumnName(name)
        .setOrdinal(ordinal)
        .build();
  }
}
