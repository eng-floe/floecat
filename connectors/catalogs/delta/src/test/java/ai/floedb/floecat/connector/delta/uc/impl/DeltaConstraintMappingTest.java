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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.IdentityMode;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaIdentityReconciler;
import ai.floedb.floecat.schema.identity.SchemaNode;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.Test;

class DeltaConstraintMappingTest {
  @Test
  void mapDeltaConstraintsEmitsNotNullForNonNullableLeafColumns() {
    StructType schema =
        new StructType()
            .add("id", LongType.LONG, false)
            .add("name", StringType.STRING, true)
            .add(
                "address",
                new StructType()
                    .add("zip", StringType.STRING, false)
                    .add("line2", StringType.STRING, true),
                false);

    List<ConstraintDefinition> constraints =
        DeltaConnector.mapDeltaConstraints(
            schema, Map.of(), identityMap("id", "name", "address", "address.zip", "address.line2"));

    assertThat(constraints).allMatch(c -> c.getType() == ConstraintType.CT_NOT_NULL);
    assertThat(constraints).allMatch(c -> c.getEnforcement() == ConstraintEnforcement.CE_ENFORCED);
    assertThat(constraints).hasSize(2);
    assertThat(constraints)
        .extracting(c -> c.getColumns(0).getColumnName())
        .containsExactlyInAnyOrder("id", "address.zip");
    assertThat(constraints).allMatch(c -> c.getColumns(0).getColumnId() > 0L);
  }

  @Test
  void mapDeltaConstraintsDoesNotEmitNotNullForNonNullableChildInsideNullableParentStruct() {
    // address is nullable; zip inside address is non-nullable.
    // nn_address.zip must NOT be emitted because address can be absent entirely.
    StructType schema =
        new StructType()
            .add("id", LongType.LONG, false)
            .add(
                "address",
                new StructType()
                    .add("zip", StringType.STRING, false)
                    .add("line2", StringType.STRING, true),
                true); // address is nullable

    List<ConstraintDefinition> constraints =
        DeltaConnector.mapDeltaConstraints(
            schema, Map.of(), identityMap("id", "address", "address.zip", "address.line2"));

    assertThat(constraints).allMatch(c -> c.getType() == ConstraintType.CT_NOT_NULL);
    assertThat(constraints).hasSize(1);
    assertThat(constraints.get(0).getColumns(0).getColumnName()).isEqualTo("id");
    // address.zip must not appear
    assertThat(constraints)
        .extracting(c -> c.getColumns(0).getColumnName())
        .doesNotContain("address.zip");
  }

  @Test
  void mapDeltaConstraintsEmitsCheckConstraintsFromDeltaProperties() {
    StructType schema = new StructType().add("amount", LongType.LONG, false);

    List<ConstraintDefinition> constraints =
        DeltaConnector.mapDeltaConstraints(
            schema,
            Map.of(
                "delta.constraints.positive_amount", "amount > 0",
                "delta.constraints.valid_amount", "amount < 1000",
                "delta.appendOnly", "true"),
            identityMap("amount"));

    List<ConstraintDefinition> checks =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_CHECK).toList();
    assertThat(checks).hasSize(2);
    assertThat(checks)
        .extracting(ConstraintDefinition::getName)
        .containsExactly("positive_amount", "valid_amount");
    assertThat(checks)
        .extracting(ConstraintDefinition::getCheckExpression)
        .containsExactly("amount > 0", "amount < 1000");
    assertThat(checks).allMatch(c -> c.getEnforcement() == ConstraintEnforcement.CE_ENFORCED);
  }

  @Test
  void mapDeltaCheckConstraintsIgnoresBlankConstraintName() {
    // "delta.constraints. " has a whitespace-only name after the prefix — should be skipped.
    StructType schema = new StructType().add("id", LongType.LONG, false);

    List<ConstraintDefinition> constraints =
        DeltaConnector.mapDeltaConstraints(
            schema,
            Map.of(
                "delta.constraints. ", "id > 0",
                "delta.constraints.ck_id", "id > 0"),
            identityMap("id"));

    List<ConstraintDefinition> checks =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_CHECK).toList();
    assertThat(checks).hasSize(1);
    assertThat(checks.get(0).getName()).isEqualTo("ck_id");
  }

  @Test
  void mapDeltaCheckConstraintsIgnoresBlankExpression() {
    // "delta.constraints.ck_foo" with empty/whitespace expression — should be skipped.
    StructType schema = new StructType().add("id", LongType.LONG, false);

    List<ConstraintDefinition> constraints =
        DeltaConnector.mapDeltaConstraints(
            schema,
            Map.of(
                "delta.constraints.ck_blank", "   ",
                "delta.constraints.ck_id", "id > 0"),
            identityMap("id"));

    List<ConstraintDefinition> checks =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_CHECK).toList();
    assertThat(checks).hasSize(1);
    assertThat(checks.get(0).getName()).isEqualTo("ck_id");
  }

  /** Builds a valid structured-path identity map for the test schema paths. */
  private static ColumnIdentityMap identityMap(String... dottedPaths) {
    List<SchemaNode> nodes =
        java.util.Arrays.stream(dottedPaths)
            .map(
                dotted -> {
                  ColumnPath path = ColumnPath.ROOT;
                  for (String name : dotted.split("\\.")) {
                    path = path.field(name);
                  }
                  return new SchemaNode(path, 1, true, OptionalInt.empty(), Optional.empty());
                })
            .toList();
    return DeltaCanonicalIdentity.toProto(
        SchemaIdentityReconciler.reconcile(
                ResolvedSchema.of(nodes), 0L, IdentityMode.STRUCTURED_PATH, Optional.empty())
            .state());
  }
}
