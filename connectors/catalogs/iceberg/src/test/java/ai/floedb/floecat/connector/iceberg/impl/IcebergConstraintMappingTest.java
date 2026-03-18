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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintEnforcement;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergConstraintMappingTest {

  @Test
  void mapIcebergConstraintsEmitsIdentifierPkAndNotNullConstraints() {
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get()),
                Types.NestedField.required(
                    3,
                    "address",
                    Types.StructType.of(
                        Types.NestedField.required(4, "zip", Types.StringType.get()),
                        Types.NestedField.optional(5, "line2", Types.StringType.get())))),
            Set.of(1));

    List<ConstraintDefinition> constraints = IcebergConnector.mapIcebergConstraints(schema);

    ConstraintDefinition pk =
        constraints.stream()
            .filter(c -> c.getType() == ConstraintType.CT_PRIMARY_KEY)
            .findFirst()
            .orElseThrow();
    assertThat(pk.getEnforcement()).isEqualTo(ConstraintEnforcement.CE_NOT_ENFORCED);
    assertThat(pk.getColumnsList()).hasSize(1);
    assertThat(pk.getColumns(0).getColumnId()).isEqualTo(1L);
    assertThat(pk.getColumns(0).getColumnName()).isEqualTo("id");

    List<ConstraintDefinition> notNull =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_NOT_NULL).toList();
    assertThat(notNull).hasSize(2);
    assertThat(notNull)
        .extracting(c -> c.getColumns(0).getColumnName())
        .containsExactlyInAnyOrder("id", "address.zip");
    assertThat(notNull).allMatch(c -> c.getEnforcement() == ConstraintEnforcement.CE_ENFORCED);
  }

  @Test
  void mapIcebergConstraintsDoesNotEmitNotNullForRequiredChildInsideOptionalParentStruct() {
    // address is optional; zip inside address is required.
    // nn_address.zip must NOT be emitted because address can be absent entirely.
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(
                    2,
                    "address",
                    Types.StructType.of(
                        Types.NestedField.required(3, "zip", Types.StringType.get()),
                        Types.NestedField.optional(4, "line2", Types.StringType.get())))));

    List<ConstraintDefinition> constraints = IcebergConnector.mapIcebergConstraints(schema);

    List<ConstraintDefinition> notNull =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_NOT_NULL).toList();
    assertThat(notNull).hasSize(1);
    assertThat(notNull.get(0).getColumns(0).getColumnName()).isEqualTo("id");
    // address.zip must not appear — its presence depends on address being non-null
    assertThat(notNull)
        .extracting(c -> c.getColumns(0).getColumnName())
        .doesNotContain("address.zip");
  }

  @Test
  void mapIcebergConstraintsEmitsNotNullForRequiredListField() {
    // A required list field (non-nullable list) should emit NOT NULL just like Delta does.
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(
                    2, "tags", Types.ListType.ofOptional(3, Types.StringType.get())),
                Types.NestedField.optional(
                    4, "scores", Types.ListType.ofOptional(5, Types.DoubleType.get()))));

    List<ConstraintDefinition> constraints = IcebergConnector.mapIcebergConstraints(schema);

    List<ConstraintDefinition> notNull =
        constraints.stream().filter(c -> c.getType() == ConstraintType.CT_NOT_NULL).toList();
    assertThat(notNull)
        .extracting(c -> c.getColumns(0).getColumnName())
        .containsExactlyInAnyOrder("id", "tags");
    // optional list 'scores' must not appear
    assertThat(notNull).extracting(c -> c.getColumns(0).getColumnName()).doesNotContain("scores");
  }

  @Test
  void mapIcebergConstraintsPkColumnsAreSortedByPathThenFieldId() {
    // Identifier fields {3, 1, 2}: paths "c", "a", "b" → expected sorted order: a(1), b(2), c(3)
    Schema schema =
        new Schema(
            List.of(
                Types.NestedField.required(1, "a", Types.LongType.get()),
                Types.NestedField.required(2, "b", Types.LongType.get()),
                Types.NestedField.required(3, "c", Types.LongType.get())),
            Set.of(3, 1, 2));

    List<ConstraintDefinition> constraints = IcebergConnector.mapIcebergConstraints(schema);

    ConstraintDefinition pk =
        constraints.stream()
            .filter(c -> c.getType() == ConstraintType.CT_PRIMARY_KEY)
            .findFirst()
            .orElseThrow();
    assertThat(pk.getColumnsList()).hasSize(3);
    assertThat(pk.getColumns(0).getColumnName()).isEqualTo("a");
    assertThat(pk.getColumns(1).getColumnName()).isEqualTo("b");
    assertThat(pk.getColumns(2).getColumnName()).isEqualTo("c");
    assertThat(pk.getColumns(0).getOrdinal()).isEqualTo(1);
    assertThat(pk.getColumns(1).getOrdinal()).isEqualTo(2);
    assertThat(pk.getColumns(2).getOrdinal()).isEqualTo(3);
  }

  // Note: we cannot test the schema.findField(id) null-guard via Iceberg's public Schema
  // constructor because it validates that all identifierFieldIds exist at construction time.
  // The guard remains as a defensive measure against data-corruption scenarios where
  // schema and identifier metadata are out of sync in storage.

  @Test
  void mergeConstraintsDedupesByPayloadAndPreservesPrimaryOrder() {
    ConstraintDefinition primary =
        ConstraintDefinition.newBuilder()
            .setName("pk_identifier_fields")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .setEnforcement(ConstraintEnforcement.CE_NOT_ENFORCED)
            .build();
    ConstraintDefinition secondaryOnly =
        ConstraintDefinition.newBuilder()
            .setName("nn_id")
            .setType(ConstraintType.CT_NOT_NULL)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .build();

    List<ConstraintDefinition> merged =
        IcebergConnector.mergeConstraints(List.of(primary), List.of(primary, secondaryOnly));

    assertThat(merged).containsExactly(primary, secondaryOnly);
  }

  @Test
  void mergeConstraintsPrimaryWinsOnExactPayloadCollision() {
    // Same logical constraint in both sources: primary-sourced instance is kept
    ConstraintDefinition constraint =
        ConstraintDefinition.newBuilder()
            .setName("nn_id")
            .setType(ConstraintType.CT_NOT_NULL)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .build();

    List<ConstraintDefinition> merged =
        IcebergConnector.mergeConstraints(List.of(constraint), List.of(constraint));

    assertThat(merged).hasSize(1);
    assertThat(merged.get(0)).isEqualTo(constraint);
  }

  @Test
  void mergeConstraintsIncludesSecondaryOnlyEntryWhenPrimaryIsNonEmpty() {
    ConstraintDefinition fromPrimary =
        ConstraintDefinition.newBuilder()
            .setName("pk_identifier_fields")
            .setType(ConstraintType.CT_PRIMARY_KEY)
            .setEnforcement(ConstraintEnforcement.CE_NOT_ENFORCED)
            .build();
    ConstraintDefinition fromSecondary =
        ConstraintDefinition.newBuilder()
            .setName("nn_id")
            .setType(ConstraintType.CT_NOT_NULL)
            .setEnforcement(ConstraintEnforcement.CE_ENFORCED)
            .build();

    List<ConstraintDefinition> merged =
        IcebergConnector.mergeConstraints(List.of(fromPrimary), List.of(fromSecondary));

    assertThat(merged).containsExactlyInAnyOrder(fromPrimary, fromSecondary);
  }
}
