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

package ai.floedb.floecat.connector.common.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Unit tests for the recursive Iceberg → {@link LogicalType} tree conversion. */
class IcebergTypeMappingsTest {

  @Test
  void listPreservesElementTypeAndNullability() {
    LogicalType optionalElems =
        IcebergTypeMappings.toLogical(Types.ListType.ofOptional(1, Types.IntegerType.get()));
    assertThat(optionalElems).isEqualTo(LogicalType.array(LogicalType.of(LogicalKind.INT), true));

    LogicalType requiredElems =
        IcebergTypeMappings.toLogical(Types.ListType.ofRequired(1, Types.IntegerType.get()));
    assertThat(requiredElems.elementNullable()).isFalse();
  }

  @Test
  void mapPreservesKeyAndValueTypes() {
    LogicalType t =
        IcebergTypeMappings.toLogical(
            Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.DoubleType.get()));
    assertThat(t.key()).isEqualTo(LogicalType.of(LogicalKind.STRING));
    assertThat(t.value()).isEqualTo(LogicalType.of(LogicalKind.DOUBLE));
    assertThat(t.valueNullable()).isFalse();
  }

  @Test
  void structPreservesFieldNamesOrderAndNullability() {
    LogicalType t =
        IcebergTypeMappings.toLogical(
            Types.StructType.of(
                Types.NestedField.required(1, "sku", Types.StringType.get()),
                Types.NestedField.optional(2, "qty", Types.IntegerType.get())));
    assertThat(t.fields()).hasSize(2);
    assertThat(t.fields().get(0).name()).isEqualTo("sku");
    assertThat(t.fields().get(0).nullable()).isFalse();
    assertThat(t.fields().get(1).name()).isEqualTo("qty");
    assertThat(t.fields().get(1).nullable()).isTrue();
  }

  @Test
  void deepNestingComposes() {
    // list<struct<sku: string, quantities: list<int>>>
    LogicalType t =
        IcebergTypeMappings.toLogical(
            Types.ListType.ofOptional(
                1,
                Types.StructType.of(
                    Types.NestedField.required(2, "sku", Types.StringType.get()),
                    Types.NestedField.optional(
                        3, "quantities", Types.ListType.ofOptional(4, Types.IntegerType.get())))));
    assertThat(t.element().fields().get(1).type().element())
        .isEqualTo(LogicalType.of(LogicalKind.INT));
  }
}
