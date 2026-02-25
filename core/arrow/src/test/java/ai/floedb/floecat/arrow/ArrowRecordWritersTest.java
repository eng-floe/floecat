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

package ai.floedb.floecat.arrow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class ArrowRecordWritersTest {

  @Test
  void fromRecordClass_buildsSchemaAndWritesRows() {
    ArrowRecordWriter<TestRow> writer = ArrowRecordWriters.fromRecordClass(TestRow.class);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(writer.schema(), allocator)) {
      writer.write(root, List.of(new TestRow("alpha", 1), new TestRow("beta", 2)));

      assertThat(root.getRowCount()).isEqualTo(2);
      assertThat(root.getSchema().getFields()).extracting("name").containsExactly("name", "id");

      VarCharVector nameVector = (VarCharVector) root.getVector("name");
      assertThat(nameVector.getObject(0).toString()).isEqualTo("alpha");
      assertThat(nameVector.getObject(1).toString()).isEqualTo("beta");
    }
  }

  @Test
  void fromRecordClass_honorsArrowFieldNameOverride() {
    ArrowRecordWriter<AliasedRow> writer = ArrowRecordWriters.fromRecordClass(AliasedRow.class);
    assertThat(writer.schema().getFields()).extracting("name").containsExactly("display_name");
  }

  @Test
  void fromRecordClass_writesNullsForNullableTypes() {
    ArrowRecordWriter<NullableRow> writer = ArrowRecordWriters.fromRecordClass(NullableRow.class);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(writer.schema(), allocator)) {
      writer.write(
          root,
          List.of(
              new NullableRow(null, null, null, null, null, null, null, null, null),
              new NullableRow(
                  "value",
                  42,
                  true,
                  99L,
                  3.5,
                  Instant.parse("2026-01-01T00:00:00Z"),
                  LocalDate.of(2026, 1, 2),
                  UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                  new byte[] {1, 2, 3})));

      VarCharVector text = (VarCharVector) root.getVector("text");
      IntVector intValue = (IntVector) root.getVector("int_value");
      BitVector boolValue = (BitVector) root.getVector("bool_value");
      BigIntVector longValue = (BigIntVector) root.getVector("long_value");
      Float8Vector doubleValue = (Float8Vector) root.getVector("double_value");
      TimeStampMicroTZVector instantValue =
          (TimeStampMicroTZVector) root.getVector("instant_value");
      DateDayVector localDateValue = (DateDayVector) root.getVector("local_date_value");
      VarBinaryVector bytesValue = (VarBinaryVector) root.getVector("bytes_value");

      assertThat(text.isNull(0)).isTrue();
      assertThat(intValue.isNull(0)).isTrue();
      assertThat(boolValue.isNull(0)).isTrue();
      assertThat(longValue.isNull(0)).isTrue();
      assertThat(doubleValue.isNull(0)).isTrue();
      assertThat(instantValue.isNull(0)).isTrue();
      assertThat(localDateValue.isNull(0)).isTrue();
      assertThat(bytesValue.isNull(0)).isTrue();

      assertThat(text.getObject(1).toString()).isEqualTo("value");
      assertThat(intValue.get(1)).isEqualTo(42);
      assertThat(boolValue.get(1)).isEqualTo(1);
      assertThat(longValue.get(1)).isEqualTo(99L);
      assertThat(doubleValue.get(1)).isEqualTo(3.5);
      assertThat(bytesValue.getObject(1)).containsExactly((byte) 1, (byte) 2, (byte) 3);
      assertThat(root.getRowCount()).isEqualTo(2);
    }
  }

  @Test
  void fromRecordClass_rejectsUnsupportedTypes() {
    assertThatThrownBy(() -> ArrowRecordWriters.fromRecordClass(UnsupportedRow.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported component type");
  }

  private record TestRow(String name, int id) {}

  private record AliasedRow(@ArrowFieldName("display_name") String label) {}

  private record NullableRow(
      String text,
      Integer intValue,
      Boolean boolValue,
      Long longValue,
      Double doubleValue,
      Instant instantValue,
      LocalDate localDateValue,
      UUID uuidValue,
      byte[] bytesValue) {}

  private record UnsupportedRow(java.math.BigDecimal unsupported) {}
}
