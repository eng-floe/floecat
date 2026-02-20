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

package ai.floedb.floecat.scanner.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.utils.ArrowConversion;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class ArrowConversionTest {

  @Test
  void fill_populatesVectors() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Field col2 = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
    Schema schema = new Schema(List.of(col1, col2));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row1 = new SystemObjectRow(new Object[] {"alice", 1});
      SystemObjectRow row2 = new SystemObjectRow(new Object[] {"bob", 2});

      ArrowConversion.fill(root, List.of(row1, row2));

      VarCharVector names = (VarCharVector) root.getVector("col1");
      IntVector ints = (IntVector) root.getVector("col2");

      assertThat(root.getRowCount()).isEqualTo(2);
      assertThat(names.getObject(0).toString()).isEqualTo("alice");
      assertThat(names.getObject(1).toString()).isEqualTo("bob");
      assertThat(ints.getObject(0)).isEqualTo(1);
      assertThat(ints.getObject(1)).isEqualTo(2);
    }
  }

  @Test
  void fill_allowsNulls() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Field col2 = new Field("col2", FieldType.nullable(new ArrowType.Int(32, true)), List.of());
    Schema schema = new Schema(List.of(col1, col2));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row = new SystemObjectRow(new Object[] {null, null});
      ArrowConversion.fill(root, List.of(row));

      VarCharVector names = (VarCharVector) root.getVector("col1");
      IntVector ints = (IntVector) root.getVector("col2");

      assertThat(root.getRowCount()).isEqualTo(1);
      assertThat(names.getObject(0)).isNull();
      assertThat(ints.getObject(0)).isNull();
    }
  }

  @Test
  void fill_requiresColumnCount() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col1 = new Field("col1", FieldType.nullable(new ArrowType.Utf8()), List.of());
    Schema schema = new Schema(List.of(col1));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow bad = new SystemObjectRow(new Object[] {"one", 2});

      assertThatThrownBy(() -> ArrowConversion.fill(root, List.of(bad)))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  void fill_rejectsUnsupportedVector() {
    Assumptions.assumeTrue(isArrowAvailable(), "Arrow memory allocator is unavailable");

    Field col = new Field("col", FieldType.nullable(new ArrowType.Decimal(10, 2)), List.of());
    Schema schema = new Schema(List.of(col));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

      SystemObjectRow row = new SystemObjectRow(new Object[] {"value"});

      assertThatThrownBy(() -> ArrowConversion.fill(root, List.of(row)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("unsupported vector type");
    }
  }

  private static boolean isArrowAvailable() {
    try (BufferAllocator ignored = new RootAllocator(Long.MAX_VALUE)) {
      return true;
    } catch (Throwable ignored) {
      return false;
    }
  }
}
