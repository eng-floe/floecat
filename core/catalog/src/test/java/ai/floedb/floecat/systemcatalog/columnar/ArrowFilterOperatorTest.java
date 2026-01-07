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

package ai.floedb.floecat.systemcatalog.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.expr.Expr;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectRow;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.Test;

class ArrowFilterOperatorTest {

  @Test
  void filtersRowsAccordingToExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}),
            new SystemObjectRow(new Object[] {3, "keep"}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("id"), new Expr.Literal("1"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      RowStreamToArrowBatchAdapter adapter = new RowStreamToArrowBatchAdapter(allocator, schema, 3);
      List<ColumnarBatch> batches = adapter.adapt(rows.stream()).toList();

      assertThat(batches).hasSize(1);
      ColumnarBatch batch = batches.get(0);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);

        IntVector idVector = (IntVector) root.getVector(0);
        assertThat(idVector.get(0)).isEqualTo(1);

        VarCharVector labelVector = (VarCharVector) root.getVector(1);
        assertThat(new String(labelVector.get(0), StandardCharsets.UTF_8)).isEqualTo("keep");
      }
    }
  }

  @Test
  void filtersRowsWithGreaterThanExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}),
            new SystemObjectRow(new Object[] {3, "keep"}));

    Expr expr = new Expr.Gt(new Expr.ColumnRef("id"), new Expr.Literal("1"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(2);

        IntVector idVector = (IntVector) root.getVector(0);
        assertThat(idVector.get(0)).isEqualTo(2);
        assertThat(idVector.get(1)).isEqualTo(3);

        VarCharVector labelVector = (VarCharVector) root.getVector(1);
        assertThat(new String(labelVector.get(0), StandardCharsets.UTF_8)).isEqualTo("skip");
        assertThat(new String(labelVector.get(1), StandardCharsets.UTF_8)).isEqualTo("keep");
      }
    }
  }

  @Test
  void filtersRowsWithBigIntGreaterThanLiteralPrecision() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("BIGINT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {9007199254740993L}),
            new SystemObjectRow(new Object[] {9007199254740992L}));

    Expr expr = new Expr.Gt(new Expr.ColumnRef("value"), new Expr.Literal("9007199254740992"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
        BigIntVector vector = (BigIntVector) filtered.root().getVector(0);
        assertThat(vector.get(0)).isEqualTo(9007199254740993L);
      }
    }
  }

  @Test
  void filtersRowsWithVarCharEmptyStringEq() {
    SchemaColumn column =
        SchemaColumn.newBuilder().setName("label").setLogicalType("VARCHAR").setFieldId(1).build();
    List<SchemaColumn> schema = List.of(column);
    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {""}), new SystemObjectRow(new Object[] {"x"}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("label"), new Expr.Literal(""));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithVarCharUtf8MultiByte() {
    SchemaColumn column =
        SchemaColumn.newBuilder().setName("label").setLogicalType("VARCHAR").setFieldId(1).build();
    List<SchemaColumn> schema = List.of(column);
    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {"café"}), new SystemObjectRow(new Object[] {"caff"}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("label"), new Expr.Literal("café"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithNotExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("flag")
                .setLogicalType("VARCHAR")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {"keep"}), new SystemObjectRow(new Object[] {"skip"}));

    Expr expr = new Expr.Not(new Expr.Eq(new Expr.ColumnRef("flag"), new Expr.Literal("skip")));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithFloat8NegativeInfinityEquality() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {Double.NEGATIVE_INFINITY}),
            new SystemObjectRow(new Object[] {1.0d}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("-Infinity"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithFloat8NegativeZeroEqualsPositiveZero() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {-0.0d}), new SystemObjectRow(new Object[] {0.0d}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("0.0"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(2);
      }
    }
  }

  @Test
  void filtersRowsWithAndExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}));

    Expr expr =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("id"), new Expr.Literal("2")),
            new Expr.Eq(new Expr.ColumnRef("label"), new Expr.Literal("keep")));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4Equality_exactMatch() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1.5f}),
            new SystemObjectRow(new Object[] {2.5f}),
            new SystemObjectRow(new Object[] {-2.0f}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("1.5"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);
        Float4Vector valueVector = (Float4Vector) root.getVector(0);
        assertThat(valueVector.get(0)).isEqualTo(1.5f);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4Equality_doesNotMatchDifferentValue() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1.5f}),
            new SystemObjectRow(new Object[] {2.5f}),
            new SystemObjectRow(new Object[] {-2.0f}));

    // Strict float equality: this should not match any stored values.
    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("1.2500001"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(0);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4Equality_invalidLiteralYieldsNoMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {1.5f}), new SystemObjectRow(new Object[] {2.5f}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("not-a-number"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(0);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4InfinityLiteralMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {Float.POSITIVE_INFINITY}),
            new SystemObjectRow(new Object[] {1.0f}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("Infinity"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4GreaterThanInfinityMatchesNone() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {Float.POSITIVE_INFINITY}),
            new SystemObjectRow(new Object[] {2.0f}));

    Expr expr = new Expr.Gt(new Expr.ColumnRef("value"), new Expr.Literal("Infinity"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(0);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4Equality_nullNeverMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {null}), new SystemObjectRow(new Object[] {1.5f}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("1.5"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);
        Float4Vector valueVector = (Float4Vector) root.getVector(0);
        assertThat(valueVector.get(0)).isEqualTo(1.5f);
      }
    }
  }

  @Test
  void filtersRowsWithFloat4Equality_nanLiteralNeverMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {Float.NaN}),
            new SystemObjectRow(new Object[] {1.5f}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("NaN"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(0);
      }
    }
  }

  @Test
  void filtersRowsWithFloat8Equality_exactMatch() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1.5d}),
            new SystemObjectRow(new Object[] {2.5d}),
            new SystemObjectRow(new Object[] {-2.0d}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("2.5"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);
        Float8Vector valueVector = (Float8Vector) root.getVector(0);
        assertThat(valueVector.get(0)).isEqualTo(2.5d);
      }
    }
  }

  @Test
  void filtersRowsWithFloat8Equality_infinityLiteralMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {Double.POSITIVE_INFINITY}),
            new SystemObjectRow(new Object[] {2.5d}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("Infinity"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        VectorSchemaRoot root = filtered.root();
        assertThat(root.getRowCount()).isEqualTo(1);
        Float8Vector valueVector = (Float8Vector) root.getVector(0);
        assertThat(valueVector.get(0)).isEqualTo(Double.POSITIVE_INFINITY);
      }
    }
  }

  @Test
  void filtersRowsWithFloat8Equality_invalidLiteralYieldsNoMatches() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(new SystemObjectRow(new Object[] {1.5d}), new SystemObjectRow(new Object[] {2.5d}));

    Expr expr = new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("NaN-but-not"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(0);
      }
    }
  }

  @Test
  void filtersRowsWithOrExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder().setName("id").setLogicalType("INT").setFieldId(1).build(),
            SchemaColumn.newBuilder()
                .setName("label")
                .setLogicalType("VARCHAR")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {1, "keep"}),
            new SystemObjectRow(new Object[] {2, "skip"}),
            new SystemObjectRow(new Object[] {3, "keep"}));

    Expr expr =
        new Expr.Or(
            new Expr.Eq(new Expr.ColumnRef("id"), new Expr.Literal("1")),
            new Expr.Eq(new Expr.ColumnRef("id"), new Expr.Literal("3")));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(2);
      }
    }
  }

  @Test
  void filtersRowsWithIsNullExpression() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("maybe")
                .setLogicalType("VARCHAR")
                .setFieldId(1)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {null}), new SystemObjectRow(new Object[] {"value"}));

    Expr expr = new Expr.IsNull(new Expr.ColumnRef("maybe"));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  @Test
  void filtersRowsWithBooleanAndFloatingPointEquality() {
    List<SchemaColumn> schema =
        List.of(
            SchemaColumn.newBuilder()
                .setName("flag")
                .setLogicalType("BOOLEAN")
                .setFieldId(1)
                .build(),
            SchemaColumn.newBuilder()
                .setName("value")
                .setLogicalType("FLOAT8")
                .setFieldId(2)
                .build());

    List<SystemObjectRow> rows =
        List.of(
            new SystemObjectRow(new Object[] {true, 1.5}),
            new SystemObjectRow(new Object[] {false, 2.5}),
            new SystemObjectRow(new Object[] {true, 2.5}));

    Expr expr =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("flag"), new Expr.Literal("true")),
            new Expr.Eq(new Expr.ColumnRef("value"), new Expr.Literal("1.5")));

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      ColumnarBatch batch = firstBatch(allocator, schema, rows);
      try (ColumnarBatch filtered = ArrowFilterOperator.filter(batch, expr, allocator)) {
        assertThat(filtered.root().getRowCount()).isEqualTo(1);
      }
    }
  }

  private ColumnarBatch firstBatch(
      BufferAllocator allocator, List<SchemaColumn> schema, List<SystemObjectRow> rows) {
    RowStreamToArrowBatchAdapter adapter =
        new RowStreamToArrowBatchAdapter(allocator, schema, rows.size());
    return adapter.adapt(rows.stream()).toList().get(0);
  }
}
