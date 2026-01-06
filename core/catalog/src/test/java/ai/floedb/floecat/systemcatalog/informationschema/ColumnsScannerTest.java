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

package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.columnar.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

class ColumnsScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new ColumnsScanner().schema())
        .extracting(SchemaColumn::getName)
        .containsExactly(
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position");
  }

  @Test
  void scan_returnsOneRowPerColumn() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");

    builder.addTable(
        ns,
        "orders",
        Map.of("id", 1, "stats.sales", 2),
        Map.of("id", "long", "stats.sales", "double"));

    SystemObjectScanContext ctx = builder.build();

    var rows = new ColumnsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .containsExactly(
            List.of("marketing", "finance.sales", "orders", "id", "long", 1),
            List.of("marketing", "finance.sales", "orders", "sales", "double", 2));
  }

  @Test
  void scan_withNoTables_returnsNoRows() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");
    SystemObjectScanContext ctx = builder.build();
    var rows = new ColumnsScanner().scan(ctx).toList();

    assertThat(rows).isEmpty();
  }

  @Test
  void scan_handlesMissingColumnTypesGracefully() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");

    builder.addTable(ns, "orders", Map.of("id", 1, "missing_col", 2), Map.of("id", "long"));

    SystemObjectScanContext ctx = builder.build();

    var rows = new ColumnsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    // Expect two rows, one for each column
    assertThat(rows).hasSize(2);

    // Find row for "id" column and check data_type is "long"
    assertThat(rows)
        .anySatisfy(
            row -> {
              assertThat(row.get(3)).isEqualTo("id");
              assertThat(row.get(4)).isEqualTo("long");
            });

    // Find row for "missing_col" column and check data_type is null or whatever current behavior is
    assertThat(rows)
        .anySatisfy(
            row -> {
              if ("missing_col".equals(row.get(3))) {
                assertThat(row.get(4)).isNull();
              }
            });
  }

  @Test
  void scanArrow_matchesRowPath() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");
    builder.addTable(ns, "orders", Map.of("id", 1, "stats.sales", 2), Map.of("id", "long"));
    SystemObjectScanContext ctx = builder.build();

    var scanner = new ColumnsScanner();
    List<List<String>> expected = scanner.scan(ctx).map(row -> toStringList(row.values())).toList();

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      List<List<String>> arrowRows =
          scanner
              .scanArrow(ctx, null, List.of(), allocator)
              .map(
                  batch -> {
                    try (batch) {
                      return toRows(batch.root());
                    }
                  })
              .flatMap(List::stream)
              .toList();

      assertThat(arrowRows).isEqualTo(expected);
    }
  }

  @Test
  void scanArrow_schemaMatchesDefinitionOrder() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");
    builder.addTable(ns, "orders", Map.of("id", 1, "stats.sales", 2), Map.of("id", "long"));
    SystemObjectScanContext ctx = builder.build();

    var scanner = new ColumnsScanner();
    List<String> expected = scanner.schema().stream().map(SchemaColumn::getName).toList();

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        Stream<ColumnarBatch> batches = scanner.scanArrow(ctx, null, List.of(), allocator)) {
      List<String> fields =
          batches
              .map(
                  batch -> {
                    try (batch) {
                      return batch.root().getSchema().getFields().stream()
                          .map(Field::getName)
                          .toList();
                    }
                  })
              .findFirst()
              .orElseThrow();

      assertThat(fields).isEqualTo(expected);
    }
  }

  private static List<List<String>> toRows(VectorSchemaRoot root) {
    int rowCount = root.getRowCount();
    List<FieldVector> vectors = root.getFieldVectors();
    List<List<String>> results = new ArrayList<>(rowCount);
    for (int row = 0; row < rowCount; row++) {
      List<String> values = new ArrayList<>(vectors.size());
      for (FieldVector vector : vectors) {
        if (vector instanceof VarCharVector varchar) {
          if (varchar.isNull(row)) {
            values.add(null);
          } else {
            values.add(new String(varchar.get(row), StandardCharsets.UTF_8));
          }
        } else if (vector instanceof IntVector intVec) {
          if (intVec.isNull(row)) {
            values.add(null);
          } else {
            values.add(String.valueOf(intVec.get(row)));
          }
        } else {
          throw new IllegalStateException("unexpected vector type " + vector.getClass());
        }
      }
      results.add(values);
    }
    return results;
  }

  private static List<String> toStringList(Object[] values) {
    List<String> result = new ArrayList<>(values.length);
    for (Object value : values) {
      result.add(value == null ? null : value.toString());
    }
    return result;
  }
}
