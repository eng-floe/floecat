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
import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

class TablesScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new TablesScanner().schema())
        .extracting(c -> c.getName())
        .containsExactly("table_catalog", "table_schema", "table_name", "table_type");
  }

  @Test
  void scan_returnsOneRowPerTable() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");

    builder.addTable(ns, "mytable", Map.of(), Map.of());
    SystemObjectScanContext ctx = builder.build();

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "public", "mytable", "BASE TABLE");
  }

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("finance.sales");

    builder.addTable(ns, "nested_table", Map.of(), Map.of());
    SystemObjectScanContext ctx = builder.build();

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsExactly("catalog", "finance.sales", "nested_table", "BASE TABLE");
  }

  @Test
  void scanArrow_matchesRowPath() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    builder.addTable(ns, "arrow_table", Map.of(), Map.of());
    SystemObjectScanContext ctx = builder.build();

    var scanner = new TablesScanner();
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
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    builder.addTable(ns, "arrow_table", Map.of(), Map.of());
    SystemObjectScanContext ctx = builder.build();

    var scanner = new TablesScanner();
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
        VarCharVector varchar = (VarCharVector) vector;
        if (varchar.isNull(row)) {
          values.add(null);
        } else {
          values.add(new String(varchar.get(row), StandardCharsets.UTF_8));
        }
      }
      results.add(values);
    }
    return results;
  }

  private static List<String> toStringList(Object[] values) {
    List<String> list = new ArrayList<>(values.length);
    for (Object value : values) {
      list.add(value == null ? null : value.toString());
    }
    return list;
  }
}
