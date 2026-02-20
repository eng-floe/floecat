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
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

class SchemataScannerTest {

  @Test
  void schema_isCorrect() {
    assertThat(new SchemataScanner().schema())
        .extracting(c -> c.getName())
        .containsExactly("catalog_name", "schema_name");
  }

  @Test
  void scan_returnsAllNamespacesInCatalog() {
    var builder = TestTableScanContextBuilder.builder("main_catalog");
    builder.addNamespace("public");
    builder.addNamespace("sales");
    SystemObjectScanContext ctx = builder.build();

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows)
        .containsExactlyInAnyOrder(
            List.of("main_catalog", "public"), List.of("main_catalog", "sales"));
  }

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    var builder = TestTableScanContextBuilder.builder("main_catalog");
    builder.addNamespace("finance.sales");
    SystemObjectScanContext ctx = builder.build();

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows).containsExactly(List.of("main_catalog", "finance.sales"));
  }

  @Test
  void scanArrow_matchesRowPath() {
    var builder = TestTableScanContextBuilder.builder("main_catalog");
    builder.addNamespace("public");
    SystemObjectScanContext ctx = builder.build();

    var scanner = new SchemataScanner();
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
    var builder = TestTableScanContextBuilder.builder("main_catalog");
    builder.addNamespace("public");
    SystemObjectScanContext ctx = builder.build();

    var scanner = new SchemataScanner();
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
