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

import ai.floedb.floecat.arrow.ColumnarBatch;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.spi.SystemScanRequest;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  void scan_dedupesLeafWhenPathAlreadyContainsDisplayName() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace(List.of("org", "sales"), "sales", "org.sales");
    builder.addTable(ns, "orders", Map.of(), Map.of());
    SystemObjectScanContext ctx = builder.build();

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "org.sales", "orders", "BASE TABLE");
  }

  @Test
  void scan_usesTopologyNamespaceRefsWithoutMaterializingNamespaces() {
    ResourceId catalogId = rid("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("namespace", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("table", ResourceKind.RK_TABLE);
    var overlay = new NamespaceListingFailsOverlay();
    overlay.addNode(
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            "catalog",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    overlay
        .withNamespaceRef(namespaceId, "sales", catalogId, List.of("finance", "sales"))
        .withRelationRef(namespaceId, tableId, "orders", ResourceKind.RK_TABLE);
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), catalogId, EngineContext.empty());

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "finance.sales", "orders", "BASE TABLE");
  }

  @Test
  void scan_pushesSchemaAndNameConstraintsIntoTopologyRefs() {
    ResourceId catalogId = rid("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("sales", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("orders", ResourceKind.RK_TABLE);
    var overlay = new NamespaceListingFailsOverlay();
    overlay.addNode(
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            "catalog",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    overlay
        .withNamespaceRef(namespaceId, "sales", catalogId, List.of("sales"))
        .withRelationRef(namespaceId, tableId, "orders", ResourceKind.RK_TABLE)
        .failNamespaceRefs("schema predicate should use namespace lookup by name")
        .failRelationRefs("table_name predicate should use relation lookup by name");
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), catalogId, EngineContext.empty());
    SystemScanRequest request =
        SystemScanRequest.of(
            new Expr.And(
                eq("table_schema", "sales"),
                new Expr.Or(eq("table_name", "orders"), eq("table_name", "customers"))),
            List.of());

    var rows = new TablesScanner().scan(ctx, request).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "sales", "orders", "BASE TABLE");
    assertThat(overlay.namespaceNames()).containsExactly("sales");
    assertThat(overlay.relationNames(namespaceId)).containsExactlyInAnyOrder("orders", "customers");
  }

  @Test
  void scan_doesNotPushDottedSchemaConstraintIntoNamespaceLookupByName() {
    ResourceId catalogId = rid("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("foo.bar", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("orders", ResourceKind.RK_TABLE);
    var overlay = new NamespaceListingFailsOverlay();
    overlay.addNode(
        new CatalogNode(
            catalogId,
            1,
            Instant.EPOCH,
            "catalog",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    overlay
        .withNamespaceRef(namespaceId, "foo.bar", catalogId, List.of())
        .withRelationRef(namespaceId, tableId, "orders", ResourceKind.RK_TABLE)
        .failNamespaceRefsByName("dotted schema names must not use direct namespace lookup");
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            overlay, NameRef.getDefaultInstance(), catalogId, EngineContext.empty());
    SystemScanRequest request =
        SystemScanRequest.of(
            new Expr.And(eq("table_schema", "foo.bar"), eq("table_name", "orders")), List.of());

    var rows = new TablesScanner().scan(ctx, request).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "foo.bar", "orders", "BASE TABLE");
    assertThat(overlay.relationNames(namespaceId)).containsExactly("orders");
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

  private static ResourceId rid(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(id).setKind(kind).build();
  }

  private static Expr eq(String column, String value) {
    return new Expr.Eq(new Expr.ColumnRef(column), new Expr.Literal(value));
  }

  private static final class NamespaceListingFailsOverlay extends TestRefCatalogOverlay {
    @Override
    public List<NamespaceNode> listNamespaces(ResourceId catalogId) {
      throw new AssertionError("ref scan should not materialize namespaces");
    }
  }
}
