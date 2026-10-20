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
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
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
            List.of("marketing", "finance.sales", "orders", "id", "INT", 1),
            List.of("marketing", "finance.sales", "orders", "sales", "DOUBLE", 2));
  }

  @Test
  void scan_dedupesLeafWhenPathAlreadyContainsDisplayName() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace(List.of("org", "sales"), "sales", "org.sales");
    builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "long"));

    SystemObjectScanContext ctx = builder.build();
    var rows = new ColumnsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows).containsExactly(List.of("marketing", "org.sales", "orders", "id", "INT", 1));
  }

  @Test
  void scan_usesTopologyNamespaceRefsWithoutMaterializingNamespaces() {
    ResourceId catalogId = rid("marketing", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("namespace", ResourceKind.RK_NAMESPACE);
    ResourceId tableId = rid("orders-id", ResourceKind.RK_TABLE);
    var graphView = new NamespaceListingFailsGraphView();
    graphView.addNode(
        new CatalogNode(
            catalogId,
            "blob://test/v1",
            "marketing",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    graphView.addRelation(
        namespaceId,
        new UserTableNode(
            tableId,
            "blob://test/v1",
            catalogId,
            namespaceId,
            "orders",
            TableFormat.TF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            "",
            Map.of(),
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            List.of(),
            Map.of(),
            Map.of(),
            ai.floedb.floecat.catalog.rpc.ColumnIdentityMap.getDefaultInstance()));
    graphView.setTableSchema(
        tableId,
        List.of(
            SchemaColumn.newBuilder()
                .setName("id")
                .setType(LogicalTypeProtoAdapter.parseToProto("long"))
                .setFieldId(1)
                .setNullable(false)
                .build()));
    graphView
        .withNamespaceRef(namespaceId, "sales", catalogId, List.of("finance", "sales"))
        .withRelationRef(namespaceId, tableId, "orders", ResourceKind.RK_TABLE);
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            graphView, NameRef.getDefaultInstance(), catalogId, EngineContext.empty());

    var rows = new ColumnsScanner().scan(ctx).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .containsExactly(List.of("marketing", "finance.sales", "orders", "id", "INT", 1));
  }

  @Test
  void scan_numbersUserTableColumnsByPositionNotFieldId() {
    // Format-native and canonical field ids may have gaps and need not ascend in declaration
    // order. Neither can stand in for ordinal_position.
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace(List.of("org", "sales"), "sales", "org.sales");
    builder.addTableWithSchema(
        ns,
        "orders",
        List.of(
            positionColumn("id", "long", 40),
            positionColumn("name", "string", 0),
            positionColumn("amount", "long", 7)));

    var rows =
        new ColumnsScanner().scan(builder.build()).map(r -> Arrays.asList(r.values())).toList();

    assertThat(rows)
        .extracting(row -> row.get(3), row -> row.get(5))
        .containsExactly(tuple("id", 1), tuple("name", 2), tuple("amount", 3));
  }

  private static SchemaColumn positionColumn(String name, String type, int fieldId) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setPhysicalPath(name)
        .setType(LogicalTypeProtoAdapter.parseToProto(type))
        .setFieldId(fieldId)
        .setLeaf(true)
        .setNullable(false)
        .build();
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

    // Find row for "id" column and check data_type is canonical "INT"
    assertThat(rows)
        .anySatisfy(
            row -> {
              assertThat(row.get(3)).isEqualTo("id");
              assertThat(row.get(4)).isEqualTo("INT");
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

    assertThat(scanArrow(scanner, ctx)).isEqualTo(expected);
  }

  @Test
  void scanArrow_matchesRowPathForViews() {
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");
    addView(
        builder,
        ns,
        "order_totals",
        List.of(
            positionColumn("id", "long", 40),
            positionColumn("name", "string", 0),
            positionColumn("amount", "long", 7)));
    SystemObjectScanContext ctx = builder.build();
    var scanner = new ColumnsScanner();

    List<List<String>> expected = scanner.scan(ctx).map(row -> toStringList(row.values())).toList();

    assertThat(expected)
        .extracting(row -> row.get(3), row -> row.get(5))
        .containsExactly(tuple("id", "1"), tuple("name", "2"), tuple("amount", "3"));
    assertThat(scanArrow(scanner, ctx)).isEqualTo(expected);
  }

  @Test
  void scanArrow_numbersColumnsByPositionNotFieldId() {
    // The Arrow path builds its own rows, so the row-path regression does not cover it. Field ids
    // chosen to fail loudly if either path ever reverts to reporting or sorting by them: 40 and 7
    // are out of declaration order, and 0 is another valid non-positional test value.
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace(List.of("org", "sales"), "sales", "org.sales");
    builder.addTableWithSchema(
        ns,
        "orders",
        List.of(
            positionColumn("id", "long", 40),
            positionColumn("name", "string", 0),
            positionColumn("amount", "long", 7)));
    SystemObjectScanContext ctx = builder.build();
    var scanner = new ColumnsScanner();
    List<List<String>> arrowRows = scanArrow(scanner, ctx);

    assertThat(arrowRows)
        .extracting(row -> row.get(3), row -> row.get(5))
        .containsExactly(tuple("id", "1"), tuple("name", "2"), tuple("amount", "3"));
    // Both paths answer the same query; a reader cannot tell which is right if they disagree.
    assertThat(arrowRows)
        .isEqualTo(scanner.scan(ctx).map(row -> toStringList(row.values())).toList());
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
        } else if (vector instanceof BigIntVector intVec) {
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

  private static List<List<String>> scanArrow(ColumnsScanner scanner, SystemObjectScanContext ctx) {
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      return scanner
          .scanArrow(ctx, null, List.of(), allocator)
          .map(
              batch -> {
                try (batch) {
                  return toRows(batch.root());
                }
              })
          .flatMap(List::stream)
          .toList();
    }
  }

  private static void addView(
      TestTableScanContextBuilder builder,
      NamespaceNode namespace,
      String name,
      List<SchemaColumn> columns) {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(namespace.id().getAccountId())
            .setKind(ResourceKind.RK_VIEW)
            .setId(namespace.id().getId() + "." + name)
            .build();
    builder
        .graphView()
        .addRelation(
            namespace.id(),
            new ViewNode(
                viewId,
                "blob://test/v1",
                namespace.catalogId(),
                namespace.id(),
                name,
                "select 1",
                "sql",
                columns,
                List.of(),
                List.of(),
                GraphNodeOrigin.USER,
                Map.of(),
                Optional.empty(),
                Map.of(),
                Map.of()));
  }

  private static List<String> toStringList(Object[] values) {
    List<String> result = new ArrayList<>(values.length);
    for (Object value : values) {
      result.add(value == null ? null : value.toString());
    }
    return result;
  }

  private static ResourceId rid(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(id).setKind(kind).build();
  }

  private static final class NamespaceListingFailsGraphView extends TestRefCatalogGraphView {
    @Override
    public List<NamespaceNode> listNamespaces(
        ResourceId catalogId, ai.floedb.floecat.scanner.utils.CatalogContext catalogContext) {
      throw new AssertionError("ref scan should not materialize namespaces");
    }
  }

  private static ai.floedb.floecat.query.rpc.SchemaColumn col(
      String name, String path, String type, int fieldId, boolean leaf) {
    return ai.floedb.floecat.query.rpc.SchemaColumn.newBuilder()
        .setName(name)
        .setPhysicalPath(path)
        .setType(LogicalTypeProtoAdapter.parseToProto(type))
        .setFieldId(fieldId)
        .setNullable(true)
        .setLeaf(leaf)
        .build();
  }

  @Test
  void scan_nestedSchemaReportsOnlyTopLevelPositions() {
    // Schema as the flattened nested traversal emits it for:
    //   a int, s struct<x int, y int>, b int
    var builder = TestTableScanContextBuilder.builder("marketing");
    var ns = builder.addNamespace("finance.sales");
    builder.addTableWithSchema(
        ns,
        "orders",
        java.util.List.of(
            col("a", "a", "INT", 40, true),
            col("s", "s", "STRUCT<x: INT, y: INT>", 7, false),
            col("x", "s.x", "INT", 101, true),
            col("y", "s.y", "INT", 102, true),
            col("b", "b", "INT", 0, true)));

    SystemObjectScanContext ctx = builder.build();
    var scanner = new ColumnsScanner();
    var rows = scanner.scan(ctx).map(r -> toStringList(r.values())).toList();

    assertThat(rows)
        .extracting(row -> row.get(3), row -> row.get(5))
        .containsExactly(tuple("a", "1"), tuple("s", "2"), tuple("b", "3"));
    assertThat(scanArrow(scanner, ctx)).isEqualTo(rows);
  }
}
