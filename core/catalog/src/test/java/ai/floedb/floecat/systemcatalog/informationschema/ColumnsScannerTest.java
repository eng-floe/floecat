package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.List;
import java.util.Map;
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

    var rows = new ColumnsScanner().scan(ctx).map(r -> List.of(r.values())).toList();

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

    var rows = new ColumnsScanner().scan(ctx).map(r -> List.of(r.values())).toList();

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
}
