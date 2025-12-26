package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.Map;
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
}
