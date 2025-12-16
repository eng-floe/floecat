package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.metagraph.model.*;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestScanContextBuilder;
import java.util.List;
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
    var builder = TestScanContextBuilder.builder().withCatalog("catalog").withNamespace("public");

    UserTableNode table = builder.newTable("mytable", Map.of());

    SystemObjectScanContext ctx = builder.withTable(table).build();

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0)).containsExactly("catalog", "public", "mytable", "BASE TABLE");
  }

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    TestScanContextBuilder builder =
        TestScanContextBuilder.builder().withCatalog("catalog").withNamespace("finance.sales");

    NamespaceNode ns = builder.newNamespace("finance.sales");

    builder.withNamespaces(List.of(ns));

    UserTableNode table = builder.newTable("nested_table", Map.of());

    SystemObjectScanContext ctx = builder.withTable(table).build();

    var rows = new TablesScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0))
        .containsExactly("catalog", "finance.sales", "nested_table", "BASE TABLE");
  }
}
