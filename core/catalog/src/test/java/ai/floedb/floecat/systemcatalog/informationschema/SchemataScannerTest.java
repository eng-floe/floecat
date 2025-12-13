package ai.floedb.floecat.systemcatalog.informationschema;

import static org.assertj.core.api.Assertions.*;

import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestScanContextBuilder;
import java.util.List;
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
    var builder =
        TestScanContextBuilder.builder()
            .withCatalog("main_catalog")
            .withNamespaces(
                List.of(
                    TestScanContextBuilder.builder()
                        .withCatalog("main_catalog")
                        .newNamespace("public"),
                    TestScanContextBuilder.builder()
                        .withCatalog("main_catalog")
                        .newNamespace("sales")));

    SystemObjectScanContext ctx = builder.build();

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows)
        .containsExactlyInAnyOrder(
            List.of("main_catalog", "public"), List.of("main_catalog", "sales"));
  }

  @Test
  void scan_usesCanonicalSchemaPathForNestedNamespaces() {
    var builder =
        TestScanContextBuilder.builder()
            .withCatalog("main_catalog")
            .withNamespaces(
                List.of(
                    TestScanContextBuilder.builder()
                        .withCatalog("main_catalog")
                        .newNamespace("finance.sales")));

    SystemObjectScanContext ctx = builder.build();

    var rows = new SchemataScanner().scan(ctx).map(r -> List.of(r.values())).toList();

    assertThat(rows).containsExactly(List.of("main_catalog", "finance.sales"));
  }
}
