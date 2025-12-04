package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.query.rpc.BuiltinRegistry;
import ai.floedb.metacat.query.rpc.SqlFunction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class BuiltinCatalogLoaderTest {

  @Test
  void loadsCatalogFromClasspath() {
    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = BuiltinCatalogLoader.DEFAULT_LOCATION;
    loader.init();

    var catalog = loader.getCatalog("floe-demo");

    // FUNCTION NAMES
    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .containsExactlyInAnyOrder(
            nameRef("pg_catalog", "int4_abs"),
            nameRef("pg_catalog", "text_length"),
            nameRef("pg_catalog", "text_upper"),
            nameRef("pg_catalog", "int4_add"),
            nameRef("pg_catalog", "text_concat"),
            nameRef("pg_catalog", "sum_int4_state"),
            nameRef("pg_catalog", "sum_int4_final"));

    // ENGINE SPECIFIC RULE exists
    assertThat(catalog.functions().get(0).engineSpecific().get(0).floeFunction()).isNotNull();

    // ENGINE KIND preserved
    assertThat(catalog.functions().get(0).engineSpecific())
        .extracting(EngineSpecificRule::engineKind)
        .contains("floe-demo");

    // OPERATORS
    assertThat(catalog.operators())
        .extracting(BuiltinOperatorDef::name)
        .contains(nameRef("", "+"), nameRef("", "||"));

    // TYPES
    assertThat(catalog.types())
        .extracting(BuiltinTypeDef::name)
        .contains(nameRef("pg_catalog", "int4"), nameRef("pg_catalog", "int8"));

    // CASTS
    assertThat(catalog.casts())
        .extracting(c -> c.sourceType(), c -> c.targetType())
        .contains(tuple(nameRef("pg_catalog", "text"), nameRef("pg_catalog", "int4")));

    // COLLATIONS
    assertThat(catalog.collations())
        .extracting(BuiltinCollationDef::name)
        .contains(nameRef("pg_catalog", "default"));

    // AGGREGATES
    assertThat(catalog.aggregates())
        .extracting(BuiltinAggregateDef::name)
        .contains(nameRef("pg_catalog", "sum"));

    // cached instance
    assertThat(loader.getCatalog("floe-demo")).isSameAs(catalog);
  }

  @Test
  void throwsWhenMissing() {
    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = BuiltinCatalogLoader.DEFAULT_LOCATION;
    loader.init();

    assertThatThrownBy(() -> loader.getCatalog("missing"))
        .isInstanceOf(BuiltinCatalogNotFoundException.class);
  }

  @Test
  void corruptFileRaisesLoadException() throws IOException {
    Path dir = Files.createTempDirectory("builtins-test");
    Path file = dir.resolve("bad-version.pbtxt");
    Files.writeString(file, "not valid proto");

    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = "file:" + dir;
    loader.init();

    assertThatThrownBy(() -> loader.getCatalog("bad-version"))
        .isInstanceOf(BuiltinCatalogLoadException.class);
  }

  @Test
  void binaryFileLoads() throws IOException {
    Path dir = Files.createTempDirectory("builtins-bin-test");
    Path file = dir.resolve("bin-version.pb");

    BuiltinRegistry proto =
        BuiltinRegistry.newBuilder()
            .addFunctions(
                SqlFunction.newBuilder()
                    .setName(nameRef("pg_catalog", "identity"))
                    .addArgumentTypes(nameRef("pg_catalog", "int4"))
                    .setReturnType(nameRef("pg_catalog", "int4")))
            .build();

    Files.write(file, proto.toByteArray());

    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = "file:" + dir;
    loader.init();

    var catalog = loader.getCatalog("bin-version");

    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .contains(nameRef("pg_catalog", "identity"));
  }

  // helper for constructing NameRef
  private static NameRef nameRef(String schema, String name) {
    NameRef.Builder b = NameRef.newBuilder().setName(name);
    if (!schema.isBlank()) {
      b.addPath(schema);
    }
    return b.build();
  }
}
