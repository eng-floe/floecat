package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.metacat.query.rpc.BuiltinRegistry;
import ai.floedb.metacat.query.rpc.SqlFunction;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class BuiltinCatalogLoaderTest {

  /** Ensures the sample pbtxt file loads and every entity matches the expected metadata. */
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
            "pg_catalog.int4_abs",
            "pg_catalog.text_length",
            "pg_catalog.text_upper",
            "pg_catalog.int4_add",
            "pg_catalog.text_concat",
            "pg_catalog.sum_int4_state",
            "pg_catalog.sum_int4_final");

    // ENGINE SPECIFIC RULE: floe_function present
    assertThat(catalog.functions().get(0).engineSpecific().get(0).floeFunction()).isNotNull();

    // ENGINE KIND comes from EngineSpecificRule.engineKind (still part of the rule)
    assertThat(catalog.functions().get(0).engineSpecific())
        .extracting(EngineSpecificRule::engineKind)
        .contains("floe-demo");

    // OPERATORS – no more functionName field in new model → removed test
    assertThat(catalog.operators()).extracting(BuiltinOperatorDef::name).contains("+", "||");

    // TYPES
    assertThat(catalog.types())
        .extracting(BuiltinTypeDef::name)
        .contains("pg_catalog.int4", "pg_catalog.int8");

    // CASTS
    assertThat(catalog.casts())
        .extracting(BuiltinCastDef::sourceType, BuiltinCastDef::targetType)
        .contains(tuple("pg_catalog.text", "pg_catalog.int4"));

    // COLLATIONS
    assertThat(catalog.collations())
        .extracting(BuiltinCollationDef::name)
        .contains("pg_catalog.default");

    // AGGREGATES
    assertThat(catalog.aggregates())
        .extracting(BuiltinAggregateDef::name)
        .contains("pg_catalog.sum");

    // Caching: loader returns same instance
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

  /** Verifies corrupted files raise a dedicated load exception. */
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

  /** Confirms the loader can parse binary .pb files in addition to pbtxt fixtures. */
  @Test
  void binaryFileLoads() throws IOException {
    Path dir = Files.createTempDirectory("builtins-bin-test");
    Path file = dir.resolve("bin-version.pb");

    BuiltinRegistry proto =
        BuiltinRegistry.newBuilder()
            .addFunctions(
                SqlFunction.newBuilder()
                    .setName("pg_catalog.identity")
                    .addArgumentTypes("pg_catalog.int4")
                    .setReturnType("pg_catalog.int4"))
            .build();

    Files.write(file, proto.toByteArray());

    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = "file:" + dir;
    loader.init();

    var catalog = loader.getCatalog("bin-version");

    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .contains("pg_catalog.identity");
  }
}
