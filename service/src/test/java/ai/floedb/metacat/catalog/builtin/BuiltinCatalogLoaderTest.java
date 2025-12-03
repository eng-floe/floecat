package ai.floedb.metacat.catalog.builtin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import ai.floedb.metacat.catalog.rpc.BuiltinCatalog;
import ai.floedb.metacat.catalog.rpc.BuiltinFunction;
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

    var catalog = loader.getCatalog("demo-pg-builtins");
    assertThat(catalog.version()).isEqualTo("demo-pg-builtins");
    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .containsExactlyInAnyOrder(
            "pg_catalog.int4_abs",
            "pg_catalog.text_length",
            "pg_catalog.text_upper",
            "pg_catalog.int4_add",
            "pg_catalog.text_concat",
            "pg_catalog.sum_int4_state",
            "pg_catalog.sum_int4_final",
            "pg_catalog.floedb_secret");
    assertThat(catalog.functions().get(0).engineSpecific().get(0).properties())
        .containsEntry("oid", "1250");
    assertThat(catalog.functions().get(0).engineSpecific())
        .extracting(EngineSpecificRule::engineKind)
        .contains("postgres");
    assertThat(catalog.operators())
        .extracting(BuiltinOperatorDef::functionName)
        .contains("pg_catalog.int4_add", "pg_catalog.text_concat");
    assertThat(catalog.types())
        .extracting(BuiltinTypeDef::name)
        .contains("pg_catalog.int4", "pg_catalog.int8");
    assertThat(catalog.casts())
        .extracting(BuiltinCastDef::sourceType, BuiltinCastDef::targetType)
        .contains(tuple("pg_catalog.text", "pg_catalog.int4"));
    assertThat(catalog.collations())
        .extracting(BuiltinCollationDef::name)
        .contains("pg_catalog.default");
    assertThat(catalog.aggregates())
        .extracting(BuiltinAggregateDef::name)
        .contains("pg_catalog.sum");
    assertThat(loader.getCatalog("demo-pg-builtins")).isSameAs(catalog);
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
    Path file = dir.resolve("builtin_catalog_bad-version.pbtxt");
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
    Path file = dir.resolve("builtin_catalog_bin-version.pb");

    BuiltinCatalog proto =
        BuiltinCatalog.newBuilder()
            .setVersion("bin-version")
            .addFunctions(
                BuiltinFunction.newBuilder()
                    .setName("pg_catalog.identity")
                    .addArgumentTypes("pg_catalog.int4")
                    .setReturnType("pg_catalog.int4"))
            .build();
    Files.write(file, proto.toByteArray());

    BuiltinCatalogLoader loader = new BuiltinCatalogLoader();
    loader.configuredLocation = "file:" + dir;
    loader.init();

    var catalog = loader.getCatalog("bin-version");
    assertThat(catalog.version()).isEqualTo("bin-version");
    assertThat(catalog.functions())
        .extracting(BuiltinFunctionDef::name)
        .contains("pg_catalog.identity");
  }
}
