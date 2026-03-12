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

package ai.floedb.floecat.extensions.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ExampleCatalogExtension}: classpath loading, filesystem loading, config-driven
 * behaviour, and graceful degradation on errors.
 *
 * <p>SmallRye Config (test-scoped) provides the {@code ConfigProvider} implementation. System
 * properties are set / cleared in {@code try/finally} blocks to avoid inter-test contamination.
 */
class ExampleCatalogExtensionTest {

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Runs {@code body} with a system property set, restoring the previous value afterwards. */
  private static void withProp(String key, String value, Runnable body) {
    String prev = System.getProperty(key);
    System.setProperty(key, value);
    try {
      body.run();
    } finally {
      if (prev == null) System.clearProperty(key);
      else System.setProperty(key, prev);
    }
  }

  private static ExampleCatalogExtension ext() {
    return new ExampleCatalogExtension();
  }

  // ---------------------------------------------------------------------------
  // Classpath loading (default — no dir configured)
  // ---------------------------------------------------------------------------

  @Test
  void defaultEngineKindIsExample() {
    assertThat(ext().engineKind()).isEqualTo("example");
  }

  @Test
  void engineKindIsConfigurableViaProp() {
    withProp(
        ExampleCatalogExtension.CONFIG_ENGINE_KIND,
        "my-engine",
        () -> assertThat(ext().engineKind()).isEqualTo("my-engine"));
  }

  @Test
  void defaultLoadReturnsNonNullCatalog() {
    assertThat(ext().loadSystemCatalog()).isNotNull();
  }

  @Test
  void bundledCatalogHasTypes() {
    SystemCatalogData catalog = ext().loadSystemCatalog();
    assertThat(catalog.types()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasFunctions() {
    assertThat(ext().loadSystemCatalog().functions()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasOperators() {
    assertThat(ext().loadSystemCatalog().operators()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasCasts() {
    assertThat(ext().loadSystemCatalog().casts()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasCollations() {
    assertThat(ext().loadSystemCatalog().collations()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasAggregates() {
    assertThat(ext().loadSystemCatalog().aggregates()).isNotEmpty();
  }

  @Test
  void bundledCatalogHasNamespaces() {
    assertThat(ext().loadSystemCatalog().namespaces()).isNotEmpty();
  }

  @Test
  void bundledCatalogContainsPlaceholderTypeNames() {
    var typeNames =
        ext().loadSystemCatalog().types().stream().map(t -> t.name().getName()).toList();
    assertThat(typeNames).contains("my_type", "my_array_type");
  }

  @Test
  void bundledCatalogContainsPlaceholderFunctionNames() {
    var fnNames =
        ext().loadSystemCatalog().functions().stream().map(f -> f.name().getName()).toList();
    assertThat(fnNames).contains("my_function", "my_versioned_function");
  }

  @Test
  void bundledCatalogContainsExpectedOperator() {
    var ops = ext().loadSystemCatalog().operators();
    assertThat(ops).anySatisfy(op -> assertThat(op.name().getName()).isEqualTo("+"));
  }

  @Test
  void bundledCatalogContainsPlaceholderCast() {
    var castNames =
        ext().loadSystemCatalog().casts().stream().map(c -> c.name().getName()).toList();
    assertThat(castNames).contains("my_explicit_cast");
  }

  @Test
  void bundledCatalogContainsPlaceholderCollation() {
    var names =
        ext().loadSystemCatalog().collations().stream().map(c -> c.name().getName()).toList();
    assertThat(names).contains("my_collation");
  }

  @Test
  void bundledCatalogContainsPlaceholderAggregate() {
    var names =
        ext().loadSystemCatalog().aggregates().stream().map(a -> a.name().getName()).toList();
    assertThat(names).contains("my_aggregate");
  }

  @Test
  void versionConstraintIsPreservedOnVersionedFunction() {
    var fn =
        ext().loadSystemCatalog().functions().stream()
            .filter(f -> f.name().getName().equals("my_versioned_function"))
            .findFirst();
    assertThat(fn).isPresent();
    var rule =
        fn.get().engineSpecific().stream().filter(r -> !r.minVersion().isEmpty()).findFirst();
    assertThat(rule).isPresent();
    assertThat(rule.get().minVersion()).isEqualTo("2.0");
  }

  @Test
  void unversionedFunctionHasNoVersionConstraint() {
    var fn =
        ext().loadSystemCatalog().functions().stream()
            .filter(f -> f.name().getName().equals("my_function"))
            .findFirst();
    assertThat(fn).isPresent();
    assertThat(fn.get().engineSpecific()).allSatisfy(r -> assertThat(r.minVersion()).isEmpty());
  }

  @Test
  void propertiesMetadataIsPreserved() {
    var type =
        ext().loadSystemCatalog().types().stream()
            .filter(t -> t.name().getName().equals("my_type"))
            .findFirst();
    assertThat(type).isPresent();
    var rules = type.get().engineSpecific();
    assertThat(rules).isNotEmpty();
    boolean hasDescription =
        rules.stream().anyMatch(r -> r.properties().containsKey("description"));
    assertThat(hasDescription).isTrue();
  }

  @Test
  void scalarTypeIsNotAnArrayType() {
    var type =
        ext().loadSystemCatalog().types().stream()
            .filter(t -> t.name().getName().equals("my_type"))
            .findFirst();
    assertThat(type).isPresent();
    assertThat(type.get().array()).isFalse();
  }

  @Test
  void arrayTypeIsMarkedAsArray() {
    var arr =
        ext().loadSystemCatalog().types().stream()
            .filter(t -> t.name().getName().equals("my_array_type"))
            .findFirst();
    assertThat(arr).isPresent();
    assertThat(arr.get().array()).isTrue();
    assertThat(arr.get().elementType()).isNotNull();
    assertThat(arr.get().elementType().getName()).isEqualTo("my_type");
  }

  @Test
  void plusOperatorIsCommutativeAndAssociative() {
    var plus =
        ext().loadSystemCatalog().operators().stream()
            .filter(op -> op.name().getName().equals("+"))
            .findFirst();
    assertThat(plus).isPresent();
    assertThat(plus.get().isCommutative()).isTrue();
    assertThat(plus.get().isAssociative()).isTrue();
  }

  @Test
  void explicitCastHasCorrectMethod() {
    var cast =
        ext().loadSystemCatalog().casts().stream()
            .filter(c -> c.name().getName().equals("my_explicit_cast"))
            .findFirst();
    assertThat(cast).isPresent();
    assertThat(cast.get().method().name()).containsIgnoringCase("explicit");
  }

  @Test
  void aggregateHasExpectedFields() {
    var agg =
        ext().loadSystemCatalog().aggregates().stream()
            .filter(a -> a.name().getName().equals("my_aggregate"))
            .findFirst();
    assertThat(agg).isPresent();
    assertThat(agg.get().argumentTypes()).hasSize(1);
    assertThat(agg.get().argumentTypes().get(0).getName()).isEqualTo("my_type");
    assertThat(agg.get().stateType().getName()).isEqualTo("my_type");
    assertThat(agg.get().returnType().getName()).isEqualTo("my_type");
  }

  // ---------------------------------------------------------------------------
  // Filesystem loading
  // ---------------------------------------------------------------------------

  @Test
  void filesystemLoadingWorkWithSingleFile(@TempDir Path dir) throws IOException {
    writeFile(
        dir,
        "10_types.pbtxt",
        """
        types {
          name { name: "my_type" path: "example" }
          category: "N"
        }
        """);

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var typeNames =
              ext().loadSystemCatalog().types().stream().map(t -> t.name().getName()).toList();
          assertThat(typeNames).contains("my_type");
        });
  }

  @Test
  void filesystemLoadingIsAlphabetical(@TempDir Path dir) throws IOException {
    // "a_" sorts before "b_"
    writeFile(
        dir,
        "a_types.pbtxt",
        """
        types {
          name { name: "alpha_type" path: "example" }
          category: "N"
        }
        """);
    writeFile(
        dir,
        "b_types.pbtxt",
        """
        types {
          name { name: "beta_type" path: "example" }
          category: "N"
        }
        """);

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var types = ext().loadSystemCatalog().types();
          var names = types.stream().map(t -> t.name().getName()).toList();
          assertThat(names).containsExactly("alpha_type", "beta_type");
        });
  }

  @Test
  void filesystemOnlyLoadsPbtxtFiles(@TempDir Path dir) throws IOException {
    writeFile(
        dir,
        "10_types.pbtxt",
        """
        types {
          name { name: "good_type" path: "example" }
          category: "N"
        }
        """);
    // Non-pbtxt files must be ignored, not cause parse errors
    writeFile(dir, "README.txt", "this is not proto text {{{");
    writeFile(dir, "config.json", "{ invalid }");

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var catalog = ext().loadSystemCatalog();
          assertThat(catalog.types()).hasSize(1);
          assertThat(catalog.types().get(0).name().getName()).isEqualTo("good_type");
        });
  }

  @Test
  void emptyDirServesEmptyCatalogWithoutThrowing(@TempDir Path dir) {
    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () ->
            assertThatNoException()
                .isThrownBy(
                    () -> {
                      var catalog = ext().loadSystemCatalog();
                      assertThat(catalog.types()).isEmpty();
                      assertThat(catalog.functions()).isEmpty();
                    }));
  }

  @Test
  void filesystemOverridesClasspathWhenDirSet(@TempDir Path dir) throws IOException {
    // Write only a custom type — bundled placeholder types must NOT appear
    writeFile(
        dir,
        "10_types.pbtxt",
        """
        types {
          name { name: "custom_type" path: "example" }
          category: "N"
        }
        """);

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var typeNames =
              ext().loadSystemCatalog().types().stream().map(t -> t.name().getName()).toList();
          assertThat(typeNames).containsExactly("custom_type");
          assertThat(typeNames).doesNotContain("my_type", "my_array_type");
        });
  }

  @Test
  void multipleFilesAreMergedFromFilesystem(@TempDir Path dir) throws IOException {
    writeFile(
        dir,
        "10_types.pbtxt",
        """
        types {
          name { name: "type_a" path: "example" }
          category: "N"
        }
        """);
    writeFile(
        dir,
        "20_more_types.pbtxt",
        """
        types {
          name { name: "type_b" path: "example" }
          category: "S"
        }
        """);

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var typeNames =
              ext().loadSystemCatalog().types().stream().map(t -> t.name().getName()).toList();
          assertThat(typeNames).contains("type_a", "type_b");
        });
  }

  // ---------------------------------------------------------------------------
  // engine_kind stripping
  // ---------------------------------------------------------------------------

  @Test
  void engineKindInEngineSpecificIsStripped(@TempDir Path dir) throws IOException {
    // Write a type whose engine_specific block carries engine_kind: "wrong-engine".
    // After stripping, the rule must still be present (properties accessible) because
    // the engine_kind filter was removed before handing the registry to fromProto().
    // If stripping were absent, fromProto() would discard the rule (wrong engine kind)
    // and the property would not appear.
    writeFile(
        dir,
        "10_types.pbtxt",
        """
        types {
          name { name: "tagged_type" path: "example" }
          category: "N"
          engine_specific {
            engine_kind: "wrong-engine"
            properties { key: "marker" value: "present" }
          }
        }
        """);

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () -> {
          var types = ext().loadSystemCatalog().types();
          var t = types.stream().filter(x -> x.name().getName().equals("tagged_type")).findFirst();
          assertThat(t).isPresent();
          // The rule must survive: engine_kind was stripped so it now matches any engine.
          var rules = t.get().engineSpecific();
          assertThat(rules).isNotEmpty();
          boolean markerPresent =
              rules.stream().anyMatch(r -> "present".equals(r.properties().get("marker")));
          assertThat(markerPresent).isTrue();
        });
  }

  // ---------------------------------------------------------------------------
  // Error handling / graceful degradation
  // ---------------------------------------------------------------------------

  @Test
  void missingDirServesEmptyCatalogWithoutThrowing() {
    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        "/tmp/nonexistent-dir-floecat-example-test-xyz",
        () ->
            assertThatNoException()
                .isThrownBy(
                    () -> {
                      var catalog = ext().loadSystemCatalog();
                      assertThat(catalog.types()).isEmpty();
                    }));
  }

  @Test
  void invalidPbtxtFileIsSkippedAndOtherFilesStillLoad(@TempDir Path dir) throws IOException {
    writeFile(
        dir,
        "10_valid.pbtxt",
        """
        types {
          name { name: "good_type" path: "example" }
          category: "N"
        }
        """);
    writeFile(dir, "20_invalid.pbtxt", "THIS IS NOT PROTO TEXT {{{ %%%");

    withProp(
        ExampleCatalogExtension.CONFIG_BUILTINS_DIR,
        dir.toString(),
        () ->
            assertThatNoException()
                .isThrownBy(
                    () -> {
                      var catalog = ext().loadSystemCatalog();
                      // good_type from valid file must be present
                      var typeNames =
                          catalog.types().stream().map(t -> t.name().getName()).toList();
                      assertThat(typeNames).contains("good_type");
                    }));
  }

  @Test
  void classpathFallbackWhenNoDirConfigured() {
    // Ensure no dir property is set
    System.clearProperty(ExampleCatalogExtension.CONFIG_BUILTINS_DIR);
    // Bundled catalog must be loaded
    assertThat(ext().loadSystemCatalog().types()).isNotEmpty();
  }

  // ---------------------------------------------------------------------------
  // SPI stubs
  // ---------------------------------------------------------------------------

  @Test
  void supportsEngineReturnsTrueForExampleKind() {
    assertThat(ext().supportsEngine("example")).isTrue();
  }

  @Test
  void supportsEngineIsCaseInsensitive() {
    assertThat(ext().supportsEngine("EXAMPLE")).isTrue();
    assertThat(ext().supportsEngine("Example")).isTrue();
  }

  @Test
  void supportsEngineReturnsFalseForOtherKind() {
    assertThat(ext().supportsEngine("floedb")).isFalse();
    assertThat(ext().supportsEngine("postgres")).isFalse();
  }

  @Test
  void supportsEngineRespectsConfiguredKind() {
    withProp(
        ExampleCatalogExtension.CONFIG_ENGINE_KIND,
        "my-custom",
        () -> {
          assertThat(ext().supportsEngine("my-custom")).isTrue();
          assertThat(ext().supportsEngine("example")).isFalse();
        });
  }

  @Test
  void supportsReturnsFalseForAnyName() {
    // supports() always returns false — the example extension has no scanner-backed objects.
    // Use a default (empty) NameRef to avoid depending on the NameRef builder's internal API.
    assertThat(ext().supports(ai.floedb.floecat.common.rpc.NameRef.getDefaultInstance(), "example"))
        .isFalse();
    assertThat(ext().supports(null, "example")).isFalse();
  }

  @Test
  void provideReturnsEmpty() {
    assertThat(ext().provide("any_scanner", "example", "1.0")).isEmpty();
  }

  @Test
  void definitionsReturnsEmptyList() {
    assertThat(ext().definitions()).isEmpty();
  }

  @Test
  void decoratorReturnsEmpty() {
    assertThat(ext().decorator()).isEmpty();
  }

  @Test
  void onLoadErrorDoesNotThrow() {
    assertThatNoException().isThrownBy(() -> ext().onLoadError(new RuntimeException("test error")));
  }

  @Test
  void validateReturnsEmptyList() {
    var catalog = ext().loadSystemCatalog();
    assertThat(ext().validate(catalog)).isEmpty();
  }

  @Test
  void validateWithNullReturnsEmptyList() {
    assertThat(ext().validate(null)).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private static void writeFile(Path dir, String filename, String content) throws IOException {
    Files.writeString(dir.resolve(filename), content, StandardCharsets.UTF_8);
  }
}
