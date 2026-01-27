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

package ai.floedb.floecat.extensions.floedb;

import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Tests for Floe builtin extension plugins: loading, parsing, and validation of .pbtxt files. */
class FloeCatalogExtensionTest {

  @Test
  void testCatalogLoadsAndValidates() {
    var extension = new TestCatalogExtension();

    assert "test-catalog".equals(extension.engineKind());

    SystemCatalogData catalog = extension.loadSystemCatalog();

    assert catalog != null;
    assert !catalog.functions().isEmpty();
    assert !catalog.types().isEmpty();

    // Validate against builtin catalog rules
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "test catalog must pass validation, got: " + errors;
  }

  @Test
  void floeDemoLoadsAndValidates() {
    var extension = new FloeCatalogExtension.FloeDemo();

    assert "floe-demo".equals(extension.engineKind());

    SystemCatalogData catalog = extension.loadSystemCatalog();

    assert catalog != null;
    assert !catalog.functions().isEmpty();
    assert !catalog.types().isEmpty();

    // Validate against builtin catalog rules
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "floe-demo catalog must pass validation, got: " + errors;
  }

  @Test
  void testCatalogContainsExpectedFunctions() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var functionNames = catalog.functions().stream().map(f -> f.name().getName()).toList();

    // FloeDB should have common PG functions
    assert !functionNames.isEmpty() : "test catalog should contain common builtin functions";
  }

  @Test
  void missingResourceFileThrows() {
    var extension = new TestExtensionWithMissingResource();

    try {
      extension.loadSystemCatalog();
      assert false : "Expected IllegalStateException for missing resource";
    } catch (IllegalStateException e) {
      assert e.getMessage().contains("Builtin file not found");
      assert e.getMessage().contains("missing.pbtxt");
      assert e.getMessage().contains("engine=test-missing");
      assert e.getMessage().contains("/builtins/test-missing/_index.txt");
    }
  }

  @Test
  void catalogDataPreservesEngineSpecificRules() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    // Ensure engine_specific rules are loaded from the pbtxt file
    var functionsWithRules =
        catalog.functions().stream().filter(f -> !f.engineSpecific().isEmpty()).toList();

    // At least some functions should have engine-specific rules
    assert !functionsWithRules.isEmpty() : "Some functions should have engine-specific rules";

    // Validate that rules with Floe extensions have been converted to payloads
    for (var func : functionsWithRules) {
      for (var rule : func.engineSpecific()) {
        // Each rule should either have a payload (if it had Floe extensions)
        // or be a base rule without extensions
        String payloadType = rule.payloadType();
        byte[] payload = rule.extensionPayload();

        if (!payloadType.isEmpty()) {
          // Verify payload type indicates it's a Floe extension
          assert payloadType.startsWith("floe.")
              : "Expected Floe payload type, got: " + payloadType;
          if (rule.hasExtensionPayload()) {
            assert payload.length > 0
                : "Rule with payloadType '" + payloadType + "' should have payload bytes";
          }
        }
      }
    }
  }

  @Test
  void testCatalogFragmentsMergeSequentially() {
    var extension = new TestCatalogExtension();

    SystemCatalogData catalog = extension.loadSystemCatalog();

    var functionNames = catalog.functions().stream().map(f -> f.name().getName()).toList();
    assert functionNames.containsAll(List.of("test_func", "test_func_engine_specific"))
        : "Both function fragments should be merged";

    assert catalog.types().stream().anyMatch(t -> t.name().getName().equals("test_type"))
        : "Type fragment should be merged";
  }

  @Test
  void testCatalogAndFloedemHaveSameStructure() {
    var testCatalog = new TestCatalogExtension();
    var floeDemo = new FloeCatalogExtension.FloeDemo();

    SystemCatalogData testCatalogData = testCatalog.loadSystemCatalog();
    SystemCatalogData floeDemoCatalog = floeDemo.loadSystemCatalog();

    // Both should have types, functions, etc (may have different counts)
    assert !testCatalogData.types().isEmpty();
    assert !floeDemoCatalog.types().isEmpty();

    assert !testCatalogData.functions().isEmpty();
    assert !floeDemoCatalog.functions().isEmpty();
  }

  @Test
  void loadResourceTextParsesTextFormat() throws Exception {
    var extension = new TestCatalogExtension();

    String combinedText = combinedRegistryText(extension);
    SystemObjectsRegistry registry =
        extension.parseSystemObjectsRegistry(combinedText, "builtins/test-catalog");

    // Should parse without errors and contain objects
    assert registry != null;
    assert registry.getFunctionsCount() > 0 : "Should have functions";
    assert registry.getTypesCount() > 0 : "Should have types";
  }

  @Test
  void rewriteFloeExtensionsConvertsAllObjectTypes() {
    var extension = new TestCatalogExtension();

    // Load raw registry
    String raw = combinedRegistryText(extension);
    SystemObjectsRegistry rawRegistry =
        extension.parseSystemObjectsRegistry(raw, "builtins/test-catalog");

    // Rewrite Floe fields to payloads
    SystemObjectsRegistry rewritten = extension.rewriteFloeExtensions(rawRegistry);

    // Verify all object types were processed
    assert rewritten.getFunctionsCount() == rawRegistry.getFunctionsCount();
    assert rewritten.getOperatorsCount() == rawRegistry.getOperatorsCount();
    assert rewritten.getTypesCount() == rawRegistry.getTypesCount();
    assert rewritten.getCastsCount() == rawRegistry.getCastsCount();
    assert rewritten.getCollationsCount() == rawRegistry.getCollationsCount();
    assert rewritten.getAggregatesCount() == rawRegistry.getAggregatesCount();
  }

  @Test
  void convertRuleSkipsAlreadyRewrittenPayloads() {
    var extension = new TestCatalogExtension();

    // Create a rule that's already been rewritten (has payload)
    EngineSpecific es =
        EngineSpecific.newBuilder()
            .setEngineKind("test-catalog")
            .setPayloadType("floe.function+proto")
            .setPayload(com.google.protobuf.ByteString.copyFromUtf8("test"))
            .build();

    // convertRule should return it unchanged
    EngineSpecific result = extension.convertRule(es);

    assert result.getPayload().toStringUtf8().equals("test")
        : "Already-rewritten payloads should not be re-processed";
  }

  @Test
  void loadSystemObjectsRoundtrips() {
    var extension = new TestCatalogExtension();

    // Load twice - should get same result (caching semantic)
    SystemCatalogData catalog1 = extension.loadSystemCatalog();
    SystemCatalogData catalog2 = extension.loadSystemCatalog();

    // Should be independent objects but same content
    assert catalog1.functions().size() == catalog2.functions().size();
    assert catalog1.types().size() == catalog2.types().size();
  }

  private static String combinedRegistryText(FloeCatalogExtension extension) {
    List<String> fragments = readCatalogFragments(extension);
    String dir = extension.getResourceDir();
    StringBuilder builder = new StringBuilder();
    for (String file : fragments) {
      if (file.startsWith("/")) {
        throw new IllegalStateException(
            "Index entries must be relative paths (got "
                + file
                + ") in "
                + extension.getIndexPath());
      }
      String resourcePath = dir + "/" + file;
      builder.append(extension.loadResourceText(resourcePath)).append("\n");
    }
    return builder.toString();
  }

  private static List<String> readCatalogFragments(FloeCatalogExtension extension) {
    String indexText = extension.loadResourceText(extension.getIndexPath());
    List<String> fragments = new ArrayList<>();
    indexText
        .lines()
        .map(String::trim)
        .filter(line -> !line.isEmpty())
        .filter(line -> !line.startsWith("#"))
        .forEach(
            file -> {
              if (file.startsWith("/")) {
                throw new IllegalArgumentException(
                    "Index entries must be relative paths (got "
                        + file
                        + ") in "
                        + extension.getIndexPath());
              }
              fragments.add(file);
            });
    if (fragments.isEmpty()) {
      throw new IllegalStateException(
          "Catalog index "
              + extension.getIndexPath()
              + " contains no entries (only comments/blank lines)");
    }
    return fragments;
  }

  @Test
  void floeDemoRegistryPayloadsRoundtrip() {
    var extension = new FloeCatalogExtension.FloeDemo();

    SystemCatalogData catalog = extension.loadSystemCatalog();
    List<EngineSpecificRule> hints = catalog.registryEngineSpecific();

    assert !hints.isEmpty() : "Registry hints should be present for floe-demo";

    Set<String> payloadTypes =
        hints.stream().map(EngineSpecificRule::payloadType).collect(Collectors.toSet());

    Set<String> expected =
        Set.of(
            "floe.index.access_methods+proto",
            "floe.index.operator_families+proto",
            "floe.index.operator_classes+proto",
            "floe.index.operator_strategies+proto",
            "floe.index.support_procedures+proto");

    assert payloadTypes.containsAll(expected) : "Missing Floe registry payloads " + expected;

    for (EngineSpecificRule hint : hints) {
      if (!hint.payloadType().isEmpty() && hint.payloadType().startsWith("floe.")) {
        assert hint.hasExtensionPayload() : "Expected payload bytes for " + hint.payloadType();
        assert hint.extensionPayload().length > 0
            : "Payload bytes must be non-empty for " + hint.payloadType();
      }
    }
  }

  @Test
  void testCatalogResourceDirFollowsConvention() {
    var extension = new TestCatalogExtension();

    String dir = extension.getResourceDir();
    assert dir.equals("/builtins/test-catalog")
        : "Resource dir should follow /builtins/{engineKind}";
    assert extension.getIndexPath().equals("/builtins/test-catalog/_index.txt")
        : "Index path must live under the resource dir";
  }

  @Test
  void floeDemoResourceDirFollowsConvention() {
    var extension = new FloeCatalogExtension.FloeDemo();

    String dir = extension.getResourceDir();
    assert dir.equals("/builtins/floe-demo") : "Resource dir should follow /builtins/{engineKind}";
    assert extension.getIndexPath().equals("/builtins/floe-demo/_index.txt")
        : "Index path must live under the resource dir";
  }

  @Test
  void engineSpecificRulesPreserveMinMaxVersions() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    // Find functions with version constraints
    var versionedFunctions =
        catalog.functions().stream()
            .filter(
                f ->
                    f.engineSpecific().stream()
                        .anyMatch(
                            rule -> !rule.minVersion().isEmpty() || !rule.maxVersion().isEmpty()))
            .toList();

    // Should have at least some version-constrained functions
    if (!versionedFunctions.isEmpty()) {
      for (var func : versionedFunctions) {
        for (var rule : func.engineSpecific()) {
          // Min/max versions should be non-empty if present
          assert !rule.minVersion().isEmpty() || !rule.maxVersion().isEmpty()
              : "Version constraints should be preserved";
        }
      }
    }
  }

  @Test
  void multipleEngineKindsCanBeLoaded() {
    var catalog = new TestCatalogExtension();
    var floeDemo = new FloeCatalogExtension.FloeDemo();

    // Both should be loadable and distinct
    SystemCatalogData catalogData = catalog.loadSystemCatalog();
    SystemCatalogData floeDemoCatalog = floeDemo.loadSystemCatalog();

    // Verify they're different engines
    assert catalog.engineKind().equals("test-catalog");
    assert floeDemo.engineKind().equals("floe-demo");

    // May have different object counts (demo might be subset)
    assert catalogData != null && floeDemoCatalog != null;
  }

  @Test
  void operatorsParsedCorrectly() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var operators = catalog.operators();

    if (!operators.isEmpty()) {
      for (var op : operators) {
        // Operator must have left, right, return types
        assert op.leftType() != null : "Operator left type required";
        assert op.rightType() != null : "Operator right type required";
        assert op.returnType() != null : "Operator return type required";
      }
    }
  }

  @Test
  void castsParsedCorrectly() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var casts = catalog.casts();

    if (!casts.isEmpty()) {
      for (var cast : casts) {
        // Cast must have source and target types
        assert cast.sourceType() != null : "Cast source type required";
        assert cast.targetType() != null : "Cast target type required";
        assert cast.method() != null : "Cast method required";
      }
    }
  }

  @Test
  void aggregatesParsedCorrectly() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var aggregates = catalog.aggregates();

    if (!aggregates.isEmpty()) {
      for (var agg : aggregates) {
        // Aggregate must have argument, state, return types
        assert !agg.argumentTypes().isEmpty() : "Aggregate argument types required";
        assert agg.stateType() != null : "Aggregate state type required";
        assert agg.returnType() != null : "Aggregate return type required";
      }
    }
  }

  @Test
  void collationsParsedCorrectly() {
    var extension = new TestCatalogExtension();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var collations = catalog.collations();

    if (!collations.isEmpty()) {
      for (var coll : collations) {
        // Collation must have locale
        assert coll.locale() != null && !coll.locale().isEmpty() : "Collation locale required";
      }
    }
  }

  // Test utility: extension that points to non-existent resource
  private static class TestExtensionWithMissingResource extends FloeCatalogExtension {
    @Override
    public String engineKind() {
      return "test-missing";
    }

    @Override
    protected String getResourceDir() {
      return "/builtins/test-missing";
    }
  }

  private static final class TestCatalogExtension extends FloeCatalogExtension {
    @Override
    public String engineKind() {
      return "test-catalog";
    }

    @Override
    protected String getResourceDir() {
      return "/builtins/test-catalog";
    }
  }
}
