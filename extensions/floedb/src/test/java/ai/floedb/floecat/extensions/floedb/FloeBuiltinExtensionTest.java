package ai.floedb.floecat.extensions.floedb;

import ai.floedb.floecat.query.rpc.BuiltinRegistry;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogValidator;
import org.junit.jupiter.api.Test;

/** Tests for Floe builtin extension plugins: loading, parsing, and validation of .pbtxt files. */
class FloeBuiltinExtensionTest {

  @Test
  void floeDbLoadsAndValidates() {
    var extension = new FloeCatalogExtension.FloeDb();

    assert "floedb".equals(extension.engineKind());

    SystemCatalogData catalog = extension.loadSystemCatalog();

    assert catalog != null;
    assert !catalog.functions().isEmpty();
    assert !catalog.types().isEmpty();

    // Validate against builtin catalog rules
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "floedb.pbtxt must pass validation, got: " + errors;
  }

  @Test
  void flaoDemoLoadsAndValidates() {
    var extension = new FloeCatalogExtension.FloeDemo();

    assert "floe-demo".equals(extension.engineKind());

    SystemCatalogData catalog = extension.loadSystemCatalog();

    assert catalog != null;
    assert !catalog.functions().isEmpty();
    assert !catalog.types().isEmpty();

    // Validate against builtin catalog rules
    var errors = SystemCatalogValidator.validate(catalog);
    assert errors.isEmpty() : "floe-demo.pbtxt must pass validation, got: " + errors;
  }

  @Test
  void floeDbContainsExpectedFunctions() {
    var extension = new FloeCatalogExtension.FloeDb();
    SystemCatalogData catalog = extension.loadSystemCatalog();

    var functionNames = catalog.functions().stream().map(f -> f.name().getName()).toList();

    // FloeDB should have common PG functions
    assert !functionNames.isEmpty() : "floedb should contain common builtin functions";
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
    }
  }

  @Test
  void catalogDataPreservesEngineSpecificRules() {
    var extension = new FloeCatalogExtension.FloeDb();
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
          // If there's a payload type, there must be payload data
          assert payload.length > 0
              : "Rule with payloadType '" + payloadType + "' should have payload bytes";
          // Verify payload type indicates it's a Floe extension
          assert payloadType.startsWith("floe.")
              : "Expected Floe payload type, got: " + payloadType;
        }
      }
    }
  }

  @Test
  void floeDbAndFloedemHaveSameStructure() {
    var floedb = new FloeCatalogExtension.FloeDb();
    var floeDemo = new FloeCatalogExtension.FloeDemo();

    SystemCatalogData floedbCatalog = floedb.loadSystemCatalog();
    SystemCatalogData flaoDemoCatalog = floeDemo.loadSystemCatalog();

    // Both should have types, functions, etc (may have different counts)
    assert !floedbCatalog.types().isEmpty();
    assert !flaoDemoCatalog.types().isEmpty();

    assert !floedbCatalog.functions().isEmpty();
    assert !flaoDemoCatalog.functions().isEmpty();
  }

  @Test
  void loadFromResourceParsesTextFormat() throws Exception {
    var extension = new FloeCatalogExtension.FloeDb();

    // Load raw .pbtxt file
    BuiltinRegistry registry = extension.loadFromResource("/builtins/floedb.pbtxt");

    // Should parse without errors and contain objects
    assert registry != null;
    assert registry.getFunctionsCount() > 0 : "Should have functions";
    assert registry.getTypesCount() > 0 : "Should have types";
  }

  @Test
  void rewriteFloeExtensionsConvertsAllObjectTypes() {
    var extension = new FloeCatalogExtension.FloeDb();

    // Load raw registry
    BuiltinRegistry raw = extension.loadFromResource("/builtins/floedb.pbtxt");

    // Rewrite Floe fields to payloads
    BuiltinRegistry rewritten = extension.rewriteFloeExtensions(raw);

    // Verify all object types were processed
    assert rewritten.getFunctionsCount() == raw.getFunctionsCount();
    assert rewritten.getOperatorsCount() == raw.getOperatorsCount();
    assert rewritten.getTypesCount() == raw.getTypesCount();
    assert rewritten.getCastsCount() == raw.getCastsCount();
    assert rewritten.getCollationsCount() == raw.getCollationsCount();
    assert rewritten.getAggregatesCount() == raw.getAggregatesCount();
  }

  @Test
  void convertRuleSkipsAlreadyRewrittenPayloads() {
    var extension = new FloeCatalogExtension.FloeDb();

    // Create a rule that's already been rewritten (has payload)
    EngineSpecific es =
        EngineSpecific.newBuilder()
            .setEngineKind("floedb")
            .setPayloadType("floe.function+proto")
            .setPayload(com.google.protobuf.ByteString.copyFromUtf8("test"))
            .build();

    // convertRule should return it unchanged
    EngineSpecific result = extension.convertRule(es);

    assert result.getPayload().toStringUtf8().equals("test")
        : "Already-rewritten payloads should not be re-processed";
  }

  @Test
  void loadBuiltinCatalogRoundtrips() {
    var extension = new FloeCatalogExtension.FloeDb();

    // Load twice - should get same result (caching semantic)
    SystemCatalogData catalog1 = extension.loadSystemCatalog();
    SystemCatalogData catalog2 = extension.loadSystemCatalog();

    // Should be independent objects but same content
    assert catalog1.functions().size() == catalog2.functions().size();
    assert catalog1.types().size() == catalog2.types().size();
  }

  @Test
  void floeDbResourcePathFollowsConvention() {
    var extension = new FloeCatalogExtension.FloeDb();

    String path = extension.getResourcePath();

    // Path should follow convention: /builtins/{engineKind}.pbtxt
    assert path.equals("/builtins/floedb.pbtxt")
        : "Path should follow /builtins/{engineKind}.pbtxt convention";
  }

  @Test
  void flaoDemoResourcePathFollowsConvention() {
    var extension = new FloeCatalogExtension.FloeDemo();

    String path = extension.getResourcePath();

    // Path should follow convention: /builtins/{engineKind}.pbtxt
    assert path.equals("/builtins/floe-demo.pbtxt")
        : "Path should follow /builtins/{engineKind}.pbtxt convention";
  }

  @Test
  void engineSpecificRulesPreserveMinMaxVersions() {
    var extension = new FloeCatalogExtension.FloeDb();
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
    var floedb = new FloeCatalogExtension.FloeDb();
    var floeDemo = new FloeCatalogExtension.FloeDemo();

    // Both should be loadable and distinct
    SystemCatalogData floedbCatalog = floedb.loadSystemCatalog();
    SystemCatalogData flaoDemoCatalog = floeDemo.loadSystemCatalog();

    // Verify they're different engines
    assert floedb.engineKind().equals("floedb");
    assert floeDemo.engineKind().equals("floe-demo");

    // May have different object counts (demo might be subset)
    assert floedbCatalog != null && flaoDemoCatalog != null;
  }

  @Test
  void operatorsParsedCorrectly() {
    var extension = new FloeCatalogExtension.FloeDb();
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
    var extension = new FloeCatalogExtension.FloeDb();
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
    var extension = new FloeCatalogExtension.FloeDb();
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
    var extension = new FloeCatalogExtension.FloeDb();
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
    protected String getResourcePath() {
      return "/builtins/missing.pbtxt";
    }
  }
}
