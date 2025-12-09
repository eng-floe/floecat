package ai.floedb.floecat.catalog.system_objects.registry;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.catalog.common.util.NameRefUtil;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectColumnSet;
import ai.floedb.floecat.catalog.system_objects.spi.SystemObjectProvider;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Tests the SystemObjectRegistry merge logic: - provider filtering - engine-specific matching -
 * name-based matching - last-provider-wins override semantics
 */
class SystemObjectRegistryTest {

  private static final SchemaColumn DUMMY_COL =
      SchemaColumn.newBuilder().setName("c").setLogicalType("INT").setFieldId(0).build();

  private static final SystemObjectColumnSet COLS =
      new SystemObjectColumnSet(new SchemaColumn[] {DUMMY_COL});

  private static SystemObjectDefinition def(
      String path, String rowGenId, List<EngineSpecificRule> rules) {
    return new SystemObjectDefinition(NameRefUtil.fromCanonical(path), COLS, rowGenId, rules);
  }

  // ------------------------------------------------------------------------
  // Single provider – basic success case
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_singleProvider_success() {
    SystemObjectProvider provider =
        new SimpleProvider(
            "spark", "3.5.0", List.of(def("information_schema.tables", "t", List.of())));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    NameRef query = NameRefUtil.name("information_schema", "tables");

    Optional<SystemObjectDefinition> resolved = registry.resolveDefinition(query, "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("t");

    Map<String, SystemObjectDefinition> map = registry.definitionsFor("spark", "3.5.0");

    assertThat(map).containsKey("information_schema.tables");
  }

  // ------------------------------------------------------------------------
  // Multiple providers – override semantics
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_lastProviderWins() {
    var first = def("information_schema.tables", "v1", List.of());
    var second = def("information_schema.tables", "v2", List.of());

    var p1 = new SimpleProvider("spark", "3.5.0", List.of(first));
    var p2 = new SimpleProvider("spark", "3.5.0", List.of(second));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p1, p2));

    NameRef query = NameRefUtil.name("information_schema", "tables");

    Optional<SystemObjectDefinition> resolved = registry.resolveDefinition(query, "spark", "3.5.0");

    assertThat(resolved.get().scannerId()).isEqualTo("v2");
  }

  // ------------------------------------------------------------------------
  // Provider supportsEngine filtering
  // ------------------------------------------------------------------------
  @Test
  void definitionsFor_filtersByEngine() {
    var def = def("information_schema.tables", "t", List.of());

    SystemObjectProvider sparkOnly =
        new SimpleProvider("spark", "3.5.0", List.of(def)) {
          @Override
          public boolean supportsEngine(String engineKind, String version) {
            return "spark".equalsIgnoreCase(engineKind);
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(sparkOnly));
    assertThat(registry.definitionsFor("spark", "3.5.0")).hasSize(1);
    assertThat(registry.definitionsFor("duckdb", "1.0.0")).isEmpty();
  }

  // ------------------------------------------------------------------------
  // supports(NameRef) filtering
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_filtersBySupportsName() {
    var good = def("information_schema.tables", "good", List.of());
    var bad = def("information_schema.other", "bad", List.of());

    SystemObjectProvider provider =
        new SimpleProvider("spark", "3.5.0", List.of(bad, good)) {
          @Override
          public boolean supports(NameRef name, String e, String v) {
            // only support names where name == tables
            return "tables".equalsIgnoreCase(name.getName());
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    var resolved = registry.resolveDefinition(ref, "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("good");
  }

  // ------------------------------------------------------------------------
  // EngineSpecificRule filtering
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_filtersByEngineSpecificRules() {
    var rule = EngineSpecificRule.exact("spark", "2.0.0");
    var definition = def("information_schema.tables", "old", List.of(rule));

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(definition));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));
    NameRef ref = NameRefUtil.name("information_schema", "tables");

    var resolved = registry.resolveDefinition(ref, "spark", "3.5.0");
    assertThat(resolved).isEmpty();
  }

  // ------------------------------------------------------------------------
  // definitionsFor last-provider-wins override
  // ------------------------------------------------------------------------
  @Test
  void definitionsFor_lastProviderWinsInMap() {
    var first = def("information_schema.tables", "v1", List.of());
    var second = def("information_schema.tables", "v2", List.of());

    var p1 = new SimpleProvider("spark", "3.5.0", List.of(first));
    var p2 = new SimpleProvider("spark", "3.5.0", List.of(second));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p1, p2));

    Map<String, SystemObjectDefinition> map = registry.definitionsFor("spark", "3.5.0");

    assertThat(map.get("information_schema.tables").scannerId()).isEqualTo("v2");
  }

  // ------------------------------------------------------------------------
  // Empty providers list → empty results
  // ------------------------------------------------------------------------
  @Test
  void emptyProviders_yieldEmptyResults() {
    SystemObjectRegistry registry = new SystemObjectRegistry(List.of());

    assertThat(
            registry.resolveDefinition(
                NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0"))
        .isEmpty();

    assertThat(registry.definitionsFor("spark", "3.5.0")).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Provider returns definitions but supports(NameRef) rejects them
  // ------------------------------------------------------------------------
  @Test
  void providerDefinitionsIgnoredWhenSupportsNameIsFalse() {
    var definition = def("information_schema.tables", "id", List.of());

    SystemObjectProvider provider =
        new SimpleProvider("spark", "3.5.0", List.of(definition)) {
          @Override
          public boolean supports(NameRef name, String e, String v) {
            return false;
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    var resolved =
        registry.resolveDefinition(
            NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0");

    assertThat(resolved).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Multiple definitions for same name within the same provider
  // ------------------------------------------------------------------------
  @Test
  void lastDefinitionWithinProviderWins() {
    var d1 = def("information_schema.tables", "v1", List.of());
    var d2 = def("information_schema.tables", "v2", List.of());

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(d1, d2));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    var resolved =
        registry.resolveDefinition(
            NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("v2");
  }

  // ------------------------------------------------------------------------
  // EngineSpecificRule: match or not
  // ------------------------------------------------------------------------
  @Test
  void definitionWithMultipleRules_matchesIfAnyRuleMatches() {
    var rule1 = EngineSpecificRule.exact("spark", "3.5.0");
    var rule2 = EngineSpecificRule.exact("spark", "4.0.0");

    var definition = def("information_schema.tables", "id", List.of(rule1, rule2));

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(definition));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    var resolved =
        registry.resolveDefinition(
            NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0");
    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("id");
  }

  @Test
  void definitionWithMultipleRules_rejectedWhenNoRuleMatches() {
    var rule1 = EngineSpecificRule.exact("spark", "4.0.0");
    var rule2 = EngineSpecificRule.exact("spark", "5.0.0");

    var definition = def("information_schema.tables", "id", List.of(rule1, rule2));

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(definition));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    assertThat(
            registry.resolveDefinition(
                NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0"))
        .isEmpty();
  }

  // ------------------------------------------------------------------------
  // Deep namespace paths
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_supportsDeepNamespacePaths() {
    var definition = def("a.b.c", "id", List.of());

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(definition));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    NameRef ref = NameRefUtil.name("a", "b", "c");

    var resolved = registry.resolveDefinition(ref, "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("id");
  }

  // ------------------------------------------------------------------------
  // supports(NameRef) filtering ignores definitions
  // ------------------------------------------------------------------------
  @Test
  void definitionsFor_ignoresDefinitionsWhenSupportsNameIsFalse() {
    var d = def("information_schema.tables", "id", List.of());

    SystemObjectProvider provider =
        new SimpleProvider("spark", "3.5.0", List.of(d)) {
          @Override
          public boolean supports(NameRef name, String e, String v) {
            return false;
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    Map<String, SystemObjectDefinition> map = registry.definitionsFor("spark", "3.5.0");

    assertThat(map).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Short-circuit when provider does not support engine
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_skipsNameCheckWhenEngineIsUnsupported() {
    var d = def("information_schema.tables", "id", List.of());

    SystemObjectProvider provider =
        new SimpleProvider("spark", "3.5.0", List.of(d)) {
          @Override
          public boolean supportsEngine(String engineKind, String version) {
            return false; // should short-circuit
          }

          @Override
          public boolean supports(NameRef name, String e, String v) {
            throw new AssertionError("should not be called");
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    assertThat(
            registry.resolveDefinition(
                NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0"))
        .isEmpty();
  }

  // ------------------------------------------------------------------------
  // Last correct matching rule within a single provider
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_choosesLastMatchingDefinitionWithinProvider() {
    var old =
        def("information_schema.tables", "old", List.of(EngineSpecificRule.exact("spark", "1.0")));
    var mid =
        def("information_schema.tables", "mid", List.of(EngineSpecificRule.exact("spark", "2.0")));
    var latest =
        def(
            "information_schema.tables",
            "latest",
            List.of(EngineSpecificRule.exact("spark", "3.5.0")));

    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of(old, mid, latest));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    var resolved =
        registry.resolveDefinition(
            NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("latest");
  }

  // ------------------------------------------------------------------------
  // definitionsFor returns all matching definitions across providers
  // ------------------------------------------------------------------------
  @Test
  void definitionsFor_returnsAllMatchingDefinitionsAcrossProviders() {
    var a = def("ns.a", "A", List.of());
    var b = def("ns.b", "B", List.of());
    var c = def("ns.c", "C", List.of());

    SystemObjectProvider p = new SimpleProvider("spark", "3.5.0", List.of(a, b, c));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p));

    Map<String, SystemObjectDefinition> map = registry.definitionsFor("spark", "3.5.0");

    assertThat(map).hasSize(3);
    assertThat(map).containsKeys("ns.a", "ns.b", "ns.c");
  }

  // ------------------------------------------------------------------------
  // EngineSpecificRule with min/max version range
  // ------------------------------------------------------------------------
  @Test
  void engineSpecificRules_respectMinMaxVersionRange() {
    var rule =
        new EngineSpecificRule(
            "spark", "3.0.0", // minVersion
            "4.0.0", // maxVersion
            null, null, Map.of());

    var d = def("x.y", "id", List.of(rule));

    SystemObjectProvider p = new SimpleProvider("spark", "3.5.0", List.of(d));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p));

    assertThat(registry.resolveDefinition(NameRefUtil.name("x", "y"), "spark", "3.5.0"))
        .isPresent();
  }

  // ------------------------------------------------------------------------
  // EngineSpecificRule with min/max version range – rejection case
  // ------------------------------------------------------------------------
  @Test
  void engineSpecificRules_rejectWhenVersionOutOfRange() {
    var rule = new EngineSpecificRule("spark", "3.0.0", "3.4.9", null, null, Map.of());

    var d = def("x.y", "id", List.of(rule));

    SystemObjectProvider p = new SimpleProvider("spark", "3.5.0", List.of(d));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p));

    assertThat(registry.resolveDefinition(NameRefUtil.name("x", "y"), "spark", "3.5.0")).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Empty definitions list → no results
  // ------------------------------------------------------------------------
  @Test
  void emptyDefinitionsList_yieldsNoResults() {
    SystemObjectProvider provider = new SimpleProvider("spark", "3.5.0", List.of());

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    assertThat(registry.definitionsFor("spark", "3.5.0")).isEmpty();
    assertThat(registry.resolveDefinition(NameRefUtil.name("x", "y"), "spark", "3.5.0")).isEmpty();
  }

  // ------------------------------------------------------------------------
  // Case-insensitive canonical name matching
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_canonicalNameIsIndependentFromProviderCaseLogic() {
    var definition = def("InfoRMAtiOn_ScheMA.TaBLeS", "id", List.of());

    SystemObjectProvider provider =
        new SimpleProvider("spark", "3.5.0", List.of(definition)) {
          @Override
          public boolean supports(NameRef name, String e, String v) {
            return true; // irrelevant
          }
        };

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(provider));

    var resolved =
        registry.resolveDefinition(
            NameRefUtil.name("information_schema", "tables"), "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("id");
  }

  // ------------------------------------------------------------------------
  // Provider accepted by engine but rejected by EngineSpecificRule
  // ------------------------------------------------------------------------
  @Test
  void providerAcceptedByEngineButRejectedByRule() {
    var rule = EngineSpecificRule.exact("spark", "9.9.9"); // not matching engineVersion

    var d = def("info.x", "id", List.of(rule));

    SystemObjectProvider p = new SimpleProvider("spark", "3.5.0", List.of(d));

    SystemObjectRegistry registry = new SystemObjectRegistry(List.of(p));

    assertThat(registry.resolveDefinition(NameRefUtil.name("info", "x"), "spark", "3.5.0"))
        .isEmpty();
  }

  // ------------------------------------------------------------------------
  // definitionsFor merges definitions from multiple providers with different names
  // ------------------------------------------------------------------------
  @Test
  void definitionsFor_handlesProvidersWithDifferentNames() {
    var p1 = new SimpleProvider("spark", "3.5.0", List.of(def("a.b.c", "A", List.of())));

    var p2 = new SimpleProvider("spark", "3.5.0", List.of(def("x.y.z", "B", List.of())));

    var registry = new SystemObjectRegistry(List.of(p1, p2));

    Map<String, SystemObjectDefinition> map = registry.definitionsFor("spark", "3.5.0");

    assertThat(map).containsKeys("a.b.c", "x.y.z");
  }

  // ------------------------------------------------------------------------
  // Provider skipping when unsupported engine before finding valid one
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_skipsUnsupportedProvidersBeforeFindingValidOne() {
    var d = def("a.b", "id", List.of());

    var p1 =
        new SimpleProvider("spark", "1.0", List.of(d)) {
          @Override
          public boolean supportsEngine(String k, String v) {
            return false;
          }
        };

    var p2 = new SimpleProvider("spark", "3.5.0", List.of(d));

    var registry = new SystemObjectRegistry(List.of(p1, p2));

    var resolved = registry.resolveDefinition(NameRefUtil.name("a", "b"), "spark", "3.5.0");

    assertThat(resolved).isPresent();
  }

  // ------------------------------------------------------------------------
  // Provider skipping when supports(NameRef) rejects before finding valid one
  // ------------------------------------------------------------------------
  @Test
  void resolveDefinition_skipsProvidersThatRejectNameButStillChecksLaterProviders() {
    var d1 = def("a.b", "ignored", List.of());
    var d2 = def("a.b", "chosen", List.of());

    var p1 =
        new SimpleProvider("spark", "3.5.0", List.of(d1)) {
          @Override
          public boolean supports(NameRef name, String e, String v) {
            return false;
          }
        };

    var p2 = new SimpleProvider("spark", "3.5.0", List.of(d2));

    var registry = new SystemObjectRegistry(List.of(p1, p2));

    var resolved = registry.resolveDefinition(NameRefUtil.name("a", "b"), "spark", "3.5.0");

    assertThat(resolved).isPresent();
    assertThat(resolved.get().scannerId()).isEqualTo("chosen");
  }

  // ------------------------------------------------------------------------
  // Helper provider implementation
  // ------------------------------------------------------------------------
  private static class SimpleProvider implements SystemObjectProvider {

    private final String engine;
    private final String version;
    private final List<SystemObjectDefinition> defs;

    SimpleProvider(String engine, String version, List<SystemObjectDefinition> defs) {
      this.engine = engine;
      this.version = version;
      this.defs = defs;
    }

    @Override
    public List<SystemObjectDefinition> definitions() {
      return defs;
    }

    @Override
    public boolean supportsEngine(String engineKind, String version) {
      return engine.equalsIgnoreCase(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind, String engineVersion) {
      return true; // simplified for tests
    }
  }
}
