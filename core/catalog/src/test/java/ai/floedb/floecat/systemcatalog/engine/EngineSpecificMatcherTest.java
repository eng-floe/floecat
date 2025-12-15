package ai.floedb.floecat.systemcatalog.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineSpecificMatcherTest {

  // Utility
  private static EngineSpecificRule rule(String kind, String min, String max) {
    return new EngineSpecificRule(
        kind == null ? "" : kind,
        min == null ? "" : min,
        max == null ? "" : max,
        "",
        null,
        Map.of());
  }

  // ----------------------------------------------------------------------
  // Version comparison tests
  // ----------------------------------------------------------------------

  @Test
  void numericSegmentsCompareNaturally() {
    EngineSpecificRule rule = rule("", "10", "");
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "2")).isFalse();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "10")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "11")).isTrue();
  }

  @Test
  void alphanumericVersionsHandledConsistently() {
    EngineSpecificRule rule = rule("", "", "16.1");

    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "16.1beta"))
        .isTrue(); // beta < release
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "16.0beta2"))
        .isTrue(); // pre-release < max OK
  }

  @Test
  void comparesMultiSegmentVersionsCorrectly() {
    assertThat(EngineSpecificMatcher.matches(List.of(rule("", "16.10", "")), "", "16.2"))
        .isFalse(); // 16.2 < 16.10

    assertThat(EngineSpecificMatcher.matches(List.of(rule("", "16.2", "")), "", "16.10"))
        .isTrue(); // 16.10 > 16.2
  }

  @Test
  void equalVersionsAreIncluded() {
    assertThat(EngineSpecificMatcher.matches(List.of(rule("", "16.0", "16.0")), "", "16.0"))
        .isTrue();
  }

  @Test
  void versionZeroSemanticsAreStable() {
    // If min is empty, everything >= 0 matches
    EngineSpecificRule r = rule("", "", "");
    assertThat(EngineSpecificMatcher.matches(List.of(r), "", "")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(r), "", null)).isTrue();
  }

  // ----------------------------------------------------------------------
  // EngineKind tests
  // ----------------------------------------------------------------------

  @Test
  void ruleWithoutEngineKindMatchesAll() {
    var rule = rule("", "1.0", "");
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "floe", "1.0")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "pg", "1.0")).isTrue();
  }

  @Test
  void ruleWithEngineKindMustMatchCaseInsensitively() {
    var rule = rule("FLOE", "1.0", "");
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "floe", "1.0")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "pg", "1.0")).isFalse();
  }

  @Test
  void ruleWithWrongEngineKindDoesNotMatch() {
    var rule = rule("pg", "1.0", "");
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "floe", "1.0")).isFalse();
  }

  // ----------------------------------------------------------------------
  // minVersion + maxVersion combined
  // ----------------------------------------------------------------------

  @Test
  void ruleWithMinAndMaxMustSatisfyBoth() {
    EngineSpecificRule rule = rule("", "10", "20");

    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "9")).isFalse();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "10")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "15")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "20")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "21")).isFalse();
  }

  @Test
  void mixedAlphaNumericSegmentsStillRespectMinAndMax() {
    EngineSpecificRule rule = rule("", "1.0beta2", "1.0");

    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "1.0beta")).isFalse();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "1.0beta2")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "1.0rc1")).isTrue();
    assertThat(EngineSpecificMatcher.matches(List.of(rule), "", "1.0")).isTrue();
  }

  // ----------------------------------------------------------------------
  // matchedRules + selectRule
  // ----------------------------------------------------------------------

  @Test
  void matchedRulesReturnsOnlyApplicableOnes() {
    var r1 = rule("floe", "1.0", "");
    var r2 = rule("pg", "1.0", "");
    var r3 = rule("", "2.0", "");

    var matched = EngineSpecificMatcher.matchedRules(List.of(r1, r2, r3), "floe", "2.5");

    assertThat(matched).containsExactly(r1, r3);
  }

  @Test
  void selectRuleReturnsFirstMatch() {
    var r1 = rule("pg", "1.0", "");
    var r2 = rule("floe", "1.0", "");

    var selected = EngineSpecificMatcher.selectRule(List.of(r1, r2), "floe", "2.0");

    assertThat(selected).contains(r2);
  }

  @Test
  void noMatchingRulesYieldsEmptyOptional() {
    var r1 = rule("pg", "1.0", "");
    var r2 = rule("pg", "2.0", "");

    var selected = EngineSpecificMatcher.selectRule(List.of(r1, r2), "floe", "10.0");

    assertThat(selected).isEmpty();
  }
}
