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

package ai.floedb.floecat.systemcatalog.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScopedMetadataMatcherTest {

  // Utility
  private static ScopedMetadataRule rule(String kind, String min, String max) {
    return new ScopedMetadataRule(
        kind == null ? "" : kind,
        min == null ? "" : min,
        max == null ? "" : max,
        "matcher.payload",
        null,
        Map.of());
  }

  // ----------------------------------------------------------------------
  // Version comparison tests
  // ----------------------------------------------------------------------

  @Test
  void numericSegmentsCompareNaturally() {
    ScopedMetadataRule rule = rule("", "10", "");
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "2")).isFalse();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "10")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "11")).isTrue();
  }

  @Test
  void alphanumericVersionsHandledConsistently() {
    ScopedMetadataRule rule = rule("", "", "16.1");

    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "16.1beta"))
        .isTrue(); // beta < release
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "16.0beta2"))
        .isTrue(); // pre-release < max OK
  }

  @Test
  void comparesMultiSegmentVersionsCorrectly() {
    assertThat(ScopedMetadataMatcher.matches(List.of(rule("", "16.10", "")), "", "16.2"))
        .isFalse(); // 16.2 < 16.10

    assertThat(ScopedMetadataMatcher.matches(List.of(rule("", "16.2", "")), "", "16.10"))
        .isTrue(); // 16.10 > 16.2
  }

  @Test
  void equalVersionsAreIncluded() {
    assertThat(ScopedMetadataMatcher.matches(List.of(rule("", "16.0", "16.0")), "", "16.0"))
        .isTrue();
  }

  @Test
  void versionZeroSemanticsAreStable() {
    // If min is empty, everything >= 0 matches
    ScopedMetadataRule r = rule("", "", "");
    assertThat(ScopedMetadataMatcher.matches(List.of(r), "", "")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(r), "", null)).isTrue();
  }

  // ----------------------------------------------------------------------
  // EngineKind tests
  // ----------------------------------------------------------------------

  @Test
  void ruleWithoutEngineKindMatchesAll() {
    var rule = rule("", "1.0", "");
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "floe", "1.0")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "pg", "1.0")).isTrue();
  }

  @Test
  void ruleWithEngineKindMustMatchCaseInsensitively() {
    var rule = rule("FLOE", "1.0", "");
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "floe", "1.0")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "pg", "1.0")).isFalse();
  }

  @Test
  void ruleWithWrongEngineKindDoesNotMatch() {
    var rule = rule("pg", "1.0", "");
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "floe", "1.0")).isFalse();
  }

  @Test
  void environmentRulesUseTheEnvironmentAxis() {
    var rule =
        new ScopedMetadataRule(
            ScopedMetadataRule.Scope.ENVIRONMENT,
            "floedb",
            "2",
            "3",
            "environment.pg_class",
            null,
            Map.of());

    assertThat(
            ScopedMetadataMatcher.matches(
                List.of(rule), ScopedMetadataRule.Scope.ENVIRONMENT, "floedb", "2"))
        .isTrue();
    assertThat(
            ScopedMetadataMatcher.matches(
                List.of(rule), ScopedMetadataRule.Scope.ENVIRONMENT, "floedb", "4"))
        .isFalse();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "floedb", "2")).isTrue();
  }

  // ----------------------------------------------------------------------
  // minVersion + maxVersion combined
  // ----------------------------------------------------------------------

  @Test
  void ruleWithMinAndMaxMustSatisfyBoth() {
    ScopedMetadataRule rule = rule("", "10", "20");

    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "9")).isFalse();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "10")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "15")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "20")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "21")).isFalse();
  }

  @Test
  void mixedAlphaNumericSegmentsStillRespectMinAndMax() {
    ScopedMetadataRule rule = rule("", "1.0beta2", "1.0");

    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "1.0beta")).isFalse();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "1.0beta2")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "1.0rc1")).isTrue();
    assertThat(ScopedMetadataMatcher.matches(List.of(rule), "", "1.0")).isTrue();
  }

  // ----------------------------------------------------------------------
  // matchedRules + selectRule
  // ----------------------------------------------------------------------

  @Test
  void matchedRulesReturnsOnlyApplicableOnes() {
    var r1 = rule("floe", "1.0", "");
    var r2 = rule("pg", "1.0", "");
    var r3 = rule("", "2.0", "");

    var matched = ScopedMetadataMatcher.matchedRules(List.of(r1, r2, r3), "floe", "2.5");

    assertThat(matched).containsExactly(r1, r3);
  }

  @Test
  void selectRuleReturnsFirstMatch() {
    var r1 = rule("pg", "1.0", "");
    var r2 = rule("floe", "1.0", "");

    var selected = ScopedMetadataMatcher.selectRule(List.of(r1, r2), "floe", "2.0");

    assertThat(selected).contains(r2);
  }

  @Test
  void noMatchingRulesYieldsEmptyOptional() {
    var r1 = rule("pg", "1.0", "");
    var r2 = rule("pg", "2.0", "");

    var selected = ScopedMetadataMatcher.selectRule(List.of(r1, r2), "floe", "10.0");

    assertThat(selected).isEmpty();
  }
}
