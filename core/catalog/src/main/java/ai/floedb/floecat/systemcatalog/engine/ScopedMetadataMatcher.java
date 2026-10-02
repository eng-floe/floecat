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

import java.util.List;
import java.util.Optional;

/** Shared helper to evaluate scoped metadata applicability rules. */
public final class ScopedMetadataMatcher {

  private ScopedMetadataMatcher() {}

  public static boolean matches(
      List<ScopedMetadataRule> rules, String engineKind, String engineVersion) {
    return matches(rules, ScopedMetadataRule.Scope.ENGINE, engineKind, engineVersion);
  }

  public static boolean matches(
      List<ScopedMetadataRule> rules, ScopedMetadataRule.Scope scope, String kind, String version) {
    if (rules == null || rules.isEmpty()) {
      return true;
    }
    List<ScopedMetadataRule> scopedRules =
        rules.stream().filter(rule -> rule != null).filter(rule -> rule.scope() == scope).toList();
    if (scopedRules.isEmpty()) {
      return true;
    }
    return scopedRules.stream().anyMatch(rule -> ruleMatches(rule, kind, version));
  }

  public static Optional<ScopedMetadataRule> selectRule(
      List<ScopedMetadataRule> rules, String engineKind, String engineVersion) {
    return selectRule(rules, ScopedMetadataRule.Scope.ENGINE, engineKind, engineVersion);
  }

  public static Optional<ScopedMetadataRule> selectRule(
      List<ScopedMetadataRule> rules, ScopedMetadataRule.Scope scope, String kind, String version) {
    if (rules == null || rules.isEmpty()) {
      return Optional.empty();
    }
    return rules.stream()
        .filter(rule -> rule != null)
        .filter(rule -> rule.scope() == scope)
        .filter(rule -> ruleMatches(rule, kind, version))
        .findFirst();
  }

  public static List<ScopedMetadataRule> matchedRules(
      List<ScopedMetadataRule> rules, String engineKind, String engineVersion) {
    return matchedRules(rules, ScopedMetadataRule.Scope.ENGINE, engineKind, engineVersion);
  }

  public static List<ScopedMetadataRule> matchedRules(
      List<ScopedMetadataRule> rules, ScopedMetadataRule.Scope scope, String kind, String version) {
    if (rules == null || rules.isEmpty()) {
      return List.of();
    }
    List<ScopedMetadataRule> scopedRules =
        rules.stream().filter(rule -> rule != null).filter(rule -> rule.scope() == scope).toList();
    if (scopedRules.isEmpty()) {
      return List.of();
    }
    return scopedRules.stream().filter(rule -> ruleMatches(rule, kind, version)).toList();
  }

  public static boolean matchesRule(
      ScopedMetadataRule rule, String engineKind, String engineVersion) {
    return matchesRule(rule, ScopedMetadataRule.Scope.ENGINE, engineKind, engineVersion);
  }

  public static boolean matchesRule(
      ScopedMetadataRule rule, ScopedMetadataRule.Scope scope, String kind, String version) {
    return rule != null && rule.scope() == scope && ruleMatches(rule, kind, version);
  }

  private static boolean ruleMatches(ScopedMetadataRule rule, String kind, String version) {
    if (rule.hasKind() && (kind == null || !rule.kind().equalsIgnoreCase(kind))) {
      return false;
    }
    if (rule.hasMinVersion()) {
      var lower = EngineVersionComparator.minBound(rule);
      if (EngineVersionComparator.compare(version, lower.version()) < 0) {
        return false;
      }
    }
    if (rule.hasMaxVersion()) {
      var upper = EngineVersionComparator.maxBound(rule);
      if (EngineVersionComparator.compare(version, upper.version()) > 0) {
        return false;
      }
    }
    return true;
  }
}
