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

/** Shared helper to evaluate engine-specific applicability rules. */
public final class EngineSpecificMatcher {

  private EngineSpecificMatcher() {}

  public static boolean matches(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) {
      return true;
    }
    return rules.stream()
        .filter(rule -> rule != null)
        .anyMatch(rule -> ruleMatches(rule, engineKind, engineVersion));
  }

  public static Optional<EngineSpecificRule> selectRule(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) {
      return Optional.empty();
    }
    return rules.stream()
        .filter(rule -> rule != null)
        .filter(rule -> ruleMatches(rule, engineKind, engineVersion))
        .findFirst();
  }

  public static List<EngineSpecificRule> matchedRules(
      List<EngineSpecificRule> rules, String engineKind, String engineVersion) {
    if (rules == null || rules.isEmpty()) {
      return List.of();
    }
    return rules.stream()
        .filter(rule -> rule != null)
        .filter(rule -> ruleMatches(rule, engineKind, engineVersion))
        .toList();
  }

  public static boolean matchesRule(
      EngineSpecificRule rule, String engineKind, String engineVersion) {
    return rule != null && ruleMatches(rule, engineKind, engineVersion);
  }

  private static boolean ruleMatches(
      EngineSpecificRule rule, String engineKind, String engineVersion) {
    if (rule.hasEngineKind()
        && (engineKind == null || !rule.engineKind().equalsIgnoreCase(engineKind))) {
      return false;
    }
    if (rule.hasMinVersion()) {
      var lower = EngineVersionComparator.minBound(rule);
      if (EngineVersionComparator.compare(engineVersion, lower.version()) < 0) {
        return false;
      }
    }
    if (rule.hasMaxVersion()) {
      var upper = EngineVersionComparator.maxBound(rule);
      if (EngineVersionComparator.compare(engineVersion, upper.version()) > 0) {
        return false;
      }
    }
    return true;
  }
}
