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

  private static boolean ruleMatches(
      EngineSpecificRule rule, String engineKind, String engineVersion) {
    if (rule.hasEngineKind()
        && (engineKind == null || !rule.engineKind().equalsIgnoreCase(engineKind))) {
      return false;
    }
    if (rule.hasMinVersion() && compareVersions(engineVersion, rule.minVersion()) < 0) {
      return false;
    }
    if (rule.hasMaxVersion() && compareVersions(engineVersion, rule.maxVersion()) > 0) {
      return false;
    }
    return true;
  }

  /**
   * Planner-agnostic SemVer-like version comparison.
   *
   * <p>Supports arbitrary vendor version strings such as: "1.0", "1.0.1", "1.0-beta", "1.0beta2",
   * "16.0rc1", "2024.01.15-alpha".
   *
   * <p>Rules: 1. Split version into (core, prerelease) parts. 2. Compare core version numerically
   * segment-by-segment. 3. A version *without* prerelease is greater than the same version *with*
   * prerelease. 4. Prerelease identifiers split on '.', compare each: - numeric identifiers compare
   * numerically - non-numeric identifiers compare lexically (case-insensitive) - numeric
   * identifiers are always greater than non-numeric ones
   */
  private static int compareVersions(String left, String right) {
    left = normalizeVersion(left);
    right = normalizeVersion(right);

    var lp = splitVersion(left);
    var rp = splitVersion(right);

    // 1. Compare core version (numeric segments)
    int coreCmp = compareDotSeparatedNumeric(lp.core, rp.core);
    if (coreCmp != 0) {
      return coreCmp;
    }

    // 2. If cores equal: release > prerelease
    boolean leftPre = !lp.prerelease.isEmpty();
    boolean rightPre = !rp.prerelease.isEmpty();
    if (leftPre && !rightPre) return -1;
    if (!leftPre && rightPre) return 1;
    if (!leftPre && !rightPre) return 0;

    // 3. Compare prerelease segments
    return comparePrerelease(lp.prerelease, rp.prerelease);
  }

  private static String normalizeVersion(String v) {
    return (v == null || v.isBlank()) ? "0" : v.trim();
  }

  private record VersionParts(String core, List<String> prerelease) {}

  private static VersionParts splitVersion(String v) {
    // Detect prerelease: split on first dash or first sequence of letters after numbers
    int dash = v.indexOf('-');
    if (dash >= 0) {
      return new VersionParts(v.substring(0, dash), List.of(v.substring(dash + 1).split("\\.")));
    }

    // Handle versions like "1.0beta2" or "16.0rc1"
    int i = 0;
    while (i < v.length() && (Character.isDigit(v.charAt(i)) || v.charAt(i) == '.')) {
      i++;
    }

    if (i == v.length()) {
      return new VersionParts(v, List.of());
    }
    return new VersionParts(v.substring(0, i), List.of(v.substring(i).split("\\.")));
  }

  private static int compareDotSeparatedNumeric(String a, String b) {
    String[] as = a.split("\\.");
    String[] bs = b.split("\\.");

    int n = Math.max(as.length, bs.length);
    for (int i = 0; i < n; i++) {
      int ai = (i < as.length) ? parseIntSafe(as[i]) : 0;
      int bi = (i < bs.length) ? parseIntSafe(bs[i]) : 0;
      int cmp = Integer.compare(ai, bi);
      if (cmp != 0) return cmp;
    }
    return 0;
  }

  private static int parseIntSafe(String s) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static int comparePrerelease(List<String> left, List<String> right) {
    int n = Math.max(left.size(), right.size());
    for (int i = 0; i < n; i++) {
      String l = (i < left.size()) ? left.get(i) : "";
      String r = (i < right.size()) ? right.get(i) : "";

      int cmp = comparePrereleaseToken(l, r);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  private static int comparePrereleaseToken(String a, String b) {
    boolean aNum = a.matches("\\d+");
    boolean bNum = b.matches("\\d+");

    // Empty (missing) prerelease segments < any actual identifier
    if (a.isEmpty() && !b.isEmpty()) return -1;
    if (!a.isEmpty() && b.isEmpty()) return 1;

    if (aNum && bNum) {
      return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
    }

    // Numeric identifiers have higher precedence than alpha identifiers
    if (aNum && !bNum) return 1;
    if (!aNum && bNum) return -1;

    // Lexical compare for non-numeric
    return a.compareToIgnoreCase(b);
  }
}
