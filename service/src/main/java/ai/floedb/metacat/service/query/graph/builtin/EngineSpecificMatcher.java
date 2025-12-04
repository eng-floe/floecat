package ai.floedb.metacat.service.query.graph.builtin;

import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
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
   * Compares engine versions using "natural" ordering rules. Examples:
   *
   * <ul>
   *   <li>"10" &gt; "2" (numeric magnitudes, ignoring leading zeros).
   *   <li>"16.1" &gt; "16.0beta2" (numeric segment beats alphanumeric suffix at the same position).
   *   <li>"16.1beta" &lt; "16.1" (letters sort before digits at the same position).
   * </ul>
   *
   * Numeric segments are compared by magnitude, non-numeric segments are compared
   * case-insensitively, and digit segments always sort after non-digit segments at the same
   * position.
   */
  private static int compareVersions(String left, String right) {
    if (left == null || left.isBlank()) {
      left = "0";
    }
    if (right == null || right.isBlank()) {
      right = "0";
    }
    int i = 0;
    int j = 0;
    int lenLeft = left.length();
    int lenRight = right.length();
    while (i < lenLeft && j < lenRight) {
      char c1 = left.charAt(i);
      char c2 = right.charAt(j);
      boolean digit1 = Character.isDigit(c1);
      boolean digit2 = Character.isDigit(c2);
      if (digit1 && digit2) {
        int start1 = i;
        while (i < lenLeft && Character.isDigit(left.charAt(i))) {
          i++;
        }
        int start2 = j;
        while (j < lenRight && Character.isDigit(right.charAt(j))) {
          j++;
        }
        String num1 = stripLeadingZeros(left.substring(start1, i));
        String num2 = stripLeadingZeros(right.substring(start2, j));
        if (num1.length() != num2.length()) {
          return Integer.compare(num1.length(), num2.length());
        }
        int cmp = num1.compareTo(num2);
        if (cmp != 0) {
          return cmp;
        }
        continue;
      }
      if (digit1 != digit2) {
        return digit1 ? 1 : -1;
      }
      char normalized1 = Character.toLowerCase(c1);
      char normalized2 = Character.toLowerCase(c2);
      if (normalized1 != normalized2) {
        return normalized1 - normalized2;
      }
      i++;
      j++;
    }
    if (i < lenLeft) {
      return 1;
    }
    if (j < lenRight) {
      return -1;
    }
    return 0;
  }

  private static String stripLeadingZeros(String value) {
    int index = 0;
    while (index < value.length() && value.charAt(index) == '0') {
      index++;
    }
    String stripped = value.substring(index);
    return stripped.isEmpty() ? "0" : stripped;
  }
}
