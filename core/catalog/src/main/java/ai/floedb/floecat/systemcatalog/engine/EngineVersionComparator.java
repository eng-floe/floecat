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

import ai.floedb.floecat.systemcatalog.engine.VersionIntervals.VersionBound;
import java.util.List;

/**
 * SemVer-aligned comparator for engine version strings.
 *
 * <p>Numeric release segments are compared segment-by-segment; release versions are greater than
 * prereleases. Prerelease identifiers (split on '.') compare numerically when both are digits and
 * lexically otherwise. When comparing numeric vs non-numeric identifiers, the numeric identifier
 * has lower precedence (per SemVer 2.0.0).
 */
public final class EngineVersionComparator {
  private EngineVersionComparator() {}

  public static int compare(String left, String right) {
    String normalizedLeft = normalize(left);
    String normalizedRight = normalize(right);

    var lp = splitVersion(normalizedLeft);
    var rp = splitVersion(normalizedRight);

    int coreCmp = compareDotSeparatedNumeric(lp.core, rp.core);
    if (coreCmp != 0) {
      return coreCmp;
    }

    boolean leftPre = !lp.prerelease.isEmpty();
    boolean rightPre = !rp.prerelease.isEmpty();
    if (leftPre && !rightPre) {
      return -1;
    }
    if (!leftPre && rightPre) {
      return 1;
    }
    if (!leftPre && !rightPre) {
      return 0;
    }
    return comparePrerelease(lp.prerelease, rp.prerelease);
  }

  public static String normalize(String version) {
    return (version == null || version.isBlank()) ? "0" : version.trim();
  }

  public static VersionBound minBound(EngineSpecificRule rule) {
    if (rule == null || !rule.hasMinVersion()) {
      return VersionBound.negativeInfinity();
    }
    return VersionBound.finite(normalize(rule.minVersion()));
  }

  public static VersionBound maxBound(EngineSpecificRule rule) {
    if (rule == null || !rule.hasMaxVersion()) {
      return VersionBound.positiveInfinity();
    }
    return VersionBound.finite(normalize(rule.maxVersion()));
  }

  private static VersionParts splitVersion(String v) {
    int dash = v.indexOf('-');
    if (dash >= 0) {
      return new VersionParts(v.substring(0, dash), List.of(v.substring(dash + 1).split("\\.")));
    }

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
      int ai = i < as.length ? parseIntSafe(as[i]) : 0;
      int bi = i < bs.length ? parseIntSafe(bs[i]) : 0;
      int cmp = Integer.compare(ai, bi);
      if (cmp != 0) {
        return cmp;
      }
    }
    return 0;
  }

  private static int parseIntSafe(String s) {
    try {
      return Integer.parseInt(s);
    } catch (NumberFormatException e) {
      // Non-numeric core segments fall back to 0, mirroring the legacy matcher’s tolerance.
      return 0;
    }
  }

  private static int comparePrerelease(List<String> left, List<String> right) {
    int n = Math.max(left.size(), right.size());
    for (int i = 0; i < n; i++) {
      String l = i < left.size() ? left.get(i) : "";
      String r = i < right.size() ? right.get(i) : "";
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

    if (a.isEmpty() && !b.isEmpty()) {
      return -1;
    }
    if (!a.isEmpty() && b.isEmpty()) {
      return 1;
    }

    if (aNum && bNum) {
      return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
    }

    if (aNum && !bNum) {
      return -1;
    }
    if (!aNum && bNum) {
      return 1;
    }
    return a.compareToIgnoreCase(b);
  }

  private record VersionParts(String core, List<String> prerelease) {}
}
