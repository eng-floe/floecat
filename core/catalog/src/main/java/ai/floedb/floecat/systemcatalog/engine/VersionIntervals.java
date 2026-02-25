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

import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Utility types and helpers for working with engine-version intervals. */
public final class VersionIntervals {
  private VersionIntervals() {}

  public record VersionBound(String value, boolean isNegInf, boolean isPosInf) {
    public static VersionBound negativeInfinity() {
      return new VersionBound("", true, false);
    }

    public static VersionBound positiveInfinity() {
      return new VersionBound("", false, true);
    }

    public static VersionBound finite(String value) {
      return new VersionBound(Objects.requireNonNull(value), false, false);
    }

    public boolean bounded() {
      return !isNegInf && !isPosInf;
    }

    public String version() {
      return value;
    }
  }

  public record VersionInterval(VersionBound min, VersionBound max) {
    public VersionInterval {
      Objects.requireNonNull(min, "min bound is required");
      Objects.requireNonNull(max, "max bound is required");
      if (compareBounds(min, max) > 0) {
        throw new IllegalArgumentException("min bound must be <= max bound");
      }
    }

    public static VersionInterval fromRule(EngineSpecificRule rule) {
      VersionBound min =
          rule != null && rule.hasMinVersion()
              ? VersionBound.finite(EngineVersionComparator.normalize(rule.minVersion()))
              : VersionBound.negativeInfinity();
      VersionBound max =
          rule != null && rule.hasMaxVersion()
              ? VersionBound.finite(EngineVersionComparator.normalize(rule.maxVersion()))
              : VersionBound.positiveInfinity();
      return new VersionInterval(min, max);
    }
  }

  public record RuleKey(String engineKindNormalizedOrEmpty, String payloadTypeNormalizedOrEmpty) {
    static RuleKey fromRule(EngineSpecificRule rule) {
      String kind =
          rule == null ? "" : EngineContextNormalizer.normalizeEngineKind(rule.engineKind());
      String payload = rule == null ? "" : rule.payloadType();
      return new RuleKey(kind, payload);
    }
  }

  public record RuleInterval(EngineSpecificRule rule, RuleKey key, VersionInterval interval) {
    static RuleInterval fromRule(EngineSpecificRule rule) {
      if (rule == null) {
        throw new IllegalArgumentException("rule is required");
      }
      return new RuleInterval(rule, RuleKey.fromRule(rule), VersionInterval.fromRule(rule));
    }
  }

  public static boolean overlaps(VersionInterval a, VersionInterval b) {
    if (a == null || b == null) {
      return false;
    }
    return compareBounds(a.min, b.max) <= 0 && compareBounds(b.min, a.max) <= 0;
  }

  public static List<VersionInterval> union(List<VersionInterval> intervals) {
    if (intervals == null || intervals.isEmpty()) {
      return List.of();
    }

    List<VersionInterval> normalized =
        intervals.stream().filter(Objects::nonNull).sorted(INTERVAL_COMPARATOR).toList();
    List<VersionInterval> merged = new ArrayList<>();
    VersionInterval current = null;

    for (VersionInterval interval : normalized) {
      if (current == null) {
        current = interval;
        continue;
      }
      if (compareBounds(current.max, interval.min) >= 0) {
        current = new VersionInterval(current.min, maxBound(current.max, interval.max));
      } else {
        merged.add(current);
        current = interval;
      }
    }
    if (current != null) {
      merged.add(current);
    }
    return List.copyOf(merged);
  }

  public static boolean covers(List<VersionInterval> union, VersionInterval target) {
    if (target == null) {
      return true;
    }
    if (union == null || union.isEmpty()) {
      return false;
    }

    List<VersionInterval> normalized =
        union.stream().filter(Objects::nonNull).sorted(INTERVAL_COMPARATOR).toList();
    VersionBound coverage = target.min;
    for (VersionInterval interval : normalized) {
      if (interval == null) {
        continue;
      }
      if (compareBounds(interval.max, coverage) < 0) {
        continue;
      }
      if (compareBounds(interval.min, coverage) > 0) {
        return false;
      }
      coverage = maxBound(coverage, interval.max);
      if (compareBounds(coverage, target.max) >= 0) {
        return true;
      }
    }
    return compareBounds(coverage, target.max) >= 0;
  }

  private static final Comparator<VersionInterval> INTERVAL_COMPARATOR =
      Comparator.comparing(VersionInterval::min, VersionIntervals::compareBounds)
          .thenComparing(VersionInterval::max, VersionIntervals::compareBounds);

  private static VersionBound maxBound(VersionBound left, VersionBound right) {
    return compareBounds(left, right) >= 0 ? left : right;
  }

  public static int compareBounds(VersionBound left, VersionBound right) {
    if (left == null || right == null) {
      throw new IllegalArgumentException("bounds must not be null");
    }
    if (left.isNegInf()) {
      return right.isNegInf() ? 0 : -1;
    }
    if (right.isNegInf()) {
      return 1;
    }
    if (left.isPosInf()) {
      return right.isPosInf() ? 0 : 1;
    }
    if (right.isPosInf()) {
      return -1;
    }
    return EngineVersionComparator.compare(left.value(), right.value());
  }
}
