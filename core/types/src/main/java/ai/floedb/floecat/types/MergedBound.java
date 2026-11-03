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

package ai.floedb.floecat.types;

/**
 * The minimum or maximum of a column over several contributors (files, row groups, partial
 * aggregates), as a bound that holds for every value of all of them.
 *
 * <p>The bound is unknown once a contributor that may hold values gives none, or gives one that
 * cannot be ordered with the others: the merged bound would otherwise miss that contributor's
 * values. A contributor that holds only NULLs and NaNs has no bound to give. A NaN bound is no
 * bound, since bounds exclude NaN. A bound the type cannot hold exactly is rounded outward.
 */
public final class MergedBound {
  private final boolean upper;
  private Object value;
  private boolean known = true;

  private MergedBound(boolean upper) {
    this.upper = upper;
  }

  public static MergedBound lower() {
    return new MergedBound(false);
  }

  public static MergedBound upper() {
    return new MergedBound(true);
  }

  /**
   * Whether a contributor of {@code values} values, {@code nulls} NULLs and {@code nans} NaNs (each
   * null when not counted) may hold a value a bound must cover: false only when its counts show
   * nothing but NULLs and NaNs. A count of no values alone does not: a partial merge counts only
   * the files that reported the column.
   */
  public static boolean mayHoldValues(Long values, Long nulls, Long nans) {
    return values == null || nulls == null || values > nulls + (nans == null ? 0L : nans);
  }

  /**
   * Folds in one contributor's {@code bound} (null when it gives none) of a column of {@code type},
   * where {@code mayHoldValues} is as {@link #mayHoldValues(Long, Long, Long)} says.
   */
  public void add(LogicalType type, Object bound, boolean mayHoldValues) {
    if (!known) {
      return;
    }
    if (bound == null || isNaN(bound)) {
      if (mayHoldValues) {
        known = false;
      }
      return;
    }
    try {
      Object normalized =
          widenZero(
              upper
                  ? LogicalComparators.normalizeUpper(type, bound)
                  : LogicalComparators.normalizeLower(type, bound));
      if (normalized == null) {
        known = false;
      } else if (value == null) {
        value = normalized;
      } else {
        int cmp = LogicalComparators.compare(type, normalized, value);
        if (upper ? cmp > 0 : cmp < 0) {
          value = normalized;
        }
      }
    } catch (RuntimeException unordered) {
      known = false;
    }
  }

  /** The bound, or null when it is unknown or no contributor gave one. */
  public Object value() {
    return known ? value : null;
  }

  /**
   * -0.0 and +0.0 are equal values, but ordered apart: a minimum of +0.0 may stand for -0.0 values
   * and a maximum of -0.0 for +0.0 ones, so each is widened to the other.
   */
  private Object widenZero(Object bound) {
    return switch (bound) {
      case Double d when d == 0.0 -> upper ? 0.0d : -0.0d;
      case Float f when f == 0.0f -> upper ? 0.0f : -0.0f;
      case null, default -> bound;
    };
  }

  private static boolean isNaN(Object bound) {
    return (bound instanceof Double d && d.isNaN()) || (bound instanceof Float f && f.isNaN());
  }
}
