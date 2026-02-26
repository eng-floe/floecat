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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Package-private helper that safely coerces any {@link Number} subtype to a canonical 64-bit
 * {@code long} representing a FloeCat {@link LogicalKind#INT} value.
 *
 * <p>Supported input types: {@link Long}, {@link Integer}, {@link Short}, {@link Byte}, {@link
 * java.math.BigInteger}, {@link java.math.BigDecimal}, {@link Double}, {@link Float}. All other
 * {@code Number} subtypes are coerced via their {@link Object#toString()} representation.
 *
 * <p>Throws {@link IllegalArgumentException} for:
 *
 * <ul>
 *   <li>Values outside the {@code long} range ({@code Long.MIN_VALUE}…{@code Long.MAX_VALUE})
 *   <li>Fractional values (e.g. {@code 1.5})
 *   <li>Non-finite floating-point values (NaN, ±Infinity)
 * </ul>
 */
final class Int64Coercions {
  private Int64Coercions() {}

  /**
   * Converts {@code value} to a {@code long}, validating that it is a finite, whole number within
   * the 64-bit integer range.
   *
   * @param value source number (never null)
   * @return the {@code long} equivalent
   * @throws IllegalArgumentException if the value is out of range, fractional, or non-finite
   */
  static long checkedLong(Number value) {
    if (value instanceof Long l) {
      return l;
    }
    if (value instanceof Integer i) {
      return i.longValue();
    }
    if (value instanceof Short s) {
      return s.longValue();
    }
    if (value instanceof Byte b) {
      return b.longValue();
    }
    if (value instanceof BigInteger bi) {
      try {
        return bi.longValueExact();
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException("INT value out of 64-bit range: " + value, e);
      }
    }
    if (value instanceof BigDecimal bd) {
      try {
        return bd.longValueExact();
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            "INT value must be a whole 64-bit number, got: " + value, e);
      }
    }
    if (value instanceof Double d) {
      return checkedFloating(d.doubleValue(), value.toString());
    }
    if (value instanceof Float f) {
      return checkedFloating(f.doubleValue(), value.toString());
    }

    try {
      return new BigDecimal(value.toString()).longValueExact();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "INT value must be a whole 64-bit number, got: " + value, e);
    }
  }

  private static long checkedFloating(double d, String raw) {
    if (!Double.isFinite(d)) {
      throw new IllegalArgumentException("INT value must be finite, got: " + raw);
    }
    if (d < Long.MIN_VALUE || d > Long.MAX_VALUE) {
      throw new IllegalArgumentException("INT value out of 64-bit range: " + raw);
    }
    if (Math.rint(d) != d) {
      throw new IllegalArgumentException("INT value must be a whole number, got: " + raw);
    }
    return (long) d;
  }
}
