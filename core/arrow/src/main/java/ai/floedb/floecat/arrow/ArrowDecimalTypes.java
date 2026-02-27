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

package ai.floedb.floecat.arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;

/** Arrow decimal helpers shared across schema and writer code. */
final class ArrowDecimalTypes {
  private ArrowDecimalTypes() {}

  static ArrowType.Decimal decimalType(int precision, int scale) {
    int bitWidth;
    if (precision <= 38) {
      bitWidth = 128;
    } else if (precision <= 76) {
      bitWidth = 256;
    } else {
      throw new IllegalArgumentException(
          "DECIMAL precision " + precision + " exceeds Arrow decimal support (max 76)");
    }
    return new ArrowType.Decimal(precision, scale, bitWidth);
  }
}
