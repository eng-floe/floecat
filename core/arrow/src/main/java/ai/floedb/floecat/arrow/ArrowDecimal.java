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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.math.RoundingMode;

/**
 * Maps a {@link java.math.BigDecimal} record component to an Arrow {@code DECIMAL(precision,
 * scale)} column. Arrow decimals require an explicit precision and scale, so {@link
 * ArrowRecordWriters} rejects {@code BigDecimal} components that are not annotated.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface ArrowDecimal {
  int precision();

  int scale();

  /**
   * How to coerce an input value whose scale exceeds {@link #scale()}. Padding to a larger scale is
   * always lossless and unaffected by this; it only applies when an input carries more fractional
   * digits than the column. Defaults to {@link RoundingMode#UNNECESSARY}, which throws rather than
   * silently dropping precision — set an explicit mode such as {@link RoundingMode#HALF_UP} to opt
   * into rounding.
   */
  RoundingMode rounding() default RoundingMode.UNNECESSARY;
}
