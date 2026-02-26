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
 * Public façade for encoding and decoding FloeCat column statistics min/max values to and from
 * canonical strings.
 *
 * <p>All logic is delegated to {@link ValueEncoders}. This class exists as a stable, narrow public
 * API surface so that callers outside the {@code types} module do not need to depend directly on
 * the broader {@link ValueEncoders} utility.
 *
 * <p>Complex types ({@link LogicalKind#ARRAY}, {@link LogicalKind#MAP}, {@link LogicalKind#STRUCT},
 * {@link LogicalKind#VARIANT}) and {@link LogicalKind#JSON} have no meaningful min/max encoding;
 * calling {@link #encode} for these kinds throws {@link IllegalArgumentException}.
 *
 * @see ValueEncoders
 */
public final class MinMaxCodec {
  private MinMaxCodec() {}

  /**
   * Encodes {@code value} to the canonical string used in ColumnStats {@code min}/{@code max}
   * fields.
   *
   * @param t the logical type governing encoding semantics (must not be null)
   * @param value the value to encode (null returns null)
   * @return the canonical string, or null if value is null
   * @throws IllegalArgumentException for complex types or if the value is incompatible with {@code
   *     t}
   */
  public static String encode(LogicalType t, Object value) {
    return ValueEncoders.encodeToString(t, value);
  }

  /**
   * Decodes an encoded stat string back to its canonical Java type.
   *
   * @param t the logical type governing decoding semantics (must not be null)
   * @param encoded the string to decode (null or blank returns null)
   * @return the decoded value, or null if encoded is null
   */
  public static Object decode(LogicalType t, String encoded) {
    return ValueEncoders.decodeFromString(t, encoded);
  }
}
