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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import com.google.protobuf.Timestamp;
import java.util.Map;
import java.util.Objects;

/**
 * Adapter between Floecat {@link LogicalType} objects and their protobuf wire representations
 * ({@code ColumnStats}, {@code UpstreamStamp}).
 *
 * <p>Encoding and decoding of type strings is delegated to {@link LogicalTypeFormat}. Encoding and
 * decoding of min/max values is delegated to {@link ValueEncoders}.
 *
 * <p>Typical usage:
 *
 * <pre>{@code
 * // Writing stats to proto
 * String typeStr = LogicalTypeProtoAdapter.encodeLogicalType(logicalType);
 * String minStr  = LogicalTypeProtoAdapter.encodeValue(logicalType, minValue);
 *
 * // Reading stats from proto
 * LogicalType t  = LogicalTypeProtoAdapter.columnLogicalType(columnStats);
 * Object min     = LogicalTypeProtoAdapter.columnMin(columnStats);
 * }</pre>
 */
public final class LogicalTypeProtoAdapter {

  private LogicalTypeProtoAdapter() {}

  /**
   * Encodes a {@link LogicalType} to its canonical wire string (e.g. {@code "INT"}, {@code
   * "DECIMAL(10,2)"}).
   *
   * @param t the logical type to encode (must not be null)
   * @return canonical string representation
   */
  public static String encodeLogicalType(LogicalType t) {
    Objects.requireNonNull(t, "logical type");
    return LogicalTypeFormat.format(t);
  }

  /**
   * Decodes a canonical type string (as written by {@link #encodeLogicalType}) back to a {@link
   * LogicalType}. Also accepts aliases (e.g. {@code "BIGINT"}, {@code "JSONB"}).
   *
   * @param s the type string to decode (must not be null or blank)
   * @return the corresponding {@link LogicalType}
   * @throws IllegalArgumentException if {@code s} is null, blank, or not recognised
   */
  public static LogicalType decodeLogicalType(String s) {
    if (s == null || s.isBlank()) {
      throw new IllegalArgumentException("Logical type must not be null/blank");
    }
    return LogicalTypeFormat.parse(s);
  }

  /**
   * Encodes a stat value to its canonical string (for storage in {@code ColumnStats.min/max}).
   * Returns an empty string for null values.
   *
   * @param type the logical type governing encoding semantics
   * @param value the value to encode (null → {@code ""})
   * @return encoded string, never null
   */
  public static String encodeValue(LogicalType type, Object value) {
    if (value == null) {
      return "";
    }

    return ValueEncoders.encodeToString(type, value);
  }

  /**
   * Decodes a stat value string (as written by {@link #encodeValue}) back to its canonical Java
   * type. Returns null for null or blank strings.
   *
   * @param type the logical type governing decoding semantics
   * @param encoded the encoded string (null or blank → null)
   * @return the decoded value, or null
   */
  public static Object decodeValue(LogicalType type, String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return null;
    }

    return ValueEncoders.decodeFromString(type, encoded);
  }

  public static UpstreamStamp upstreamStamp(
      TableFormat system,
      String tableNativeId,
      String commitRef,
      Timestamp fetchedAt,
      Map<String, String> properties) {

    UpstreamStamp.Builder b = UpstreamStamp.newBuilder().setSystem(system);
    if (tableNativeId != null) {
      b.setTableNativeId(tableNativeId);
    }

    if (commitRef != null) {
      b.setCommitRef(commitRef);
    }

    if (fetchedAt != null) {
      b.setFetchedAt(fetchedAt);
    }

    if (properties != null && !properties.isEmpty()) {
      b.putAllProperties(properties);
    }

    return b.build();
  }

  public static LogicalType columnLogicalType(ColumnStats cs) {
    return decodeLogicalType(cs.getLogicalType());
  }

  public static Object columnMin(ColumnStats cs) {
    LogicalType t = columnLogicalType(cs);
    return decodeValue(t, cs.getMin());
  }

  public static Object columnMax(ColumnStats cs) {
    LogicalType t = columnLogicalType(cs);
    return decodeValue(t, cs.getMax());
  }

  /**
   * Compares two encoded stat value strings by decoding both and delegating to {@link
   * LogicalComparators#compare}.
   *
   * @param type the logical type governing comparison semantics
   * @param a first encoded value (null treated as less than everything)
   * @param b second encoded value (null treated as less than everything)
   * @return negative, zero, or positive per {@link Comparable#compareTo} contract
   * @throws IllegalArgumentException if the type is not stats-orderable
   */
  public static int compareEncoded(LogicalType type, String a, String b) {
    Object va = decodeValue(type, a);
    Object vb = decodeValue(type, b);
    return LogicalComparators.compare(type, va, vb);
  }
}
