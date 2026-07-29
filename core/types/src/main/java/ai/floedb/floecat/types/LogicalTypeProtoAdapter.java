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

import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import com.google.protobuf.Timestamp;
import java.util.Map;
import java.util.Objects;

/**
 * Adapter between Floecat {@link LogicalType} objects and their protobuf wire representations
 * ({@code ScalarStats}, {@code UpstreamStamp}).
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
   * Encodes a stat value to its canonical string (for storage in {@code ScalarStats.min/max}).
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

  /**
   * Converts a {@link LogicalType} to its recursive protobuf wire message ({@code
   * floecat.types.LogicalType}), preserving the full nested shape including element/value/field
   * nullability that the string grammar cannot carry.
   */
  public static ai.floedb.floecat.types.rpc.LogicalType toProto(LogicalType t) {
    return toProto(t, 0);
  }

  private static ai.floedb.floecat.types.rpc.LogicalType toProto(LogicalType t, int depth) {
    Objects.requireNonNull(t, "logical type");
    // Writers enforce the same nesting cap as fromProto: without it a deeply nested type would
    // persist successfully and then throw on every decode.
    if (depth > LogicalTypeFormat.MAX_NESTING_DEPTH) {
      throw new IllegalArgumentException(
          "logical type nesting depth exceeds " + LogicalTypeFormat.MAX_NESTING_DEPTH);
    }
    ai.floedb.floecat.types.rpc.LogicalType.Kind wireKind;
    try {
      wireKind = ai.floedb.floecat.types.rpc.LogicalType.Kind.valueOf("TK_" + t.kind().name());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("No wire kind for logical kind: " + t.kind(), e);
    }
    ai.floedb.floecat.types.rpc.LogicalType.Builder b =
        ai.floedb.floecat.types.rpc.LogicalType.newBuilder().setKind(wireKind);
    if (t.precision() != null) {
      b.setPrecision(t.precision());
    }
    if (t.scale() != null) {
      b.setScale(t.scale());
    }
    if (t.temporalPrecision() != null) {
      b.setTemporalPrecision(t.temporalPrecision());
    }
    if (t.intervalRange() != null && t.intervalRange() != IntervalRange.UNSPECIFIED) {
      b.setIntervalRange(
          switch (t.intervalRange()) {
            case YEAR_TO_MONTH ->
                ai.floedb.floecat.types.rpc.LogicalType.IntervalRange.IR_YEAR_TO_MONTH;
            case DAY_TO_SECOND ->
                ai.floedb.floecat.types.rpc.LogicalType.IntervalRange.IR_DAY_TO_SECOND;
            case UNSPECIFIED ->
                ai.floedb.floecat.types.rpc.LogicalType.IntervalRange.IR_UNSPECIFIED;
          });
    }
    if (t.intervalLeadingPrecision() != null) {
      b.setIntervalLeadingPrecision(t.intervalLeadingPrecision());
    }
    if (t.intervalFractionalPrecision() != null) {
      b.setIntervalFractionalPrecision(t.intervalFractionalPrecision());
    }
    if (t.element() != null) {
      b.setArray(
          ai.floedb.floecat.types.rpc.ArrayShape.newBuilder()
              .setElement(toProto(t.element(), depth + 1))
              .setElementNullable(t.elementNullable()));
    } else if (t.key() != null) {
      b.setMap(
          ai.floedb.floecat.types.rpc.MapShape.newBuilder()
              .setKey(toProto(t.key(), depth + 1))
              .setValue(toProto(t.value(), depth + 1))
              .setValueNullable(t.valueNullable()));
    } else if (t.fields() != null) {
      ai.floedb.floecat.types.rpc.StructShape.Builder struct =
          ai.floedb.floecat.types.rpc.StructShape.newBuilder();
      for (LogicalField field : t.fields()) {
        struct.addFields(
            ai.floedb.floecat.types.rpc.LogicalField.newBuilder()
                .setName(field.name())
                .setNullable(field.nullable())
                .setType(toProto(field.type(), depth + 1)));
      }
      b.setStruct(struct);
    }
    return b.build();
  }

  /**
   * Converts a recursive protobuf wire message back to a {@link LogicalType}. A complex kind
   * without a shape yields the legacy non-parameterised container tag.
   *
   * @throws IllegalArgumentException on unknown kinds, invalid parameters, or excessive nesting
   */
  public static LogicalType fromProto(ai.floedb.floecat.types.rpc.LogicalType p) {
    Objects.requireNonNull(p, "logical type proto");
    return fromProto(p, 0);
  }

  private static LogicalType fromProto(ai.floedb.floecat.types.rpc.LogicalType p, int depth) {
    if (depth > LogicalTypeFormat.MAX_NESTING_DEPTH) {
      throw new IllegalArgumentException(
          "logical type nesting depth exceeds " + LogicalTypeFormat.MAX_NESTING_DEPTH);
    }
    LogicalKind kind;
    try {
      kind = LogicalKind.valueOf(p.getKind().name().substring("TK_".length()));
    } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Unrecognized logical type kind: " + p.getKind(), e);
    }
    switch (p.getShapeCase()) {
      case ARRAY -> {
        if (kind != LogicalKind.ARRAY) {
          throw new IllegalArgumentException("array shape on non-ARRAY kind: " + kind);
        }
        return LogicalType.array(
            fromProto(p.getArray().getElement(), depth + 1), p.getArray().getElementNullable());
      }
      case MAP -> {
        if (kind != LogicalKind.MAP) {
          throw new IllegalArgumentException("map shape on non-MAP kind: " + kind);
        }
        return LogicalType.map(
            fromProto(p.getMap().getKey(), depth + 1),
            fromProto(p.getMap().getValue(), depth + 1),
            p.getMap().getValueNullable());
      }
      case STRUCT -> {
        if (kind != LogicalKind.STRUCT) {
          throw new IllegalArgumentException("struct shape on non-STRUCT kind: " + kind);
        }
        java.util.List<LogicalField> fields = new java.util.ArrayList<>();
        for (ai.floedb.floecat.types.rpc.LogicalField f : p.getStruct().getFieldsList()) {
          fields.add(
              new LogicalField(f.getName(), f.getNullable(), fromProto(f.getType(), depth + 1)));
        }
        return LogicalType.struct(fields);
      }
      default -> {
        // No shape: scalar kinds, or the legacy non-parameterised container tag.
      }
    }
    if (kind == LogicalKind.DECIMAL) {
      return LogicalType.decimal(p.getPrecision(), p.getScale());
    }
    if (kind == LogicalKind.TIME
        || kind == LogicalKind.TIMESTAMP
        || kind == LogicalKind.TIMESTAMPTZ) {
      return p.hasTemporalPrecision()
          ? LogicalType.temporal(kind, p.getTemporalPrecision())
          : LogicalType.of(kind);
    }
    if (kind == LogicalKind.INTERVAL) {
      IntervalRange range =
          switch (p.getIntervalRange()) {
            case IR_YEAR_TO_MONTH -> IntervalRange.YEAR_TO_MONTH;
            case IR_DAY_TO_SECOND -> IntervalRange.DAY_TO_SECOND;
            default -> IntervalRange.UNSPECIFIED;
          };
      return LogicalType.interval(
          range,
          p.hasIntervalLeadingPrecision() ? p.getIntervalLeadingPrecision() : null,
          p.hasIntervalFractionalPrecision() ? p.getIntervalFractionalPrecision() : null);
    }
    return LogicalType.of(kind);
  }

  /**
   * Parses a type string (canonical name, SQL alias, or nested grammar) directly to the wire
   * message. Convenience for declaring schemas in code, e.g. system-object scanners.
   */
  public static ai.floedb.floecat.types.rpc.LogicalType parseToProto(String s) {
    return toProto(LogicalTypeFormat.parse(s));
  }

  public static LogicalType columnLogicalType(ScalarStats cs) {
    return decodeLogicalType(cs.getLogicalType());
  }

  public static Object columnMin(ScalarStats cs) {
    LogicalType t = columnLogicalType(cs);
    return decodeValue(t, cs.getMin());
  }

  public static Object columnMax(ScalarStats cs) {
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
