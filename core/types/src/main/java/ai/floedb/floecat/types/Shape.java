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

import java.util.List;
import java.util.Objects;

/**
 * Nested structure of a complex {@link LogicalType}, mirroring the wire model's {@code oneof shape}
 * so the Java and proto representations cannot drift: which children a container carries is fixed
 * by its shape variant, nullability lives on the shape that owns it (primitive, no tri-state), and
 * adapters dispatch exhaustively instead of probing field presence.
 *
 * <p>{@code Array} for {@link LogicalKind#ARRAY}, {@code Map} for {@link LogicalKind#MAP}, {@code
 * Struct} for {@link LogicalKind#STRUCT}. A complex {@link LogicalType} without a shape is the
 * legacy non-parameterised container tag. Map keys carry no nullability — they are always required.
 */
public sealed interface Shape {

  /** Shape of an ARRAY: element type plus element nullability. */
  record Array(LogicalType element, boolean elementNullable) implements Shape {
    public Array {
      Objects.requireNonNull(element, "element type");
    }
  }

  /** Shape of a MAP: key and value types plus value nullability (keys are always required). */
  record Map(LogicalType key, LogicalType value, boolean valueNullable) implements Shape {
    public Map {
      Objects.requireNonNull(key, "key type");
      Objects.requireNonNull(value, "value type");
    }
  }

  /**
   * Shape of a STRUCT: ordered named fields. An empty list is an explicitly known empty struct —
   * distinct from the legacy non-parameterised tag, which has no shape at all.
   */
  record Struct(List<LogicalField> fields) implements Shape {
    public Struct {
      Objects.requireNonNull(fields, "struct fields");
      fields = List.copyOf(fields);
    }
  }
}
