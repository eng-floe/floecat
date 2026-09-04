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

import java.util.Objects;

/**
 * A named field of a {@link LogicalKind#STRUCT} logical type.
 *
 * <p>Field names are preserved verbatim (case-sensitive) — they are source-format identifiers, not
 * canonical type names.
 *
 * @param name the field name (must be non-blank)
 * @param nullable whether the field accepts nulls
 * @param type the field's logical type
 * @param columnId stable source-format identity, or zero when unavailable
 * @see LogicalType#struct(java.util.List)
 */
public record LogicalField(String name, boolean nullable, LogicalType type, long columnId) {
  /** Constructs a field whose source does not expose a stable identity. */
  public LogicalField(String name, boolean nullable, LogicalType type) {
    this(name, nullable, type, 0L);
  }

  public LogicalField {
    Objects.requireNonNull(name, "field name");
    Objects.requireNonNull(type, "field type");
    if (name.isBlank()) {
      throw new IllegalArgumentException("struct field name must not be blank");
    }
    if (columnId < 0) {
      throw new IllegalArgumentException("struct field column id must not be negative");
    }
  }
}
