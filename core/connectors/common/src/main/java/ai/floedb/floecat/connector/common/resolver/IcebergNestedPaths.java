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

package ai.floedb.floecat.connector.common.resolver;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * The single recursive traversal over an Iceberg schema's nested fields, defining the canonical
 * physical-path notation shared by schema construction ({@link IcebergSchemaMapper}) and the
 * fieldId→path/ordinal maps used by stats. Both must visit the same nodes with the same paths — two
 * independent walks drift, leaving stats referring to paths with no matching schema column.
 *
 * <p>Path notation: struct children as {@code parent.child}, list elements as {@code parent[]}, map
 * keys as {@code parent.key}, map values as {@code parent{}}. Iceberg's synthetic
 * "element"/"key"/"value" field names never appear in paths. Ordinals are 1-based within the parent
 * (list element = 1; map key = 1, value = 2).
 */
public final class IcebergNestedPaths {

  private IcebergNestedPaths() {}

  /** Callback invoked once per visited field, including synthetic element/key/value nodes. */
  public interface Visitor {
    void visit(Types.NestedField field, String path, int ordinal);
  }

  /** Walks every field of the schema in pre-order, top-level columns first. */
  public static void walk(Schema schema, Visitor visitor) {
    int ordinal = 0;
    for (Types.NestedField field : schema.columns()) {
      walkField(field, field.name(), ++ordinal, visitor);
    }
  }

  private static void walkField(Types.NestedField field, String path, int ordinal, Visitor v) {
    v.visit(field, path, ordinal);

    Type t = field.type();
    if (t.isStructType()) {
      int childOrdinal = 0;
      for (Types.NestedField child : t.asStructType().fields()) {
        walkField(child, path + "." + child.name(), ++childOrdinal, v);
      }
    } else if (t.isListType()) {
      walkField(t.asListType().fields().get(0), path + "[]", 1, v);
    } else if (t.isMapType()) {
      Types.MapType mt = t.asMapType();
      walkField(mt.fields().get(0), path + ".key", 1, v);
      walkField(mt.fields().get(1), path + "{}", 2, v);
    }
  }
}
