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

package ai.floedb.floecat.schema.identity.delta;

import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Structural index for walking a Parquet column path onto a schema node.
 *
 * <p>Resolution is a walk, not a text rewrite: a segment is only read as a Parquet structural
 * wrapper when the node it would apply to actually is a collection. That keeps {@code list}, {@code
 * element}, {@code key_value}, {@code key} and {@code value} usable as ordinary Delta field names.
 */
final class DeltaParquetPathIndex {

  static final String LIST = "list";
  static final String ELEMENT = "element";
  static final String KEY_VALUE = "key_value";
  static final String KEY = "key";
  static final String VALUE = "value";

  private final Map<String, DeltaParquetPathIndex> structChildren = new LinkedHashMap<>();
  private DeltaParquetPathIndex elementChild;
  private DeltaParquetPathIndex keyChild;
  private DeltaParquetPathIndex valueChild;
  private SchemaNode node;

  DeltaParquetPathIndex structChild(String physicalName, String logicalName, boolean mapped) {
    DeltaParquetPathIndex child = new DeltaParquetPathIndex();
    // Parquet writes physical names, so those bind first. The logical alias is a convenience for
    // unmapped tables and must not shadow another column's physical name under mapping.
    structChildren.putIfAbsent(physicalName, child);
    if (!mapped) {
      structChildren.putIfAbsent(logicalName, child);
    }
    return child;
  }

  DeltaParquetPathIndex elementChild() {
    elementChild = new DeltaParquetPathIndex();
    return elementChild;
  }

  DeltaParquetPathIndex keyChild() {
    keyChild = new DeltaParquetPathIndex();
    return keyChild;
  }

  DeltaParquetPathIndex valueChild() {
    valueChild = new DeltaParquetPathIndex();
    return valueChild;
  }

  void setNode(SchemaNode node) {
    this.node = node;
  }

  Optional<SchemaNode> resolve(String parquetPath) {
    if (parquetPath == null || parquetPath.isBlank()) {
      return Optional.empty();
    }
    String[] segments = parquetPath.trim().split("\\.");
    DeltaParquetPathIndex current = this;
    int i = 0;
    while (i < segments.length) {
      String segment = segments[i];
      DeltaParquetPathIndex child = current.structChildren.get(segment);
      if (child != null) {
        current = child;
        i++;
        continue;
      }
      if (current.elementChild != null) {
        if (LIST.equals(segment) && i + 1 < segments.length && ELEMENT.equals(segments[i + 1])) {
          current = current.elementChild;
          i += 2;
          continue;
        }
        if (ELEMENT.equals(segment)) {
          current = current.elementChild;
          i++;
          continue;
        }
      }
      if (current.keyChild != null || current.valueChild != null) {
        if (KEY_VALUE.equals(segment) && i + 1 < segments.length) {
          DeltaParquetPathIndex mapChild = current.mapChild(segments[i + 1]);
          if (mapChild != null) {
            current = mapChild;
            i += 2;
            continue;
          }
        }
        DeltaParquetPathIndex mapChild = current.mapChild(segment);
        if (mapChild != null) {
          current = mapChild;
          i++;
          continue;
        }
      }
      return Optional.empty();
    }
    return Optional.ofNullable(current.node);
  }

  private DeltaParquetPathIndex mapChild(String segment) {
    if (KEY.equals(segment)) {
      return keyChild;
    }
    return VALUE.equals(segment) ? valueChild : null;
  }
}
