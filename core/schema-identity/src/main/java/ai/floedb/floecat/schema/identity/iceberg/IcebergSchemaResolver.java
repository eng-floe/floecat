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

package ai.floedb.floecat.schema.identity.iceberg;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.schema.identity.ColumnPath;
import ai.floedb.floecat.schema.identity.NodeKind;
import ai.floedb.floecat.schema.identity.ResolvedSchema;
import ai.floedb.floecat.schema.identity.SchemaNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Produces a {@link ResolvedSchema} from an Iceberg schema.
 *
 * <p>Iceberg models list elements and map keys and values as real fields with their own ids, so
 * every node here has a native id and {@link
 * ai.floedb.floecat.schema.identity.NativeIdentityAssigner} can always adopt them. Iceberg has no
 * per-node physical name, so {@link SchemaNode#sourcePhysicalPath()} is always empty.
 *
 * <p>The traversal deliberately matches the Delta producer's: struct children as {@code
 * parent.child}, list elements as {@code parent[]}, map keys as {@code parent.key}, map values as
 * <code>parent{}</code>, ordinals 1-based within the parent with a map key at 1 and its value at 2.
 * Iceberg's synthetic {@code element}/{@code key}/{@code value} field names never enter a path —
 * the node kind carries that instead, so the two formats yield the same node universe.
 */
public final class IcebergSchemaResolver {

  private IcebergSchemaResolver() {}

  /** Parses Iceberg schema JSON. A blank schema yields an empty result; a malformed one throws. */
  public static ResolvedSchema resolve(String schemaJson) {
    if (schemaJson == null || schemaJson.isBlank()) {
      return ResolvedSchema.of(TableFormat.TF_ICEBERG, List.of());
    }
    Schema schema;
    try {
      schema = SchemaParser.fromJson(schemaJson);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse Iceberg schema JSON", e);
    }
    return resolve(schema);
  }

  /** Walks an already-parsed Iceberg schema. */
  public static ResolvedSchema resolve(Schema schema) {
    List<SchemaNode> nodes = new ArrayList<>();
    int ordinal = 0;
    for (Types.NestedField field : schema.columns()) {
      walk(field, ColumnPath.ROOT.field(field.name()), NodeKind.FIELD, ++ordinal, nodes);
    }
    return ResolvedSchema.of(TableFormat.TF_ICEBERG, nodes);
  }

  private static void walk(
      Types.NestedField field,
      ColumnPath path,
      NodeKind kind,
      int ordinal,
      List<SchemaNode> nodes) {
    Type type = field.type();
    boolean leaf = !type.isStructType() && !type.isListType() && !type.isMapType();
    nodes.add(
        new SchemaNode(
            path, kind, ordinal, leaf, OptionalInt.of(field.fieldId()), Optional.empty()));

    if (type.isStructType()) {
      int childOrdinal = 0;
      for (Types.NestedField child : type.asStructType().fields()) {
        walk(child, path.field(child.name()), NodeKind.FIELD, ++childOrdinal, nodes);
      }
    } else if (type.isListType()) {
      walk(
          type.asListType().fields().get(0), path.arrayElement(), NodeKind.ARRAY_ELEMENT, 1, nodes);
    } else if (type.isMapType()) {
      Types.MapType map = type.asMapType();
      walk(map.fields().get(0), path.mapKey(), NodeKind.MAP_KEY, 1, nodes);
      walk(map.fields().get(1), path.mapValue(), NodeKind.MAP_VALUE, 2, nodes);
    }
  }
}
