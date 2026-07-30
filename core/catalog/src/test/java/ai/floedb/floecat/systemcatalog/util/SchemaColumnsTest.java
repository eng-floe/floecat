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

package ai.floedb.floecat.systemcatalog.util;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.rpc.LogicalType;
import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaColumnsTest {

  private static SchemaColumn col(String name, String path, LogicalType.Kind kind) {
    return SchemaColumn.newBuilder()
        .setName(name)
        .setPhysicalPath(path)
        .setType(LogicalType.newBuilder().setKind(kind))
        .build();
  }

  @Test
  void keepsTopLevelColumnsWhoseNamesLookLikeContainerPaths() {
    var columns =
        List.of(
            col("items[]", "items[]", LogicalType.Kind.TK_INT),
            col("m{}", "m{}", LogicalType.Kind.TK_STRING),
            col("plain", "plain", LogicalType.Kind.TK_INT));

    assertThat(SchemaColumns.withoutSyntheticNodes(columns))
        .extracting(SchemaColumn::getName)
        .containsExactly("items[]", "m{}", "plain");
  }

  @Test
  void dropsSyntheticNestedPlaceholdersButKeepsStructFields() {
    var columns =
        List.of(
            col("items", "items", LogicalType.Kind.TK_ARRAY),
            col("element", "items[]", LogicalType.Kind.TK_STRUCT),
            col("sku", "items[].sku", LogicalType.Kind.TK_STRING),
            col("m", "m", LogicalType.Kind.TK_MAP),
            col("key", "m.key", LogicalType.Kind.TK_STRING),
            col("value", "m{}", LogicalType.Kind.TK_INT));

    assertThat(SchemaColumns.withoutSyntheticNodes(columns))
        .extracting(SchemaColumn::getPhysicalPath)
        .containsExactly("items", "items[].sku", "m");
  }

  @Test
  void keepsStructFieldNamedKeyWhoseParentIsNotAMap() {
    var columns =
        List.of(
            col("s", "s", LogicalType.Kind.TK_STRUCT),
            col("key", "s.key", LogicalType.Kind.TK_STRING));

    assertThat(SchemaColumns.withoutSyntheticNodes(columns))
        .extracting(SchemaColumn::getPhysicalPath)
        .containsExactly("s", "s.key");
  }
}
