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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * View output-column selection must be top-level membership, not stats-leaf eligibility. Filtering
 * on {@code leaf} dropped complex top-level outputs (a view whose only output is {@code
 * ARRAY<STRING>} appeared to have no outputs) and replaced STRUCT outputs with their flattened
 * children.
 */
class ViewOutputColumnsTest {

  private final LogicalSchemaMapper mapper = new LogicalSchemaMapper();

  private SchemaDescriptor iceberg(String schemaJson) {
    return mapper.mapRaw(
        ColumnIdAlgorithm.CID_PATH_ORDINAL, TableFormat.TF_ICEBERG, schemaJson, Set.of());
  }

  private static LogicalType type(SchemaColumn c) {
    return LogicalTypeProtoAdapter.columnType(c);
  }

  @Test
  void arrayOnlyViewKeepsItsSingleOutputColumn() {
    SchemaDescriptor schema =
        iceberg(
            """
            {"type":"struct","fields":[
              {"id":1,"name":"tags","required":false,
               "type":{"type":"list","element-id":2,"element":"string","element-required":false}}
            ]}
            """);

    List<SchemaColumn> out = QueuedReconcileWorkerSupport.topLevelOutputColumns(schema);

    assertThat(out).hasSize(1);
    SchemaColumn tags = out.get(0);
    assertThat(tags.getName()).isEqualTo("tags");
    assertThat(type(tags).kind()).isEqualTo(LogicalKind.ARRAY);
    assertThat(type(tags).element()).isEqualTo(LogicalType.of(LogicalKind.STRING));
  }

  @Test
  void structOnlyViewKeepsTopLevelStructNotFlattenedChildren() {
    SchemaDescriptor schema =
        iceberg(
            """
            {"type":"struct","fields":[
              {"id":1,"name":"info","required":false,
               "type":{"type":"struct","fields":[
                 {"id":2,"name":"a","required":true,"type":"int"},
                 {"id":3,"name":"b","required":false,"type":"string"}
               ]}}
            ]}
            """);

    List<SchemaColumn> out = QueuedReconcileWorkerSupport.topLevelOutputColumns(schema);

    assertThat(out).hasSize(1);
    SchemaColumn info = out.get(0);
    assertThat(info.getName()).isEqualTo("info");
    assertThat(type(info).kind()).isEqualTo(LogicalKind.STRUCT);
    assertThat(type(info).fields()).hasSize(2);
    assertThat(type(info).fields().get(0).name()).isEqualTo("a");
  }

  @Test
  void mixedViewKeepsAllTopLevelColumnsInOrderWithMetadata() {
    SchemaDescriptor schema =
        iceberg(
            """
            {"type":"struct","fields":[
              {"id":1,"name":"id","required":true,"type":"long"},
              {"id":2,"name":"tags","required":false,
               "type":{"type":"list","element-id":5,"element":"string","element-required":false}},
              {"id":3,"name":"info","required":false,
               "type":{"type":"struct","fields":[
                 {"id":4,"name":"x","required":true,"type":"int"}
               ]}}
            ]}
            """);

    List<SchemaColumn> out = QueuedReconcileWorkerSupport.topLevelOutputColumns(schema);

    assertThat(out).extracting(SchemaColumn::getName).containsExactly("id", "tags", "info");
    // Nested child rows (info.x) must not leak into view outputs.
    assertThat(out).noneMatch(c -> c.getPhysicalPath().contains("."));
    // Typed columns keep nullability, field IDs, and ordinals; the internal id is cleared.
    SchemaColumn id = out.get(0);
    assertThat(id.getNullable()).isFalse();
    assertThat(id.getFieldId()).isEqualTo(1);
    assertThat(id.getOrdinal()).isEqualTo(1);
    assertThat(id.getId()).isZero();
    // leaf keeps its stats meaning without deciding membership.
    assertThat(id.getLeaf()).isTrue();
    assertThat(out.get(1).getLeaf()).isFalse();
  }

  @Test
  void genericFormatViewKeepsArrayOutput() {
    SchemaDescriptor schema =
        mapper.mapRaw(
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            TableFormat.TF_UNSPECIFIED,
            """
            {"cols":[
              {"name":"id","type":"int"},
              {"name":"tags","type":"ARRAY<STRING>"}
            ]}
            """,
            Set.of());

    List<SchemaColumn> out = QueuedReconcileWorkerSupport.topLevelOutputColumns(schema);

    assertThat(out).extracting(SchemaColumn::getName).containsExactly("id", "tags");
    assertThat(type(out.get(1)).kind()).isEqualTo(LogicalKind.ARRAY);
  }
}
