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
package ai.floedb.floecat.extensions.floedb.hints;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.systemcatalog.hint.HintClearContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.JsonFormat;
import java.util.List;
import org.junit.jupiter.api.Test;

class FloeHintClearPolicyTest {

  private static final JsonFormat.Printer PRINTER =
      JsonFormat.printer().omittingInsignificantWhitespace();

  @Test
  void displayNameChange_dropsAll() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl2", "ns", schemaJson(column(1, "a")));
    HintClearContext context = newHintContext(before, after, relationMask());
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllRelationHints()).isTrue();
    assertThat(decision.clearAllColumnHints()).isTrue();
  }

  @Test
  void namespaceChange_dropsAll() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns2", schemaJson(column(1, "a")));
    HintClearContext context = newHintContext(before, after, relationMask());
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllColumnHints()).isTrue();
    assertThat(decision.clearAllRelationHints()).isTrue();
  }

  @Test
  void upstreamMask_triggersDropAll() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    HintClearContext context =
        newHintContext(before, after, FieldMask.newBuilder().addPaths("upstream.format").build());
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllRelationHints()).isTrue();
  }

  @Test
  void schemaUnchangedMaskNull_noOp() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.columnIds()).isEmpty();
    assertThat(decision.clearAllColumnHints()).isFalse();
    assertThat(decision.clearAllRelationHints()).isFalse();
  }

  @Test
  void columnAddition_detectsId() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a"), column(2, "b")));
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllColumnHints()).isFalse();
    assertThat(decision.columnIds()).containsExactly(2L);
  }

  @Test
  void displayNameMask_aloneTriggersDropAll() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    HintClearContext context = newHintContext(before, after, relationMask());
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllRelationHints()).isTrue();
  }

  @Test
  void schemaJsonChangeWithoutDiff_clearsAllColumns() {
    Table before = baseTable("tbl", "ns", "garbage");
    Table after = baseTable("tbl", "ns", "garbage2");
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllColumnHints()).isTrue();
  }

  @Test
  void schemaMask_triggersColumnDiffPath() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a"), column(2, "b")));
    HintClearContext context =
        newHintContext(before, after, FieldMask.newBuilder().addPaths("schema_json").build());
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllColumnHints()).isFalse();
    assertThat(decision.columnIds()).containsExactly(2L);
  }

  @Test
  void columnTypeChange_returnsId() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    SchemaColumn changed = column(1, "a");
    Table after =
        baseTable("tbl", "ns", schemaJson(changed.toBuilder().setLogicalType("BIGINT").build()));
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.columnIds()).containsExactly(1L);
  }

  @Test
  void columnRemoval_returnsId() {
    Table before = baseTable("tbl", "ns", schemaJson(column(1, "a"), column(2, "b")));
    Table after = baseTable("tbl", "ns", schemaJson(column(1, "a")));
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.columnIds()).containsExactly(2L);
  }

  @Test
  void malformedSchema_fallsBackToClearAllColumns() {
    Table before = baseTable("tbl", "ns", "not json");
    Table after = baseTable("tbl", "ns", "also bad");
    HintClearContext context = newHintContext(before, after, null);
    FloeHintClearPolicy policy = new FloeHintClearPolicy();

    HintClearDecision decision = policy.decide(context);

    assertThat(decision.clearAllColumnHints()).isTrue();
  }

  private static Table baseTable(String name, String namespaceId, String schemaJson) {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setId(name)
                .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_TABLE)
                .build())
        .setDisplayName(name)
        .setNamespaceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setId(namespaceId)
                .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_NAMESPACE)
                .build())
        .setSchemaJson(schemaJson)
        .build();
  }

  private static SchemaColumn column(long id, String name) {
    return SchemaColumn.newBuilder()
        .setId(id)
        .setName(name)
        .setFieldId((int) id)
        .setLogicalType("INT")
        .build();
  }

  private static String schemaJson(SchemaColumn... columns) {
    SchemaDescriptor descriptor =
        SchemaDescriptor.newBuilder().addAllColumns(List.of(columns)).build();
    try {
      return PRINTER.print(descriptor);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static HintClearContext newHintContext(Table before, Table after, FieldMask mask) {
    return new HintClearContext(before.getResourceId(), mask, before, after, null, null);
  }

  private static FieldMask relationMask() {
    return FieldMask.newBuilder().addPaths("display_name").build();
  }
}
