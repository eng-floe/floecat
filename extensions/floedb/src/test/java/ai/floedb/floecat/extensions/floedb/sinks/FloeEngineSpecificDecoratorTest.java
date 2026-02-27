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

package ai.floedb.floecat.extensions.floedb.sinks;

import static ai.floedb.floecat.extensions.floedb.pgcatalog.PgCatalogTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.extensions.floedb.proto.FloeDecorationFailureCode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class FloeEngineSpecificDecoratorTest {

  private final FloeEngineSpecificDecorator decorator = new FloeEngineSpecificDecorator(null);

  @Test
  void decorateColumn_mapsMissingLogicalTypeToLogicalTypeMissingExtensionCode() {
    NamespaceNode userNs = userNamespace("public", Map.of());
    SchemaColumn column = SchemaColumn.newBuilder().setId(101).setName("c_missing").build();
    TableNode table = userTable(userNs.id(), "t_missing", List.of(column), Map.of(), Map.of());
    SystemObjectScanContext ctx = contextWithRelations(userNs, table);

    RelationDecoration relation = relationDecoration(table, ctx, List.of(column));
    ColumnDecoration decoration =
        new ColumnDecoration(
            ColumnInfo.newBuilder().setId(101).setName("c_missing"), column, null, 1, relation);

    assertThatThrownBy(() -> decorator.decorateColumn(ENGINE_CTX, decoration))
        .isInstanceOf(DecorationException.class)
        .satisfies(
            throwable -> {
              DecorationException de = (DecorationException) throwable;
              assertThat(de.code())
                  .isEqualTo(ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION);
              assertThat(de.hasExtensionCodeValue()).isTrue();
              assertThat(de.extensionCodeValue())
                  .isEqualTo(
                      FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_LOGICAL_TYPE_MISSING
                          .getNumber());
            });
  }

  @Test
  void decorateColumn_mapsMissingTypeMappingToTypeMappingMissingExtensionCode() {
    NamespaceNode userNs = userNamespace("public", Map.of());
    SchemaColumn column =
        SchemaColumn.newBuilder().setId(202).setName("c_unmapped").setLogicalType("INT").build();
    TableNode table = userTable(userNs.id(), "t_unmapped", List.of(column), Map.of(), Map.of());
    SystemObjectScanContext ctx = contextWithRelations(userNs, table);

    RelationDecoration relation = relationDecoration(table, ctx, List.of(column));
    ColumnDecoration decoration =
        new ColumnDecoration(
            ColumnInfo.newBuilder().setId(202).setName("c_unmapped"), column, null, 1, relation);

    assertThatThrownBy(() -> decorator.decorateColumn(ENGINE_CTX, decoration))
        .isInstanceOf(DecorationException.class)
        .satisfies(
            throwable -> {
              DecorationException de = (DecorationException) throwable;
              assertThat(de.code())
                  .isEqualTo(ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION);
              assertThat(de.hasExtensionCodeValue()).isTrue();
              assertThat(de.extensionCodeValue())
                  .isEqualTo(
                      FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_TYPE_MAPPING_MISSING
                          .getNumber());
            });
  }

  @Test
  void decorateColumn_runtimeFailureStaysCoreDecorationError() {
    NamespaceNode userNs = userNamespace("public", Map.of());
    SchemaColumn column =
        SchemaColumn.newBuilder().setId(303).setName("c_runtime").setLogicalType("INT32").build();
    TableNode table = userTable(userNs.id(), "t_runtime", List.of(column), Map.of(), Map.of());
    SystemObjectScanContext ctx = contextWithRelations(userNs, table);

    RelationDecoration relation =
        new RelationDecoration(
            RelationInfo.newBuilder().setRelationId(table.id()),
            table.id(),
            null,
            List.of(column),
            List.of(column),
            ctx);
    ColumnDecoration decoration =
        new ColumnDecoration(
            ColumnInfo.newBuilder().setId(303).setName("c_runtime"), column, null, 1, relation);

    assertThatThrownBy(() -> decorator.decorateColumn(ENGINE_CTX, decoration))
        .isInstanceOf(DecorationException.class)
        .satisfies(
            throwable -> {
              DecorationException de = (DecorationException) throwable;
              assertThat(de.code()).isEqualTo(ColumnFailureCode.COLUMN_FAILURE_CODE_INTERNAL_ERROR);
              assertThat(de.hasExtensionCodeValue()).isFalse();
            });
  }

  private static RelationDecoration relationDecoration(
      TableNode table, SystemObjectScanContext ctx, List<SchemaColumn> schema) {
    return new RelationDecoration(
        RelationInfo.newBuilder().setRelationId(table.id()),
        table.id(),
        table,
        schema,
        schema,
        ctx);
  }
}
