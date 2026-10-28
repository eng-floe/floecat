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
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RelationMapperTest {
  @Test
  void canonicalTableWithNoGraphSchemaFailsFast() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(id)
            .setDisplayName("events")
            .setSchemaJson("{\"type\":\"struct\",\"fields\":[]}")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_DELTA)
                    .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_CANONICAL_MAP))
            .build();
    CatalogGraphView graph = mock(CatalogGraphView.class);
    CatalogContext context = CatalogContext.empty();
    when(graph.tableSchema(id, context)).thenReturn(List.of());
    when(graph.tableName(id, context)).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                new RelationMapper(graph, context, ignored -> Optional.empty())
                    .fromTable(table, true, false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Canonical column identity map is required");
  }

  @Test
  void pathOrdinalDeltaTableMapsWithoutIdentityMap() {
    ResourceId id =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table")
            .build();
    Table table =
        Table.newBuilder()
            .setResourceId(id)
            .setDisplayName("events")
            .setSchemaJson(
                "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}]}")
            .setUpstream(
                UpstreamRef.newBuilder()
                    .setFormat(TableFormat.TF_DELTA)
                    .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_PATH_ORDINAL))
            .build();
    CatalogGraphView graph = mock(CatalogGraphView.class);
    CatalogContext context = CatalogContext.empty();
    when(graph.tableSchema(id, context)).thenReturn(List.of());
    when(graph.tableName(id, context)).thenReturn(Optional.empty());

    var relation =
        new RelationMapper(graph, context, ignored -> Optional.empty())
            .fromTable(table, true, false);

    assertThat(relation.getSchema().getColumnsCount()).isEqualTo(1);
    assertThat(relation.getSchema().getColumns(0).getName()).isEqualTo("id");
    assertThat(relation.getSchema().getColumns(0).getId()).isNotZero();
  }
}
