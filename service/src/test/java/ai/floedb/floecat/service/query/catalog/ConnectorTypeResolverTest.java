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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ConnectorTypeResolverTest {

  @Test
  void mapsIcebergAndDeltaFormats() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();

    when(tableRepository.getById(tableId))
        .thenReturn(Optional.of(tableWithFormat(tableId, TableFormat.TF_ICEBERG)));
    assertThat(ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId))
        .isEqualTo("iceberg");

    when(tableRepository.getById(tableId))
        .thenReturn(Optional.of(tableWithFormat(tableId, TableFormat.TF_DELTA)));
    assertThat(ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId)).isEqualTo("delta");
  }

  @Test
  void returnsEmptyForUnmappedOrMissingUpstreamFormat() {
    TableRepository tableRepository = Mockito.mock(TableRepository.class);
    ResourceId tableId = ResourceId.newBuilder().setAccountId("acct").setId("tbl").build();

    when(tableRepository.getById(tableId))
        .thenReturn(Optional.of(tableWithFormat(tableId, TableFormat.TF_UNSPECIFIED)));
    assertThat(ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId)).isEmpty();

    when(tableRepository.getById(tableId))
        .thenReturn(Optional.of(Table.newBuilder().setResourceId(tableId).build()));
    assertThat(ConnectorTypeResolver.connectorTypeFor(tableRepository, tableId)).isEmpty();
  }

  private static Table tableWithFormat(ResourceId tableId, TableFormat format) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setUpstream(
            ai.floedb.floecat.catalog.rpc.UpstreamRef.newBuilder().setFormat(format).build())
        .build();
  }
}
