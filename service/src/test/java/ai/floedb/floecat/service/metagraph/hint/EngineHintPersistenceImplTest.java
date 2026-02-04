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

package ai.floedb.floecat.service.metagraph.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EngineHintPersistenceImplTest {

  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("table-1")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final ResourceId VIEW_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("view-1")
          .setKind(ResourceKind.RK_VIEW)
          .build();
  private static final String PAYLOAD_TYPE = "floe.relation+proto";
  private static final String ENGINE_KIND = "floedb";
  private static final String ENGINE_VERSION = "1.0";
  private static final byte[] PAYLOAD = new byte[] {1, 2, 3};

  @Mock private TableRepository tableRepository;
  @Mock private ViewRepository viewRepository;

  @InjectMocks private EngineHintPersistenceImpl persistence;

  @BeforeEach
  void setUp() {
    // nothing to do; @InjectMocks handles wiring.
  }

  @Test
  void persistRelationHint_writesTableProperty() {
    Table table = Table.newBuilder().setResourceId(TABLE_ID).build();
    when(tableRepository.getById(TABLE_ID)).thenReturn(Optional.of(table));
    when(tableRepository.metaForSafe(TABLE_ID)).thenReturn(meta(42L));
    when(tableRepository.update(any(Table.class), eq(42L))).thenReturn(true);

    persistence.persistRelationHint(TABLE_ID, PAYLOAD_TYPE, ENGINE_KIND, ENGINE_VERSION, PAYLOAD);

    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    verify(tableRepository).update(tableCaptor.capture(), eq(42L));
    Table updated = tableCaptor.getValue();
    String key = EngineHintMetadata.tableHintKey(PAYLOAD_TYPE);
    assertThat(updated.getPropertiesMap()).containsEntry(key, encode());
    verify(viewRepository, never()).update(any(), anyLong());
  }

  @Test
  void persistRelationHint_writesViewProperty() {
    View view = View.newBuilder().setResourceId(VIEW_ID).build();
    when(viewRepository.getById(VIEW_ID)).thenReturn(Optional.of(view));
    when(viewRepository.metaForSafe(VIEW_ID)).thenReturn(meta(100L));
    when(viewRepository.update(any(View.class), eq(100L))).thenReturn(true);

    persistence.persistRelationHint(VIEW_ID, PAYLOAD_TYPE, ENGINE_KIND, ENGINE_VERSION, PAYLOAD);

    ArgumentCaptor<View> viewCaptor = ArgumentCaptor.forClass(View.class);
    verify(viewRepository).update(viewCaptor.capture(), eq(100L));
    View updated = viewCaptor.getValue();
    String key = EngineHintMetadata.tableHintKey(PAYLOAD_TYPE);
    assertThat(updated.getPropertiesMap()).containsEntry(key, encode());
    verify(tableRepository, never()).update(any(), anyLong());
  }

  @Test
  void persistRelationHint_skipsWhenUnchanged() {
    Table table =
        Table.newBuilder()
            .setResourceId(TABLE_ID)
            .putProperties(EngineHintMetadata.tableHintKey(PAYLOAD_TYPE), encode())
            .build();
    when(tableRepository.getById(TABLE_ID)).thenReturn(Optional.of(table));
    persistence.persistRelationHint(TABLE_ID, PAYLOAD_TYPE, ENGINE_KIND, ENGINE_VERSION, PAYLOAD);

    verify(tableRepository, never()).update(any(), anyLong());
  }

  @Test
  void persistColumnHint_writesColumnProperty() {
    Table table = Table.newBuilder().setResourceId(TABLE_ID).build();
    when(tableRepository.getById(TABLE_ID)).thenReturn(Optional.of(table));
    when(tableRepository.metaForSafe(TABLE_ID)).thenReturn(meta(77L));
    when(tableRepository.update(any(Table.class), eq(77L))).thenReturn(true);

    persistence.persistColumnHints(
        TABLE_ID,
        ENGINE_KIND,
        ENGINE_VERSION,
        List.of(new EngineHintPersistence.ColumnHint("floe.column+proto", 5L, PAYLOAD)));

    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    verify(tableRepository).update(tableCaptor.capture(), eq(77L));
    Table updated = tableCaptor.getValue();
    String key = EngineHintMetadata.columnHintKey("floe.column+proto", 5L);
    assertThat(updated.getPropertiesMap()).containsEntry(key, encode());
  }

  private static MutationMeta meta(long version) {
    return MutationMeta.newBuilder().setPointerVersion(version).build();
  }

  private static String encode() {
    return EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, PAYLOAD);
  }
}
