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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EngineHintPersistenceImplTest {

  private static final ResourceId TABLE_ID = id("table", ResourceKind.RK_TABLE);
  private static final ResourceId VIEW_ID = id("view", ResourceKind.RK_VIEW);
  private static final byte[] PAYLOAD = new byte[] {1, 2, 3};

  @Mock TableRepository tables;
  @Mock ViewRepository views;
  @Mock HintCache hints;

  private EngineHintPersistenceImpl persistence;

  @BeforeEach
  void setUp() {
    persistence = new EngineHintPersistenceImpl(tables, views, hints);
  }

  @Test
  void writesTableHintsThroughTheDedicatedHintModule() {
    MutationMeta relation = meta("blob://table/v1");
    when(tables.pointerMetaForSafe(TABLE_ID)).thenReturn(relation);
    when(tables.pointerMetaForSafeConsistent(TABLE_ID)).thenReturn(relation);
    List<EngineHintPersistence.ColumnHint> columns =
        List.of(new EngineHintPersistence.ColumnHint("column", 7L, PAYLOAD));

    persistence.persistRelationAndColumnHints(
        TABLE_ID, "relation", PAYLOAD, "floedb", "1", columns);

    verify(hints).persist(TABLE_ID, relation, "floedb", "1", "relation", PAYLOAD, columns);
    verify(tables, never()).update(any(Table.class), anyLong());
  }

  @Test
  void identicalCachedHintsDoNotReachAuthoritativeStorage() {
    MutationMeta relation = meta("blob://table/v1");
    List<EngineHintPersistence.ColumnHint> columns =
        List.of(new EngineHintPersistence.ColumnHint("column", 7L, PAYLOAD));
    when(tables.pointerMetaForSafe(TABLE_ID)).thenReturn(relation);
    when(hints.containsAll(
            TABLE_ID, relation.getBlobUri(), "floedb", "1", "relation", PAYLOAD, columns))
        .thenReturn(true);

    persistence.persistRelationAndColumnHints(
        TABLE_ID, "relation", PAYLOAD, "floedb", "1", columns);

    verify(tables, never()).pointerMetaForSafeConsistent(TABLE_ID);
    verify(hints, never()).persist(any(), any(), any(), any(), any(), any(), anyList());
  }

  @Test
  void usesTheViewIdentityWithoutRewritingTheView() {
    MutationMeta relation = meta("blob://view/v1");
    when(views.pointerMetaForSafe(VIEW_ID)).thenReturn(relation);
    when(views.pointerMetaForSafeConsistent(VIEW_ID)).thenReturn(relation);

    persistence.persistColumnHint(VIEW_ID, 3L, "column", "floedb", "1", PAYLOAD);

    verify(hints)
        .persist(eq(VIEW_ID), eq(relation), eq("floedb"), eq("1"), isNull(), isNull(), anyList());
    verify(views, never()).update(any(View.class), anyLong());
  }

  @Test
  void persistenceRemainsBestEffortWhenTheRelationDisappears() {
    when(tables.pointerMetaForSafe(TABLE_ID)).thenReturn(meta("blob://table/v1"));
    when(tables.pointerMetaForSafeConsistent(TABLE_ID)).thenThrow(new RuntimeException("gone"));

    persistence.persistRelationHint(TABLE_ID, "relation", "floedb", "1", PAYLOAD);

    verify(hints, never()).persist(any(), any(), any(), any(), any(), any(), anyList());
  }

  private static ResourceId id(String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("account").setId(id).setKind(kind).build();
  }

  private static MutationMeta meta(String blobUri) {
    return MutationMeta.newBuilder()
        .setPointerKey("/relations/current")
        .setBlobUri(blobUri)
        .setPointerVersion(1L)
        .build();
  }
}
