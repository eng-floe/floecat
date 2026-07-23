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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Guards the phantom-cleanup hazard: {@code tableRepo.delete} returning {@code false} (a concurrent
 * update/rename won the canonical-pointer CAS, so the table is still alive) must never trigger the
 * table's owned-state purge. In the guarded recursive path this aborts for retry; in the unguarded
 * account-teardown path it is skipped so cleanup never raises a retryable abort.
 */
class RecursiveResourceDropperTest {

  private static final ResourceId CATALOG =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("cat")
          .setKind(ResourceKind.RK_CATALOG)
          .build();
  private static final ResourceId ROOT_NS =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("ns")
          .setKind(ResourceKind.RK_NAMESPACE)
          .build();
  private static final ResourceId TABLE_ID =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private NamespaceRepository namespaceRepo;
  private TableRepository tableRepo;
  private TableRootRepository tableRoots;
  private ViewRepository viewRepo;
  private PointerStore pointerStore;
  private RecursiveResourceDropper dropper;

  private final Namespace root =
      Namespace.newBuilder()
          .setResourceId(ROOT_NS)
          .setCatalogId(CATALOG)
          .setDisplayName("ns")
          .build();
  private final Table table =
      Table.newBuilder()
          .setResourceId(TABLE_ID)
          .setCatalogId(CATALOG)
          .setNamespaceId(ROOT_NS)
          .setDisplayName("orders")
          .build();

  @BeforeEach
  void setUp() {
    namespaceRepo = mock(NamespaceRepository.class);
    tableRepo = mock(TableRepository.class);
    tableRoots = mock(TableRootRepository.class);
    viewRepo = mock(ViewRepository.class);
    pointerStore = mock(PointerStore.class);

    dropper = new RecursiveResourceDropper();
    dropper.namespaceRepo = namespaceRepo;
    dropper.tableRepo = tableRepo;
    dropper.tableRoots = tableRoots;
    dropper.viewRepo = viewRepo;
    dropper.metadataGraph = mock(UserGraph.class);
    dropper.topology = mock(TopologyGraph.class);
    dropper.markerStore = mock(MarkerStore.class);
    dropper.pointerStore = pointerStore;

    // No descendants: dropNamespaceContents processes only the root's own relations.
    when(namespaceRepo.list(anyString(), anyString(), any(), anyInt(), anyString(), any()))
        .thenReturn(List.of());
    // One table under the root, single page.
    when(tableRepo.list(anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of(table));
    when(viewRepo.list(anyString(), anyString(), anyString(), anyInt(), anyString(), any()))
        .thenReturn(List.of());
  }

  @Test
  void guardedDropAbortsWhenTableDeleteDoesNotCommit() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(false);

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () -> dropper.dropNamespaceContents(root, true));

    // The surviving table's owned state must not be purged.
    verify(pointerStore, never()).deleteByPrefix(anyString());
    verify(tableRoots, never()).purgeRoot(any());
  }

  @Test
  void unguardedDropSkipsCleanupWhenTableDeleteDoesNotCommit() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(false);

    var summary = dropper.dropNamespaceContents(root, false);

    // No retryable abort (account teardown must not re-enter deleteAccount), and no phantom purge.
    assertEquals(0, summary.tablesDeleted);
    assertEquals(0, summary.snapshotPrefixesDeleted);
    verify(pointerStore, never()).deleteByPrefix(anyString());
    verify(tableRoots, never()).purgeRoot(any());
  }

  @Test
  void committedTableDeletePurgesOwnedState() {
    when(tableRepo.delete(eq(TABLE_ID))).thenReturn(true);

    var summary = dropper.dropNamespaceContents(root, true);

    assertEquals(1, summary.tablesDeleted);
    assertEquals(1, summary.snapshotPrefixesDeleted);
    verify(tableRoots).purgeRoot(eq(TABLE_ID));
  }
}
