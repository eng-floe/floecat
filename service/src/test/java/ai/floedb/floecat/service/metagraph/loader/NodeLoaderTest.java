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

package ai.floedb.floecat.service.metagraph.loader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.testsupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.FakeViewRepository;
import org.junit.jupiter.api.Test;

class NodeLoaderTest {

  @Test
  void tableHydratesWithASinglePointerReadAndNoGetById() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader =
        new NodeLoader(
            new FakeCatalogRepository(),
            new FakeNamespaceRepository(),
            tableRepo,
            new FakeViewRepository());

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("t1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("orders")
            .setSchemaJson("{}")
            .build(),
        MutationMeta.newBuilder().setPointerVersion(1L).setBlobUri("blob/t1").build());

    assertThat(loader.table(tableId)).isPresent();

    // One pointer read (the meta), then hydrate from the blob it names — not a second pointer read
    // via getById.
    assertThat(tableRepo.metaForSafeCount(tableId)).isEqualTo(1);
    assertThat(tableRepo.getByIdCount(tableId)).isEqualTo(0);
  }

  @Test
  void tableFromBlobReadsThePinnedBlobAndNeverTheCurrentPointer() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader =
        new NodeLoader(
            new FakeCatalogRepository(),
            new FakeNamespaceRepository(),
            tableRepo,
            new FakeViewRepository());

    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("t1")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    tableRepo.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setDisplayName("orders")
            .setSchemaJson("{}")
            .build(),
        MutationMeta.newBuilder().setPointerVersion(1L).setBlobUri("blob/pinned").build());

    assertThat(loader.tableFromBlob(tableId, "blob/pinned")).isPresent();

    // Read straight from the pinned blob: no pointer read, no getById fallback to current state.
    assertThat(tableRepo.metaForSafeCount(tableId)).isEqualTo(0);
    assertThat(tableRepo.getByIdCount(tableId)).isEqualTo(0);
  }

  @Test
  void tableFromBlobIsEmptyWhenThePinnedBlobIsGone() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader =
        new NodeLoader(
            new FakeCatalogRepository(),
            new FakeNamespaceRepository(),
            tableRepo,
            new FakeViewRepository());
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("t1")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    // No fallback to the current pointer: a missing pinned blob is empty, so the caller fails hard.
    assertThat(loader.tableFromBlob(tableId, "blob/absent")).isEmpty();
    assertThat(tableRepo.getByIdCount(tableId)).isEqualTo(0);
  }

  @Test
  void loadRebuildsFromTheLivePointerWhenTheBlobMoved() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader = loaderWith(tableRepo);
    ResourceId tableId = tableId();
    // The live pointer already moved on: the blob the caller's meta names was superseded + swept.
    tableRepo.put(orders(tableId), meta(2L, "blob/live"));

    var node = loader.load(tableId, meta(1L, "blob/swept"));

    // Rebuilt from the blob the LIVE pointer names — identity matches the content it carries.
    assertThat(node).isPresent();
    assertThat(node.get().cacheIdentity()).isEqualTo("blob/live");
    assertThat(tableRepo.getByIdCount(tableId)).isEqualTo(0);
  }

  @Test
  void loadFailsLoudWhenTheLivePointerStillNamesTheMissingBlob() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader = loaderWith(tableRepo);
    ResourceId tableId = tableId();
    // Pointer exists and names a blob nobody can load: the dangling-pointer corruption shape.
    tableRepo.putMeta(tableId, meta(1L, "blob/dangling"));

    assertThrows(
        BaseResourceRepository.CorruptionException.class,
        () -> loader.load(tableId, meta(1L, "blob/dangling")),
        "a current pointer whose blob is lost must surface as corruption, never as absence");
  }

  @Test
  void loadIsEmptyWhenTheResourceWasDropped() {
    FakeTableRepository tableRepo = new FakeTableRepository();
    NodeLoader loader = loaderWith(tableRepo);
    ResourceId tableId = tableId();
    // The pointer was deleted under the reader: the live meta names no blob — genuine absence.
    tableRepo.putMeta(tableId, MutationMeta.newBuilder().setPointerVersion(0L).build());

    assertThat(loader.load(tableId, meta(1L, "blob/swept"))).isEmpty();
  }

  private static NodeLoader loaderWith(FakeTableRepository tableRepo) {
    return new NodeLoader(
        new FakeCatalogRepository(),
        new FakeNamespaceRepository(),
        tableRepo,
        new FakeViewRepository());
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("account")
        .setId("t1")
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static MutationMeta meta(long version, String blobUri) {
    return MutationMeta.newBuilder().setPointerVersion(version).setBlobUri(blobUri).build();
  }

  private static Table orders(ResourceId tableId) {
    return Table.newBuilder()
        .setResourceId(tableId)
        .setDisplayName("orders")
        .setSchemaJson("{}")
        .build();
  }

  @Test
  void parseFqn_nameOnly() {
    NameRef ref = NodeLoader.parseFqn("tbl");
    assertEquals("tbl", ref.getName());
    assertEquals("", ref.getCatalog());
    assertEquals(0, ref.getPathCount());
  }

  @Test
  void parseFqn_catalogAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(0, ref.getPathCount());
    assertEquals("tbl", ref.getName());
  }

  @Test
  void parseFqn_catalogPathAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.sales.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(1, ref.getPathCount());
    assertEquals("sales", ref.getPath(0));
    assertEquals("tbl", ref.getName());
  }

  @Test
  void parseFqn_catalogTwoPathsAndName() {
    NameRef ref = NodeLoader.parseFqn("cat.ns1.ns2.tbl");
    assertEquals("cat", ref.getCatalog());
    assertEquals(2, ref.getPathCount());
    assertEquals("ns1", ref.getPath(0));
    assertEquals("ns2", ref.getPath(1));
    assertEquals("tbl", ref.getName());
  }
}
