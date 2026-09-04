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

package ai.floedb.floecat.service.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.repo.impl.RelationHintsRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HintCacheTest {

  @Test
  void persistsAndAttachesTheWholeEngineScopedHintSet() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode table = table("blob://table/v1", Map.of());

    cache.persist(
        table.id(),
        relationMeta(table, pointers),
        "floedb",
        "1",
        "floe.relation+proto",
        new byte[] {1},
        List.of(
            new EngineHintPersistence.ColumnHint("floe.column+proto", 7L, new byte[] {2}),
            new EngineHintPersistence.ColumnHint("floe.type+proto", 7L, new byte[] {3})));

    UserTableNode attached = (UserTableNode) cache.attach(table, EngineContext.of("floedb", "1"));
    assertThat(attached.engineHints())
        .containsKey(new EngineHintKey("floedb", "1", "floe.relation+proto"));
    assertThat(attached.columnHints().get(7L))
        .containsKeys(
            new EngineHintKey("floedb", "1", "floe.column+proto"),
            new EngineHintKey("floedb", "1", "floe.type+proto"));
    assertThat(cache.family()).isEqualTo(CacheFamily.HINT);
  }

  @Test
  void recognizesOnlyAnIdenticalCurrentHintSet() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode table = table("blob://table/v1", Map.of());
    List<EngineHintPersistence.ColumnHint> columns =
        List.of(new EngineHintPersistence.ColumnHint("floe.column+proto", 7L, bytes(2)));
    cache.persist(
        table.id(),
        relationMeta(table, pointers),
        "floedb",
        "1",
        "floe.relation+proto",
        bytes(1),
        columns);

    assertThat(
            cache.containsAll(
                table.id(),
                table.cacheIdentity(),
                "floedb",
                "1",
                "floe.relation+proto",
                bytes(1),
                columns))
        .isTrue();
    assertThat(
            cache.containsAll(
                table.id(),
                table.cacheIdentity(),
                "floedb",
                "1",
                "floe.relation+proto",
                bytes(9),
                columns))
        .isFalse();
    assertThat(
            cache.containsAll(
                table.id(),
                table.cacheIdentity(),
                "floedb",
                "1",
                "floe.relation+proto",
                bytes(1),
                List.of(new EngineHintPersistence.ColumnHint("floe.column+proto", 7L, bytes(9)))))
        .isFalse();
    assertThat(
            cache.containsAll(
                table.id(),
                "blob://table/v2",
                "floedb",
                "1",
                "floe.relation+proto",
                bytes(1),
                columns))
        .isFalse();
  }

  @Test
  void engineVersionsCannotOverwriteEachOther() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode table = table("blob://table/v1", Map.of());
    MutationMeta relation = relationMeta(table, pointers);
    cache.persist(table.id(), relation, "floedb", "1", "type", bytes(1), List.of());
    cache.persist(table.id(), relation, "floedb", "2", "type", bytes(2), List.of());

    UserTableNode v1 = (UserTableNode) cache.attach(table, EngineContext.of("floedb", "1"));
    UserTableNode v2 = (UserTableNode) cache.attach(table, EngineContext.of("floedb", "2"));

    assertThat(v1.engineHint("floedb", "1", "type").orElseThrow().payload())
        .containsExactly((byte) 1);
    assertThat(v2.engineHint("floedb", "2", "type").orElseThrow().payload())
        .containsExactly((byte) 2);
  }

  @Test
  void unspecifiedEngineVersionRemainsAStableIndependentKey() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode table = table("blob://table/v1", Map.of());
    cache.persist(
        table.id(), relationMeta(table, pointers), "floedb", "", "type", bytes(1), List.of());

    UserTableNode attached = (UserTableNode) cache.attach(table, EngineContext.of("floedb", ""));

    assertThat(attached.engineHint("floedb", "", "type")).isPresent();
  }

  @Test
  void relationIdentityRejectsHintsComputedForOlderDdl() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode oldTable = table("blob://table/old", Map.of());
    cache.persist(
        oldTable.id(),
        relationMeta(oldTable, pointers),
        "floedb",
        "1",
        "type",
        bytes(1),
        List.of());

    UserTableNode changed =
        (UserTableNode)
            cache.attach(table("blob://table/new", Map.of()), EngineContext.of("floedb", "1"));

    assertThat(changed.engineHints()).isEmpty();
  }

  @Test
  void fallsBackToPropertiesWrittenByOlderReleases() {
    Map<String, String> properties =
        Map.of(
            EngineHintMetadata.tableHintKey("floe.relation+proto"),
            EngineHintMetadata.encodeValue("floedb", "1", bytes(1)),
            EngineHintMetadata.columnHintKey("floe.column+proto", 9L),
            EngineHintMetadata.encodeValue("floedb", "1", bytes(2)));
    var cache =
        HintCache.forTesting(
            new RelationHintsRepository(new InMemoryPointerStore(), new InMemoryBlobStore()));

    UserTableNode attached =
        (UserTableNode)
            cache.attach(table("blob://table/v1", properties), EngineContext.of("floedb", "1"));

    assertThat(attached.engineHints()).hasSize(1);
    assertThat(attached.columnHints()).containsKey(9L);
  }

  @Test
  void decodedBodyIsReadOnceAcrossWarmLookups() {
    var pointers = new InMemoryPointerStore();
    var blobs = new CountingBlobStore();
    var repository = new RelationHintsRepository(pointers, blobs);
    UserTableNode table = table("blob://table/v1", Map.of());
    HintCache.forTesting(repository)
        .persist(
            table.id(), relationMeta(table, pointers), "floedb", "1", "type", bytes(1), List.of());
    blobs.gets.set(0);
    var coldCache = HintCache.forTesting(repository);

    coldCache.attach(table, EngineContext.of("floedb", "1"));
    coldCache.attach(table, EngineContext.of("floedb", "1"));

    assertThat(blobs.gets).hasValue(1);
  }

  @Test
  void absentHintsReuseTheDecodedLegacyAnswer() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    Map<String, String> properties =
        Map.of(
            EngineHintMetadata.tableHintKey("type"),
            EngineHintMetadata.encodeValue("floedb", "1", bytes(1)));
    UserTableNode table = table("blob://table/v1", properties);

    UserTableNode first = (UserTableNode) cache.attach(table, EngineContext.of("floedb", "1"));
    UserTableNode second = (UserTableNode) cache.attach(table, EngineContext.of("floedb", "1"));

    assertThat(first.engineHints()).isSameAs(second.engineHints());
    assertThat(cache.entryCount()).isOne();
  }

  @Test
  void anAbsentEmptyHintSetDoesNotCopyTheRelationNode() {
    var cache =
        HintCache.forTesting(
            new RelationHintsRepository(new InMemoryPointerStore(), new InMemoryBlobStore()));
    UserTableNode table = table("blob://table/v1", Map.of());

    RelationNode attached = cache.attach(table, EngineContext.of("floedb", "1"));

    assertThat(attached).isSameAs(table);
  }

  @Test
  void relationDeletionRemovesEveryEngineVersion() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    var cache = HintCache.forTesting(repository);
    UserTableNode table = table("blob://table/v1", Map.of());
    MutationMeta relation = relationMeta(table, pointers);
    cache.persist(table.id(), relation, "floedb", "1", "type", bytes(1), List.of());
    cache.persist(table.id(), relation, "floedb", "2", "type", bytes(2), List.of());

    cache.deleteRelation(table.id());

    assertThat(repository.getForMutation(table.id(), "floedb", "1")).isEmpty();
    assertThat(repository.getForMutation(table.id(), "floedb", "2")).isEmpty();
    assertThat(cache.entryCount()).isZero();
  }

  private static byte[] bytes(int value) {
    return new byte[] {(byte) value};
  }

  private static MutationMeta relationMeta(UserTableNode table, InMemoryPointerStore pointers) {
    String key = Keys.tablePointerById(table.id().getAccountId(), table.id().getId());
    Pointer pointer =
        Pointer.newBuilder().setKey(key).setBlobUri(table.cacheIdentity()).setVersion(1L).build();
    pointers.compareAndSet(key, 0L, pointer);
    return MutationMeta.newBuilder()
        .setPointerKey(key)
        .setPointerVersion(pointers.get(key).orElseThrow().getVersion())
        .setBlobUri(table.cacheIdentity())
        .build();
  }

  private static UserTableNode table(String blobUri, Map<String, String> properties) {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    return new UserTableNode(
        tableId,
        blobUri,
        tableId,
        tableId,
        "orders",
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        "{}",
        properties,
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.of());
  }

  private static final class CountingBlobStore extends InMemoryBlobStore {
    private final AtomicInteger gets = new AtomicInteger();

    @Override
    public byte[] get(String uri) {
      gets.incrementAndGet();
      return super.get(uri);
    }
  }
}
