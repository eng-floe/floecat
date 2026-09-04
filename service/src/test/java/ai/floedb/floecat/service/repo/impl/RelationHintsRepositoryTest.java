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

package ai.floedb.floecat.service.repo.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.EngineHintPayload;
import ai.floedb.floecat.catalog.rpc.RelationHintsResource;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

class RelationHintsRepositoryTest {

  @Test
  void independentlyPublishesEachEngineVersion() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    ResourceId relationId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("orders")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    RelationHintsResource v1 = hints(relationId, "1", "v1");
    RelationHintsResource v2 = hints(relationId, "2", "v2");
    MutationMeta relation = relationMeta(relationId, pointers);
    repository.create(v1, relation).orElseThrow();
    repository.create(v2, relation).orElseThrow();

    assertEquals(v1, repository.getForMutation(relationId, "floedb", "1").orElseThrow().value());
    assertEquals(v2, repository.getForMutation(relationId, "floedb", "2").orElseThrow().value());
    assertTrue(repository.getForMutation(relationId, "floedb", "3").isEmpty());
  }

  @Test
  void updateIsFencedByTheObservedPointerVersion() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    ResourceId relationId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("orders")
            .setKind(ResourceKind.RK_VIEW)
            .build();
    MutationMeta relation = relationMeta(relationId, pointers);
    var created = repository.create(hints(relationId, "1", "before"), relation).orElseThrow();

    assertTrue(repository.update(hints(relationId, "1", "lost"), 0L, relation).isEmpty());
    assertTrue(
        repository
            .update(hints(relationId, "1", "after"), created.meta().getPointerVersion(), relation)
            .isPresent());
    assertEquals(
        "after",
        repository
            .getForMutation(relationId, "floedb", "1")
            .orElseThrow()
            .value()
            .getRelationIdentity());
  }

  @Test
  void relationVersionFencesHintWrites() {
    var pointers = new InMemoryPointerStore();
    var repository = new RelationHintsRepository(pointers, new InMemoryBlobStore());
    ResourceId relationId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("orders")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    MutationMeta relation = relationMeta(relationId, pointers);
    pointers.compareAndDelete(relation.getPointerKey(), relation.getPointerVersion());

    assertTrue(repository.create(hints(relationId, "1", "stale"), relation).isEmpty());
  }

  private static MutationMeta relationMeta(ResourceId relationId, InMemoryPointerStore pointers) {
    String key =
        relationId.getKind() == ResourceKind.RK_VIEW
            ? Keys.viewPointerById(relationId.getAccountId(), relationId.getId())
            : Keys.tablePointerById(relationId.getAccountId(), relationId.getId());
    Pointer pointer =
        Pointer.newBuilder().setKey(key).setBlobUri("blob://relation").setVersion(1L).build();
    pointers.compareAndSet(key, 0L, pointer);
    return MutationMeta.newBuilder()
        .setPointerKey(key)
        .setPointerVersion(pointers.get(key).orElseThrow().getVersion())
        .setBlobUri("blob://relation")
        .build();
  }

  private static RelationHintsResource hints(
      ResourceId relationId, String engineVersion, String identity) {
    return RelationHintsResource.newBuilder()
        .setRelationId(relationId)
        .setEngineKind("floedb")
        .setEngineVersion(engineVersion)
        .setRelationIdentity(identity)
        .putRelationHints(
            "floe.relation+proto",
            EngineHintPayload.newBuilder().setPayload(ByteString.copyFromUtf8(identity)).build())
        .build();
  }
}
