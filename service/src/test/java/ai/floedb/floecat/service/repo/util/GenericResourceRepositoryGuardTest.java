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

package ai.floedb.floecat.service.repo.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class GenericResourceRepositoryGuardTest {

  @Test
  void refusesToDropAGuardCheckForAnUnconstrainedBatchedKey() {
    String key = "/resource/by-name/name";
    BatchGuard guard =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of(new PointerStore.CasCheck(key, 3L));
          }

          @Override
          public Outcome reevaluate() {
            return Outcome.HOLDS;
          }

          @Override
          public String describe() {
            return "test guard";
          }
        };

    assertThatThrownBy(
            () -> GenericResourceRepository.appendGuardOps(new ArrayList<>(), Set.of(key), guard))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("unconstrained mutation key");
  }

  @Test
  void guardRetriesIssueAtMostCasMaxBatches() {
    var attempts = new AtomicInteger();
    var pointers =
        new InMemoryPointerStore() {
          @Override
          public boolean compareAndSetBatch(List<PointerStore.CasOp> ops) {
            attempts.incrementAndGet();
            return false;
          }
        };
    var repository = new NamespaceRepository(pointers, new InMemoryBlobStore());
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("cat")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    var namespace =
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns")
            .build();
    BatchGuard retryingGuard =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of();
          }

          @Override
          public Outcome reevaluate() {
            return Outcome.RETRY;
          }

          @Override
          public String describe() {
            return "always moving";
          }
        };

    assertThatThrownBy(() -> repository.create(namespace, retryingGuard))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class)
        .hasMessageContaining("after " + BaseResourceRepository.CAS_MAX + " attempts");
    assertThat(attempts).hasValue(BaseResourceRepository.CAS_MAX);
  }

  @Test
  void guardedDeleteSurfacesARefreshableGuardAsRetryable() {
    String blocker = "/guard/blocker";
    var pointers = new InMemoryPointerStore();
    pointers.compareAndSet(
        blocker,
        0L,
        ai.floedb.floecat.common.rpc.Pointer.newBuilder().setKey(blocker).setVersion(1L).build());
    var repository = new NamespaceRepository(pointers, new InMemoryBlobStore());
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("ns")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("cat")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    repository.create(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns")
            .build());
    long version = repository.metaFor(namespaceId).getPointerVersion();
    BatchGuard movingGuard =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of(new PointerStore.CasCheckAbsent(blocker));
          }

          @Override
          public Outcome reevaluate() {
            return Outcome.RETRY;
          }

          @Override
          public String describe() {
            return "moving cleanup index";
          }
        };

    assertThatThrownBy(() -> repository.deleteWithPrecondition(namespaceId, version, movingGuard))
        .isInstanceOf(BaseResourceRepository.AbortRetryableException.class)
        .hasMessageContaining("guard moved");
  }
}
