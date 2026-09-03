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

package ai.floedb.floecat.service.metagraph.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.cache.CachingPointerStore;
import ai.floedb.floecat.service.repo.cache.PointerCache;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class NameResolverPointerCacheTest {

  private static final String ACCOUNT = "account";

  private static final class CountingPointerStore extends InMemoryPointerStore {
    private final AtomicInteger reads = new AtomicInteger();

    @Override
    public Optional<Pointer> get(String key) {
      reads.incrementAndGet();
      return super.get(key);
    }

    @Override
    public Optional<Pointer> getConsistent(String key) {
      reads.incrementAndGet();
      return super.getConsistent(key);
    }

    @Override
    public Map<String, Pointer> getBatch(List<String> keys) {
      reads.incrementAndGet();
      return super.getBatch(keys);
    }

    @Override
    public Map<String, Pointer> getBatchConsistent(List<String> keys) {
      reads.incrementAndGet();
      return super.getBatchConsistent(keys);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      reads.incrementAndGet();
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    void resetReads() {
      reads.set(0);
    }
  }

  @Test
  void aRelationResolveMissDoesNotReachKvOnceTheAccountIndexIsComplete() {
    CountingPointerStore raw = new CountingPointerStore();
    PointerCache pointers = new PointerCache(raw, 1024L * 1024L, CacheEvents.none());
    var cached = new CachingPointerStore(raw, pointers);
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(cached, blobs);
    var namespaces = new NamespaceRepository(cached, blobs);
    var tables = new TableRepository(cached, blobs);
    var views = new ViewRepository(cached, blobs);
    var resolver = new NameResolver(catalogs, namespaces, tables, views);

    ResourceId catalogId = id("catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = id("namespace", ResourceKind.RK_NAMESPACE);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    namespaces.create(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("namespace")
            .build());

    // Loading any complete addressing key blocks until the whole account index is authoritative.
    assertThat(cached.get(Keys.relationPointerByName(ACCOUNT, "catalog", "namespace", "warmup")))
        .isEmpty();
    assertThat(pointers.completeAccountCount()).isEqualTo(1L);
    raw.resetReads();

    NameRef missing =
        NameRef.newBuilder().setCatalog("catalog").addPath("namespace").setName("missing").build();
    assertThat(resolver.resolveRelationId(ACCOUNT, missing)).isEmpty();
    assertThat(raw.reads)
        .as("the relation claim and legacy table/view fallbacks are authoritative memory misses")
        .hasValue(0);
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT).setId(value).setKind(kind).build();
  }
}
