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

package ai.floedb.floecat.service.metagraph.cache;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.scanner.spi.TopologyNames;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.CacheMetrics;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Application-scoped cache for catalog topology: namespace and relation (table/view) refs without
 * full proto materialization.
 *
 * <p>Cache population goes to DynamoDB only (pointer prefix scan) — no S3. Warm hits are
 * sub-millisecond; cold population of 200K tables across 10 namespaces is ~50ms (parallelism added
 * in Layer 5).
 *
 * <p>Two Caffeine caches:
 *
 * <ul>
 *   <li>{@code nsRefs}: keyed by {@code (accountId, catalogId)} — 15-min access TTL, 10K entries
 *   <li>{@code relRefs}: keyed by {@code (accountId, namespaceId)} — 15-min access TTL, 5K entries
 * </ul>
 *
 * <p>A {@code reverseMap} stores {@code relId → nsId} so that {@link #evict} can find the parent
 * namespace for a relation being deleted, even after the relation's own pointer is gone.
 */
@ApplicationScoped
public class CatalogTopologyCache {

  private final Cache<NsCacheKey, List<NamespaceRef>> nsRefs;
  private final Cache<RelCacheKey, List<RelationRef>> relRefs;
  private final ConcurrentMap<ResourceId, ResourceId> reverseMap;

  private final NamespaceRepository nsRepo;
  private final TableRepository tableRepo;
  private final ViewRepository viewRepo;

  private final CacheMetrics nsCacheMetrics;
  private final CacheMetrics relCacheMetrics;

  @Inject
  public CatalogTopologyCache(
      NamespaceRepository nsRepo,
      TableRepository tableRepo,
      ViewRepository viewRepo,
      Observability observability,
      @ConfigProperty(name = "floecat.topology.ns-cache-size", defaultValue = "10000")
          long nsCacheSize,
      @ConfigProperty(name = "floecat.topology.rel-cache-size", defaultValue = "5000")
          long relCacheSize,
      @ConfigProperty(name = "floecat.topology.cache-ttl-minutes", defaultValue = "15")
          long cacheTtlMinutes) {
    this.nsRepo = Objects.requireNonNull(nsRepo, "nsRepo");
    this.tableRepo = Objects.requireNonNull(tableRepo, "tableRepo");
    this.viewRepo = Objects.requireNonNull(viewRepo, "viewRepo");

    Duration ttl = Duration.ofMinutes(cacheTtlMinutes);
    this.nsRefs =
        Caffeine.newBuilder().maximumSize(Math.max(1L, nsCacheSize)).expireAfterAccess(ttl).build();
    this.reverseMap = new ConcurrentHashMap<>();
    this.relRefs =
        Caffeine.newBuilder()
            .maximumSize(Math.max(1L, relCacheSize))
            .expireAfterAccess(ttl)
            .<RelCacheKey, List<RelationRef>>removalListener(
                (RelCacheKey key, List<RelationRef> value, RemovalCause cause) -> {
                  if (value != null) {
                    for (RelationRef ref : value) {
                      reverseMap.remove(ref.id());
                    }
                  }
                })
            .build();

    this.nsCacheMetrics =
        new CacheMetrics(observability, "service", "topology-cache", "topology-ns");
    this.relCacheMetrics =
        new CacheMetrics(observability, "service", "topology-cache", "topology-rel");

    nsCacheMetrics.trackSize(nsRefs::estimatedSize, "ns-refs cache size");
    relCacheMetrics.trackSize(relRefs::estimatedSize, "rel-refs cache size");
  }

  public List<NamespaceRef> listNamespaceRefs(ResourceId catalogId) {
    var key = new NsCacheKey(catalogId.getAccountId(), catalogId.getId());
    var cached = nsRefs.getIfPresent(key);
    if (cached != null) {
      nsCacheMetrics.recordHit();
      return cached;
    }
    nsCacheMetrics.recordMiss();
    var loaded = nsRepo.listRefs(catalogId.getAccountId(), catalogId.getId());
    nsRefs.put(key, loaded);
    return loaded;
  }

  public List<NamespaceRef> listNamespaceRefsByName(ResourceId catalogId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    var key = new NsCacheKey(catalogId.getAccountId(), catalogId.getId());
    var cached = nsRefs.getIfPresent(key);
    if (cached != null) {
      nsCacheMetrics.recordHit();
      return cached.stream()
          .filter(
              ref -> names.contains(TopologyNames.namespaceName(ref.pathSegments(), ref.name())))
          .toList();
    }
    nsCacheMetrics.recordMiss();
    return nsRepo.listRefsByName(catalogId.getAccountId(), catalogId.getId(), names);
  }

  public List<RelationRef> listRelationRefs(ResourceId catalogId, ResourceId namespaceId) {
    var key = new RelCacheKey(namespaceId.getAccountId(), namespaceId.getId());
    var cached = relRefs.getIfPresent(key);
    if (cached != null) {
      relCacheMetrics.recordHit();
      return cached;
    }
    relCacheMetrics.recordMiss();
    var loaded = loadRelationRefs(catalogId, namespaceId);
    relRefs.put(key, loaded);
    for (var ref : loaded) {
      reverseMap.put(ref.id(), namespaceId);
    }
    return loaded;
  }

  public List<RelationRef> listRelationRefsByName(
      ResourceId catalogId, ResourceId namespaceId, Set<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    var key = new RelCacheKey(namespaceId.getAccountId(), namespaceId.getId());
    var cached = relRefs.getIfPresent(key);
    if (cached != null) {
      relCacheMetrics.recordHit();
      return cached.stream().filter(r -> names.contains(r.name())).collect(Collectors.toList());
    }
    relCacheMetrics.recordMiss();
    List<RelationRef> refs = new ArrayList<>();
    refs.addAll(
        tableRepo.listRefsByName(
            catalogId.getAccountId(), catalogId.getId(), namespaceId.getId(), names));
    refs.addAll(
        viewRepo.listRefsByName(
            catalogId.getAccountId(), catalogId.getId(), namespaceId.getId(), names));
    for (var ref : refs) {
      reverseMap.put(ref.id(), namespaceId);
    }
    return refs;
  }

  public void evict(ResourceId resourceId) {
    if (resourceId == null) {
      return;
    }
    ResourceId nsId = reverseMap.remove(resourceId);
    if (nsId != null) {
      relRefs.invalidate(new RelCacheKey(nsId.getAccountId(), nsId.getId()));
    }
  }

  public void evictRelationRefs(ResourceId namespaceId) {
    if (namespaceId != null) {
      relRefs.invalidate(new RelCacheKey(namespaceId.getAccountId(), namespaceId.getId()));
    }
  }

  public void evictNamespaceRefs(ResourceId catalogId) {
    if (catalogId != null) {
      nsRefs.invalidate(new NsCacheKey(catalogId.getAccountId(), catalogId.getId()));
    }
  }

  private List<RelationRef> loadRelationRefs(ResourceId catalogId, ResourceId namespaceId) {
    String accountId = catalogId.getAccountId();
    String catId = catalogId.getId();
    String nsId = namespaceId.getId();

    List<RelationRef> refs = new ArrayList<>();
    refs.addAll(tableRepo.listRefs(accountId, catId, nsId));
    refs.addAll(viewRepo.listRefs(accountId, catId, nsId));
    return refs;
  }

  private record NsCacheKey(String accountId, String catalogId) {}

  private record RelCacheKey(String accountId, String namespaceId) {}
}
