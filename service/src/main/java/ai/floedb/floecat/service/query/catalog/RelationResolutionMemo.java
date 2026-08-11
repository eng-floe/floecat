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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.utils.EngineContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * Per-request memo for name-to-id and id-to-node resolution during one GetUserObjects stream. Both
 * maps resolve each key at most once: a repeated lookup returns the stored {@link Optional}
 * (present or empty). {@link ConcurrentHashMap#computeIfAbsent} gives single-flight — the resolve
 * runs once per key even under the concurrent select stage that shares this memo across its fan-out
 * tasks — and first-touch semantics: the first caller resolves, later callers read. Every resolve's
 * elapsed time plus its hit/miss verdict is recorded into the shared request {@link
 * TimingAccumulator}.
 */
final class RelationResolutionMemo {

  private final CatalogOverlay overlay;
  private final String correlationId;
  // Engine captured at construction is threaded through every lookup: re-reading it from the
  // request context per lookup is fragile across executor hops, and an empty engine silently
  // un-resolves engine-gated system objects (eng-floe/floecat#361).
  private final EngineContext engineContext;
  private final TimingAccumulator timings;

  private final Map<NormalizedNameRef, Optional<ResourceId>> nameResolutionCache =
      new ConcurrentHashMap<>();
  private final Map<ResourceId, Optional<GraphNode>> nodeResolutionCache =
      new ConcurrentHashMap<>();
  private final Map<ResourceId, Optional<NameRef>> canonicalNameCache = new ConcurrentHashMap<>();

  RelationResolutionMemo(
      CatalogOverlay overlay,
      String correlationId,
      EngineContext engineContext,
      TimingAccumulator timings) {
    this.overlay = overlay;
    this.correlationId = correlationId;
    this.engineContext = engineContext;
    this.timings = timings;
  }

  Optional<ResourceId> resolveName(NameRef ref) {
    Memoized<ResourceId> m =
        memoize(
            nameResolutionCache,
            normalizedNameRef(ref),
            () -> overlay.resolveName(correlationId, ref, engineContext),
            timings::addNameResolveNanos);
    if (m.resolved()) {
      timings.recordNameCacheMiss();
    } else {
      timings.recordNameCacheHit();
    }
    return m.value();
  }

  Optional<GraphNode> resolveNode(ResourceId id) {
    Memoized<GraphNode> m =
        memoize(
            nodeResolutionCache,
            id,
            () -> overlay.resolve(id, engineContext),
            timings::addNodeResolveNanos);
    if (m.resolved()) {
      timings.recordNodeCacheMiss();
    } else {
      timings.recordNodeCacheHit();
    }
    return m.value();
  }

  /** Resolve and memoize the overlay-owned canonical name for one relation. */
  NameRef canonicalName(RelationNode node) {
    NameRef nameOnly = NameRef.newBuilder().setName(node.displayName()).build();
    Optional<NameRef> canonical =
        canonicalNameCache.computeIfAbsent(
            node.id(),
            id ->
                switch (node.kind()) {
                  case TABLE -> overlay.tableName(id, engineContext);
                  case VIEW -> overlay.viewName(id, engineContext);
                  default -> Optional.empty();
                });
    return canonical.orElse(nameOnly);
  }

  /** Live count of distinct names resolved so far; read at telemetry flush. */
  int nameEntries() {
    return nameResolutionCache.size();
  }

  /** Live count of distinct ids resolved so far; read at telemetry flush. */
  int nodeEntries() {
    return nodeResolutionCache.size();
  }

  /** A memoized value together with whether this call resolved it (a miss) vs. found it cached. */
  private record Memoized<V>(Optional<V> value, boolean resolved) {}

  /**
   * Return {@code cache.get(key)}, resolving and storing it once if absent. The resolve runs at
   * most once per key even under concurrent callers (computeIfAbsent single-flight); its elapsed
   * time is reported to {@code addNanos}. {@link Memoized#resolved()} is true when this call ran
   * the resolve.
   */
  private <K, V> Memoized<V> memoize(
      Map<K, Optional<V>> cache, K key, Supplier<Optional<V>> resolve, LongConsumer addNanos) {
    boolean[] resolvedHere = {false};
    Optional<V> value =
        cache.computeIfAbsent(
            key,
            k -> {
              resolvedHere[0] = true;
              long startNs = System.nanoTime();
              try {
                return resolve.get();
              } finally {
                addNanos.accept(System.nanoTime() - startNs);
              }
            });
    return new Memoized<>(value, resolvedHere[0]);
  }

  private NormalizedNameRef normalizedNameRef(NameRef ref) {
    List<String> normalizedPath = new ArrayList<>(ref.getPathCount());
    for (String segment : ref.getPathList()) {
      normalizedPath.add(normalizeNameToken(segment));
    }
    return new NormalizedNameRef(
        normalizeNameToken(ref.getCatalog()),
        List.copyOf(normalizedPath),
        normalizeNameToken(ref.getName()));
  }

  private String normalizeNameToken(String token) {
    if (token == null) {
      return "";
    }
    return token.trim();
  }

  private record NormalizedNameRef(String catalog, List<String> path, String name) {}
}
