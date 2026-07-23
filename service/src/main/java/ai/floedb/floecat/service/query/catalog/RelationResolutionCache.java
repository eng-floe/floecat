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
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.PlannedInput;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.TimingAccumulator;
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
 * runs once per key even under the concurrent select stage that shares this cache across its
 * fan-out tasks — and first-touch semantics: the first caller resolves, later callers read. Every
 * resolve's elapsed time plus its hit/miss verdict is recorded into the shared request {@link
 * TimingAccumulator}.
 */
final class RelationResolutionCache {

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

  RelationResolutionCache(
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

  /**
   * The fully-qualified name for a relation, resolving its namespace and catalog ancestors through
   * the same per-request node memo as everything else. A namespace shared by several relations is
   * therefore resolved once, not once per relation: without this, each relation's name was built by
   * a separate walk to the shared namespace/catalog that bypassed this memo, so the concurrent
   * select/build fan-out raced them into one live pointer read apiece. Falls back to the bare
   * display name when the relation is not a table/view or an ancestor cannot be resolved --
   * matching the name the builder produced before.
   */
  NameRef canonicalName(RelationNode node) {
    NameRef nameOnly = NameRef.newBuilder().setName(node.displayName()).build();
    if (node.kind() != GraphNodeKind.TABLE && node.kind() != GraphNodeKind.VIEW) {
      return nameOnly;
    }
    if (!(resolveNode(node.namespaceId()).orElse(null) instanceof NamespaceNode ns)) {
      return nameOnly;
    }
    if (!(resolveNode(ns.catalogId()).orElse(null) instanceof CatalogNode catalog)) {
      return nameOnly;
    }
    return ns.relationNameRef(node.displayName(), node.id(), catalog.displayName());
  }

  /**
   * Resolve every NAME candidate of the plan in one batch, seeding the name memo. Names sharing a
   * catalog/namespace resolve their scope once here instead of once per candidate during select,
   * and the memo turns each candidate's later {@link #resolveName} into a hit. Already-cached names
   * (e.g. from a prior chunk's base-table drain) are left as they are.
   */
  void seed(List<PlannedInput> plan) {
    List<NameRef> refs = new ArrayList<>();
    for (PlannedInput planned : plan) {
      for (QueryInput candidate : planned.normalized()) {
        if (candidate.getTargetCase() == QueryInput.TargetCase.NAME) {
          refs.add(candidate.getName());
        }
      }
    }
    if (refs.isEmpty()) {
      return;
    }
    long startNs = System.nanoTime();
    try {
      overlay
          .resolveNames(correlationId, refs)
          .forEach((ref, id) -> nameResolutionCache.putIfAbsent(normalizedNameRef(ref), id));
    } finally {
      timings.addNameResolveNanos(System.nanoTime() - startNs);
    }
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
