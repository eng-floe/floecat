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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public final class ConstraintProviderFactory {

  private static final Logger LOG = Logger.getLogger(ConstraintProviderFactory.class);

  private final ConstraintRepository repository;
  private final SnapshotRepository snapshots;

  private final CatalogOverlay overlay;
  private final ConstraintProvider systemProvider;

  @Inject
  public ConstraintProviderFactory(
      ConstraintRepository repository,
      SnapshotRepository snapshots,
      CatalogOverlay overlay,
      SystemConstraintProvider systemProvider) {
    this(repository, snapshots, overlay, (ConstraintProvider) systemProvider);
  }

  static ConstraintProviderFactory forTesting(
      ConstraintRepository repository,
      SnapshotRepository snapshots,
      CatalogOverlay overlay,
      ConstraintProvider systemProvider) {
    return new ConstraintProviderFactory(repository, snapshots, overlay, systemProvider);
  }

  private ConstraintProviderFactory(
      ConstraintRepository repository,
      SnapshotRepository snapshots,
      CatalogOverlay overlay,
      ConstraintProvider systemProvider) {
    this.repository = repository;
    this.snapshots = snapshots;
    this.overlay = overlay;
    this.systemProvider = systemProvider == null ? ConstraintProvider.NONE : systemProvider;
  }

  /**
   * A fresh provider per call, intended to live for a single query. Its {@link
   * CachedUserConstraintProvider} memoizes bundles (and their versions) with no invalidation, which
   * is safe ONLY because a bundle for a given (table, snapshot) is stable across one query — the
   * caller invokes this once per request. Do not hoist it to a longer-lived scope: constraints
   * mutate in place under a stable snapshot, so a reused instance would serve stale bundles.
   */
  public ConstraintProvider provider() {
    ConstraintProvider userProvider = new CachedUserConstraintProvider(repository, snapshots);
    return new RoutedConstraintProvider(userProvider, systemProvider, overlay);
  }

  /**
   * Provider for pinned-query serving: SYSTEM relations resolve as usual, but USER relations yield
   * empty — a pinned query serves user-table constraints only from the immutable bundle ref frozen
   * on its pin, never from the live pointer this factory's user provider reads.
   */
  public ConstraintProvider pinnedQueryProvider() {
    return new RoutedConstraintProvider(ConstraintProvider.NONE, systemProvider, overlay);
  }

  private static final class RoutedConstraintProvider implements ConstraintProvider {

    private final ConstraintProvider userProvider;
    private final ConstraintProvider systemProvider;
    private final CatalogOverlay overlay;

    private RoutedConstraintProvider(
        ConstraintProvider userProvider,
        ConstraintProvider systemProvider,
        CatalogOverlay overlay) {
      this.userProvider = userProvider;
      this.systemProvider = systemProvider;
      this.overlay = overlay;
    }

    @Override
    public Optional<ConstraintSetView> constraints(ResourceId relationId, OptionalLong snapshotId) {
      if (isSystemRelation(relationId)) {
        return systemProvider.constraints(relationId, OptionalLong.empty());
      }
      return userProvider.constraints(relationId, snapshotId);
    }

    @Override
    public Optional<ConstraintSetView> latestConstraints(ResourceId relationId) {
      if (isSystemRelation(relationId)) {
        return systemProvider.latestConstraints(relationId);
      }
      return userProvider.latestConstraints(relationId);
    }

    private boolean isSystemRelation(ResourceId relationId) {
      try {
        return overlay
            .resolve(relationId)
            .map(node -> node.origin() == GraphNodeOrigin.SYSTEM)
            .orElse(false);
      } catch (RuntimeException e) {
        LOG.debugf(e, "constraint provider routing failed for %s", relationId);
        return false;
      }
    }
  }

  private static final class CachedUserConstraintProvider implements ConstraintProvider {

    private final ConstraintRepository repository;
    private final SnapshotRepository snapshots;
    private final ConcurrentMap<
            SnapshotScopedRelationKey, Optional<ConstraintProvider.ConstraintSetView>>
        constraintsCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<RelationKey, Optional<ConstraintProvider.ConstraintSetView>>
        latestConstraintsCache = new ConcurrentHashMap<>();

    private CachedUserConstraintProvider(
        ConstraintRepository repository, SnapshotRepository snapshots) {
      this.repository = repository;
      this.snapshots = snapshots;
    }

    @Override
    public Optional<ConstraintSetView> constraints(ResourceId relationId, OptionalLong snapshotId) {
      if (snapshotId.isEmpty()) {
        return Optional.empty();
      }
      long sid = snapshotId.getAsLong();
      return constraintsCache.computeIfAbsent(
          SnapshotScopedRelationKey.of(relationId, sid),
          key ->
              repository
                  .getSnapshotConstraints(relationId, sid)
                  .map(sc -> constraintSetView(sc, bundleVersion(relationId, sid))));
    }

    @Override
    public Optional<ConstraintSetView> latestConstraints(ResourceId relationId) {
      return latestConstraintsCache.computeIfAbsent(
          RelationKey.of(relationId),
          key ->
              // Latest = latest QUERY-VISIBLE: the repository's default current-snapshot read is
              // gate-aware, so metadata scans agree with what queries can read.
              snapshots
                  .getCurrentSnapshot(relationId)
                  .flatMap(
                      snapshot ->
                          repository
                              .getSnapshotConstraints(relationId, snapshot.getSnapshotId())
                              .map(
                                  sc ->
                                      constraintSetView(
                                          sc,
                                          bundleVersion(relationId, snapshot.getSnapshotId())))));
    }

    /** The constraint bundle's pointer version for (table, snapshot); 0 when none exists. */
    private long bundleVersion(ResourceId relationId, long snapshotId) {
      MutationMeta meta = repository.metaForSafe(relationId, snapshotId);
      return meta == null ? 0L : meta.getPointerVersion();
    }
  }

  static ConstraintProvider.ConstraintSetView constraintSetView(
      SnapshotConstraints constraints, long version) {
    return new ConstraintSetViewImpl(
        constraints.getTableId(),
        constraints.getConstraintsList(),
        constraints.getPropertiesMap(),
        version);
  }

  private static final class ConstraintSetViewImpl implements ConstraintProvider.ConstraintSetView {
    private final ResourceId relationId;
    private final List<ConstraintDefinition> constraints;
    private final Map<String, String> properties;
    private final long version;

    private ConstraintSetViewImpl(
        ResourceId relationId,
        List<ConstraintDefinition> constraints,
        Map<String, String> properties,
        long version) {
      this.relationId = relationId;
      this.constraints = List.copyOf(constraints);
      this.properties = Map.copyOf(properties);
      this.version = version;
    }

    @Override
    public ResourceId relationId() {
      return relationId;
    }

    @Override
    public List<ConstraintDefinition> constraints() {
      return constraints;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }
  }

  private record RelationKey(String accountId, String relationId, int kindValue) {
    static RelationKey of(ResourceId tableId) {
      return new RelationKey(tableId.getAccountId(), tableId.getId(), tableId.getKindValue());
    }
  }
}
