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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.ConstraintProvider;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
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

  private final CatalogOverlay overlay;
  private final ConstraintProvider systemProvider;

  @Inject
  public ConstraintProviderFactory(ConstraintRepository repository, CatalogOverlay overlay) {
    this(repository, overlay, ConstraintProvider.NONE);
  }

  ConstraintProviderFactory(
      ConstraintRepository repository, CatalogOverlay overlay, ConstraintProvider systemProvider) {
    this.repository = repository;
    this.overlay = overlay;
    this.systemProvider = systemProvider == null ? ConstraintProvider.NONE : systemProvider;
  }

  public ConstraintProvider provider() {
    ConstraintProvider userProvider = new CachedUserConstraintProvider(repository);
    return new RoutedConstraintProvider(userProvider, systemProvider, overlay);
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
    private final ConcurrentMap<
            SnapshotScopedRelationKey, Optional<ConstraintProvider.ConstraintSetView>>
        constraintsCache = new ConcurrentHashMap<>();

    private CachedUserConstraintProvider(ConstraintRepository repository) {
      this.repository = repository;
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
                  .map(ConstraintProviderFactory::constraintSetView));
    }
  }

  static ConstraintProvider.ConstraintSetView constraintSetView(SnapshotConstraints constraints) {
    return new ConstraintSetViewImpl(
        constraints.getTableId(), constraints.getConstraintsList(), constraints.getPropertiesMap());
  }

  private static final class ConstraintSetViewImpl implements ConstraintProvider.ConstraintSetView {
    private final ResourceId relationId;
    private final List<ConstraintDefinition> constraints;
    private final Map<String, String> properties;

    private ConstraintSetViewImpl(
        ResourceId relationId,
        List<ConstraintDefinition> constraints,
        Map<String, String> properties) {
      this.relationId = relationId;
      this.constraints = List.copyOf(constraints);
      this.properties = Map.copyOf(properties);
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
    public Map<String, String> properties() {
      return properties;
    }
  }
}
