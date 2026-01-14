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

package ai.floedb.floecat.systemcatalog.spi.scanner;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.Objects;
import java.util.Optional;

/** Shared context holding the overlay + catalog identity for metadata resolution. */
public interface MetadataResolutionContext {

  CatalogOverlay overlay();

  ResourceId catalogId();

  EngineContext engineContext();

  default StatsProvider statsProvider() {
    return StatsProvider.NONE;
  }

  default Optional<StatsProvider.TableStatsView> tableStats(ResourceId tableId) {
    return statsProvider().tableStats(tableId);
  }

  default Optional<StatsProvider.ColumnStatsView> columnStats(ResourceId tableId, int columnId) {
    return statsProvider().columnStats(tableId, columnId);
  }

  static MetadataResolutionContext of(
      CatalogOverlay overlay, ResourceId catalogId, EngineContext engineContext) {
    return of(overlay, catalogId, engineContext, StatsProvider.NONE);
  }

  static MetadataResolutionContext of(
      CatalogOverlay overlay,
      ResourceId catalogId,
      EngineContext engineContext,
      StatsProvider statsProvider) {
    return new DefaultMetadataResolutionContext(overlay, catalogId, engineContext, statsProvider);
  }

  final class DefaultMetadataResolutionContext implements MetadataResolutionContext {

    private final CatalogOverlay overlay;
    private final ResourceId catalogId;
    private final EngineContext engineContext;
    private final StatsProvider statsProvider;

    public DefaultMetadataResolutionContext(
        CatalogOverlay overlay,
        ResourceId catalogId,
        EngineContext engineContext,
        StatsProvider statsProvider) {
      this.overlay = Objects.requireNonNull(overlay, "overlay");
      this.catalogId = Objects.requireNonNull(catalogId, "catalogId");
      this.engineContext = engineContext == null ? EngineContext.empty() : engineContext;
      this.statsProvider = statsProvider == null ? StatsProvider.NONE : statsProvider;
    }

    @Override
    public CatalogOverlay overlay() {
      return overlay;
    }

    @Override
    public ResourceId catalogId() {
      return catalogId;
    }

    @Override
    public EngineContext engineContext() {
      return engineContext;
    }

    @Override
    public StatsProvider statsProvider() {
      return statsProvider;
    }
  }
}
