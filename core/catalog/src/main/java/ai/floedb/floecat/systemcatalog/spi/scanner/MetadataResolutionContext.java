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

/** Shared context holding the overlay + catalog identity for metadata resolution. */
public interface MetadataResolutionContext {

  CatalogOverlay overlay();

  ResourceId catalogId();

  EngineContext engineContext();

  static MetadataResolutionContext of(
      CatalogOverlay overlay, ResourceId catalogId, EngineContext engineContext) {
    return new DefaultMetadataResolutionContext(overlay, catalogId, engineContext);
  }

  final class DefaultMetadataResolutionContext implements MetadataResolutionContext {

    private final CatalogOverlay overlay;
    private final ResourceId catalogId;
    private final EngineContext engineContext;

    public DefaultMetadataResolutionContext(
        CatalogOverlay overlay, ResourceId catalogId, EngineContext engineContext) {
      this.overlay = Objects.requireNonNull(overlay, "overlay");
      this.catalogId = Objects.requireNonNull(catalogId, "catalogId");
      this.engineContext = engineContext == null ? EngineContext.empty() : engineContext;
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
  }
}
