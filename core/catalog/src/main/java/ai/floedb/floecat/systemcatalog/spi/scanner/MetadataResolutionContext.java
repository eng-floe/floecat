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
