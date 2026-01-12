package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Shared base for decoration contexts.
 *
 * <p>Provides: - MetadataResolutionContext (overlay/catalog-layer inputs) - per-object attribute
 * bag for multi-step decoration
 */
public abstract class AbstractDecoration {

  private final MetadataResolutionContext resolutionContext;
  private final Map<String, Object> attributes = new HashMap<>();

  protected AbstractDecoration(MetadataResolutionContext resolutionContext) {
    this.resolutionContext = Objects.requireNonNull(resolutionContext, "resolutionContext");
  }

  public final MetadataResolutionContext resolutionContext() {
    return resolutionContext;
  }

  @SuppressWarnings("unchecked")
  public final <T> T attribute(String key) {
    return (T) attributes.get(key);
  }

  public final void attribute(String key, Object value) {
    if (value == null) {
      attributes.remove(key);
    } else {
      attributes.put(key, value);
    }
  }
}
