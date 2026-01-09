package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.Optional;

/**
 * Provider that yields an engine-specific {@link EngineMetadataDecorator} for a normalized context.
 */
public interface EngineMetadataDecoratorProvider {

  /**
   * Returns the decorator for the provided engine context, if available.
   *
   * <p>The provided {@link EngineContext} is expected to already contain normalized kind/version
   * values.
   */
  Optional<EngineMetadataDecorator> decorator(EngineContext ctx);
}
