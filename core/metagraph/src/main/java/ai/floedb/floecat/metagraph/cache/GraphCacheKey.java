package ai.floedb.floecat.metagraph.cache;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Objects;

/**
 * Cache key combining a resource identifier and its pointer version.
 *
 * <p>Pointer versions are monotonically increasing, so this key automatically invalidates cached
 * nodes when underlying metadata changes.
 */
public record GraphCacheKey(ResourceId id, long version) {

  public GraphCacheKey {
    Objects.requireNonNull(id, "id");
  }
}
