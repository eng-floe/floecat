package ai.floedb.metacat.service.repo.model;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Declarative wiring for a resource type: - how to build canonical pointer & blob from the Key K -
 * how to compute all secondary pointer keys from the Value T - how to extract K from T
 */
public final class ResourceSchema<T, K extends ResourceKey> {

  /** Canonical pointer from key (unique per resource). */
  public final Function<K, String> canonicalPointerForKey;

  /** Blob URI from key (unique per resource). */
  public final Function<K, String> blobUriForKey;

  /** All secondary index pointers from the VALUE (e.g., by-name, by-path, by-time). */
  public final Function<T, Map<String, String>> secondaryPointersFromValue;

  /** Extract Key (K) from the VALUE. */
  public final Function<T, K> keyFromValue;

  /** Friendly name for diagnostics/metrics. */
  public final String resourceName;

  private ResourceSchema(
      String resourceName,
      Function<K, String> canonicalPointerForKey,
      Function<K, String> blobUriForKey,
      Function<T, Map<String, String>> secondaryPointersFromValue,
      Function<T, K> keyFromValue) {
    this.resourceName = Objects.requireNonNull(resourceName, "resourceName");
    this.canonicalPointerForKey =
        Objects.requireNonNull(canonicalPointerForKey, "canonicalPointerForKey");
    this.blobUriForKey = Objects.requireNonNull(blobUriForKey, "blobUriForKey");
    this.secondaryPointersFromValue =
        Objects.requireNonNull(secondaryPointersFromValue, "secondaryPointersFromValue");
    this.keyFromValue = Objects.requireNonNull(keyFromValue, "keyFromValue");
  }

  public static <T, K extends ResourceKey> ResourceSchema<T, K> of(
      String resourceName,
      Function<K, String> canonicalPointerForKey,
      Function<K, String> blobUriForKey,
      Function<T, Map<String, String>> secondaryPointersFromValue,
      Function<T, K> keyFromValue) {
    return new ResourceSchema<>(
        resourceName,
        canonicalPointerForKey,
        blobUriForKey,
        secondaryPointersFromValue,
        keyFromValue);
  }
}
