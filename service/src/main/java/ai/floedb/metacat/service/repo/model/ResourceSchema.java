package ai.floedb.metacat.service.repo.model;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class ResourceSchema<T, K extends ResourceKey> {

  public final Function<K, String> canonicalPointerForKey;

  public final Function<K, String> blobUriForKey;

  public final Function<T, Map<String, String>> secondaryPointersFromValue;

  public final Function<T, K> keyFromValue;

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
