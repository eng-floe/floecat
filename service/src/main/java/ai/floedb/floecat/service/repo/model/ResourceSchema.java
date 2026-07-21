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

package ai.floedb.floecat.service.repo.model;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public final class ResourceSchema<T, K extends ResourceKey> {

  public final Function<K, String> canonicalPointerForKey;

  public final Function<K, String> blobUriForKey;

  public final Function<T, Map<String, String>> secondaryPointersFromValue;

  public final Function<T, K> keyFromValue;

  public final String resourceName;

  public final boolean casBlobs;

  // Null for resources that don't participate in topology caching (snapshots, stats, …).
  public final Function<T, ResourceId> resourceIdFromValue;
  public final Function<T, String> displayNameFromValue;

  // Null for resources that cannot be system-owned (snapshots, stats, transactions, …). When set,
  // maps a key to the ResourceId whose system-marker the repository write path checks so that no
  // mutation can persist against a system object. See GenericResourceRepository#guardSystemObject.
  public final Function<K, ResourceId> resourceIdFromKey;

  private ResourceSchema(
      String resourceName,
      Function<K, String> canonicalPointerForKey,
      Function<K, String> blobUriForKey,
      Function<T, Map<String, String>> secondaryPointersFromValue,
      Function<T, K> keyFromValue,
      boolean casBlobs,
      Function<T, ResourceId> resourceIdFromValue,
      Function<T, String> displayNameFromValue,
      Function<K, ResourceId> resourceIdFromKey) {
    this.resourceName = Objects.requireNonNull(resourceName, "resourceName");
    this.canonicalPointerForKey =
        Objects.requireNonNull(canonicalPointerForKey, "canonicalPointerForKey");
    this.blobUriForKey = Objects.requireNonNull(blobUriForKey, "blobUriForKey");
    this.secondaryPointersFromValue =
        Objects.requireNonNull(secondaryPointersFromValue, "secondaryPointersFromValue");
    this.keyFromValue = Objects.requireNonNull(keyFromValue, "keyFromValue");
    this.casBlobs = casBlobs;
    this.resourceIdFromValue = resourceIdFromValue;
    this.displayNameFromValue = displayNameFromValue;
    this.resourceIdFromKey = resourceIdFromKey;
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
        keyFromValue,
        false,
        null,
        null,
        null);
  }

  public ResourceSchema<T, K> withCasBlobs() {
    return new ResourceSchema<>(
        resourceName,
        canonicalPointerForKey,
        blobUriForKey,
        secondaryPointersFromValue,
        keyFromValue,
        true,
        resourceIdFromValue,
        displayNameFromValue,
        resourceIdFromKey);
  }

  public ResourceSchema<T, K> withPointerMeta(
      Function<T, ResourceId> resourceId, Function<T, String> displayName) {
    return new ResourceSchema<>(
        resourceName,
        canonicalPointerForKey,
        blobUriForKey,
        secondaryPointersFromValue,
        keyFromValue,
        casBlobs,
        Objects.requireNonNull(resourceId, "resourceId"),
        Objects.requireNonNull(displayName, "displayName"),
        resourceIdFromKey);
  }

  /**
   * Marks this resource as one that can be system-owned, wiring the key→ResourceId mapping the
   * repository write path uses to reject any create/update/delete that targets a system object.
   */
  public ResourceSchema<T, K> withSystemGuard(Function<K, ResourceId> resourceIdFromKey) {
    return new ResourceSchema<>(
        resourceName,
        canonicalPointerForKey,
        blobUriForKey,
        secondaryPointersFromValue,
        keyFromValue,
        casBlobs,
        resourceIdFromValue,
        displayNameFromValue,
        Objects.requireNonNull(resourceIdFromKey, "resourceIdFromKey"));
  }
}
