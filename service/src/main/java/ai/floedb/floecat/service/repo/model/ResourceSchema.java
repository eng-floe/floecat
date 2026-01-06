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

  private ResourceSchema(
      String resourceName,
      Function<K, String> canonicalPointerForKey,
      Function<K, String> blobUriForKey,
      Function<T, Map<String, String>> secondaryPointersFromValue,
      Function<T, K> keyFromValue,
      boolean casBlobs) {
    this.resourceName = Objects.requireNonNull(resourceName, "resourceName");
    this.canonicalPointerForKey =
        Objects.requireNonNull(canonicalPointerForKey, "canonicalPointerForKey");
    this.blobUriForKey = Objects.requireNonNull(blobUriForKey, "blobUriForKey");
    this.secondaryPointersFromValue =
        Objects.requireNonNull(secondaryPointersFromValue, "secondaryPointersFromValue");
    this.keyFromValue = Objects.requireNonNull(keyFromValue, "keyFromValue");
    this.casBlobs = casBlobs;
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
        false);
  }

  public ResourceSchema<T, K> withCasBlobs() {
    return new ResourceSchema<>(
        resourceName,
        canonicalPointerForKey,
        blobUriForKey,
        secondaryPointersFromValue,
        keyFromValue,
        true);
  }
}
