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

package ai.floedb.floecat.systemcatalog.spi.decorator;

import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared base for decoration contexts.
 *
 * <p>Provides: - MetadataResolutionContext (overlay/catalog-layer inputs) - per-object attribute
 * bag for multi-step decoration
 */
public abstract class AbstractDecoration {

  private final MetadataResolutionContext resolutionContext;
  private final ConcurrentHashMap<String, Object> attributes = new ConcurrentHashMap<>();

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
