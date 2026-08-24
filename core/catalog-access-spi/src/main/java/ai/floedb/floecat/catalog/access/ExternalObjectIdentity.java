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

package ai.floedb.floecat.catalog.access;

import java.util.Objects;

/**
 * Provider identity for a captured object. Path-derived identities are explicitly marked unstable.
 */
public record ExternalObjectIdentity(String value, boolean stable) {
  public ExternalObjectIdentity {
    value = Objects.requireNonNull(value, "value").trim();
    if (value.isEmpty()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }

  public static ExternalObjectIdentity pathFallback(CatalogObjectName name) {
    Objects.requireNonNull(name, "name");
    StringBuilder encoded = new StringBuilder("path:v1:");
    name.namespace().segments().forEach(segment -> appendSegment(encoded, segment));
    appendSegment(encoded, name.name());
    return new ExternalObjectIdentity(encoded.toString(), false);
  }

  public static ExternalObjectIdentity stable(String value) {
    return new ExternalObjectIdentity(value, true);
  }

  private static void appendSegment(StringBuilder encoded, String segment) {
    encoded.append(segment.length()).append(':').append(segment);
  }
}
