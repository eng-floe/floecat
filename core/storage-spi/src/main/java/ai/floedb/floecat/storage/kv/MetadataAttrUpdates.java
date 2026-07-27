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
package ai.floedb.floecat.storage.kv;

import java.util.Map;

/**
 * Argument validation shared by every {@link KvStore#updateMetadataAttrsIfExists} implementation,
 * so that what one store rejects the others reject identically.
 */
public final class MetadataAttrUpdates {

  private MetadataAttrUpdates() {}

  /**
   * Validates the arguments of {@link KvStore#updateMetadataAttrsIfExists}, throwing before any
   * request is issued.
   */
  public static void validate(
      KvStore.Key key, Map<String, AttrValue> sets, Map<String, Long> increments) {
    if (key == null) throw new IllegalArgumentException("key must not be null");
    if (sets == null) throw new IllegalArgumentException("sets must not be null");
    if (increments == null) throw new IllegalArgumentException("increments must not be null");
    if (sets.isEmpty() && increments.isEmpty()) {
      throw new IllegalArgumentException("at least one of sets/increments must be non-empty");
    }
    checkNames(sets, "sets");
    checkNames(increments, "increments");
    for (var name : increments.keySet()) {
      if (sets.containsKey(name)) {
        throw new IllegalArgumentException(
            "attr is both set and incremented, which is ambiguous: " + name);
      }
    }
  }

  private static void checkNames(Map<String, ?> attrs, String what) {
    for (var e : attrs.entrySet()) {
      var name = e.getKey();
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException(what + " contains a blank attr name");
      }
      if (KvAttributes.RESERVED_ATTRS.contains(name)) {
        throw new IllegalArgumentException(
            "attr name is reserved by the backend: " + name + " (in " + what + ")");
      }
      // Rejected outright here, where whole-record writes accept the string form (see
      // AttrWriteRules#checkExpiryIsString). An increment can only produce the numeric form this
      // primitive must not write, and no caller needs to bump an expiry without rewriting the
      // record that carries it — so a flat rule beats a per-form one.
      if (KvAttributes.ATTR_EXPIRES_AT.equals(name)) {
        throw new IllegalArgumentException(
            "attr must not be updated by itself, only by a whole-record write: "
                + name
                + " (in "
                + what
                + ")");
      }
      if (e.getValue() == null) {
        throw new IllegalArgumentException(what + " contains a null value for attr " + name);
      }
    }
  }
}
