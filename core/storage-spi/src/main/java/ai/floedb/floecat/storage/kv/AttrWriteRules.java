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

/** Attribute-typing rules enforced identically by every {@link KvStore} write path. */
public final class AttrWriteRules {

  private AttrWriteRules() {}

  /**
   * Rejects a numeric {@link KvAttributes#ATTR_EXPIRES_AT} before any store issues a write.
   *
   * <p>Rollout constraint: a replica that predates typed attributes drops every non-string
   * attribute on read, decodes such a record as having no expiry, and its next whole-record write
   * persists that loss. Reads stay lenient ({@link AttrValue#asLong()} accepts either form) so rows
   * typed by other writers remain readable. Drop this rule once no deployed replica predates typed
   * attributes.
   */
  public static void checkExpiryIsString(Map<String, AttrValue> attrs) {
    if (attrs.get(KvAttributes.ATTR_EXPIRES_AT) instanceof AttrValue.NumberValue) {
      throw new IllegalArgumentException(
          "attr must be written as a string for as long as replicas that predate typed attributes"
              + " can read it: "
              + KvAttributes.ATTR_EXPIRES_AT);
    }
  }
}
