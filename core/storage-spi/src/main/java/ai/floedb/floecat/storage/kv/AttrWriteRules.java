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
 * Attribute-typing rules every {@link KvStore} write path enforces, so that what one store rejects
 * the others reject identically. A laxer in-memory store would let a test pass here and fail in
 * production.
 */
public final class AttrWriteRules {

  private AttrWriteRules() {}

  /**
   * Rejects a numeric {@link KvAttributes#ATTR_EXPIRES_AT}, before any store issues a write.
   *
   * <p>A rollout constraint, not a modelling choice. A replica older than typed attributes drops
   * every non-string attribute as it reads, so it decodes such a record as carrying no expiry at
   * all, and its next whole-record write persists that loss permanently. Writers therefore pin the
   * expiry stamp to a {@link AttrValue.StringValue} for as long as the fleet can still hold such a
   * replica.
   *
   * <p>Reads stay lenient in both directions — {@link AttrValue#asLong()} accepts either form —
   * because a row that some other writer typed as a number still has to be readable. The asymmetry
   * is the point: tolerate on the way in, never produce on the way out.
   *
   * <p>Drop this rule once no deployed replica predates typed attributes.
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
