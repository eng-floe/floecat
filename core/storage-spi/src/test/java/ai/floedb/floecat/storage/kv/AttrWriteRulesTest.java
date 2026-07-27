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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The write-side typing rule itself. Both stores call it on every whole-record write — that each of
 * them does belongs to their {@code KvStoreContractTest} classes; this pins the rule.
 */
public class AttrWriteRulesTest {

  @Test
  void rejects_numeric_expiry_stamp() {
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AttrWriteRules.checkExpiryIsString(
                    Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of(2L))));
    assertTrue(
        thrown.getMessage().contains(KvAttributes.ATTR_EXPIRES_AT),
        "expected the attr name in the message but was: " + thrown.getMessage());
  }

  @Test
  void accepts_string_expiry_stamp() {
    assertDoesNotThrow(
        () ->
            AttrWriteRules.checkExpiryIsString(
                Map.of(KvAttributes.ATTR_EXPIRES_AT, AttrValue.of("2"))));
  }

  @Test
  void accepts_absent_expiry_stamp() {
    assertDoesNotThrow(() -> AttrWriteRules.checkExpiryIsString(Map.of()));
  }

  @Test
  void accepts_numeric_values_for_every_other_attr() {
    // The rule is about one attribute's rollout constraint, not about numbers being suspect: index
    // bookkeeping is numeric precisely so it can be incremented server-side.
    // Deliberately not ATTR_TTL: that name is reserved (DynamoDB expires rows by it), so it is no
    // example of an ordinary attr — KvStoreRecordTest pins its rejection.
    assertDoesNotThrow(
        () ->
            AttrWriteRules.checkExpiryIsString(
                Map.of("useCount", AttrValue.of(7L), "hits", AttrValue.of(9L))));
  }
}
