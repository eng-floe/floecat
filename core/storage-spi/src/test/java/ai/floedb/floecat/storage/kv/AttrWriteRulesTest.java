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
 * Pins the typing rule itself; that each store enforces it on every write belongs to the {@code
 * KvStoreContractTest} classes.
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
    // One attribute's rollout constraint, not numbers being suspect. Deliberately not ATTR_TTL,
    // which is reserved rather than ordinary.
    assertDoesNotThrow(
        () ->
            AttrWriteRules.checkExpiryIsString(
                Map.of("useCount", AttrValue.of(7L), "hits", AttrValue.of(9L))));
  }
}
