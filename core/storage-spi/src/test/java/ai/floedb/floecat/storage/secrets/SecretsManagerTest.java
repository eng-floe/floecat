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

package ai.floedb.floecat.storage.secrets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SecretsManagerTest {

  @Test
  void buildSecretKey_composes_segments() {
    String key = SecretsManager.buildSecretKey("acct", "connectors", "conn-1");
    assertEquals("accounts/acct/connectors/conn-1", key);
  }

  @Test
  void buildSecretKey_rejects_blank_parts() {
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey(null, "t", "id"));
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey(" ", "t", "id"));
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey("a", null, "id"));
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey("a", " ", "id"));
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey("a", "t", null));
    assertThrows(
        IllegalArgumentException.class, () -> SecretsManager.buildSecretKey("a", "t", " "));
  }
}
