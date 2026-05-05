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

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PersistedSecretPropertyValidatorTest {
  @Test
  void detectsCommonSecretBearingKeys() {
    assertTrue(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("secret"));
    assertTrue(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("session-token"));
    assertTrue(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("privateKeyPem"));
    assertTrue(
        PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("aws.secret_access_key"));
    assertTrue(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("api-key"));
    assertTrue(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("s3.access-key-id"));
    assertTrue(
        PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("fs.s3a.secret-access-key"));
  }

  @Test
  void ignoresNormalMetadataKeys() {
    assertFalse(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("location"));
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("metadata-location"));
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("write.metadata.path"));
    assertFalse(PersistedSecretPropertyValidator.isForbiddenPersistedSecretKey("token_type"));
  }

  @Test
  void generalMetadataValidationAllowsNonSecretKeySuffixesAndTokenPhrases() {
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("primary_key"));
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("foreign_key"));
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("partition_key"));
    assertFalse(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("token_endpoint"));
  }

  @Test
  void generalMetadataValidationStillRejectsExactSecretKeys() {
    assertTrue(PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("secret"));
    assertTrue(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("secret_access_key"));
    assertTrue(
        PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("private_key_pem"));
    assertTrue(PersistedSecretPropertyValidator.isForbiddenGeneralMetadataSecretKey("access_key"));
  }
}
