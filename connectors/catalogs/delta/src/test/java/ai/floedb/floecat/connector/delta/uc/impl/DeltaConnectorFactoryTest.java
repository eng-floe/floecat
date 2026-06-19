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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.connector.common.auth.RefreshingAwsCredentialsProviderRegistry;
import ai.floedb.floecat.connector.common.auth.RegistryBackedAwsCredentialsProvider;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

class DeltaConnectorFactoryTest {

  @Test
  void selectSourceSupportsGlue() {
    var source = DeltaConnectorFactory.selectSource(Map.of("delta.source", "glue"));
    assertEquals(DeltaConnectorFactory.DeltaSource.GLUE, source);
  }

  @Test
  void filesystemSourceRequiresTableRoot() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.validateOptions(
                    DeltaConnectorFactory.DeltaSource.FILESYSTEM, ""));
    assertTrue(ex.getMessage().contains("delta.table-root"));
  }

  @Test
  void tableRootRequiresFilesystemSource() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeltaConnectorFactory.validateOptions(
                    DeltaConnectorFactory.DeltaSource.GLUE, "s3://bucket/table"));
    assertTrue(ex.getMessage().contains("delta.source=filesystem"));
  }

  @Test
  void resolveCredentialsPrefersRegisteredRefreshingProvider() throws Exception {
    var method = DeltaConnectorFactory.class.getDeclaredMethod("resolveCredentials", Map.class);
    method.setAccessible(true);

    AwsCredentialsProvider provider =
        (AwsCredentialsProvider)
            method.invoke(
                null,
                Map.of(
                    RefreshingAwsCredentialsProviderRegistry.OPTION_PROVIDER_ID,
                    "provider-1",
                    "s3.access-key-id",
                    "akid",
                    "s3.secret-access-key",
                    "secret"));

    assertInstanceOf(RegistryBackedAwsCredentialsProvider.class, provider);
  }
}
