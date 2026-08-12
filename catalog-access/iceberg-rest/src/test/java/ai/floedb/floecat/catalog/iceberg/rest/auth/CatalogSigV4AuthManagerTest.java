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

package ai.floedb.floecat.catalog.iceberg.rest.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Map;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.junit.jupiter.api.Test;

class CatalogSigV4AuthManagerTest {
  @Test
  void canBeLoadedByIcebergWithoutConnectorClasses() {
    AuthManager manager =
        AuthManagers.loadAuthManager(
            "test-catalog", Map.of("rest.auth.type", CatalogSigV4AuthManager.class.getName()));
    try {
      assertInstanceOf(CatalogSigV4AuthManager.class, manager);
    } finally {
      manager.close();
    }
  }

  @Test
  void catalogProviderReplacesStorageProviderForSigningOnly() {
    Map<String, String> properties =
        Map.of(
            RefreshingAwsCredentialsRegistry.CATALOG_PROVIDER_ID,
            "catalog-provider",
            RefreshingAwsCredentialsRegistry.STORAGE_PROVIDER_ID,
            "storage-provider",
            "client.credentials-provider",
            "storage-provider-class",
            "client.credentials-provider.floecat-provider-id",
            "storage-provider");

    Map<String, String> catalogProperties =
        CatalogSigV4AuthManager.catalogAuthProperties(properties);

    assertEquals(
        RegistryBackedAwsCredentialsProvider.class.getName(),
        catalogProperties.get("client.credentials-provider"));
    assertEquals(
        "catalog-provider",
        catalogProperties.get("client.credentials-provider.floecat-provider-id"));
    assertEquals(
        AwsCredentialScope.CATALOG.name(),
        catalogProperties.get("client.credentials-provider.floecat-credential-scope"));
    assertFalse(catalogProperties.containsValue("storage-provider-class"));
  }
}
