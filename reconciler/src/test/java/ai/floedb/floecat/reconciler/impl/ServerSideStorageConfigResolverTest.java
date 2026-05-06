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

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ServerSideStorageConfigResolverTest {

  @Test
  void restWarehouseUsesS3PrefixForAuthorityLookup() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of(
                "iceberg.source", "rest",
                "warehouse", "warehouse",
                "s3.endpoint", "http://minio:9000"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertEquals("s3://warehouse/", ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void filesystemConnectorUsesMetadataLocationForAuthorityLookup() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "filesystem",
            "s3://warehouse/ns/table/metadata/00001.metadata.json",
            Map.of("iceberg.source", "filesystem"),
            new ConnectorConfig.Auth("none", Map.of(), Map.of()));

    assertEquals(
        "s3://warehouse/ns/table/metadata/00001.metadata.json",
        ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void nonS3RestWarehouseDoesNotResolveAuthorityLookupLocation() {
    ConnectorConfig config =
        new ConnectorConfig(
            ConnectorConfig.Kind.ICEBERG,
            "rest",
            "http://polaris:8181/api/catalog",
            Map.of("iceberg.source", "rest", "warehouse", "file:///tmp/warehouse"),
            new ConnectorConfig.Auth("oauth2", Map.of(), Map.of()));

    assertNull(ServerSideStorageConfigResolver.storageAuthorityLookupLocation(config));
  }

  @Test
  void mergeResolvedStorageConfigAddsServerSideSecretsAndClientSafeProps() {
    ResolveStorageAuthorityResponse response =
        ResolveStorageAuthorityResponse.newBuilder()
            .putClientSafeConfig("s3.endpoint", "http://minio:9000")
            .putClientSafeConfig("s3.path-style-access", "true")
            .addStorageCredentials(
                VendedStorageCredential.newBuilder()
                    .putConfig("type", "s3")
                    .putConfig("s3.access-key-id", "minio")
                    .putConfig("s3.secret-access-key", "miniostorage"))
            .build();

    Map<String, String> merged =
        ServerSideStorageConfigResolver.mergeResolvedStorageConfig(
            Map.of("iceberg.source", "rest", "warehouse", "warehouse"), response);

    assertEquals("http://minio:9000", merged.get("s3.endpoint"));
    assertEquals("true", merged.get("s3.path-style-access"));
    assertEquals("minio", merged.get("s3.access-key-id"));
    assertEquals("miniostorage", merged.get("s3.secret-access-key"));
    assertNull(merged.get("type"));
  }
}
