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

package ai.floedb.floecat.service.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ServerSideFileIoPropertiesResolverTest {
  private ServerSideFileIoPropertiesResolver service;
  private StorageAuthorityRepository repo;
  private SnapshotRepository snapshotRepo;

  @BeforeEach
  void setUp() {
    service = new ServerSideFileIoPropertiesResolver();
    repo = mock(StorageAuthorityRepository.class);
    snapshotRepo = mock(SnapshotRepository.class);

    StorageAuthorityResolver resolver = new StorageAuthorityResolver();
    resolver.secretsManager = new StaticSecretsManager();

    service.repo = repo;
    service.snapshotRepo = snapshotRepo;
    service.resolver = resolver;
  }

  @Test
  void applyToTablePropertiesReplacesLocalstackIoSettingsForResolvedAwsAuthority() {
    when(repo.list(eq("acct"), anyInt(), eq(""), any())).thenReturn(List.of(databricksAuthority()));

    Map<String, String> props =
        service.applyToTableProperties(
            table(),
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.json",
            Map.of(
                "s3.endpoint", "http://localhost:19110",
                "s3.path-style-access", "true",
                "owner", "analytics"));

    assertEquals("analytics", props.get("owner"));
    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("akid", props.get("s3.access-key-id"));
    assertEquals("secret", props.get("s3.secret-access-key"));
    assertFalse(props.containsKey("s3.endpoint"));
    assertFalse(props.containsKey("s3.path-style-access"));
  }

  @Test
  void resolvePrefersStorageLocationOverSourceMetadataLocation() {
    when(repo.list(eq("acct"), anyInt(), eq(""), any()))
        .thenReturn(List.of(storageAuthority(), databricksAuthority()));

    Map<String, String> props =
        service.applyToTableProperties(tableWithStorageAndMetadataLocation(), null, Map.of());

    assertEquals("akid", props.get("s3.access-key-id"));
    assertTrue(props.get("s3.region").equals("us-west-2"));
  }

  @Test
  void resolveFallsBackToSnapshotMetadataRootWhenNoTableRootExists() {
    when(repo.list(eq("acct"), anyInt(), eq(""), any())).thenReturn(List.of(databricksAuthority()));
    when(snapshotRepo.getById(tableWithSnapshotMetadataOnly().getResourceId(), 77L))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableWithSnapshotMetadataOnly().getResourceId())
                    .setSnapshotId(77L)
                    .setMetadataLocation(
                        "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.metadata.json")
                    .build()));

    Map<String, String> props =
        service.applyToTableProperties(tableWithSnapshotMetadataOnly(), null, Map.of());

    assertEquals("us-east-1", props.get("s3.region"));
  }

  private static Table table() {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("tbl-1")
                .build())
        .putProperties("location", "s3://localstack-output/warehouse/orders")
        .setUpstream(
            UpstreamRef.newBuilder()
                .setUri("s3://floedb-databricks-metastore-367509577365/metastore/table")
                .build())
        .build();
  }

  private static StorageAuthority databricksAuthority() {
    return StorageAuthority.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                .setId("sa-db")
                .build())
        .setDisplayName("databricks")
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://floedb-databricks-metastore-367509577365")
        .setRegion("us-east-1")
        .build();
  }

  private static StorageAuthority storageAuthority() {
    return StorageAuthority.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                .setId("sa-storage")
                .build())
        .setDisplayName("warehouse")
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://warehouse/orders")
        .setRegion("us-west-2")
        .build();
  }

  private static Table tableWithStorageAndMetadataLocation() {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("tbl-1")
                .build())
        .putProperties("storage_location", "s3://warehouse/orders")
        .putProperties(
            "source_metadata_location",
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.metadata.json")
        .build();
  }

  private static Table tableWithSnapshotMetadataOnly() {
    return Table.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId("acct")
                .setKind(ResourceKind.RK_TABLE)
                .setId("tbl-1")
                .build())
        .putProperties("current-snapshot-id", "77")
        .build();
  }

  private static final class StaticSecretsManager implements SecretsManager {
    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public Optional<byte[]> get(String accountId, String secretType, String secretId) {
      return Optional.of(
          AuthCredentials.newBuilder()
              .setAws(
                  AuthCredentials.AwsCredentials.newBuilder()
                      .setAccessKeyId("akid")
                      .setSecretAccessKey("secret"))
              .build()
              .toByteArray());
    }

    @Override
    public void update(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public void delete(String accountId, String secretType, String secretId) {}
  }
}
