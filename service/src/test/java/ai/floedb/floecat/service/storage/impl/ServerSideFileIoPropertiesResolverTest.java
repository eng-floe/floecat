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
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import ai.floedb.floecat.storage.errors.SourceCatalogVendingGrpcStatus;
import ai.floedb.floecat.storage.rpc.ResolveStorageAuthorityResponse;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.rpc.VendedStorageCredential;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import io.grpc.StatusRuntimeException;
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
    when(snapshotRepo.latestRegisteredSnapshot(tableWithSnapshotMetadataOnly().getResourceId()))
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

  @Test
  void noMatchingAuthorityFallsBackToSourceCatalogVending() {
    // A table captured through a delegating Iceberg REST catalog has no storage authority to
    // match, because the catalog vends its own credentials. Reading it back has to take the same
    // fallback the vend RPC takes, or capture succeeds and every scan of the result fails.
    when(repo.list(eq("acct"), anyInt(), eq(""), any())).thenReturn(List.of());
    RecordingVendor vendor =
        new RecordingVendor(
            ResolveStorageAuthorityResponse.newBuilder()
                .putClientSafeConfig("s3.region", "us-east-1")
                .addStorageCredentials(
                    VendedStorageCredential.newBuilder()
                        .setPrefix("s3://localstack-output/warehouse/orders")
                        .putConfig("s3.access-key-id", "VENDEDKEY")
                        .putConfig("s3.secret-access-key", "vended-secret")
                        .putConfig("s3.session-token", "vended-token"))
                .build());
    service.sourceCatalogVendor = vendor;

    Map<String, String> props =
        service.applyToTableProperties(table(), null, Map.of("owner", "analytics"));

    assertEquals(1, vendor.calls);
    assertEquals("VENDEDKEY", props.get("s3.access-key-id"));
    assertEquals("vended-secret", props.get("s3.secret-access-key"));
    assertEquals("vended-token", props.get("s3.session-token"));
    assertEquals("us-east-1", props.get("s3.region"));
    assertEquals("analytics", props.get("owner"));
  }

  @Test
  void matchingAuthorityStaysAuthoritativeOverSourceCatalogVending() {
    // Vending is a fallback, never a preference: a table already covered by an authority must not
    // start paying for a connector build and catalog round-trip on every scan.
    when(repo.list(eq("acct"), anyInt(), eq(""), any())).thenReturn(List.of(databricksAuthority()));
    RecordingVendor vendor = new RecordingVendor(null);
    service.sourceCatalogVendor = vendor;

    Map<String, String> props =
        service.applyToTableProperties(
            table(),
            "s3://floedb-databricks-metastore-367509577365/metastore/table/metadata/00001.json",
            Map.of());

    assertEquals(0, vendor.calls);
    assertEquals("akid", props.get("s3.access-key-id"));
  }

  @Test
  void noAuthorityAndNoVendingStillRaisesTheStructuredMissingAuthorityError() {
    // The fallback must not turn a genuine misconfiguration into an empty property map: a scan
    // handed no credentials at all fails later and further away, as an opaque 403 on the first
    // read rather than a named configuration error at planning time.
    when(repo.list(eq("acct"), anyInt(), eq(""), any())).thenReturn(List.of());
    service.sourceCatalogVendor = new RecordingVendor(null);

    StatusRuntimeException error =
        assertThrows(
            StatusRuntimeException.class,
            () -> service.applyToTableProperties(table(), null, Map.of()));

    assertTrue(SourceCatalogVendingGrpcStatus.isNoMatchingStorageAuthority(error));
  }

  /** Stands in for the catalog round-trip so the wiring can be tested without a live catalog. */
  private static final class RecordingVendor extends SourceCatalogCredentialVendor {
    private final ResolveStorageAuthorityResponse response;
    private int calls;

    private RecordingVendor(ResolveStorageAuthorityResponse response) {
      this.response = response;
    }

    @Override
    ResolveStorageAuthorityResponse vendForTable(
        Table table,
        String responseLocationPrefix,
        SourceCatalogCredentialVendor.CredentialUse use) {
      calls++;
      return response;
    }
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
        .build();
  }

  private static final class StaticSecretsManager implements SecretsManager {
    @Override
    public void put(String accountId, String secretType, String secretId, byte[] payload) {}

    @Override
    public boolean putIfAbsent(
        String accountId, String secretType, String secretId, byte[] payload) {
      return true;
    }

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
