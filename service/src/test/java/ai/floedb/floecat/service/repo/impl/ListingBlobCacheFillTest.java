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

package ai.floedb.floecat.service.repo.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ReconcilePolicy;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.concurrent.MetadataIoRunner;
import ai.floedb.floecat.service.concurrent.MetadataResourceReader;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.service.repo.util.MetadataRepositoryFactory;
import ai.floedb.floecat.service.testsupport.CountingBlobStore;
import ai.floedb.floecat.service.testsupport.DiskBlobCacheTestSupport;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.telemetry.NoopObservability;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A listing that admits misses serves a repeat listing without reading the blob store; the default
 * listing only consumes resident entries. Records are written through uncached repositories on the
 * same stores so every listing starts cold.
 */
class ListingBlobCacheFillTest {

  @TempDir Path tempDir;

  private InMemoryPointerStore pointers;
  private CountingBlobStore blobs;
  private BlobCacheAccess cache;
  private MetadataRepositoryFactory repositories;

  @BeforeEach
  void setUp() {
    pointers = new InMemoryPointerStore();
    blobs = new CountingBlobStore();
    cache = DiskBlobCacheTestSupport.create(tempDir.resolve("blob-cache"));
    repositories =
        new MetadataRepositoryFactory(
            pointers,
            pointers,
            blobs,
            cache,
            new MetadataResourceReader(new MetadataIoRunner(new NoopObservability())));
  }

  @Test
  void aRepeatAccountListingIsServedFromTheBlobCache() {
    AccountRepository writer = new AccountRepository(pointers, blobs);
    for (String id : List.of("acct-a", "acct-b", "acct-c")) {
      writer.create(account(id));
    }
    AccountRepository accounts = new AccountRepository(repositories);

    // Each account's blob is its own cache partition; the cold page still reads in one call.
    blobs.resetReads();
    assertThat(accounts.list(50, "", new StringBuilder())).hasSize(3);
    assertThat(blobs.bodies()).isEqualTo(3);
    assertThat(blobs.batchGets()).isEqualTo(1);
    assertThat(blobs.pointGets()).isZero();

    blobs.resetReads();
    assertThat(accounts.list(50, "", new StringBuilder())).hasSize(3);
    assertThat(blobs.bodies()).isZero();
  }

  @Test
  void aRepeatConnectorListingIsServedFromTheBlobCache() {
    ConnectorRepository writer = new ConnectorRepository(pointers, blobs);
    for (String id : List.of("conn-a", "conn-b")) {
      writer.create(connector("acct", id, true));
    }
    ConnectorRepository connectors = new ConnectorRepository(repositories);

    assertListingReadsOnce(() -> connectors.list("acct", 200, "", new StringBuilder()), 2);
  }

  @Test
  void aRepeatStorageAuthorityListingIsServedFromTheBlobCache() {
    StorageAuthorityRepository writer = new StorageAuthorityRepository(pointers, blobs);
    for (String id : List.of("sa-a", "sa-b")) {
      writer.create(storageAuthority("acct", id));
    }
    StorageAuthorityRepository authorities = new StorageAuthorityRepository(repositories);

    assertListingReadsOnce(
        () -> authorities.list("acct", Integer.MAX_VALUE, "", new StringBuilder()), 2);
  }

  @Test
  void aRepeatCatalogIntegrationListingIsServedFromTheBlobCache() {
    CatalogIntegrationRepository writer = new CatalogIntegrationRepository(pointers, blobs);
    for (String id : List.of("int-a", "int-b")) {
      writer.create(integration("acct", id));
    }
    CatalogIntegrationRepository integrations =
        new CatalogIntegrationRepository(pointers, pointers, blobs, cache);

    assertListingReadsOnce(() -> integrations.list("acct", 50, "", new StringBuilder()), 2);
  }

  @Test
  void aRepeatCatalogOverlayListingIsServedFromTheBlobCache() {
    CatalogOverlayRepository writer = new CatalogOverlayRepository(pointers, blobs);
    for (String id : List.of("ov-a", "ov-b")) {
      writer.create(overlay("acct", id, "int-a"));
    }
    CatalogOverlayRepository overlays =
        new CatalogOverlayRepository(pointers, pointers, blobs, cache);

    assertListingReadsOnce(() -> overlays.list("acct", 50, "", new StringBuilder()), 2);
  }

  @Test
  void aRepeatCatalogOverlayListingByIntegrationIsServedFromTheBlobCache() {
    CatalogOverlayRepository writer = new CatalogOverlayRepository(pointers, blobs);
    for (String id : List.of("ov-a", "ov-b")) {
      writer.create(overlay("acct", id, "int-a"));
    }
    writer.create(overlay("acct", "ov-c", "int-b"));
    CatalogOverlayRepository overlays =
        new CatalogOverlayRepository(pointers, pointers, blobs, cache);

    assertListingReadsOnce(
        () -> overlays.listByIntegration("acct", "int-a", 50, "", new StringBuilder()), 2);
  }

  @Test
  void aCachedConnectorListingSeesUpdatesCreatesAndDeletes() {
    ConnectorRepository writer = new ConnectorRepository(pointers, blobs);
    for (String id : List.of("conn-a", "conn-b")) {
      writer.create(connector("acct", id, true));
    }
    ConnectorRepository connectors = new ConnectorRepository(repositories);
    assertThat(connectors.list("acct", 200, "", new StringBuilder())).hasSize(2);

    Connector a = connector("acct", "conn-a", true);
    long version = writer.metaFor(a.getResourceId()).getPointerVersion();
    assertThat(writer.update(connector("acct", "conn-a", false), version)).isTrue();
    writer.create(connector("acct", "conn-c", true));
    assertThat(writer.delete(connector("acct", "conn-b", true).getResourceId())).isTrue();

    // Only the rewritten and the new body are read; the deleted one is gone.
    blobs.resetReads();
    Map<String, Connector> listed = byId(connectors.list("acct", 200, "", new StringBuilder()));
    assertThat(listed).containsOnlyKeys("conn-a", "conn-c");
    assertThat(listed.get("conn-a").getPolicy().getEnabled()).isFalse();
    assertThat(blobs.bodies()).isEqualTo(2);
  }

  @Test
  void theDefaultListingConsumesResidentEntriesButDoesNotAdmitMisses() {
    GenericResourceRepository<Account, ?> generic =
        new GenericResourceRepository<>(
            pointers,
            blobs,
            Schemas.ACCOUNT,
            Account::parseFrom,
            Account::toByteArray,
            "application/x-protobuf",
            cache);
    AccountRepository writer = new AccountRepository(pointers, blobs);
    for (String id : List.of("acct-a", "acct-b")) {
      writer.create(account(id));
    }
    String prefix = Keys.accountPointerByNamePrefix();

    blobs.resetReads();
    assertThat(generic.listByPrefix(prefix, 50, "", new StringBuilder())).hasSize(2);
    assertThat(generic.listByPrefix(prefix, 50, "", new StringBuilder())).hasSize(2);
    assertThat(blobs.bodies()).isEqualTo(4);

    // Once a relisting has admitted the bodies, the default listing reads them from the cache.
    assertThat(generic.listByPrefixForRelisting(prefix, 50, "", new StringBuilder())).hasSize(2);
    blobs.resetReads();
    assertThat(generic.listByPrefix(prefix, 50, "", new StringBuilder())).hasSize(2);
    assertThat(blobs.bodies()).isZero();
  }

  /** The first listing reads every body from the store; the second reads none. */
  private void assertListingReadsOnce(Supplier<List<?>> listing, int expected) {
    blobs.resetReads();
    assertThat(listing.get()).hasSize(expected);
    assertThat(blobs.bodies()).isEqualTo(expected);

    blobs.resetReads();
    assertThat(listing.get()).hasSize(expected);
    assertThat(blobs.bodies()).isZero();
  }

  private static Map<String, Connector> byId(List<Connector> connectors) {
    return connectors.stream()
        .collect(Collectors.toMap(c -> c.getResourceId().getId(), Function.identity()));
  }

  private static Account account(String id) {
    return Account.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(id)
                .setId(id)
                .setKind(ResourceKind.RK_ACCOUNT)
                .build())
        .setDisplayName("name-" + id)
        .build();
  }

  private static Connector connector(String accountId, String id, boolean policyEnabled) {
    return Connector.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(id)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build())
        .setDisplayName("name-" + id)
        .setPolicy(ReconcilePolicy.newBuilder().setEnabled(policyEnabled))
        .build();
  }

  private static CatalogIntegration integration(String accountId, String id) {
    return CatalogIntegration.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(id)
                .setKind(ResourceKind.RK_CATALOG_INTEGRATION)
                .build())
        .setDisplayName("name-" + id)
        .build();
  }

  private static CatalogOverlay overlay(String accountId, String id, String integrationId) {
    return CatalogOverlay.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(id)
                .setKind(ResourceKind.RK_CATALOG_OVERLAY)
                .build())
        .setDisplayName("name-" + id)
        .setIntegrationId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(integrationId)
                .setKind(ResourceKind.RK_CATALOG_INTEGRATION))
        .setCatalogId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId("cat-" + id)
                .setKind(ResourceKind.RK_CATALOG))
        .build();
  }

  private static StorageAuthority storageAuthority(String accountId, String id) {
    return StorageAuthority.newBuilder()
        .setResourceId(
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(id)
                .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
                .build())
        .setDisplayName("name-" + id)
        .setEnabled(true)
        .setType("s3")
        .setLocationPrefix("s3://bucket/" + id)
        .build();
  }
}
