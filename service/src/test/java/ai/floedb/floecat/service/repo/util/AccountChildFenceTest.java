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

package ai.floedb.floecat.service.repo.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The account child fence makes top-level publication and account deletion mutually exclusive.
 *
 * <p>The top of the same chain {@link CatalogChildFenceTest} and {@code NamespaceChildFenceTest}
 * cover further down. Without it, a create authorized while the account was live can pause, let
 * teardown remove the account pointer and sweep its prefixes, and then commit behind the sweep —
 * leaving a live catalog or connector under an account that no longer exists and that teardown has
 * already reported complete.
 */
class AccountChildFenceTest {
  private static final String ACCOUNT = "acct-1";
  private static final ResourceId ACCOUNT_ID =
      ResourceId.newBuilder().setId(ACCOUNT).setKind(ResourceKind.RK_ACCOUNT).build();
  private static final ResourceId CATALOG_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("cat-1")
          .setKind(ResourceKind.RK_CATALOG)
          .build();
  private static final ResourceId CONNECTOR_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("conn-1")
          .setKind(ResourceKind.RK_CONNECTOR)
          .build();
  private static final ResourceId AUTHORITY_ID =
      ResourceId.newBuilder()
          .setAccountId(ACCOUNT)
          .setId("authority-1")
          .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
          .build();

  private MarkerStore markers;
  private AccountRepository accounts;
  private CatalogRepository catalogs;
  private ConnectorRepository connectors;
  private StorageAuthorityRepository authorities;

  @BeforeEach
  void setUp() {
    var pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    markers = new MarkerStore();
    markers.pointerStore = pointers;
    accounts = new AccountRepository(pointers, blobs);
    catalogs = new CatalogRepository(pointers, blobs);
    connectors = new ConnectorRepository(pointers, blobs);
    authorities = new StorageAuthorityRepository(pointers, blobs);
    accounts.create(Account.newBuilder().setResourceId(ACCOUNT_ID).setDisplayName("t-1").build());
  }

  private static Catalog catalog() {
    return Catalog.newBuilder().setResourceId(CATALOG_ID).setDisplayName("warehouse").build();
  }

  private static Connector connector() {
    return Connector.newBuilder().setResourceId(CONNECTOR_ID).setDisplayName("feed").build();
  }

  private static StorageAuthority authority() {
    return StorageAuthority.newBuilder()
        .setResourceId(AUTHORITY_ID)
        .setDisplayName("warehouse credentials")
        .build();
  }

  private BatchGuard accountLive() {
    return markers.accountLiveGuard(ACCOUNT).orElseThrow();
  }

  @Test
  void aLiveAccountYieldsAGuardAndTheCreateCommitsUnderIt() {
    catalogs.create(catalog(), accountLive());
    connectors.create(connector(), accountLive());
    authorities.create(authority(), accountLive());

    assertThat(catalogs.getById(CATALOG_ID)).isPresent();
    assertThat(connectors.getById(CONNECTOR_ID)).isPresent();
    assertThat(authorities.getById(AUTHORITY_ID)).isPresent();
  }

  @Test
  void aCatalogPublishFailsWhenTheAccountDeleteCommittedFirst() {
    var publishGuard = accountLive();

    assertThat(accounts.delete(ACCOUNT_ID)).isTrue();

    assertThatThrownBy(() -> catalogs.create(catalog(), publishGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(ACCOUNT);
    assertThat(catalogs.getById(CATALOG_ID)).isEmpty();
  }

  @Test
  void aConnectorPublishFailsWhenTheAccountDeleteCommittedFirst() {
    var publishGuard = accountLive();

    assertThat(accounts.delete(ACCOUNT_ID)).isTrue();

    assertThatThrownBy(() -> connectors.create(connector(), publishGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(ACCOUNT);
    assertThat(connectors.getById(CONNECTOR_ID)).isEmpty();
  }

  @Test
  void aStorageAuthorityPublishFailsWhenTheAccountDeleteCommittedFirst() {
    var publishGuard = accountLive();

    assertThat(accounts.delete(ACCOUNT_ID)).isTrue();

    assertThatThrownBy(() -> authorities.create(authority(), publishGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class)
        .hasMessageContaining(ACCOUNT);
    assertThat(authorities.getById(AUTHORITY_ID)).isEmpty();
  }

  @Test
  void thereIsNoGuardToMintOnceTheAccountPointerIsGone() {
    assertThat(accounts.delete(ACCOUNT_ID)).isTrue();

    // The caller refuses rather than publishing unguarded: a create that finds the pointer already
    // absent is exactly a create a running sweep can no longer see.
    assertThat(markers.accountLiveGuard(ACCOUNT)).isEmpty();
  }

  @Test
  void theGuardIsACheckSoSiblingCreatesDoNotContendWithEachOther() {
    var first = accountLive();
    var second = accountLive();

    catalogs.create(catalog(), first);
    connectors.create(connector(), second);

    // Publishing does not advance the account pointer, so a guard minted before a sibling create
    // still holds afterwards — the fence serialises against deletion, not against other children.
    assertThat(catalogs.getById(CATALOG_ID)).isPresent();
    assertThat(connectors.getById(CONNECTOR_ID)).isPresent();
    assertThat(first.reevaluate()).isEqualTo(BatchGuard.Outcome.HOLDS);
  }

  @Test
  void anAccountUpdateBreaksAnInFlightGuardRatherThanLettingItCommitStale() {
    var publishGuard = accountLive();
    long version = accounts.metaFor(ACCOUNT_ID).getPointerVersion();

    assertThat(
            accounts.update(
                Account.newBuilder()
                    .setResourceId(ACCOUNT_ID)
                    .setDisplayName("t-1")
                    .setDescription("renamed")
                    .build(),
                version))
        .isTrue();

    // Conservative in the same way the catalog fence is: CasCheck cannot express "exists at any
    // version", so an unrelated UpdateAccount trips this too. It costs a retry, which re-resolves
    // the account and succeeds against its new version.
    assertThatThrownBy(() -> catalogs.create(catalog(), publishGuard))
        .isInstanceOf(BaseResourceRepository.BatchGuardFailedException.class);
    assertThat(catalogs.getById(CATALOG_ID)).isEmpty();

    catalogs.create(catalog(), accountLive());
    assertThat(catalogs.getById(CATALOG_ID)).isPresent();
  }
}
