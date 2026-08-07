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

package ai.floedb.floecat.service.it;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.account.rpc.UpdateAccountRequest;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.StorageAuthorityRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.storage.impl.StorageAuthorityResolver;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.rpc.StorageAuthority;
import ai.floedb.floecat.storage.secrets.SecretsManager;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class AccountMutationIT {

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub tenancy;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub view;

  String accountPrefix = this.getClass().getSimpleName() + "_";
  String seedAccountId;

  @Inject AccountRepository accountRepository;
  @Inject CatalogRepository catalogRepository;
  @Inject NamespaceRepository namespaceRepository;
  @Inject TableRepository tableRepository;
  @Inject ViewRepository viewRepository;
  @Inject StorageAuthorityRepository storageAuthorityRepository;
  @Inject SecretsManager secretsManager;
  @Inject ai.floedb.floecat.service.repo.util.MarkerStore markerStore;
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private static final Schema SIMPLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    seedAccountId =
        accountRepository
            .getByName(TestSupport.DEFAULT_SEED_ACCOUNT)
            .orElseThrow()
            .getResourceId()
            .getId();
  }

  @Test
  void accountExists() throws Exception {
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t1")
            .setDescription("desc")
            .build();

    var r1 = tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(spec).build());

    assertNotNull(r1.getAccount());
    assertEquals(accountPrefix + "t1", r1.getAccount().getDisplayName());

    var newSpec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t1")
            .setDescription("desc")
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(newSpec).build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, "already exists");
  }

  @Test
  void accountCreateUpdateDelete() throws Exception {
    var spec1 =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "t_pre")
            .setDescription("pre")
            .build();

    var created = tenancy.createAccount(CreateAccountRequest.newBuilder().setSpec(spec1).build());

    var id = created.getAccount().getResourceId();
    assertEquals(ResourceKind.RK_ACCOUNT, id.getKind());

    FieldMask mask =
        FieldMask.newBuilder().addAllPaths(List.of("display_name", "description")).build();
    var upd1 =
        tenancy.updateAccount(
            UpdateAccountRequest.newBuilder()
                .setAccountId(id)
                .setSpec(
                    AccountSpec.newBuilder()
                        .setDisplayName(accountPrefix + "t_pre")
                        .setDescription("desc1")
                        .build())
                .setUpdateMask(mask)
                .build());

    var m1 = upd1.getMeta();
    assertTrue(m1.getPointerVersion() >= 1);
    assertEquals(accountPrefix + "t_pre", upd1.getAccount().getDisplayName());
    assertEquals("desc1", upd1.getAccount().getDescription());

    String expectedName = accountPrefix + "t_pre_2";

    var updOk =
        tenancy.updateAccount(
            UpdateAccountRequest.newBuilder()
                .setAccountId(id)
                .setSpec(
                    AccountSpec.newBuilder()
                        .setDisplayName(expectedName)
                        .setDescription("desc2")
                        .build())
                .setUpdateMask(mask)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());

    assertEquals(expectedName, updOk.getAccount().getDisplayName());
    assertEquals("desc2", updOk.getAccount().getDescription());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    String next = "";
    boolean hasMatch = false;
    do {
      var resp = tenancy.listAccounts(ListAccountsRequest.newBuilder().build());

      hasMatch |=
          resp.getAccountsList().stream().anyMatch(t -> t.getDisplayName().equals(expectedName));

      next = resp.getPage().getNextPageToken();
    } while (!next.isEmpty());

    assertTrue(hasMatch, "Expected to find account with displayName=" + expectedName);

    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.updateAccount(
                    UpdateAccountRequest.newBuilder()
                        .setAccountId(id)
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "t_pre_3")
                                .build())
                        .setUpdateMask(mask)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(424242L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var m2 = updOk.getMeta();
    var del =
        tenancy.deleteAccount(
            DeleteAccountRequest.newBuilder()
                .setAccountId(id)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m2.getPointerVersion())
                        .setExpectedEtag(m2.getEtag())
                        .build())
                .build());
    assertEquals(m2.getPointerKey(), del.getMeta().getPointerKey());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () -> tenancy.getAccount(GetAccountRequest.newBuilder().setAccountId(id).build()));

    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Account not found");

    var seededAccountId =
        ResourceId.newBuilder()
            .setAccountId(seedAccountId)
            .setId(seedAccountId)
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    var delSeeded =
        tenancy.deleteAccount(
            DeleteAccountRequest.newBuilder()
                .setAccountId(seededAccountId)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(seededMeta.getPointerVersion())
                        .setExpectedEtag(seededMeta.getEtag())
                        .build())
                .build());
    assertEquals(seededMeta.getPointerKey(), delSeeded.getMeta().getPointerKey());

    var seededNotFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.getAccount(
                    GetAccountRequest.newBuilder().setAccountId(seededAccountId).build()));

    TestSupport.assertGrpcAndMc(
        seededNotFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Account not found");
  }

  @Test
  void accountCreateIdempotent() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-1").build();
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "idem_account")
            .setDescription("x")
            .build();

    var r1 =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getAccount().getResourceId().getId(), r2.getAccount().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void accountCreateIdempotencyMismatch() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-2").build();

    tenancy.createAccount(
        CreateAccountRequest.newBuilder()
            .setSpec(
                AccountSpec.newBuilder().setDisplayName(accountPrefix + "idem_account2").build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "idem_account2_DIFFERENT")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void accountCreateIdempotencyMismatchOnDescription() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(accountPrefix + "k-ten-3").build();

    tenancy.createAccount(
        CreateAccountRequest.newBuilder()
            .setSpec(
                AccountSpec.newBuilder()
                    .setDisplayName(accountPrefix + "idem_account3")
                    .setDescription("desc-a")
                    .build())
            .setIdempotency(key)
            .build());

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(
                            AccountSpec.newBuilder()
                                .setDisplayName(accountPrefix + "idem_account3")
                                .setDescription("desc-b")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void accountCreateWithExplicitIdUsesProvidedId() throws Exception {
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "explicit-id")
            .setDescription("explicit")
            .build();
    String principalId = seedAccountId;
    var accountId =
        ResourceId.newBuilder()
            .setAccountId(principalId)
            .setId("acct-explicit-1")
            .setKind(ResourceKind.RK_ACCOUNT)
            .build();

    var created =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder().setSpec(spec).setAccountId(accountId).build());

    assertEquals("acct-explicit-1", created.getAccount().getResourceId().getId());
    assertEquals(principalId, created.getAccount().getResourceId().getAccountId());

    var fetched =
        tenancy.getAccount(GetAccountRequest.newBuilder().setAccountId(accountId).build());
    assertEquals("acct-explicit-1", fetched.getAccount().getResourceId().getId());
    assertEquals(principalId, fetched.getAccount().getResourceId().getAccountId());
    assertEquals(accountPrefix + "explicit-id", fetched.getAccount().getDisplayName());
    assertEquals("explicit", fetched.getAccount().getDescription());
  }

  @Test
  void accountCreateWithExplicitIdMissingIdFails() throws Exception {
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "explicit-missing")
            .setDescription("explicit")
            .build();
    var accountId = ResourceId.newBuilder().setKind(ResourceKind.RK_ACCOUNT).build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(spec)
                        .setAccountId(accountId)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "Account id is required");
  }

  @Test
  void accountCreateWithExplicitIdWrongKindFails() throws Exception {
    var spec =
        AccountSpec.newBuilder()
            .setDisplayName(accountPrefix + "explicit-wrong-kind")
            .setDescription("explicit")
            .build();
    var accountId =
        ResourceId.newBuilder().setId("acct-wrong-kind").setKind(ResourceKind.RK_CATALOG).build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                tenancy.createAccount(
                    CreateAccountRequest.newBuilder()
                        .setSpec(spec)
                        .setAccountId(accountId)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "RK_ACCOUNT");
  }

  @Test
  void deleteAccountCleansPagedCatalogsAndTables() throws Exception {
    var baseCatalog = TestSupport.createCatalog(catalog, accountPrefix + "cascade_base", "");
    var namespace =
        TestSupport.createNamespace(
            this.namespace,
            baseCatalog.getResourceId(),
            "cascade_ns",
            List.of("cascade"),
            "namespace");
    var schemaJson = SchemaParser.toJson(SIMPLE_SCHEMA);

    for (int i = 0; i < 205; i++) {
      TestSupport.createTable(
          table,
          baseCatalog.getResourceId(),
          namespace.getResourceId(),
          accountPrefix + "table_" + i,
          "s3://bucket/",
          schemaJson,
          "table");
    }

    for (int i = 0; i < 205; i++) {
      TestSupport.createCatalog(catalog, accountPrefix + "cascade_cat_" + i, "");
    }

    assertTrue(
        tableRepository.count(
                seedAccountId,
                baseCatalog.getResourceId().getId(),
                namespace.getResourceId().getId())
            >= 205);
    assertTrue(catalogRepository.count(seedAccountId) >= 206);

    var seededAccountId = seedAccountResourceId();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(seededAccountId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(seededMeta.getPointerVersion())
                    .setExpectedEtag(seededMeta.getEtag())
                    .build())
            .build());

    assertEquals(
        0,
        tableRepository.count(
            seedAccountId, baseCatalog.getResourceId().getId(), namespace.getResourceId().getId()));
    assertEquals(0, catalogRepository.count(seedAccountId));
    assertEquals(0, ptr.countByPrefix(Keys.tableRootPrefix(seedAccountId)));
    assertEquals(0, ptr.countByPrefix(Keys.catalogPointerByIdPrefix(seedAccountId)));
    assertEquals(0, ptr.countByPrefix(Keys.catalogPointerByNamePrefix(seedAccountId)));
  }

  @Test
  void deleteAccountRemovesStorageAuthoritiesAndTheirCredentials() {
    var authorityId =
        ResourceId.newBuilder()
            .setAccountId(seedAccountId)
            .setId("account-cleanup-authority")
            .setKind(ResourceKind.RK_STORAGE_AUTHORITY)
            .build();
    storageAuthorityRepository.create(
        StorageAuthority.newBuilder()
            .setResourceId(authorityId)
            .setDisplayName("account cleanup authority")
            .setType("s3")
            .setLocationPrefix("s3://warehouse")
            .build());
    secretsManager.put(
        seedAccountId,
        StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE,
        authorityId.getId(),
        AuthCredentials.newBuilder()
            .setBearer(AuthCredentials.BearerToken.newBuilder().setToken("old-secret"))
            .build()
            .toByteArray());

    var seededAccountId = seedAccountResourceId();
    tenancy.deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(seededAccountId).build());

    assertTrue(storageAuthorityRepository.getById(authorityId).isEmpty());
    assertTrue(
        secretsManager
            .get(
                seedAccountId,
                StorageAuthorityResolver.STORAGE_AUTHORITY_SECRET_TYPE,
                authorityId.getId())
            .isEmpty());
    assertEquals(0, ptr.countByPrefix(Keys.storageAuthorityCredentialCleanupPrefix(seedAccountId)));
  }

  @Test
  void deleteAccountDeletesViewsDuringNamespaceCleanup() throws Exception {
    var cat = TestSupport.createCatalog(catalog, accountPrefix + "view_cat", "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "views_ns", List.of("cascade"), "namespace");

    var createdView =
        TestSupport.createView(
            view,
            cat.getResourceId(),
            ns.getResourceId(),
            "account_cleanup_view",
            "SELECT 1",
            "cleanup view");
    assertEquals(
        1,
        viewRepository.count(
            seedAccountId, cat.getResourceId().getId(), ns.getResourceId().getId()));

    var seededAccountId = seedAccountResourceId();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(seededAccountId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(seededMeta.getPointerVersion())
                    .setExpectedEtag(seededMeta.getEtag())
                    .build())
            .build());

    assertEquals(
        0,
        viewRepository.count(
            seedAccountId, cat.getResourceId().getId(), ns.getResourceId().getId()));
    assertTrue(
        ptr.get(Keys.viewPointerById(seedAccountId, createdView.getResourceId().getId()))
            .isEmpty());
    assertEquals(0, ptr.countByPrefix(Keys.viewRootPrefix(seedAccountId)));
  }

  @Test
  void deleteAccountCleansNestedNamespaceTree() throws Exception {
    // Account teardown must clean an entire nested namespace tree with relations at several depths.
    // The dropper runs unguarded here, so cleanup cannot raise AbortRetryableException and orphan
    // resources by retrying the account delete after its pointer is gone (regression for #397).
    var cat = TestSupport.createCatalog(catalog, accountPrefix + "nested_cat", "");
    var db =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "db", List.of(), "nested db");
    var schema =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "schema", List.of("db"), "nested schema");
    var sub =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "sub", List.of("db", "schema"), "nested sub");
    var schemaJson = SchemaParser.toJson(SIMPLE_SCHEMA);

    TestSupport.createTable(
        table,
        cat.getResourceId(),
        schema.getResourceId(),
        accountPrefix + "schema_table",
        "s3://bucket/",
        schemaJson,
        "table");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        sub.getResourceId(),
        accountPrefix + "sub_table",
        "s3://bucket/",
        schemaJson,
        "table");
    TestSupport.createView(
        view,
        cat.getResourceId(),
        schema.getResourceId(),
        accountPrefix + "schema_view",
        "SELECT 1",
        "view");

    assertEquals(3, namespaceRepository.listIds(seedAccountId, cat.getResourceId().getId()).size());

    var seededAccountId = seedAccountResourceId();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(seededAccountId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(seededMeta.getPointerVersion())
                    .setExpectedEtag(seededMeta.getEtag())
                    .build())
            .build());

    assertEquals(0, namespaceRepository.listIds(seedAccountId, cat.getResourceId().getId()).size());
    assertEquals(
        0,
        tableRepository.count(
            seedAccountId, cat.getResourceId().getId(), schema.getResourceId().getId()));
    assertEquals(
        0,
        tableRepository.count(
            seedAccountId, cat.getResourceId().getId(), sub.getResourceId().getId()));
    assertEquals(
        0,
        viewRepository.count(
            seedAccountId, cat.getResourceId().getId(), schema.getResourceId().getId()));
    assertEquals(0, catalogRepository.count(seedAccountId));
    assertEquals(0, ptr.countByPrefix(Keys.tableRootPrefix(seedAccountId)));
  }

  @Test
  void deleteAccountCleansUpDespiteACorruptNamespaceBlob() throws Exception {
    // Teardown runs after the account pointer is gone, so anything that throws in it cannot be
    // retried — the retry finds no account and reports success. Namespace discovery therefore must
    // not depend on every namespace blob being parseable, or one corrupt blob orphans everything
    // cleanup had not reached yet (regression for #397).
    var cat = TestSupport.createCatalog(catalog, accountPrefix + "corrupt_cat", "");
    var db =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "db", List.of(), "corrupt db");
    var other =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "other", List.of(), "second ns");
    TestSupport.createTable(
        table,
        cat.getResourceId(),
        other.getResourceId(),
        accountPrefix + "other_table",
        "s3://bucket/",
        SchemaParser.toJson(SIMPLE_SCHEMA),
        "table");

    // One namespace's pointer still resolves; its bytes no longer parse as a Namespace.
    String dbById = Keys.namespacePointerById(seedAccountId, db.getResourceId().getId());
    String corruptBlob = ptr.get(dbById).orElseThrow().getBlobUri();
    blobs.put(
        corruptBlob, new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}, "application/x-protobuf");

    var seededAccountId = seedAccountResourceId();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(seededAccountId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(seededMeta.getPointerVersion())
                    .setExpectedEtag(seededMeta.getEtag())
                    .build())
            .build());

    // Nothing is left behind: not the corrupt namespace, not the intact one, not its table.
    assertEquals(
        0,
        namespaceRepository
            .listRefsUnder(seedAccountId, cat.getResourceId().getId(), List.of())
            .size());
    assertEquals(
        0,
        tableRepository.count(
            seedAccountId, cat.getResourceId().getId(), other.getResourceId().getId()));
    assertEquals(0, catalogRepository.count(seedAccountId));
    assertEquals(0, ptr.countByPrefix(Keys.tableRootPrefix(seedAccountId)));
  }

  @Test
  void deleteAccountCleansUpDespiteACorruptCatalogBlob() throws Exception {
    // Same argument one level up: teardown enumerated catalogs by parsing every catalog blob, so a
    // single unreadable one aborted the sweep after the account pointer was already gone. Nothing
    // retries a CorruptionException, and the client's retry lands on the same unparseable blob, so
    // every catalog behind it — with its namespaces, tables, views and roots — was orphaned for
    // good.
    var broken = TestSupport.createCatalog(catalog, accountPrefix + "broken_cat", "");
    var intact = TestSupport.createCatalog(catalog, accountPrefix + "intact_cat", "");
    var ns =
        TestSupport.createNamespace(
            namespace, intact.getResourceId(), "db", List.of(), "intact db");
    TestSupport.createTable(
        table,
        intact.getResourceId(),
        ns.getResourceId(),
        accountPrefix + "kept_table",
        "s3://bucket/",
        SchemaParser.toJson(SIMPLE_SCHEMA),
        "table");

    String brokenById = Keys.catalogPointerById(seedAccountId, broken.getResourceId().getId());
    blobs.put(
        ptr.get(brokenById).orElseThrow().getBlobUri(),
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        "application/x-protobuf");

    var seededAccountId = seedAccountResourceId();
    var seededMeta = accountRepository.metaForSafe(seededAccountId);
    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(seededAccountId)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(seededMeta.getPointerVersion())
                    .setExpectedEtag(seededMeta.getEtag())
                    .build())
            .build());

    // Both catalogs go, and so does everything under the one the sweep would never have reached.
    assertEquals(0, catalogRepository.count(seedAccountId));
    assertEquals(0, ptr.countByPrefix(Keys.catalogPointerByIdPrefix(seedAccountId)));
    assertEquals(
        0,
        namespaceRepository
            .listRefsUnder(seedAccountId, intact.getResourceId().getId(), List.of())
            .size());
    assertEquals(
        0,
        tableRepository.count(
            seedAccountId, intact.getResourceId().getId(), ns.getResourceId().getId()));
    assertEquals(0, ptr.countByPrefix(Keys.tableRootPrefix(seedAccountId)));
  }

  @Test
  void deleteAccountRecoversNestedResourcesWhenTheCatalogCanonicalPointerIsGone() throws Exception {
    var cat = TestSupport.createCatalog(catalog, accountPrefix + "headless_cat", "");
    var ns =
        TestSupport.createNamespace(
            namespace, cat.getResourceId(), "db", List.of(), "headless catalog db");
    var tbl =
        TestSupport.createTable(
            table,
            cat.getResourceId(),
            ns.getResourceId(),
            accountPrefix + "headless_table",
            "s3://bucket/",
            SchemaParser.toJson(SIMPLE_SCHEMA),
            "table");

    assertTrue(ptr.delete(Keys.catalogPointerById(seedAccountId, cat.getResourceId().getId())));
    assertTrue(
        ptr.get(
                Keys.namespacePointerByPath(
                    seedAccountId, cat.getResourceId().getId(), List.of("db")))
            .isPresent());

    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder().setAccountId(seedAccountResourceId()).build());

    assertTrue(
        ptr.get(Keys.namespacePointerById(seedAccountId, ns.getResourceId().getId())).isEmpty());
    assertTrue(
        ptr.get(Keys.tablePointerById(seedAccountId, tbl.getResourceId().getId())).isEmpty());
    assertEquals(0, ptr.countByPrefix(Keys.catalogRootPrefix(seedAccountId)));
  }

  /**
   * A conditional delete of an account that is already gone sweeps and succeeds.
   *
   * <p>It used to answer NOT_FOUND, on a guard that decided "never existed" from whether the
   * request carried a precondition — a fact about the request, not the account. From an absent
   * pointer those two cases are indistinguishable, and NOT_FOUND is the answer a teardown scheduler
   * is most likely to read as "done" and stop on. So the precondition is not consulted once the
   * pointer is provably absent, and the resumable sweep runs either way.
   */
  @Test
  void aConditionalDeleteOfAnAlreadyGoneAccountSweepsAndSucceeds() throws Exception {
    var created =
        tenancy.createAccount(
            CreateAccountRequest.newBuilder()
                .setSpec(AccountSpec.newBuilder().setDisplayName(accountPrefix + "twice"))
                .build());
    var id = created.getAccount().getResourceId();
    var meta = accountRepository.metaForSafe(id);

    tenancy.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(id)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(meta.getPointerVersion())
                    .setExpectedEtag(meta.getEtag()))
            .build());

    // The same conditional request again, against a pointer that is now gone: the precondition can
    // no longer be satisfied by anything, and the answer is that the account is gone.
    var again =
        tenancy.deleteAccount(
            DeleteAccountRequest.newBuilder()
                .setAccountId(id)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(meta.getPointerVersion())
                        .setExpectedEtag(meta.getEtag()))
                .build());
    assertEquals(0, again.getMeta().getPointerVersion());
  }

  /**
   * A children marker whose resource is already gone is unreachable by every identity walk, so
   * account teardown has to sweep the marker families directly.
   *
   * <p>Both deleteCatalogMarker and deleteNamespaceMarker are called by a walk that resolves the
   * resource and removes its marker alongside its pointer. A catalog missing its by-id row is not
   * enumerated by that walk, so its marker is never visited — and nothing else covers either
   * family: the residual sweep is scoped to by-id and by-name, and the namespace root, where
   * namespace markers live, has no residual sweep at all.
   */
  @Test
  void deleteAccountReclaimsMarkersWhoseResourceIsAlreadyGone() throws Exception {
    var cat = TestSupport.createCatalog(catalog, accountPrefix + "marker_cat", "");
    var ns =
        TestSupport.createNamespace(namespace, cat.getResourceId(), "db", List.of(), "marker db");

    // Give both a children marker, the way a publish into each would.
    markerStore.bumpCatalogMarker(cat.getResourceId());
    assertTrue(
        markerStore.advanceNamespaceMarker(
            ns.getResourceId(), markerStore.namespaceMarkerVersion(ns.getResourceId())));
    String catalogMarker = Keys.catalogChildrenMarker(seedAccountId, cat.getResourceId().getId());
    String namespaceMarker =
        Keys.namespaceChildrenMarker(seedAccountId, ns.getResourceId().getId());
    assertTrue(ptr.get(catalogMarker).isPresent());
    assertTrue(ptr.get(namespaceMarker).isPresent());

    // Now remove the canonical pointers out from under both, as an interrupted delete or a pointer
    // GC reaping a dangling row would: the identity walks can no longer reach either resource.
    assertTrue(ptr.delete(Keys.catalogPointerById(seedAccountId, cat.getResourceId().getId())));
    assertTrue(ptr.delete(Keys.namespacePointerById(seedAccountId, ns.getResourceId().getId())));

    var seededAccountId = seedAccountResourceId();
    tenancy.deleteAccount(DeleteAccountRequest.newBuilder().setAccountId(seededAccountId).build());

    assertTrue(
        ptr.get(catalogMarker).isEmpty(), "the catalog's marker must not outlive the account");
    assertTrue(
        ptr.get(namespaceMarker).isEmpty(), "the namespace's marker must not outlive the account");
  }

  private ResourceId seedAccountResourceId() {
    return ResourceId.newBuilder()
        .setAccountId(seedAccountId)
        .setId(seedAccountId)
        .setKind(ResourceKind.RK_ACCOUNT)
        .build();
  }
}
