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

import ai.floedb.floecat.catalog.rpc.*;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class CatalogMutationIT {
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  String catalogPrefix = this.getClass().getSimpleName() + "_";

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;
  @Inject IdempotencyRepository idempotencyStore;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void createMissingDisplayName() throws Exception {
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(CatalogSpec.newBuilder().setDescription("x"))
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "Invalid value for display_name");
  }

  @Test
  void createSameNameReturnsExistingWithoutIdempotency() throws Exception {
    TestSupport.createCatalog(catalog, catalogPrefix + "same_name", "d1");
    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(
                            CatalogSpec.newBuilder()
                                .setDisplayName(catalogPrefix + "same_name")
                                .setDescription("d1")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, "already exists");
  }

  @Test
  void updateInvalidMaskPath() throws Exception {
    var c = TestSupport.createCatalog(catalog, catalogPrefix + "upd_mask_bad", "desc");
    var badMask = FieldMask.newBuilder().addPaths("not_a_real_field").build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(c.getResourceId())
                        .setSpec(CatalogSpec.newBuilder().build())
                        .setUpdateMask(badMask)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex,
        Status.Code.INVALID_ARGUMENT,
        ErrorCode.MC_INVALID_ARGUMENT,
        "Update field mask is invalid: not_a_real_field");
  }

  @Test
  void updateWrongKindOnCatalogId() throws Exception {
    var c = TestSupport.createCatalog(catalog, catalogPrefix + "upd_kind", "desc");

    var wrongKind =
        ResourceId.newBuilder()
            .setAccountId(c.getResourceId().getAccountId())
            .setId(c.getResourceId().getId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(wrongKind)
                        .setSpec(CatalogSpec.newBuilder().build())
                        .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.INVALID_ARGUMENT, ErrorCode.MC_INVALID_ARGUMENT, "kind");
  }

  @Test
  void noopUpdateWithCorrectPreconditionsSucceeds() {
    var resp =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder()
                .setSpec(
                    CatalogSpec.newBuilder()
                        .setDisplayName("noop_ok")
                        .setDescription("desc")
                        .build())
                .build());
    var metaBefore = resp.getMeta();
    var c = resp.getCatalog();

    var mask = FieldMask.newBuilder().addPaths("display_name").build();

    catalog.updateCatalog(
        UpdateCatalogRequest.newBuilder()
            .setCatalogId(c.getResourceId())
            .setSpec(CatalogSpec.newBuilder().setDisplayName(c.getDisplayName()).build())
            .setUpdateMask(mask)
            .setPrecondition(
                Precondition.newBuilder()
                    .setExpectedVersion(metaBefore.getPointerVersion())
                    .setExpectedEtag(metaBefore.getEtag())
                    .build())
            .build());

    assertEquals(metaBefore.getPointerVersion(), resp.getMeta().getPointerVersion());
    assertEquals(metaBefore.getEtag(), resp.getMeta().getEtag());
    assertEquals(c.getDisplayName(), resp.getCatalog().getDisplayName());
  }

  @Test
  void CatalogIdempotentCreateReturnsSameResult() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-1").build();
    var spec =
        CatalogSpec.newBuilder()
            .setDisplayName(catalogPrefix + "cat1")
            .setDescription("cat1")
            .build();
    var first =
        catalog
            .createCatalog(
                CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build())
            .getCatalog();
    var second =
        catalog
            .createCatalog(
                CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build())
            .getCatalog();

    assertEquals(first.getResourceId(), second.getResourceId());
  }

  @Test
  void CatalogInvalidId() throws Exception {
    var badId =
        ResourceId.newBuilder()
            .setAccountId(TestSupport.DEFAULT_SEED_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId("00000000-0000-0000-0000-000000000001")
            .build();

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> catalog.getCatalog(GetCatalogRequest.newBuilder().setCatalogId(badId).build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }

  @Test
  void CatalogExistsConflictWhenDifferentSpec() throws Exception {
    TestSupport.createCatalog(catalog, catalogPrefix + "cat1", "cat1");

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> TestSupport.createCatalog(catalog, catalogPrefix + "cat1", "cat1 catalog"));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ALREADY_EXISTS, ErrorCode.MC_CONFLICT, "already exists");
  }

  @Test
  void catalogCreateUpdateDelete() throws Exception {
    var c1 = TestSupport.createCatalog(catalog, catalogPrefix + "cat_pre", "desc");
    var id = c1.getResourceId();

    assertEquals(ResourceKind.RK_CATALOG, id.getKind());
    assertEquals(c1.getResourceId().getAccountId(), id.getAccountId());
    assertTrue(id.getId().matches("^[0-9a-fA-F-]{36}$"), "id must look like UUID");

    FieldMask mask_name = FieldMask.newBuilder().addPaths("display_name").build();
    var m1 =
        catalog
            .updateCatalog(
                UpdateCatalogRequest.newBuilder()
                    .setCatalogId(id)
                    .setSpec(
                        CatalogSpec.newBuilder()
                            .setDisplayName(catalogPrefix + "cat_pre")
                            .setDescription("desc")
                            .build())
                    .setUpdateMask(mask_name)
                    .build())
            .getMeta();

    var resolved =
        directory.resolveCatalog(
            ResolveCatalogRequest.newBuilder()
                .setRef(NameRef.newBuilder().setCatalog(catalogPrefix + "cat_pre"))
                .build());
    assertEquals(id.getId(), resolved.getResourceId().getId());

    var spec2 =
        CatalogSpec.newBuilder()
            .setDisplayName(catalogPrefix + "cat_pre_2")
            .setDescription("desc2")
            .build();
    var updOk =
        catalog.updateCatalog(
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setSpec(spec2)
                .setUpdateMask(mask_name)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m1.getPointerVersion())
                        .setExpectedEtag(m1.getEtag())
                        .build())
                .build());
    assertEquals(catalogPrefix + "cat_pre_2", updOk.getCatalog().getDisplayName());
    assertTrue(updOk.getMeta().getPointerVersion() > m1.getPointerVersion());

    var bad =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.updateCatalog(
                    UpdateCatalogRequest.newBuilder()
                        .setCatalogId(id)
                        .setSpec(
                            CatalogSpec.newBuilder().setDisplayName(catalogPrefix + "cat_pre_3"))
                        .setUpdateMask(mask_name)
                        .setPrecondition(
                            Precondition.newBuilder()
                                .setExpectedVersion(123456L)
                                .setExpectedEtag("bogus")
                                .build())
                        .build()));
    TestSupport.assertGrpcAndMc(
        bad, Status.Code.FAILED_PRECONDITION, ErrorCode.MC_PRECONDITION_FAILED, "mismatch");

    var m2 = updOk.getMeta();
    var delOk =
        catalog.deleteCatalog(
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(id)
                .setRequireEmpty(true)
                .setPrecondition(
                    Precondition.newBuilder()
                        .setExpectedVersion(m2.getPointerVersion())
                        .setExpectedEtag(m2.getEtag())
                        .build())
                .build());
    assertEquals(m2.getPointerKey(), delOk.getMeta().getPointerKey());

    var notFound =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                directory.resolveCatalog(
                    ResolveCatalogRequest.newBuilder()
                        .setRef(NameRef.newBuilder().setCatalog(catalogPrefix + "cat_pre_2"))
                        .build()));
    TestSupport.assertGrpcAndMc(
        notFound, Status.Code.NOT_FOUND, ErrorCode.MC_NOT_FOUND, "Catalog not found");
  }

  @Test
  void catalogCreateIdempotent() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-1").build();
    var spec =
        CatalogSpec.newBuilder()
            .setDisplayName(catalogPrefix + "idem_cat")
            .setDescription("x")
            .build();

    var r1 =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build());
    var r2 =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(key).build());

    assertEquals(r1.getCatalog().getResourceId().getId(), r2.getCatalog().getResourceId().getId());
    assertEquals(r1.getMeta().getPointerKey(), r2.getMeta().getPointerKey());
    assertEquals(r1.getMeta().getPointerVersion(), r2.getMeta().getPointerVersion());
    assertEquals(r1.getMeta().getEtag(), r2.getMeta().getEtag());
  }

  @Test
  void catalogCreateIdempotencyMismatch() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-2").build();

    CreateCatalogResponse ccr =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder()
                .setSpec(
                    CatalogSpec.newBuilder().setDisplayName(catalogPrefix + "idem_cat2").build())
                .setIdempotency(key)
                .build());

    ResourceId catId = ccr.getCatalog().getResourceId();
    awaitIdemVisible(catId.getAccountId(), "CreateCatalog", key.getKey(), Duration.ofSeconds(2));

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(
                            CatalogSpec.newBuilder()
                                .setDisplayName(catalogPrefix + "idem_cat2_DIFFERENT")
                                .build())
                        .setIdempotency(key)
                        .build()));
    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  @Test
  void catalogCreateIdempotencyMismatchOnDescription() throws Exception {
    var key = IdempotencyKey.newBuilder().setKey(catalogPrefix + "k-cat-3").build();

    CreateCatalogResponse ccr =
        catalog.createCatalog(
            CreateCatalogRequest.newBuilder()
                .setSpec(
                    CatalogSpec.newBuilder()
                        .setDisplayName(catalogPrefix + "idem_cat3")
                        .setDescription("desc-a")
                        .build())
                .setIdempotency(key)
                .build());

    ResourceId catId = ccr.getCatalog().getResourceId();
    awaitIdemVisible(catId.getAccountId(), "CreateCatalog", key.getKey(), Duration.ofSeconds(2));

    var ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                catalog.createCatalog(
                    CreateCatalogRequest.newBuilder()
                        .setSpec(
                            CatalogSpec.newBuilder()
                                .setDisplayName(catalogPrefix + "idem_cat3")
                                .setDescription("desc-b")
                                .build())
                        .setIdempotency(key)
                        .build()));

    TestSupport.assertGrpcAndMc(
        ex, Status.Code.ABORTED, ErrorCode.MC_CONFLICT, "Idempotency key mismatch");
  }

  private void awaitIdemVisible(String account, String op, String key, Duration timeout)
      throws InterruptedException {
    long until = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < until) {
      var recOpt = idempotencyStore.get(Keys.idempotencyKey(account, op, key));
      if (recOpt.isPresent() && recOpt.get().getStatus() == IdempotencyRecord.Status.SUCCEEDED) {
        return;
      }
      Thread.sleep(25);
    }
    fail("Idempotency record not visible in time");
  }
}
