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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.AuthModeDevProfile;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthModeDevProfile.class)
class AuthModeDevIT {

  private static final Metadata.Key<String> DEV_ACCOUNT_HEADER =
      Metadata.Key.of("x-floe-account", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void listAccountsAcceptsDevContext() {
    var response =
        TestSupport.callWhenGrpcReady(
            () -> accounts.listAccounts(ListAccountsRequest.getDefaultInstance()));
    assertFalse(response.getAccountsList().isEmpty());
  }

  @Test
  void listAccountsRejectsUnknownDevAccountHeader() {
    Metadata metadata = new Metadata();
    metadata.put(DEV_ACCOUNT_HEADER, "00000000-0000-0000-0000-000000000001");

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listCatalogsRejectsUnknownDevAccountHeader() {
    Metadata metadata = new Metadata();
    metadata.put(DEV_ACCOUNT_HEADER, "00000000-0000-0000-0000-000000000001");

    var stub = catalogs.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listCatalogs(ListCatalogsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }
}
