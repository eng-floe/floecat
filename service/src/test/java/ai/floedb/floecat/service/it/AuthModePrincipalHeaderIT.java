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
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.AuthModePrincipalHeaderProfile;
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
@TestProfile(AuthModePrincipalHeaderProfile.class)
class AuthModePrincipalHeaderIT {

  private static final Metadata.Key<byte[]> PRINCIPAL_BIN =
      Metadata.Key.of("x-principal-bin", Metadata.BINARY_BYTE_MARSHALLER);
  private static final Metadata.Key<String> AUTH_HEADER =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
  }

  @Test
  void listAccountsAcceptsPrincipalHeader() {
    Metadata metadata = new Metadata();
    metadata.put(PRINCIPAL_BIN, principal().toByteArray());

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    var response = stub.listAccounts(ListAccountsRequest.getDefaultInstance());

    assertFalse(response.getAccountsList().isEmpty());
  }

  @Test
  void listAccountsRejectsAuthorizationHeaderWhenPrincipalRequired() {
    Metadata metadata = new Metadata();
    metadata.put(AUTH_HEADER, "Bearer not-a-jwt");

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  private static PrincipalContext principal() {
    ResourceId accountId = TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT);
    return PrincipalContext.newBuilder()
        .setAccountId(accountId.getId())
        .setSubject("it-user")
        .addPermissions("account.read")
        .build();
  }
}
