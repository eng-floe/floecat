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
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.AuthModeOidcProfile;
import ai.floedb.floecat.service.it.util.TestKeyPair;
import ai.floedb.floecat.service.util.TestDataResetter;
import ai.floedb.floecat.service.util.TestSupport;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.jwt.build.Jwt;
import jakarta.inject.Inject;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthModeOidcProfile.class)
class AuthModeOidcIT {

  private static final String ACCOUNT_ID =
      TestSupport.createAccountId(TestSupport.DEFAULT_SEED_ACCOUNT).getId();

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
  void listAccountsRejectsMissingAuthorizationHeader() {
    Metadata metadata = new Metadata();

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsAcceptsAuthorizationHeader() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(AUTH_HEADER, "Bearer " + sessionJwt());

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    var response = stub.listAccounts(ListAccountsRequest.getDefaultInstance());

    assertFalse(response.getAccountsList().isEmpty());
  }

  private static String sessionJwt() throws Exception {
    var now = Instant.now();
    return Jwt.claims()
        .issuer("https://floecat.test")
        .subject("it-user")
        .claim("account_id", ACCOUNT_ID)
        .issuedAt(now)
        .expiresAt(now.plusSeconds(7L * 365 * 24 * 3600))
        .audience("floecat-client")
        .sign(TestKeyPair.privateKey());
  }
}
