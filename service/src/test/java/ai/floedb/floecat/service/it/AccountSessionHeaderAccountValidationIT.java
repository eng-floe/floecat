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
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.OidcSessionHeaderValidateAccountProfile;
import ai.floedb.floecat.service.it.util.TestKeyPair;
import ai.floedb.floecat.service.util.TestDataResetter;
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
@TestProfile(OidcSessionHeaderValidateAccountProfile.class)
class AccountSessionHeaderAccountValidationIT {

  private static final Metadata.Key<String> SESSION_HEADER =
      Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER);

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
  void listAccountsRejectsUnknownAccountId() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, sessionJwtForAccount("account-does-not-exist"));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  private static String sessionJwtForAccount(String accountId) throws Exception {
    var now = Instant.now();
    return Jwt.claims()
        .issuer("https://floecat.test")
        .subject("it-user")
        .audience("floecat-client")
        .claim("account_id", accountId)
        .issuedAt(now)
        .expiresAt(now.plusSeconds(3600))
        .sign(TestKeyPair.privateKey());
  }
}
