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
import ai.floedb.floecat.service.it.profiles.OidcSessionHeaderProfile;
import ai.floedb.floecat.service.it.util.TestKeyPair;
import ai.floedb.floecat.service.repo.impl.AccountRepository;
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
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OidcSessionHeaderProfile.class)
class AccountSessionHeaderIT {

  private static final Metadata.Key<String> SESSION_HEADER =
      Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER);

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  private String accountId;

  @Inject AccountRepository accountRepository;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  @BeforeEach
  void resetStores() {
    resetter.wipeAll();
    seeder.seedData();
    accountId =
        accountRepository
            .getByName(TestSupport.DEFAULT_SEED_ACCOUNT)
            .orElseThrow()
            .getResourceId()
            .getId();
  }

  @Test
  void listAccountsAcceptsSessionHeaderJwt() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, sessionJwt());

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    var response = stub.listAccounts(ListAccountsRequest.getDefaultInstance());

    assertFalse(response.getAccountsList().isEmpty());
  }

  @Test
  void listAccountsRejectsMissingSessionHeader() {
    Metadata metadata = new Metadata();

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsWrongSessionHeaderName() {
    Metadata metadata = new Metadata();
    metadata.put(
        Metadata.Key.of("x-floe-session-typo", Metadata.ASCII_STRING_MARSHALLER),
        "eyJhbGciOiJIUzI1NiJ9");

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsAcceptsMissingPrincipalHeader() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, sessionJwt());

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    var response = stub.listAccounts(ListAccountsRequest.getDefaultInstance());

    assertFalse(response.getAccountsList().isEmpty());
  }

  @Test
  void listAccountsRejectsMalformedJwt() {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, "not-a-jwt");

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsMissingAudience() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, sessionJwtWithKeyAndAudience(TestKeyPair.privateKey(), null));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsInvalidSignature() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(SESSION_HEADER, sessionJwtWithKey(generatePrivateKey()));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsWrongAudience() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(
        SESSION_HEADER, sessionJwtWithKeyAndAudience(TestKeyPair.privateKey(), "wrong-audience"));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsInvalidExpiration() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(
        SESSION_HEADER, sessionJwtWithKeyAndTiming(TestKeyPair.privateKey(), -300, -600, 0));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsExpiredToken() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(
        SESSION_HEADER, sessionJwtWithKeyAndTiming(TestKeyPair.privateKey(), -3600, -300, 0));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  @Test
  void listAccountsRejectsTokenNotYetValid() throws Exception {
    Metadata metadata = new Metadata();
    metadata.put(
        SESSION_HEADER, sessionJwtWithKeyAndTiming(TestKeyPair.privateKey(), 0, 3600, 300));

    var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> stub.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
  }

  private String sessionJwt() throws Exception {
    return sessionJwtWithKey(TestKeyPair.privateKey());
  }

  private String sessionJwtWithKey(PrivateKey privateKey) throws Exception {
    return sessionJwtWithKeyAndAudience(privateKey, "floecat-client");
  }

  private String sessionJwtWithKeyAndAudience(PrivateKey privateKey, String audience)
      throws Exception {
    return sessionJwtWithKeyAndTiming(privateKey, audience, 0, 7L * 365 * 24 * 3600, 0);
  }

  private String sessionJwtWithKeyAndTiming(
      PrivateKey privateKey,
      long issuedAtOffsetSeconds,
      long expiresInSeconds,
      long notBeforeOffset)
      throws Exception {
    return sessionJwtWithKeyAndTiming(
        privateKey, "floecat-client", issuedAtOffsetSeconds, expiresInSeconds, notBeforeOffset);
  }

  private String sessionJwtWithKeyAndTiming(
      PrivateKey privateKey,
      String audience,
      long issuedAtOffsetSeconds,
      long expiresInSeconds,
      long notBeforeOffset)
      throws Exception {
    var now = Instant.now();
    var builder =
        Jwt.claims()
            .issuer("https://floecat.test")
            .subject("it-user")
            .claim("account_id", accountId)
            .issuedAt(now.plusSeconds(issuedAtOffsetSeconds))
            .expiresAt(now.plusSeconds(expiresInSeconds));
    if (audience != null) {
      builder.audience(audience);
    }
    if (notBeforeOffset != 0) {
      builder.claim("nbf", now.plusSeconds(notBeforeOffset).getEpochSecond());
    }
    return builder.sign(privateKey);
  }

  private static PrivateKey generatePrivateKey() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    return generator.generateKeyPair().getPrivate();
  }
}
