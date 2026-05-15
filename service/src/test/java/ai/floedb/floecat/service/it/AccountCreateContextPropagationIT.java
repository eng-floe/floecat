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

import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
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
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(OidcSessionHeaderProfile.class)
class AccountCreateContextPropagationIT {

  private static final Metadata.Key<String> SESSION_HEADER =
      Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> CORRELATION_HEADER =
      Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
  private static final int DEFAULT_ATTEMPTS = 250;

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @Inject AccountRepository accountRepository;
  @Inject TestDataResetter resetter;
  @Inject SeedRunner seeder;

  private String accountId;

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
  void repeatedCreateAccountWithValidSessionJwtDoesNotLosePrincipalContext() throws Exception {
    int attempts = Integer.getInteger("floecat.test.create-account.max-attempts", DEFAULT_ATTEMPTS);
    String jwt = initAccountJwt();
    String runId = "ctx-prop-" + UUID.randomUUID();
    int successes = 0;

    for (int attempt = 1; attempt <= attempts; attempt++) {
      String correlationId = runId + "-attempt-" + attempt;
      Metadata metadata = new Metadata();
      metadata.put(SESSION_HEADER, jwt);
      metadata.put(CORRELATION_HEADER, correlationId);
      var stub = accounts.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

      String displayName = "ctx-prop-account-" + correlationId;
      try {
        stub.createAccount(
            CreateAccountRequest.newBuilder()
                .setSpec(
                    AccountSpec.newBuilder()
                        .setDisplayName(displayName)
                        .setDescription("attempt-" + attempt)
                        .build())
                .build());
        successes++;
      } catch (StatusRuntimeException e) {
        FailureClassification failure = classifyFailure(e);
        if (failure.contextPropagationFailure()) {
          fail(
              "Context propagation failure on createAccount attempt "
                  + attempt
                  + "/"
                  + attempts
                  + " after "
                  + successes
                  + " successes"
                  + ": correlationId="
                  + correlationId
                  + " code="
                  + e.getStatus().getCode()
                  + " description="
                  + e.getStatus().getDescription()
                  + " evidence="
                  + failure.evidence(),
              e);
        }
        throw new AssertionError(
            "Inconclusive createAccount failure on attempt "
                + attempt
                + "/"
                + attempts
                + " after "
                + successes
                + " successes"
                + ": correlationId="
                + correlationId
                + " code="
                + e.getStatus().getCode()
                + " description="
                + e.getStatus().getDescription()
                + " evidence="
                + failure.evidence(),
            e);
      }
    }

    System.out.println(
        "Completed "
            + successes
            + "/"
            + attempts
            + " createAccount attempts without reproducing principal-context loss.");
  }

  private String initAccountJwt() throws Exception {
    var now = Instant.now();
    return Jwt.claims()
        .issuer("https://floecat.test")
        .subject("it-init-account")
        .claim("account_id", accountId)
        .claim("roles", java.util.List.of("init-account"))
        .issuedAt(now)
        .expiresAt(now.plusSeconds(7L * 365 * 24 * 3600))
        .audience("floecat-client")
        .sign(TestKeyPair.privateKey());
  }

  private static FailureClassification classifyFailure(StatusRuntimeException exception) {
    Status.Code code = exception.getStatus().getCode();
    String description = exception.getStatus().getDescription();

    if (code == Status.Code.PERMISSION_DENIED) {
      return new FailureClassification(true, "grpc-code=PERMISSION_DENIED");
    }
    if (code == Status.Code.UNKNOWN
        && description != null
        && description.contains("createAccount principal context missing required fields")) {
      return new FailureClassification(
          true, "grpc-description=createAccount principal context missing required fields");
    }

    return new FailureClassification(false, "no-context-propagation-signature");
  }

  private record FailureClassification(boolean contextPropagationFailure, String evidence) {}
}
