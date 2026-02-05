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

import static org.junit.jupiter.api.Assertions.assertFalse;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.service.bootstrap.impl.SeedRunner;
import ai.floedb.floecat.service.it.profiles.AuthModeDevProfile;
import ai.floedb.floecat.service.util.TestDataResetter;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthModeDevProfile.class)
class AuthModeDevIT {

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
  void listAccountsAcceptsDevContext() {
    var response = accounts.listAccounts(ListAccountsRequest.getDefaultInstance());
    assertFalse(response.getAccountsList().isEmpty());
  }
}
