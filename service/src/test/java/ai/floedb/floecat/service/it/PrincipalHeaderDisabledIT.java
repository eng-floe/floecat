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
import ai.floedb.floecat.service.it.profiles.PrincipalHeaderDisabledProfile;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PrincipalHeaderDisabledProfile.class)
class PrincipalHeaderDisabledIT {

  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @Test
  void listAccountsRejectsMissingPrincipalHeaderWithExplicitMessage() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> accounts.listAccounts(ListAccountsRequest.getDefaultInstance()));

    assertEquals(Status.Code.UNAUTHENTICATED, ex.getStatus().getCode());
    assertEquals(
        "missing session header and x-principal-bin disabled", ex.getStatus().getDescription());
  }
}
