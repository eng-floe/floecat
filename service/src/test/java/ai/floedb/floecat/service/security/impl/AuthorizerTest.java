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

package ai.floedb.floecat.service.security.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the missing-identity vs. missing-grant distinction in {@link Authorizer}
 * (eng-floe/floecat#361): only the exact default-instance principal — what every context channel
 * degrades to on a total propagation miss — is reported as UNAUTHENTICATED. Any populated
 * principal, however sparse, is a real caller and keeps getting PERMISSION_DENIED.
 */
class AuthorizerTest {

  private final Authorizer authz = new Authorizer();

  @Test
  void grantedPermissionPasses() {
    PrincipalContext principal =
        PrincipalContext.newBuilder()
            .setSubject("tester")
            .setAccountId("acct")
            .addPermissions("catalog.read")
            .build();
    assertDoesNotThrow(() -> authz.require(principal, "catalog.read"));
    assertDoesNotThrow(() -> authz.require(principal, List.of("other", "catalog.read")));
  }

  @Test
  void populatedPrincipalWithoutGrantIsPermissionDenied() {
    PrincipalContext principal =
        PrincipalContext.newBuilder().setSubject("tester").setAccountId("acct").build();
    StatusRuntimeException e =
        assertThrows(StatusRuntimeException.class, () -> authz.require(principal, "catalog.read"));
    assertEquals(Status.Code.PERMISSION_DENIED, e.getStatus().getCode());
  }

  /**
   * The drift guard from review: a future auth mode could legitimately produce a principal with a
   * blank subject and zero permissions. As long as it carries anything at all (here a correlation
   * id), it is a real — denied — caller, not a lost context.
   */
  @Test
  void blankSubjectNoPermissionsButOtherwisePopulatedIsStillPermissionDenied() {
    PrincipalContext principal = PrincipalContext.newBuilder().setCorrelationId("corr-1").build();
    StatusRuntimeException e =
        assertThrows(StatusRuntimeException.class, () -> authz.require(principal, "catalog.read"));
    assertEquals(Status.Code.PERMISSION_DENIED, e.getStatus().getCode());
  }

  /** The default instance is what a lost call context degrades to — reported as UNAUTHENTICATED. */
  @Test
  void defaultInstancePrincipalIsUnauthenticated() {
    StatusRuntimeException single =
        assertThrows(
            StatusRuntimeException.class,
            () -> authz.require(PrincipalContext.getDefaultInstance(), "catalog.read"));
    assertEquals(Status.Code.UNAUTHENTICATED, single.getStatus().getCode());

    StatusRuntimeException multi =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                authz.require(
                    PrincipalContext.getDefaultInstance(), List.of("catalog.read", "other")));
    assertEquals(Status.Code.UNAUTHENTICATED, multi.getStatus().getCode());
  }
}
