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
package ai.floedb.floecat.service.testsupport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;

/** Shared principal/authorizer stubbing for service-impl unit tests. */
public final class TestPrincipals {

  public static final String ACCOUNT_ID = "acct";
  public static final String CORRELATION_ID = "corr";

  private TestPrincipals() {}

  /**
   * Wires the standard test principal onto the given mocks: correlation id {@code "corr"}, account
   * {@code "acct"}, and an authorizer that permits everything. Returns the principal context for
   * tests that stub more on it.
   */
  public static PrincipalContext stubPrincipal(PrincipalProvider principal, Authorizer authz) {
    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn(CORRELATION_ID);
    when(pc.getAccountId()).thenReturn(ACCOUNT_ID);
    doNothing().when(authz).require(any(), anyString());
    return pc;
  }
}
