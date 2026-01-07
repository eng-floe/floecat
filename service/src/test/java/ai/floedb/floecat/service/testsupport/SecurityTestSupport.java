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

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;

/** Shared context helpers for tests that need to simulate security/engine metadata. */
public final class SecurityTestSupport {

  private SecurityTestSupport() {}

  public static final class FakePrincipalProvider extends PrincipalProvider {
    private PrincipalContext ctx;

    public FakePrincipalProvider(String accountId) {
      this.ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    public void setAccount(String accountId) {
      this.ctx = PrincipalContext.newBuilder().setAccountId(accountId).build();
    }

    @Override
    public PrincipalContext get() {
      return ctx;
    }
  }
}
