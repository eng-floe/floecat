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

package ai.floedb.floecat.service.account;

import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/**
 * Standalone account scope.
 *
 * <p>Standalone Floecat owns every account. In Floe-managed deployments, Core/runtime controls
 * routing and lifecycle; Floecat owns the account-scoped admission seam. Snapshot retention, rather
 * than query snapshot selections or process-local state, is the GC safety mechanism.
 */
@ApplicationScoped
public class AccountAssignment implements AccountScope, PlanningPointerIndex.Ownership {

  @Inject
  public AccountAssignment() {}

  public static AccountAssignment forTesting() {
    return new AccountAssignment();
  }

  /**
   * Pointer ownership is the single mutation seam. Managed deployments replace this bean with the
   * routing-backed implementation supplied by floecat-runtime; standalone Floecat owns every
   * account and therefore needs no assignment state.
   */
  @Override
  public Optional<PlanningPointerIndex.Ownership.Permit> acquire(String accountId, Access access) {
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(PlanningPointerIndex.Ownership.Permit.NOOP);
  }

  @Override
  public PlanningPointerIndex.Ownership.Permit admitResolution(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("Account is not admitted: " + accountId);
    }
    return PlanningPointerIndex.Ownership.Permit.NOOP;
  }

  @Override
  public Optional<GcPermit> tryAcquireGc(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    return Optional.of(new StandaloneGcPermit(accountId));
  }

  private static final class StandaloneGcPermit implements GcPermit {
    private final String accountId;

    private StandaloneGcPermit(String accountId) {
      this.accountId = accountId;
    }

    @Override
    public String accountId() {
      return accountId;
    }

    @Override
    public long generation() {
      return 0L;
    }

    @Override
    public boolean valid() {
      return true;
    }

    @Override
    public void close() {}
  }
}
