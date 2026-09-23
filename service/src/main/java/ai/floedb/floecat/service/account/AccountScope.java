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

import ai.floedb.floecat.service.repo.cache.PlanningPointerIndex.Ownership.Permit;
import java.util.Optional;

/**
 * Whether this process may work on an account, and under what permit. Every caller that asks
 * "should I do this for this account?" — a mutation, a pin resolution, a collection pass — asks it
 * here rather than reading assignment state directly.
 *
 * <p>The default implementation is process-local and serves every account until lifecycle drain. A
 * deployment that needs leases, fences or a coordinator binds its own implementation here without
 * changing query, cache, mutation or GC code.
 */
public interface AccountScope {

  /** Admits one account mutation; refused unless this process serves the account. */
  Permit admitMutation(String accountId);

  /** Admits one pin resolution; released when the pin is rooted or the resolution is abandoned. */
  Permit admitResolution(String accountId);

  /** A collection permit, empty unless the account is served and collection is allowed for it. */
  Optional<GcPermit> tryAcquireGc(String accountId);

  /** A permit that can be taken away mid-pass, so a long collection has to keep re-checking it. */
  interface GcPermit extends Permit {
    String accountId();

    /** Changes whenever this process's ownership of the account starts or ends. */
    long generation();

    boolean valid();

    default void requireValid() {
      if (!valid()) {
        throw new GcPermitRevokedException(accountId());
      }
    }
  }

  /** Control-flow signal: ownership of the account ended while a collector held its permit. */
  final class GcPermitRevokedException extends RuntimeException {
    private final String accountId;

    public GcPermitRevokedException(String accountId) {
      super("GC permit revoked for account " + accountId);
      this.accountId = accountId;
    }

    public String accountId() {
      return accountId;
    }
  }
}
