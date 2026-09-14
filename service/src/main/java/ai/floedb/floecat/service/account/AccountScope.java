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
 * <p>The answer is a decision, never a computation: it does not depend on where the process is
 * deployed, how many replicas exist, or how accounts hash onto them. {@link AccountAssignment}
 * answers it from the assignment Core pushed and is the only implementation; a deployment that
 * decided differently would bind its own without touching a single caller.
 *
 * <p>Permission only. How a write is then held to the account — the store fence and the version it
 * carries — is Floecat's own business and stays off this interface, so an implementation decides
 * who serves what without also having to maintain the mechanism that enforces it. The permit is the
 * index's, so a caller holding one holds the same thing whichever seam handed it over.
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
