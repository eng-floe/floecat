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

/**
 * Whether this process may work on an account. Every caller that asks "should I do this for this
 * account?" — a mutation or snapshot resolution — asks it here rather than reading ownership state
 * directly.
 *
 * <p>The default implementation is process-local and serves every account. Managed deployments bind
 * their routing-backed account policy here without changing query, cache, mutation or GC code.
 */
public interface AccountScope {

  /** Admits one snapshot resolution; released when the resolution is complete or abandoned. */
  Permit admitResolution(String accountId);
}
