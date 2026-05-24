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

package ai.floedb.floecat.service.reconciler.jobs.durable.store.inmemory;

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobLease;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryReconcileLeaseState {
  final Map<String, StoredJobLease> leasesByJob = new ConcurrentHashMap<>();
  final Map<String, String> laneOwnerByKey = new ConcurrentHashMap<>();
  final Map<String, String> snapshotOwnerByKey = new ConcurrentHashMap<>();

  static String jobKey(String accountId, String jobId) {
    return blankToEmpty(accountId) + "\u0000" + blankToEmpty(jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
