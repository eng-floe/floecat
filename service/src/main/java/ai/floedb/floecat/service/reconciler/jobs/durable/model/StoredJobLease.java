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

package ai.floedb.floecat.service.reconciler.jobs.durable.model;

public class StoredJobLease {
  public String accountId;
  public String jobId;
  public String epoch;
  public long expiresAtMs;

  public static StoredJobLease empty(String accountId, String jobId) {
    StoredJobLease lease = new StoredJobLease();
    lease.accountId = accountId;
    lease.jobId = jobId;
    lease.epoch = "";
    lease.expiresAtMs = 0L;
    return lease;
  }

  public static StoredJobLease active(
      String accountId, String jobId, String epoch, long expiresAtMs) {
    StoredJobLease lease = empty(accountId, jobId);
    lease.epoch = epoch == null ? "" : epoch;
    lease.expiresAtMs = expiresAtMs;
    return lease;
  }
}
