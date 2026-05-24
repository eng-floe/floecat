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

package ai.floedb.floecat.service.reconciler.jobs.durable.store;

final class ProjectionBackendSupport {
  static final String KIND_CONTRIBUTION = "ReconcileJobContribution";
  static final String KIND_RESULT_REFERENCE = "ReconcileJobResultReference";
  static final String ATTR_ACCOUNT_ID = "account_id";
  static final String ATTR_PARENT_JOB_ID = "parent_job_id";
  static final String ATTR_CHILD_JOB_ID = "child_job_id";
  static final String ATTR_JOB_ID = "job_id";
  static final String ATTR_BLOB_URI = "blob_uri";

  private ProjectionBackendSupport() {}

  static String contributionPartitionKey(String accountId, String parentJobId) {
    return "reconcile-projection-contribution/"
        + blankToEmpty(accountId)
        + "/"
        + blankToEmpty(parentJobId);
  }

  static String contributionSortKey(String childJobId) {
    return "child/" + blankToEmpty(childJobId);
  }

  static String resultReferencePartitionKey(String accountId) {
    return "reconcile-projection-result/" + blankToEmpty(accountId);
  }

  static String resultReferenceSortKey(String jobId) {
    return "job/" + blankToEmpty(jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }
}
