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

import java.util.List;
import java.util.Optional;

public interface ReconcileProjectionBackend {
  sealed interface ProjectionWriteOp permits ContributionUpsert, ResultReferenceUpsert {}

  record ContributionUpsert(
      String accountId, String parentJobId, String childJobId, long expectedVersion, String blobUri)
      implements ProjectionWriteOp {}

  record ResultReferenceUpsert(String accountId, String jobId, long expectedVersion, String blobUri)
      implements ProjectionWriteOp {}

  record ProjectionWriteBatch(List<ProjectionWriteOp> writes) {}

  record ContributionSnapshot(
      String accountId, String parentJobId, String childJobId, String blobUri, long version) {}

  record ResultReferenceSnapshot(String accountId, String jobId, String blobUri, long version) {}

  Optional<ContributionSnapshot> loadContribution(
      String accountId, String parentJobId, String childJobId);

  List<ContributionSnapshot> listContributions(String accountId, String parentJobId);

  List<ContributionSnapshot> listContributionsForChild(String accountId, String childJobId);

  Optional<ResultReferenceSnapshot> loadResultReference(String accountId, String jobId);

  boolean deleteContribution(String accountId, String parentJobId, String childJobId);

  boolean deleteResultReference(String accountId, String jobId);

  boolean compareAndSetBatch(ProjectionWriteBatch batch);
}
