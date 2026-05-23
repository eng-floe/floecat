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

import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredJobContribution;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

final class InMemoryReconcileProjectionState {
  private final Map<String, Map<String, StoredJobContribution>> contributionsByParent =
      new ConcurrentHashMap<>();
  private final Map<String, String> resultBlobByJob = new ConcurrentHashMap<>();

  List<StoredJobContribution> loadDirectContributions(String accountId, String parentJobId) {
    Map<String, StoredJobContribution> byChild =
        contributionsByParent.get(parentKey(accountId, parentJobId));
    if (byChild == null || byChild.isEmpty()) {
      return List.of();
    }
    List<StoredJobContribution> out = new ArrayList<>(byChild.values());
    out.sort(
        Comparator.comparing((StoredJobContribution c) -> blankToEmpty(c.childJobId))
            .thenComparingLong(c -> c.updatedAtMs));
    return out;
  }

  void upsertContribution(StoredJobContribution contribution) {
    contributionsByParent
        .computeIfAbsent(
            parentKey(contribution.accountId, contribution.parentJobId),
            ignored -> new ConcurrentHashMap<>())
        .put(contribution.childJobId, contribution);
  }

  String currentResultBlobUri(String accountId, String jobId) {
    return resultBlobByJob.get(jobKey(accountId, jobId));
  }

  void upsertResultBlobUri(String accountId, String jobId, String blobUri) {
    resultBlobByJob.put(jobKey(accountId, jobId), blobUri);
  }

  private static String parentKey(String accountId, String parentJobId) {
    return blankToEmpty(accountId) + "\u0000" + blankToEmpty(parentJobId);
  }

  private static String jobKey(String accountId, String jobId) {
    return blankToEmpty(accountId) + "\u0000" + blankToEmpty(jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
