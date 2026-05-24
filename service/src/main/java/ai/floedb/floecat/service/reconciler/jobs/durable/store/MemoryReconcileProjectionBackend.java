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

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@IfBuildProperty(name = "floecat.kv", stringValue = "memory")
public class MemoryReconcileProjectionBackend implements ReconcileProjectionBackend {
  private final Map<String, ContributionSnapshot> contributions = new ConcurrentHashMap<>();
  private final Map<String, ResultReferenceSnapshot> resultReferences = new ConcurrentHashMap<>();

  @Inject
  public MemoryReconcileProjectionBackend() {}

  public void bind() {}

  @Override
  public Optional<ContributionSnapshot> loadContribution(
      String accountId, String parentJobId, String childJobId) {
    return Optional.ofNullable(
        contributions.get(contributionKey(accountId, parentJobId, childJobId)));
  }

  @Override
  public List<ContributionSnapshot> listContributions(String accountId, String parentJobId) {
    String prefix = contributionPrefix(accountId, parentJobId);
    List<ContributionSnapshot> out = new ArrayList<>();
    for (var entry : contributions.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        out.add(entry.getValue());
      }
    }
    out.sort(
        Comparator.comparing(ContributionSnapshot::childJobId)
            .thenComparingLong(ContributionSnapshot::version));
    return out;
  }

  @Override
  public List<ContributionSnapshot> listContributionsForChild(String accountId, String childJobId) {
    List<ContributionSnapshot> out = new ArrayList<>();
    for (var snapshot : contributions.values()) {
      if (blankToEmpty(accountId).equals(blankToEmpty(snapshot.accountId()))
          && blankToEmpty(childJobId).equals(blankToEmpty(snapshot.childJobId()))) {
        out.add(snapshot);
      }
    }
    out.sort(
        Comparator.comparing(ContributionSnapshot::parentJobId)
            .thenComparing(ContributionSnapshot::childJobId)
            .thenComparingLong(ContributionSnapshot::version));
    return out;
  }

  @Override
  public Optional<ResultReferenceSnapshot> loadResultReference(String accountId, String jobId) {
    return Optional.ofNullable(resultReferences.get(resultKey(accountId, jobId)));
  }

  @Override
  public boolean deleteContribution(String accountId, String parentJobId, String childJobId) {
    synchronized (this) {
      return contributions.remove(contributionKey(accountId, parentJobId, childJobId)) != null;
    }
  }

  @Override
  public boolean deleteResultReference(String accountId, String jobId) {
    synchronized (this) {
      return resultReferences.remove(resultKey(accountId, jobId)) != null;
    }
  }

  @Override
  public boolean compareAndSetBatch(ProjectionWriteBatch batch) {
    if (batch == null || batch.writes().isEmpty()) {
      return true;
    }
    synchronized (this) {
      for (ProjectionWriteOp write : batch.writes()) {
        if (write instanceof ContributionUpsert upsert) {
          ContributionSnapshot existing =
              contributions.get(
                  contributionKey(upsert.accountId(), upsert.parentJobId(), upsert.childJobId()));
          long actual = existing == null ? 0L : existing.version();
          if (actual != upsert.expectedVersion()) {
            return false;
          }
        } else if (write instanceof ResultReferenceUpsert upsert) {
          ResultReferenceSnapshot existing =
              resultReferences.get(resultKey(upsert.accountId(), upsert.jobId()));
          long actual = existing == null ? 0L : existing.version();
          if (actual != upsert.expectedVersion()) {
            return false;
          }
        }
      }
      for (ProjectionWriteOp write : batch.writes()) {
        if (write instanceof ContributionUpsert upsert) {
          contributions.put(
              contributionKey(upsert.accountId(), upsert.parentJobId(), upsert.childJobId()),
              new ContributionSnapshot(
                  upsert.accountId(),
                  upsert.parentJobId(),
                  upsert.childJobId(),
                  upsert.blobUri(),
                  upsert.expectedVersion() + 1L));
        } else if (write instanceof ResultReferenceUpsert upsert) {
          resultReferences.put(
              resultKey(upsert.accountId(), upsert.jobId()),
              new ResultReferenceSnapshot(
                  upsert.accountId(),
                  upsert.jobId(),
                  upsert.blobUri(),
                  upsert.expectedVersion() + 1L));
        }
      }
      return true;
    }
  }

  private static String contributionPrefix(String accountId, String parentJobId) {
    return blankToEmpty(accountId) + "\u0000" + blankToEmpty(parentJobId) + "\u0000";
  }

  private static String contributionKey(String accountId, String parentJobId, String childJobId) {
    return contributionPrefix(accountId, parentJobId) + blankToEmpty(childJobId);
  }

  private static String resultKey(String accountId, String jobId) {
    return blankToEmpty(accountId) + "\u0000" + blankToEmpty(jobId);
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
