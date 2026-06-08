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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileResult;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;

@ApplicationScoped
public class SnapshotFinalizeChildStateService {
  @Inject ReconcileJobStore jobs;

  public ChildState childState(
      String accountId,
      String parentJobId,
      String finalizerJobId,
      List<ReconcileFileGroupTask> expectedGroups) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return new ChildState(
          0, 0, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }
    LinkedHashMap<String, ReconcileJobStore.ReconcileJob> childByGroupKey = new LinkedHashMap<>();
    LinkedHashSet<String> duplicateGroups = new LinkedHashSet<>();
    for (ReconcileJobStore.ReconcileJob child : childJobs(accountId, parentJobId)) {
      if (child == null
          || child.jobId == null
          || child.jobId.equals(finalizerJobId)
          || child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
        continue;
      }
      String groupKey = groupKey(child.fileGroupTask);
      if (groupKey.isBlank()) {
        duplicateGroups.add("unkeyed-child:" + child.jobId);
        continue;
      }
      ReconcileJobStore.ReconcileJob previous = childByGroupKey.putIfAbsent(groupKey, child);
      if (previous != null) {
        duplicateGroups.add(describeGroup(child.fileGroupTask));
      }
    }
    int completedGroups = 0;
    List<ReconcileFileGroupTask> completedGroupTasks = new ArrayList<>();
    LinkedHashSet<String> pendingGroups = new LinkedHashSet<>();
    LinkedHashSet<String> failedGroups = new LinkedHashSet<>();
    LinkedHashSet<String> cancelledGroups = new LinkedHashSet<>();
    LinkedHashSet<String> missingGroups = new LinkedHashSet<>();
    LinkedHashSet<String> invalidSucceededGroups = new LinkedHashSet<>();
    List<ReconcileFileGroupTask> groups =
        expectedGroups == null ? List.<ReconcileFileGroupTask>of() : expectedGroups;
    for (ReconcileFileGroupTask expectedGroup : groups) {
      String groupKey = groupKey(expectedGroup);
      String description = describeGroup(expectedGroup);
      if (groupKey.isBlank()) {
        missingGroups.add(description);
        continue;
      }
      ReconcileJobStore.ReconcileJob child = childByGroupKey.get(groupKey);
      if (child == null) {
        missingGroups.add(description);
        continue;
      }
      if ("JS_SUCCEEDED".equals(child.state)) {
        if (hasPersistedSuccessResults(expectedGroup, child.fileGroupTask)) {
          completedGroups++;
          completedGroupTasks.add(child.fileGroupTask);
        } else {
          invalidSucceededGroups.add(description);
        }
      } else if ("JS_FAILED".equals(child.state)) {
        failedGroups.add(describeFailure(child, expectedGroup));
      } else if ("JS_CANCELLED".equals(child.state)) {
        cancelledGroups.add(describeFailure(child, expectedGroup));
      } else {
        pendingGroups.add(description + "(" + blankToUnknown(child.state) + ")");
      }
    }
    return new ChildState(
        groups.size(),
        completedGroups,
        List.copyOf(pendingGroups),
        List.copyOf(failedGroups),
        List.copyOf(cancelledGroups),
        List.copyOf(duplicateGroups),
        List.copyOf(missingGroups),
        List.copyOf(invalidSucceededGroups),
        List.copyOf(completedGroupTasks));
  }

  List<ReconcileJobStore.ReconcileJob> childJobs(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<ReconcileJobStore.ReconcileJob> out = new ArrayList<>();
    String pageToken = "";
    do {
      ReconcileJobStore.ReconcileJobPage page =
          jobs.childJobsPage(accountId, parentJobId, 200, pageToken);
      if (page == null || page.jobs == null || page.jobs.isEmpty()) {
        break;
      }
      out.addAll(page.jobs);
      pageToken = page.nextPageToken == null ? "" : page.nextPageToken;
    } while (!pageToken.isBlank());
    return List.copyOf(out);
  }

  static boolean hasPersistedSuccessResults(
      ReconcileFileGroupTask expectedGroup, ReconcileFileGroupTask persistedGroup) {
    if (expectedGroup == null || persistedGroup == null) {
      return false;
    }
    if (!groupKey(expectedGroup).equals(groupKey(persistedGroup))) {
      return false;
    }
    LinkedHashMap<String, ReconcileFileResult.State> statesByFile = new LinkedHashMap<>();
    for (ReconcileFileResult result : persistedGroup.fileResults()) {
      if (result == null || result.filePath().isBlank()) {
        continue;
      }
      statesByFile.put(result.filePath(), result.state());
    }
    for (String filePath : expectedGroup.filePaths()) {
      if (statesByFile.get(filePath) != ReconcileFileResult.State.SUCCEEDED) {
        return false;
      }
    }
    return !expectedGroup.filePaths().isEmpty() || !persistedGroup.fileResults().isEmpty();
  }

  static String groupKey(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "";
    }
    String planId = fileGroupTask.planId() == null ? "" : fileGroupTask.planId().trim();
    String groupId = fileGroupTask.groupId() == null ? "" : fileGroupTask.groupId().trim();
    if (planId.isBlank() || groupId.isBlank()) {
      return "";
    }
    return planId + "|" + groupId;
  }

  static String describeGroup(ReconcileFileGroupTask fileGroupTask) {
    if (fileGroupTask == null) {
      return "unknown-group";
    }
    String planId = fileGroupTask.planId() == null ? "" : fileGroupTask.planId().trim();
    String groupId = fileGroupTask.groupId() == null ? "" : fileGroupTask.groupId().trim();
    if (planId.isBlank() && groupId.isBlank()) {
      return "unknown-group";
    }
    return planId + "/" + groupId;
  }

  private static String describeFailure(
      ReconcileJobStore.ReconcileJob child, ReconcileFileGroupTask expectedGroup) {
    String message = child == null || child.message == null ? "" : child.message.trim();
    return message.isBlank()
        ? describeGroup(expectedGroup)
        : describeGroup(expectedGroup) + ": " + message;
  }

  private static String blankToUnknown(String value) {
    return value == null || value.isBlank() ? "unknown" : value;
  }

  public record ChildState(
      int expectedGroups,
      int completedGroups,
      List<String> pendingGroups,
      List<String> failedGroups,
      List<String> cancelledGroups,
      List<String> duplicateGroups,
      List<String> missingGroups,
      List<String> invalidSucceededGroups,
      List<ReconcileFileGroupTask> completedGroupTasks) {}
}
