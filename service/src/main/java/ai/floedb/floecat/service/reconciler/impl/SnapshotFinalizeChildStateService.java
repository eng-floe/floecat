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

import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
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

  public ChildState compactChildState(
      String accountId, String parentJobId, String finalizerJobId, int expectedGroupCount) {
    LinkedHashMap<String, ReconcileFileGroupResultDescriptor> descriptorByGroupKey =
        new LinkedHashMap<>();
    List<ReconcileJobStore.ChildJobState> childStates = childStates(accountId, parentJobId);
    for (ReconcileJobStore.ChildJobState childState : childStates) {
      ReconcileFileGroupResultDescriptor descriptor = childState.fileGroupResultDescriptor();
      if (descriptor == null) {
        continue;
      }
      String key = groupKey(descriptor);
      if (!key.isBlank() && descriptorByGroupKey.putIfAbsent(key, descriptor) != null) {
        return duplicateDescriptorState(expectedGroupCount, descriptor);
      }
    }
    int completed = 0;
    int childCount = 0;
    List<String> pending = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    List<String> cancelled = new ArrayList<>();
    List<String> duplicates = new ArrayList<>();
    List<String> invalid = new ArrayList<>();
    List<ReconcileFileGroupResultDescriptor> completedDescriptors = new ArrayList<>();
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    for (ReconcileJobStore.ChildJobState childState : childStates) {
      ReconcileJobStore.ReconcileJob child = childState.job();
      if (child == null
          || child.jobId == null
          || child.jobId.equals(finalizerJobId)
          || child.jobKind != ReconcileJobKind.EXEC_FILE_GROUP) {
        continue;
      }
      childCount++;
      String key = groupKey(child.fileGroupTask);
      String description = describeGroup(child.fileGroupTask);
      if (key.isBlank() || !seen.add(key)) {
        duplicates.add(description);
        continue;
      }
      if ("JS_SUCCEEDED".equals(child.state)) {
        ReconcileFileGroupResultDescriptor descriptor = descriptorByGroupKey.get(key);
        int planned = Math.max(0, child.fileGroupTask.fileCount());
        if (descriptor != null
            && descriptor.plannedFileCount() == planned
            && descriptor.succeededFileCount() == planned
            && descriptor.failedFileCount() == 0
            && descriptor.skippedFileCount() == 0) {
          completed++;
          completedDescriptors.add(descriptor);
        } else {
          invalid.add(description);
        }
      } else if ("JS_FAILED".equals(child.state)) {
        failed.add(describeFailure(child, child.fileGroupTask));
      } else if ("JS_CANCELLED".equals(child.state)) {
        cancelled.add(describeFailure(child, child.fileGroupTask));
      } else {
        pending.add(description + "(" + blankToUnknown(child.state) + ")");
      }
    }
    List<String> missing =
        childCount < expectedGroupCount
            ? List.of("expected=" + expectedGroupCount + " actual=" + childCount)
            : List.of();
    if (childCount > expectedGroupCount) {
      duplicates.add("expected=" + expectedGroupCount + " actual=" + childCount);
    }
    return new ChildState(
        expectedGroupCount,
        completed,
        List.copyOf(pending),
        List.copyOf(failed),
        List.copyOf(cancelled),
        List.copyOf(duplicates),
        missing,
        List.copyOf(invalid),
        List.copyOf(completedDescriptors));
  }

  private static ChildState duplicateDescriptorState(
      int expectedGroupCount, ReconcileFileGroupResultDescriptor descriptor) {
    return new ChildState(
        expectedGroupCount,
        0,
        List.of(),
        List.of(),
        List.of(),
        List.of(descriptor.planId() + "/" + descriptor.groupId()),
        List.of(),
        List.of(),
        List.of());
  }

  List<ReconcileJobStore.ChildJobState> childStates(String accountId, String parentJobId) {
    if (accountId == null || accountId.isBlank() || parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    List<ReconcileJobStore.ChildJobState> out = new ArrayList<>();
    String pageToken = "";
    do {
      ReconcileJobStore.ChildJobStatePage page =
          jobs.childJobStatesPage(accountId, parentJobId, 200, pageToken);
      if (page == null || page.states().isEmpty()) {
        break;
      }
      out.addAll(page.states());
      pageToken = page.nextPageToken();
    } while (!pageToken.isBlank());
    return List.copyOf(out);
  }

  List<ReconcileJobStore.ReconcileJob> childJobs(String accountId, String parentJobId) {
    return childStates(accountId, parentJobId).stream()
        .map(ReconcileJobStore.ChildJobState::job)
        .toList();
  }

  static String groupKey(ReconcileFileGroupResultDescriptor descriptor) {
    if (descriptor == null) {
      return "";
    }
    String planId = descriptor.planId() == null ? "" : descriptor.planId().trim();
    String groupId = descriptor.groupId() == null ? "" : descriptor.groupId().trim();
    return planId.isBlank() || groupId.isBlank() ? "" : planId + "|" + groupId;
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
      List<ReconcileFileGroupResultDescriptor> completedGroupDescriptors) {}
}
