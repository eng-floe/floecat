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

package ai.floedb.floecat.gateway.iceberg.rest.services.planning;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TablePlanTasksResponseDto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

@ApplicationScoped
public class PlanTaskManager {
  private static final long MIN_TTL_SECONDS = 60L;
  private static final int DEFAULT_FILES_PER_TASK = 128;

  private final ConcurrentMap<String, PlanEntry> plans = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, PlanTaskPayload> planTasks = new ConcurrentHashMap<>();
  private final Duration ttl;
  private final int filesPerTask;

  @Inject
  public PlanTaskManager(IcebergGatewayConfig config) {
    Duration configuredTtl = config.planTaskTtl();
    if (configuredTtl == null || configuredTtl.isNegative() || configuredTtl.isZero()) {
      configuredTtl = Duration.ofSeconds(MIN_TTL_SECONDS);
    }
    if (configuredTtl.getSeconds() < MIN_TTL_SECONDS) {
      configuredTtl = Duration.ofSeconds(MIN_TTL_SECONDS);
    }
    this.ttl = configuredTtl;
    int configuredChunk = config.planTaskFilesPerTask();
    if (configuredChunk <= 0) {
      configuredChunk = DEFAULT_FILES_PER_TASK;
    }
    this.filesPerTask = configuredChunk;
  }

  public PlanDescriptor registerCompletedPlan(
      String planId,
      String namespace,
      String table,
      List<FileScanTaskDto> fileScanTasks,
      List<ContentFileDto> deleteFiles,
      List<StorageCredentialDto> credentials) {
    expire();
    Objects.requireNonNull(planId, "planId is required");
    List<FileScanTaskDto> files =
        fileScanTasks == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(fileScanTasks));
    List<ContentFileDto> deletes =
        deleteFiles == null
            ? List.of()
            : Collections.unmodifiableList(new ArrayList<>(deleteFiles));
    PlanEntry entry =
        new PlanEntry(
            planId,
            namespace,
            table,
            credentials == null ? List.of() : List.copyOf(credentials),
            files,
            deletes,
            PlanStatus.COMPLETED);
    if (!files.isEmpty()) {
      int chunkSize = Math.max(1, filesPerTask);
      for (int offset = 0; offset < files.size(); offset += chunkSize) {
        int end = Math.min(files.size(), offset + chunkSize);
        List<FileScanTaskDto> chunk = files.subList(offset, end);
        TablePlanTasksResponseDto payload =
            new TablePlanTasksResponseDto(null, List.copyOf(chunk), deletes);
        String taskId = planId + "-task-" + offset / chunkSize;
        entry.addTask(taskId);
        planTasks.put(taskId, new PlanTaskPayload(taskId, planId, namespace, table, payload));
      }
    }
    plans.put(planId, entry);
    return entry.toDescriptor();
  }

  public PlanDescriptor registerSubmittedPlan(String planId, String namespace, String table) {
    expire();
    Objects.requireNonNull(planId, "planId is required");
    PlanEntry entry =
        new PlanEntry(
            planId, namespace, table, List.of(), List.of(), List.of(), PlanStatus.SUBMITTED);
    plans.put(planId, entry);
    return entry.toDescriptor();
  }

  public PlanDescriptor registerFailedPlan(String planId, String namespace, String table) {
    expire();
    Objects.requireNonNull(planId, "planId is required");
    PlanEntry entry =
        new PlanEntry(planId, namespace, table, List.of(), List.of(), List.of(), PlanStatus.FAILED);
    plans.put(planId, entry);
    return entry.toDescriptor();
  }

  public Optional<PlanDescriptor> findPlan(String planId) {
    expire();
    PlanEntry entry = plans.get(planId);
    return entry == null ? Optional.empty() : Optional.of(entry.toDescriptor());
  }

  public Optional<TablePlanTasksResponseDto> consumeTask(
      String namespace, String table, String planTaskId) {
    expire();
    PlanTaskPayload payload = planTasks.remove(planTaskId);
    if (payload == null) {
      return Optional.empty();
    }
    if (!Objects.equals(namespace, payload.namespace())
        || !Objects.equals(table, payload.table())) {
      planTasks.put(payload.planTaskId(), payload);
      return Optional.empty();
    }
    PlanEntry entry = plans.get(payload.planId());
    if (entry != null) {
      entry.removeTask(planTaskId);
      if (entry.planTaskIds().isEmpty()) {
        plans.remove(entry.planId(), entry);
      }
    }
    return Optional.of(payload.payload());
  }

  public void cancelPlan(String planId) {
    expire();
    PlanEntry entry = plans.get(planId);
    if (entry == null) {
      return;
    }
    for (String taskId : entry.planTaskIds()) {
      planTasks.remove(taskId);
    }
    entry.clearTasks();
    entry.markCancelled();
  }

  private void expire() {
    Instant cutoff = Instant.now().minus(ttl);
    for (var entry : plans.values()) {
      if (entry.isExpired(cutoff)) {
        cancelPlan(entry.planId());
        plans.remove(entry.planId(), entry);
      }
    }
    for (var task : planTasks.values()) {
      if (task.createdAt().isBefore(cutoff)) {
        planTasks.remove(task.planTaskId(), task);
      }
    }
  }

  public enum PlanStatus {
    SUBMITTED("submitted"),
    COMPLETED("completed"),
    FAILED("failed"),
    CANCELLED("cancelled");

    private final String value;

    PlanStatus(String value) {
      this.value = value;
    }

    public String value() {
      return value;
    }
  }

  public record PlanDescriptor(
      String planId,
      String namespace,
      String table,
      PlanStatus status,
      List<String> planTasks,
      List<StorageCredentialDto> credentials,
      List<FileScanTaskDto> fileScanTasks,
      List<ContentFileDto> deleteFiles) {}

  private static final class PlanEntry {
    private final String planId;
    private final String namespace;
    private final String table;
    private final List<StorageCredentialDto> credentials;
    private final List<FileScanTaskDto> fileScanTasks;
    private final List<ContentFileDto> deleteFiles;
    private final CopyOnWriteArrayList<String> taskIds = new CopyOnWriteArrayList<>();
    private volatile PlanStatus status;
    private volatile Instant updatedAt = Instant.now();

    PlanEntry(
        String planId,
        String namespace,
        String table,
        List<StorageCredentialDto> credentials,
        List<FileScanTaskDto> fileScanTasks,
        List<ContentFileDto> deleteFiles,
        PlanStatus status) {
      this.planId = planId;
      this.namespace = namespace;
      this.table = table;
      this.credentials = credentials;
      this.fileScanTasks = fileScanTasks;
      this.deleteFiles = deleteFiles;
      this.status = status == null ? PlanStatus.SUBMITTED : status;
    }

    void addTask(String taskId) {
      taskIds.add(taskId);
      touch();
    }

    void removeTask(String taskId) {
      taskIds.remove(taskId);
      touch();
    }

    void clearTasks() {
      taskIds.clear();
      touch();
    }

    void markCancelled() {
      status = PlanStatus.CANCELLED;
      touch();
    }

    List<String> planTaskIds() {
      return List.copyOf(taskIds);
    }

    String planId() {
      return planId;
    }

    boolean isExpired(Instant cutoff) {
      return updatedAt.isBefore(cutoff);
    }

    PlanDescriptor toDescriptor() {
      return new PlanDescriptor(
          planId, namespace, table, status, planTaskIds(), credentials, fileScanTasks, deleteFiles);
    }

    private void touch() {
      updatedAt = Instant.now();
    }
  }

  private record PlanTaskPayload(
      String planTaskId,
      String planId,
      String namespace,
      String table,
      TablePlanTasksResponseDto payload,
      Instant createdAt) {
    PlanTaskPayload(
        String planTaskId,
        String planId,
        String namespace,
        String table,
        TablePlanTasksResponseDto payload) {
      this(planTaskId, planId, namespace, table, payload, Instant.now());
    }
  }
}
