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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitSideEffectService.ConnectorSyncResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class PostCommitSyncOutboxService {
  private static final Logger LOG = Logger.getLogger(PostCommitSyncOutboxService.class);
  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final int DEFAULT_DRAIN_BATCH_SIZE = 8;
  private static final long DEFAULT_BACKOFF_MS = 500L;
  private static final long MAX_BACKOFF_MS = 30_000L;

  private final Map<String, SyncTask> tasksById = new LinkedHashMap<>();
  private final Map<String, String> dedupeIndex = new LinkedHashMap<>();

  @Inject TableCommitSideEffectService sideEffectService;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject ObjectMapper mapper;
  @Inject Config config;

  private boolean enabled = true;
  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private int drainBatchSize = DEFAULT_DRAIN_BATCH_SIZE;
  private long baseBackoffMs = DEFAULT_BACKOFF_MS;
  private Path storageDir;
  private long pollEveryMs = 2_000L;
  private boolean workerEnabled = true;
  private ScheduledExecutorService worker;

  @PostConstruct
  void init() {
    if (config != null) {
      enabled =
          config
              .getOptionalValue("floecat.gateway.post-commit-outbox.enabled", Boolean.class)
              .orElse(true);
      maxAttempts =
          Math.max(
              1,
              config
                  .getOptionalValue(
                      "floecat.gateway.post-commit-outbox.max-attempts", Integer.class)
                  .orElse(DEFAULT_MAX_ATTEMPTS));
      drainBatchSize =
          Math.max(
              1,
              config
                  .getOptionalValue(
                      "floecat.gateway.post-commit-outbox.drain-batch-size", Integer.class)
                  .orElse(DEFAULT_DRAIN_BATCH_SIZE));
      baseBackoffMs =
          Math.max(
              100L,
              config
                  .getOptionalValue(
                      "floecat.gateway.post-commit-outbox.base-backoff-ms", Long.class)
                  .orElse(DEFAULT_BACKOFF_MS));
      String dir =
          config
              .getOptionalValue("floecat.gateway.post-commit-outbox.store-dir", String.class)
              .map(String::trim)
              .orElse("");
      pollEveryMs =
          Math.max(
              250L,
              config
                  .getOptionalValue("floecat.gateway.post-commit-outbox.poll-every-ms", Long.class)
                  .orElse(2_000L));
      workerEnabled =
          config
              .getOptionalValue("floecat.gateway.post-commit-outbox.worker-enabled", Boolean.class)
              .orElse(true);
      if (!dir.isBlank()) {
        try {
          storageDir = Path.of(dir);
          Files.createDirectories(storageDir);
          loadPersistedTasks();
        } catch (Exception e) {
          LOG.warnf(e, "Unable to initialize post-commit outbox directory %s", dir);
          storageDir = null;
        }
      }
    }
    if (enabled && workerEnabled) {
      worker = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "post-commit-outbox"));
      worker.scheduleWithFixedDelay(
          this::drainSafely, pollEveryMs, pollEveryMs, TimeUnit.MILLISECONDS);
    }
  }

  @PreDestroy
  void close() {
    ScheduledExecutorService current = worker;
    if (current != null) {
      current.shutdownNow();
      worker = null;
    }
  }

  public void enqueueConnectorSync(
      String dedupeKey,
      TableGatewaySupport support,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return;
    }
    if (!enabled) {
      runOnce(
          new SyncTask(
              hashValue(connectorId.getId() + "|" + namespacePath + "|" + tableName),
              dedupeKey,
              connectorId.getAccountId(),
              connectorId.getId(),
              connectorId.getKindValue(),
              namespacePath == null ? List.of() : List.copyOf(namespacePath),
              tableName,
              0,
              0L,
              "INLINE_DISABLED"));
      return;
    }
    SyncTask task;
    synchronized (tasksById) {
      String dedupe = normalizeDedupeKey(dedupeKey, connectorId, namespacePath, tableName);
      String existingId = dedupeIndex.get(dedupe);
      if (existingId != null && tasksById.containsKey(existingId)) {
        return;
      }
      String taskId = hashValue(dedupe + "|" + System.nanoTime());
      task =
          new SyncTask(
              taskId,
              dedupe,
              connectorId.getAccountId(),
              connectorId.getId(),
              connectorId.getKindValue(),
              namespacePath == null ? List.of() : List.copyOf(namespacePath),
              tableName,
              0,
              System.currentTimeMillis(),
              "QUEUED");
      tasksById.put(task.id, task);
      dedupeIndex.put(dedupe, task.id);
      persistTask(task);
    }
    runOnce(task, support);
  }

  private void drainSafely() {
    try {
      drainNow(drainBatchSize);
    } catch (Throwable t) {
      LOG.warnf(t, "Post-commit outbox drain loop failed");
    }
  }

  void drainNow(int limit) {
    if (!enabled) {
      return;
    }
    int remaining = Math.max(1, limit);
    while (remaining-- > 0) {
      SyncTask next = leaseNextDue();
      if (next == null) {
        return;
      }
      runOnce(next, tableGatewaySupport);
    }
  }

  private SyncTask leaseNextDue() {
    long now = System.currentTimeMillis();
    synchronized (tasksById) {
      for (SyncTask task : tasksById.values()) {
        if (task.nextAttemptAtMs <= now && !task.exhausted(maxAttempts)) {
          task.state = "RUNNING";
          persistTask(task);
          return task;
        }
      }
    }
    return null;
  }

  private void runOnce(SyncTask task) {
    runOnce(task, tableGatewaySupport);
  }

  private void runOnce(SyncTask task, TableGatewaySupport support) {
    ResourceId.Builder connectorBuilder = ResourceId.newBuilder().setId(task.connectorId);
    if (task.connectorAccountId != null && !task.connectorAccountId.isBlank()) {
      connectorBuilder.setAccountId(task.connectorAccountId);
    }
    if (task.connectorKind > 0) {
      ResourceKind kind = ResourceKind.forNumber(task.connectorKind);
      if (kind != null
          && kind != ResourceKind.UNRECOGNIZED
          && kind != ResourceKind.RK_UNSPECIFIED) {
        connectorBuilder.setKind(kind);
      }
    }
    ResourceId connectorId = connectorBuilder.build();
    ConnectorSyncResult result =
        sideEffectService.runConnectorSyncAttempt(
            support == null ? tableGatewaySupport : support,
            connectorId,
            task.namespacePath,
            task.tableName);
    if (result.success()) {
      markSucceeded(task);
      return;
    }
    markFailedAttempt(task, result);
  }

  private void markSucceeded(SyncTask task) {
    synchronized (tasksById) {
      tasksById.remove(task.id);
      dedupeIndex.remove(task.dedupeKey);
      deleteTask(task.id);
    }
  }

  private void markFailedAttempt(SyncTask task, ConnectorSyncResult result) {
    long now = System.currentTimeMillis();
    synchronized (tasksById) {
      SyncTask current = tasksById.get(task.id);
      if (current == null) {
        return;
      }
      current.attempts += 1;
      current.lastError =
          "captureOk=" + result.captureOk() + ",reconcileOk=" + result.reconcileOk();
      if (current.exhausted(maxAttempts)) {
        current.state = "DEAD";
        persistTask(current);
        LOG.warnf(
            "Post-commit sync outbox exhausted attempts taskId=%s connectorId=%s namespace=%s table=%s",
            current.id,
            current.connectorId,
            String.join(".", current.namespacePath),
            current.tableName);
        return;
      }
      current.state = "QUEUED";
      current.nextAttemptAtMs = now + backoffMs(current.attempts);
      persistTask(current);
    }
  }

  private long backoffMs(int attempts) {
    long backoff = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(MAX_BACKOFF_MS, backoff);
  }

  private String normalizeDedupeKey(
      String dedupeKey, ResourceId connectorId, List<String> namespacePath, String tableName) {
    if (dedupeKey != null && !dedupeKey.isBlank()) {
      return dedupeKey.trim();
    }
    return "connector:"
        + connectorId.getId()
        + "|ns:"
        + (namespacePath == null ? "" : String.join(".", namespacePath))
        + "|table:"
        + (tableName == null ? "" : tableName);
  }

  private void loadPersistedTasks() {
    if (storageDir == null) {
      return;
    }
    try (var stream = Files.list(storageDir)) {
      List<Path> files =
          stream.filter(path -> path.getFileName().toString().endsWith(".json")).toList();
      for (Path file : files) {
        try {
          SyncTask task = mapper.readValue(file.toFile(), SyncTask.class);
          if (task == null || task.id == null || task.id.isBlank()) {
            continue;
          }
          if ("DEAD".equals(task.state)) {
            continue;
          }
          tasksById.put(task.id, task);
          if (task.dedupeKey != null && !task.dedupeKey.isBlank()) {
            dedupeIndex.put(task.dedupeKey, task.id);
          }
        } catch (Exception e) {
          LOG.warnf(e, "Unable to load post-commit task file %s", file);
        }
      }
    } catch (Exception e) {
      LOG.warnf(e, "Unable to list post-commit task directory %s", storageDir);
    }
  }

  private void persistTask(SyncTask task) {
    if (storageDir == null || task == null || task.id == null || task.id.isBlank()) {
      return;
    }
    try {
      byte[] bytes = mapper.writeValueAsBytes(task);
      Files.write(
          storageDir.resolve(task.id + ".json"),
          bytes,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
    } catch (Exception e) {
      LOG.warnf(e, "Unable to persist post-commit task %s", task.id);
    }
  }

  private void deleteTask(String id) {
    if (storageDir == null || id == null || id.isBlank()) {
      return;
    }
    try {
      Files.deleteIfExists(storageDir.resolve(id + ".json"));
    } catch (Exception e) {
      LOG.warnf(e, "Unable to delete post-commit task %s", id);
    }
  }

  private String hashValue(String value) {
    try {
      byte[] payload = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(payload));
    } catch (Exception e) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }
  }

  static final class SyncTask {
    public String id;
    public String dedupeKey;
    public String connectorAccountId;
    public String connectorId;
    public int connectorKind;
    public List<String> namespacePath = new ArrayList<>();
    public String tableName;
    public int attempts;
    public long nextAttemptAtMs;
    public String state;
    public String lastError;

    SyncTask() {}

    SyncTask(
        String id,
        String dedupeKey,
        String connectorAccountId,
        String connectorId,
        int connectorKind,
        List<String> namespacePath,
        String tableName,
        int attempts,
        long nextAttemptAtMs,
        String state) {
      this.id = id;
      this.dedupeKey = dedupeKey;
      this.connectorAccountId = connectorAccountId;
      this.connectorId = connectorId;
      this.connectorKind = connectorKind;
      this.namespacePath = namespacePath == null ? List.of() : new ArrayList<>(namespacePath);
      this.tableName = tableName;
      this.attempts = attempts;
      this.nextAttemptAtMs = nextAttemptAtMs;
      this.state = state;
    }

    boolean exhausted(int maxAttempts) {
      return attempts >= maxAttempts;
    }
  }
}
