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

import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
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
import java.util.function.Supplier;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CommitOperationTracker {
  private static final Logger LOG = Logger.getLogger(CommitOperationTracker.class);
  private static final long DEFAULT_IN_PROGRESS_TIMEOUT_MS = 120_000L;

  private final Map<OperationKey, OperationRecord> records = new LinkedHashMap<>();

  @Inject ObjectMapper mapper;
  @Inject Config config;
  private boolean initialized;
  private long inProgressTimeoutMs = DEFAULT_IN_PROGRESS_TIMEOUT_MS;
  private Path storageDir;

  public Response execute(OperationKey key, Object payload, Supplier<Response> operation) {
    if (key == null || operation == null) {
      return operation == null ? Response.serverError().build() : operation.get();
    }
    ensureInitialized();
    String hash = payloadHash(payload);
    long now = System.currentTimeMillis();
    synchronized (records) {
      OperationRecord existing = loadRecordIfNeeded(key);
      if (existing != null) {
        if (!existing.requestHash.equals(hash)) {
          return IcebergErrorResponses.conflict("Idempotent operation payload mismatch");
        }
        if (existing.state == OperationState.IN_PROGRESS) {
          if ((now - existing.updatedAtMs) <= inProgressTimeoutMs) {
            return IcebergErrorResponses.conflict("Idempotent operation already in progress");
          }
          existing.steps.add("RETRY_STALE_IN_PROGRESS");
          existing.updatedAtMs = now;
          persistRecord(key, existing);
        } else if (existing.state == OperationState.FAILED && existing.retryableFailure()) {
          existing.state = OperationState.IN_PROGRESS;
          existing.steps.add("RETRY_PREVIOUS_FAILURE");
          existing.updatedAtMs = now;
          persistRecord(key, existing);
        } else {
          return existing.toResponse();
        }
      } else {
        OperationRecord created = OperationRecord.inProgress(hash, now);
        records.put(key, created);
        persistRecord(key, created);
      }
    }

    Response response;
    try {
      response = operation.get();
    } catch (RuntimeException e) {
      synchronized (records) {
        OperationRecord current = records.get(key);
        if (current != null) {
          current.state = OperationState.FAILED;
          current.status = 500;
          current.entity = Map.of("message", "Operation failed with exception");
          current.steps.add("FAILED_EXCEPTION");
          current.updatedAtMs = System.currentTimeMillis();
          persistRecord(key, current);
        }
      }
      throw e;
    }

    synchronized (records) {
      OperationRecord current = records.get(key);
      if (current != null) {
        current.status = response == null ? 500 : response.getStatus();
        current.entity = response == null ? null : response.getEntity();
        current.state = current.status >= 400 ? OperationState.FAILED : OperationState.SUCCEEDED;
        current.updatedAtMs = System.currentTimeMillis();
        persistRecord(key, current);
      }
    }
    return response;
  }

  public void markStep(OperationKey key, String step) {
    if (key == null || step == null || step.isBlank()) {
      return;
    }
    ensureInitialized();
    synchronized (records) {
      OperationRecord current = records.get(key);
      if (current == null || current.state != OperationState.IN_PROGRESS) {
        return;
      }
      current.steps.add(step);
      current.updatedAtMs = System.currentTimeMillis();
      persistRecord(key, current);
    }
  }

  private void ensureInitialized() {
    synchronized (records) {
      if (initialized) {
        return;
      }
      initialized = true;
      if (config != null) {
        long configuredTimeout =
            config
                .getOptionalValue("floecat.gateway.commit-op.in-progress-timeout-ms", Long.class)
                .orElse(DEFAULT_IN_PROGRESS_TIMEOUT_MS);
        inProgressTimeoutMs = Math.max(5_000L, configuredTimeout);
        String dir =
            config
                .getOptionalValue("floecat.gateway.commit-op.store-dir", String.class)
                .map(String::trim)
                .orElse("");
        if (!dir.isBlank()) {
          storageDir = Path.of(dir);
          try {
            Files.createDirectories(storageDir);
          } catch (IOException e) {
            LOG.warnf(e, "Unable to initialize commit operation store dir %s", storageDir);
            storageDir = null;
          }
        }
      }
    }
  }

  private OperationRecord loadRecordIfNeeded(OperationKey key) {
    OperationRecord existing = records.get(key);
    if (existing != null || storageDir == null) {
      return existing;
    }
    Path file = fileFor(key);
    if (!Files.exists(file)) {
      return null;
    }
    try {
      StoredRecord stored = mapper.readValue(file.toFile(), StoredRecord.class);
      if (stored == null || stored.requestHash == null || stored.state == null) {
        return null;
      }
      OperationRecord loaded = OperationRecord.fromStored(stored);
      records.put(key, loaded);
      return loaded;
    } catch (Exception e) {
      LOG.warnf(e, "Unable to load commit operation record %s", file);
      return null;
    }
  }

  private void persistRecord(OperationKey key, OperationRecord record) {
    if (storageDir == null || key == null || record == null) {
      return;
    }
    Path file = fileFor(key);
    try {
      StoredRecord stored = record.toStored();
      byte[] bytes = mapper.writeValueAsBytes(stored);
      Files.write(
          file,
          bytes,
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING,
          StandardOpenOption.WRITE);
    } catch (Exception e) {
      LOG.warnf(e, "Unable to persist commit operation record %s", file);
    }
  }

  private Path fileFor(OperationKey key) {
    String digest = payloadHash(key.accountId + "|" + key.scope + "|" + key.operationId);
    return storageDir.resolve(digest + ".json");
  }

  private String payloadHash(Object payload) {
    try {
      byte[] bytes = mapper.writeValueAsBytes(payload == null ? Map.of() : payload);
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(bytes));
    } catch (Exception e) {
      String fallback = String.valueOf(payload);
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(fallback.getBytes(StandardCharsets.UTF_8));
    }
  }

  public record OperationKey(String accountId, String scope, String operationId) {}

  private enum OperationState {
    IN_PROGRESS,
    SUCCEEDED,
    FAILED
  }

  private static final class OperationRecord {
    final String requestHash;
    final List<String> steps = new ArrayList<>();
    OperationState state;
    int status;
    Object entity;
    long createdAtMs;
    long updatedAtMs;

    private OperationRecord(String requestHash, OperationState state, long now) {
      this.requestHash = requestHash;
      this.state = state;
      this.createdAtMs = now;
      this.updatedAtMs = now;
    }

    static OperationRecord inProgress(String requestHash, long now) {
      return new OperationRecord(requestHash, OperationState.IN_PROGRESS, now);
    }

    static OperationRecord fromStored(StoredRecord stored) {
      OperationRecord out =
          new OperationRecord(
              stored.requestHash, OperationState.valueOf(stored.state), stored.createdAtMs);
      out.status = stored.status;
      out.entity = stored.entity;
      out.updatedAtMs = stored.updatedAtMs <= 0 ? stored.createdAtMs : stored.updatedAtMs;
      if (stored.steps != null) {
        out.steps.addAll(stored.steps);
      }
      return out;
    }

    StoredRecord toStored() {
      StoredRecord out = new StoredRecord();
      out.requestHash = requestHash;
      out.steps = List.copyOf(steps);
      out.state = state.name();
      out.status = status;
      out.entity = entity;
      out.createdAtMs = createdAtMs;
      out.updatedAtMs = updatedAtMs;
      return out;
    }

    boolean retryableFailure() {
      return state == OperationState.FAILED && status >= 500;
    }

    Response toResponse() {
      int responseStatus = status <= 0 ? 500 : status;
      Response.ResponseBuilder builder = Response.status(responseStatus);
      if (entity != null) {
        builder.entity(entity);
      }
      return builder.build();
    }
  }

  private static final class StoredRecord {
    public String requestHash;
    public List<String> steps;
    public String state;
    public int status;
    public Object entity;
    public long createdAtMs;
    public long updatedAtMs;
  }
}
