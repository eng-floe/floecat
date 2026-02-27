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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Set;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class ReconcileJobGc {

  private static final Set<String> TERMINAL_STATES =
      Set.of("JS_SUCCEEDED", "JS_FAILED", "JS_CANCELLED");

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject ObjectMapper mapper;

  public record AccountResult(
      int scanned,
      int expired,
      int ptrDeleted,
      int blobDeleted,
      int dedupeDeleted,
      int readyDeleted,
      String nextJobToken,
      String nextDedupeToken) {}

  public record GlobalResult(int scanned, int deleted, String nextToken) {}

  public AccountResult runAccountSlice(String accountId, String jobTokenIn, String dedupeTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.batch-limit", Integer.class).orElse(1000);
    final long sliceMillis =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.slice-millis", Long.class).orElse(4000L);
    final long retentionMs =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.retention-ms", Long.class)
            .orElse(7L * 24L * 60L * 60L * 1000L);

    final long nowMs = System.currentTimeMillis();
    final long deadline = nowMs + sliceMillis;

    String jobToken = jobTokenIn == null ? "" : jobTokenIn;
    String dedupeToken = dedupeTokenIn == null ? "" : dedupeTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;
    int dedupeDeleted = 0;
    int readyDeleted = 0;

    String jobPrefix = Keys.reconcileJobPointerByIdPrefix(accountId);
    while (scanned < batchLimit && System.currentTimeMillis() < deadline) {
      StringBuilder next = new StringBuilder();
      var pointers = pointerStore.listPointersByPrefix(jobPrefix, pageSize, jobToken, next);
      jobToken = next.toString();
      if (pointers.isEmpty()) {
        break;
      }

      for (Pointer canonical : pointers) {
        if (scanned >= batchLimit || System.currentTimeMillis() >= deadline) {
          break;
        }
        scanned++;

        JsonNode record = readRecord(canonical);
        if (record == null) {
          if (pointerStore.compareAndDelete(canonical.getKey(), canonical.getVersion())) {
            ptrDeleted++;
            if (blobStore.delete(canonical.getBlobUri())) {
              blobDeleted++;
            }
            String jobId = decodeJobId(jobPrefix, canonical.getKey());
            if (jobId != null) {
              deletePointerIfPresent(Keys.reconcileJobLookupPointerById(jobId));
              blobStore.deletePrefix(Keys.reconcileJobBlobPrefix(accountId, jobId));
            }
          }
          continue;
        }

        String state = text(record, "state");
        long updatedAt =
            longValue(
                record,
                "updatedAtMs",
                longValue(record, "finishedAtMs", longValue(record, "createdAtMs", nowMs)));

        if (TERMINAL_STATES.contains(state)) {
          // Terminal jobs must never hold queue/dedupe references.
          String dedupeKey = text(record, "dedupeKey");
          if (!dedupeKey.isBlank() && deleteDedupePointer(accountId, dedupeKey)) {
            dedupeDeleted++;
          }
          String readyKey = text(record, "readyPointerKey");
          if (!readyKey.isBlank() && deletePointerIfPresent(readyKey)) {
            readyDeleted++;
          }

          if (updatedAt <= nowMs - retentionMs) {
            if (pointerStore.compareAndDelete(canonical.getKey(), canonical.getVersion())) {
              expired++;
              ptrDeleted++;
              if (blobStore.delete(canonical.getBlobUri())) {
                blobDeleted++;
              }
              String jobId = decodeJobId(jobPrefix, canonical.getKey());
              if (jobId != null) {
                deletePointerIfPresent(Keys.reconcileJobLookupPointerById(jobId));
                blobStore.deletePrefix(Keys.reconcileJobBlobPrefix(accountId, jobId));
              }
            }
          }
        }
      }

      if (jobToken.isBlank()) {
        break;
      }
    }

    String dedupePrefix = Keys.reconcileDedupePointerPrefix(accountId);
    while (scanned < batchLimit && System.currentTimeMillis() < deadline) {
      StringBuilder next = new StringBuilder();
      var dedupePointers =
          pointerStore.listPointersByPrefix(dedupePrefix, pageSize, dedupeToken, next);
      dedupeToken = next.toString();
      if (dedupePointers.isEmpty()) {
        break;
      }

      for (Pointer dedupe : dedupePointers) {
        if (scanned >= batchLimit || System.currentTimeMillis() >= deadline) {
          break;
        }
        scanned++;

        JsonNode record = readRecordByBlobUri(dedupe.getBlobUri());
        if (record == null) {
          if (pointerStore.compareAndDelete(dedupe.getKey(), dedupe.getVersion())) {
            dedupeDeleted++;
          }
          continue;
        }

        if (TERMINAL_STATES.contains(text(record, "state"))) {
          if (pointerStore.compareAndDelete(dedupe.getKey(), dedupe.getVersion())) {
            dedupeDeleted++;
          }
        }
      }

      if (dedupeToken.isBlank()) {
        break;
      }
    }

    return new AccountResult(
        scanned,
        expired,
        ptrDeleted,
        blobDeleted,
        dedupeDeleted,
        readyDeleted,
        jobToken,
        dedupeToken);
  }

  public GlobalResult runReadySlice(String pageTokenIn) {
    var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.reconcile-jobs.global-ready-batch-limit", Integer.class)
            .orElse(1000);

    int scanned = 0;
    int deleted = 0;
    String token = pageTokenIn == null ? "" : pageTokenIn;

    while (scanned < batchLimit) {
      StringBuilder next = new StringBuilder();
      var readyPointers =
          pointerStore.listPointersByPrefix(
              Keys.reconcileReadyPointerPrefix(), pageSize, token, next);
      token = next.toString();
      if (readyPointers.isEmpty()) {
        break;
      }

      for (Pointer ready : readyPointers) {
        if (scanned >= batchLimit) {
          break;
        }
        scanned++;

        JsonNode record = readRecordByBlobUri(ready.getBlobUri());
        if (record == null) {
          if (pointerStore.compareAndDelete(ready.getKey(), ready.getVersion())) {
            deleted++;
          }
          continue;
        }

        String state = text(record, "state");
        String preferredReadyKey = text(record, "readyPointerKey");
        boolean stale = !"JS_QUEUED".equals(state);
        if (!stale && !preferredReadyKey.isBlank() && !ready.getKey().equals(preferredReadyKey)) {
          stale = true;
        }

        if (stale && pointerStore.compareAndDelete(ready.getKey(), ready.getVersion())) {
          deleted++;
        }
      }

      if (token.isBlank()) {
        break;
      }
    }

    return new GlobalResult(scanned, deleted, token);
  }

  private JsonNode readRecord(Pointer canonical) {
    return readRecordByBlobUri(canonical.getBlobUri());
  }

  private JsonNode readRecordByBlobUri(String blobUri) {
    byte[] payload = blobStore.get(blobUri);
    if (payload == null || payload.length == 0) {
      return null;
    }
    try {
      return mapper.readTree(payload);
    } catch (Exception ignored) {
      return null;
    }
  }

  private static String text(JsonNode node, String field) {
    if (node == null || field == null) {
      return "";
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return "";
    }
    String value = child.asText("");
    return value == null ? "" : value;
  }

  private static long longValue(JsonNode node, String field, long defaultValue) {
    if (node == null || field == null) {
      return defaultValue;
    }
    JsonNode child = node.get(field);
    if (child == null || child.isNull()) {
      return defaultValue;
    }
    return child.asLong(defaultValue);
  }

  private static String decodeJobId(String prefix, String canonicalKey) {
    if (canonicalKey == null || prefix == null || !canonicalKey.startsWith(prefix)) {
      return null;
    }
    String suffix = canonicalKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
  }

  private boolean deleteDedupePointer(String accountId, String dedupeKey) {
    String key = Keys.reconcileDedupePointer(accountId, hashValue(dedupeKey));
    return deletePointerIfPresent(key);
  }

  private boolean deletePointerIfPresent(String key) {
    Pointer pointer = pointerStore.get(key).orElse(null);
    if (pointer == null) {
      return false;
    }
    return pointerStore.compareAndDelete(key, pointer.getVersion());
  }

  private static String hashValue(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] payload = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest(payload));
    } catch (Exception e) {
      return Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
    }
  }
}
