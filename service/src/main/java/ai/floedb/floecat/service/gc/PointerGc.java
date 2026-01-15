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
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class PointerGc {

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;

  public record Result(int scanned, int deleted, int missingBlobs, int staleSecondaries) {}

  public Result runGlobalAccountPointers(long deadlineMs) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.pointer.page-size", Integer.class)
            .orElse(500);
    long minAgeMs =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.pointer.min-age-ms", Long.class)
            .orElse(30_000L);
    long nowMs = System.currentTimeMillis();
    Map<String, Boolean> blobCache = new HashMap<>();

    int scanned = 0;
    int deleted = 0;
    int missingBlobs = 0;
    int staleSecondaries = 0;

    Result byId =
        scanPrefix("/accounts/by-id/", pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
    scanned += byId.scanned;
    deleted += byId.deleted;
    missingBlobs += byId.missingBlobs;
    staleSecondaries += byId.staleSecondaries;

    Result byName =
        scanPrefix(
            "/accounts/by-name/", pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
    scanned += byName.scanned;
    deleted += byName.deleted;
    missingBlobs += byName.missingBlobs;
    staleSecondaries += byName.staleSecondaries;

    return new Result(scanned, deleted, missingBlobs, staleSecondaries);
  }

  public Result runForAccount(String accountId, long deadlineMs) {
    int pageSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.pointer.page-size", Integer.class)
            .orElse(500);
    long minAgeMs =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gc.pointer.min-age-ms", Long.class)
            .orElse(30_000L);
    long nowMs = System.currentTimeMillis();

    Map<String, Boolean> blobCache = new HashMap<>();
    int scanned = 0;
    int deleted = 0;
    int missingBlobs = 0;
    int staleSecondaries = 0;

    String acct = encode(accountId);

    List<String> tableIds = new ArrayList<>();

    Result tablesById =
        scanPrefix(
            "/accounts/" + acct + "/tables/by-id/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += tablesById.scanned;
    deleted += tablesById.deleted;
    missingBlobs += tablesById.missingBlobs;
    staleSecondaries += tablesById.staleSecondaries;

    collectIds("/accounts/" + acct + "/tables/by-id/", pageSize, tableIds);

    Result catalogsById =
        scanPrefix(
            "/accounts/" + acct + "/catalogs/by-id/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += catalogsById.scanned;
    deleted += catalogsById.deleted;
    missingBlobs += catalogsById.missingBlobs;
    staleSecondaries += catalogsById.staleSecondaries;

    Result namespacesById =
        scanPrefix(
            "/accounts/" + acct + "/namespaces/by-id/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += namespacesById.scanned;
    deleted += namespacesById.deleted;
    missingBlobs += namespacesById.missingBlobs;
    staleSecondaries += namespacesById.staleSecondaries;

    Result viewsById =
        scanPrefix(
            "/accounts/" + acct + "/views/by-id/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += viewsById.scanned;
    deleted += viewsById.deleted;
    missingBlobs += viewsById.missingBlobs;
    staleSecondaries += viewsById.staleSecondaries;

    Result connectorsById =
        scanPrefix(
            "/accounts/" + acct + "/connectors/by-id/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += connectorsById.scanned;
    deleted += connectorsById.deleted;
    missingBlobs += connectorsById.missingBlobs;
    staleSecondaries += connectorsById.staleSecondaries;

    Result connectorsByName =
        scanPrefix(
            "/accounts/" + acct + "/connectors/by-name/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += connectorsByName.scanned;
    deleted += connectorsByName.deleted;
    missingBlobs += connectorsByName.missingBlobs;
    staleSecondaries += connectorsByName.staleSecondaries;

    Result catalogsByName =
        scanPrefix(
            "/accounts/" + acct + "/catalogs/by-name/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += catalogsByName.scanned;
    deleted += catalogsByName.deleted;
    missingBlobs += catalogsByName.missingBlobs;
    staleSecondaries += catalogsByName.staleSecondaries;

    Result catalogIndexPointers =
        scanPrefix(
            "/accounts/" + acct + "/catalogs/",
            pageSize,
            deadlineMs,
            blobCache,
            p -> {
              String key = p.getKey();
              return key != null
                  && (key.contains("/namespaces/by-path/")
                      || key.contains("/tables/by-name/")
                      || key.contains("/views/by-name/"));
            },
            nowMs,
            minAgeMs);
    scanned += catalogIndexPointers.scanned;
    deleted += catalogIndexPointers.deleted;
    missingBlobs += catalogIndexPointers.missingBlobs;
    staleSecondaries += catalogIndexPointers.staleSecondaries;

    for (String tableId : tableIds) {
      if (System.currentTimeMillis() >= deadlineMs) {
        break;
      }
      String snapshotsById = "/accounts/" + acct + "/tables/" + tableId + "/snapshots/by-id/";
      Result snapshotById =
          scanPrefix(snapshotsById, pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
      scanned += snapshotById.scanned;
      deleted += snapshotById.deleted;
      missingBlobs += snapshotById.missingBlobs;
      staleSecondaries += snapshotById.staleSecondaries;

      String snapshotsByTime = "/accounts/" + acct + "/tables/" + tableId + "/snapshots/by-time/";
      Result snapshotByTime =
          scanPrefix(snapshotsByTime, pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
      scanned += snapshotByTime.scanned;
      deleted += snapshotByTime.deleted;
      missingBlobs += snapshotByTime.missingBlobs;
      staleSecondaries += snapshotByTime.staleSecondaries;

      String snapshotsRoot = "/accounts/" + acct + "/tables/" + tableId + "/snapshots/";
      Result statsPointers =
          scanPrefix(
              snapshotsRoot,
              pageSize,
              deadlineMs,
              blobCache,
              p -> p.getKey() != null && p.getKey().contains("/stats/"),
              nowMs,
              minAgeMs);
      scanned += statsPointers.scanned;
      deleted += statsPointers.deleted;
      missingBlobs += statsPointers.missingBlobs;
      staleSecondaries += statsPointers.staleSecondaries;
    }

    return new Result(scanned, deleted, missingBlobs, staleSecondaries);
  }

  private Result scanPrefix(
      String prefix,
      int pageSize,
      long deadlineMs,
      Map<String, Boolean> blobCache,
      Predicate<Pointer> filter,
      long nowMs,
      long minAgeMs) {
    String token = "";
    int scanned = 0;
    int deleted = 0;
    int missingBlobs = 0;
    int staleSecondaries = 0;

    while (System.currentTimeMillis() < deadlineMs) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      if (pointers.isEmpty()) {
        break;
      }

      for (Pointer p : pointers) {
        if (System.currentTimeMillis() >= deadlineMs) {
          break;
        }
        if (filter != null && !filter.test(p)) {
          continue;
        }
        if (shouldSkipPointer(p.getKey())) {
          continue;
        }

        scanned++;
        String blobUri = p.getBlobUri();
        if (blobUri == null || blobUri.isBlank()) {
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            deleted++;
          }
          continue;
        }

        Boolean exists = blobCache.get(blobUri);
        if (exists == null) {
          var header = blobStore.head(blobUri).orElse(null);
          exists = header != null;
          blobCache.put(blobUri, exists);
          if (exists && minAgeMs > 0) {
            long lastModified = header.getLastModifiedAt().getSeconds() * 1000L;
            if (nowMs - lastModified < minAgeMs) {
              continue;
            }
          }
        } else if (exists && minAgeMs > 0) {
          var header = blobStore.head(blobUri).orElse(null);
          if (header != null) {
            long lastModified = header.getLastModifiedAt().getSeconds() * 1000L;
            if (nowMs - lastModified < minAgeMs) {
              continue;
            }
          }
        }

        if (!exists) {
          missingBlobs++;
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            deleted++;
          }
          continue;
        }

        String canonicalKey = canonicalPointerForBlobUri(blobUri);
        if (canonicalKey == null || canonicalKey.equals(p.getKey())) {
          continue;
        }

        Optional<Pointer> canonical = pointerStore.get(canonicalKey);
        if (canonical.isEmpty() || !blobUri.equals(canonical.get().getBlobUri())) {
          staleSecondaries++;
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            deleted++;
          }
        }
      }

      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }

    return new Result(scanned, deleted, missingBlobs, staleSecondaries);
  }

  private void collectIds(String prefix, int pageSize, List<String> out) {
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : pointers) {
        String id = decodeSuffix(prefix, p.getKey());
        if (id != null && !id.isBlank()) {
          out.add(id);
        }
      }
      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }
  }

  private static boolean shouldSkipPointer(String key) {
    if (key == null || key.isBlank()) {
      return true;
    }
    return key.contains("/idempotency/") || key.contains("/markers/");
  }

  private static String canonicalPointerForBlobUri(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return null;
    }
    String normalized = blobUri.startsWith("/") ? blobUri.substring(1) : blobUri;
    String[] parts = normalized.split("/");
    if (parts.length < 4) {
      return null;
    }
    if (!"accounts".equals(parts[0])) {
      return null;
    }

    String accountId = parts[1];
    String scope = parts[2];

    if ("account".equals(scope)) {
      return "/accounts/by-id/" + accountId;
    }

    if ("catalogs".equals(scope) && parts.length >= 5 && "catalog".equals(parts[4])) {
      return "/accounts/" + accountId + "/catalogs/by-id/" + parts[3];
    }

    if ("namespaces".equals(scope) && parts.length >= 5 && "namespace".equals(parts[4])) {
      return "/accounts/" + accountId + "/namespaces/by-id/" + parts[3];
    }

    if ("tables".equals(scope) && parts.length >= 5) {
      String tableId = parts[3];
      String sub = parts[4];
      if ("table".equals(sub)) {
        return "/accounts/" + accountId + "/tables/by-id/" + tableId;
      }
      if ("snapshots".equals(sub) && parts.length >= 7 && "snapshot".equals(parts[6])) {
        String snapshotId = parts[5];
        return "/accounts/" + accountId + "/tables/" + tableId + "/snapshots/by-id/" + snapshotId;
      }
      if ("table-stats".equals(sub) || "column-stats".equals(sub) || "file-stats".equals(sub)) {
        return null;
      }
    }

    if ("views".equals(scope) && parts.length >= 5 && "view".equals(parts[4])) {
      return "/accounts/" + accountId + "/views/by-id/" + parts[3];
    }

    if ("connectors".equals(scope) && parts.length >= 5 && "connector".equals(parts[4])) {
      return "/accounts/" + accountId + "/connectors/by-id/" + parts[3];
    }

    return null;
  }

  private static String decodeSuffix(String prefix, String fullKey) {
    if (fullKey == null || !fullKey.startsWith(prefix)) {
      return null;
    }
    String suffix = fullKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return suffix;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }
}
