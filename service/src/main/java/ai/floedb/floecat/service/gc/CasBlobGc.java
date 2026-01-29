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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class CasBlobGc {

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;

  public record Result(
      int pointersScanned, int blobsScanned, int blobsDeleted, int referenced, int tablesScanned) {}

  public Result runForAccount(String accountId) {
    final var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.cas.page-size", Integer.class).orElse(500);
    final long minAgeMs =
        cfg.getOptionalValue("floecat.gc.cas.min-age-ms", Long.class).orElse(30_000L);
    final long nowMs = System.currentTimeMillis();

    Set<String> referenced = new HashSet<>();
    List<String> tableIds = new ArrayList<>();
    int pointersScanned = 0;

    var accountPtr = pointerStore.get(Keys.accountPointerById(accountId)).orElse(null);
    if (accountPtr != null && !accountPtr.getBlobUri().isBlank()) {
      referenced.add(normalizeKey(accountPtr.getBlobUri()));
      pointersScanned++;
    }

    pointersScanned +=
        collectPointers(Keys.catalogPointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.namespacePointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.tablePointerByIdPrefix(accountId), referenced, tableIds, pageSize);
    pointersScanned +=
        collectPointers(Keys.viewPointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.connectorPointerByIdPrefix(accountId), referenced, null, pageSize);

    int tablesScanned = 0;
    for (String tableId : tableIds) {
      tablesScanned++;
      String snapshotsById = Keys.snapshotPointerByIdPrefix(accountId, tableId);
      pointersScanned += collectPointers(snapshotsById, referenced, null, pageSize);

      String snapshotsRoot = Keys.snapshotRootPrefix(accountId, tableId);
      pointersScanned +=
          collectPointers(
              snapshotsRoot,
              referenced,
              null,
              pageSize,
              p -> p.getKey() != null && p.getKey().contains(Keys.SEG_STATS));
    }

    int blobsScanned = 0;
    int blobsDeleted = 0;

    var account =
        deleteUnreferenced(
            Keys.accountBlobPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_ACCOUNT),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += account.scanned();
    blobsDeleted += account.deleted();

    var catalogs =
        deleteUnreferenced(
            Keys.catalogRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_CATALOG),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += catalogs.scanned();
    blobsDeleted += catalogs.deleted();

    var namespaces =
        deleteUnreferenced(
            Keys.namespaceRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_NAMESPACE),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += namespaces.scanned();
    blobsDeleted += namespaces.deleted();

    var tables =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_TABLE),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += tables.scanned();
    blobsDeleted += tables.deleted();

    var snapshots =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_SNAPSHOTS) && key.contains(Keys.SEG_SNAPSHOT),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += snapshots.scanned();
    blobsDeleted += snapshots.deleted();

    var tableStats =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_TABLE_STATS),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += tableStats.scanned();
    blobsDeleted += tableStats.deleted();

    var columnStats =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_COLUMN_STATS),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += columnStats.scanned();
    blobsDeleted += columnStats.deleted();

    var fileStats =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_FILE_STATS),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += fileStats.scanned();
    blobsDeleted += fileStats.deleted();

    var views =
        deleteUnreferenced(
            Keys.viewRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_VIEW),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += views.scanned();
    blobsDeleted += views.deleted();

    var connectors =
        deleteUnreferenced(
            Keys.connectorRootPrefix(accountId),
            referenced,
            key -> key.contains(Keys.SEG_CONNECTOR),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += connectors.scanned();
    blobsDeleted += connectors.deleted();

    return new Result(
        pointersScanned, blobsScanned, blobsDeleted, referenced.size(), tablesScanned);
  }

  private int collectPointers(
      String prefix, Set<String> referenced, List<String> tableIds, int pageSize) {
    return collectPointers(prefix, referenced, tableIds, pageSize, null);
  }

  private int collectPointers(
      String prefix,
      Set<String> referenced,
      List<String> tableIds,
      int pageSize,
      Predicate<Pointer> filter) {
    String token = "";
    int scanned = 0;

    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : pointers) {
        if (filter != null && !filter.test(p)) {
          continue;
        }
        scanned++;
        if (p.getBlobUri() != null && !p.getBlobUri().isBlank()) {
          referenced.add(normalizeKey(p.getBlobUri()));
        }
        if (tableIds != null) {
          String id = decodeSuffix(prefix, p.getKey());
          if (id != null && !id.isBlank()) {
            tableIds.add(id);
          }
        }
      }

      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }
    return scanned;
  }

  private record DeleteResult(int scanned, int deleted) {}

  private DeleteResult deleteUnreferenced(
      String prefix,
      Set<String> referenced,
      Predicate<String> isCandidate,
      int pageSize,
      long nowMs,
      long minAgeMs) {
    String token = "";
    int scanned = 0;
    int deleted = 0;

    while (true) {
      BlobStore.Page page = blobStore.list(prefix, pageSize, token);
      for (String key : page.keys()) {
        scanned++;
        String normalized = normalizeKey(key);
        if (!isCandidate.test(normalized)) {
          continue;
        }
        if (!referenced.contains(normalized)) {
          if (minAgeMs > 0) {
            var header = blobStore.head(key).orElse(null);
            if (header != null) {
              long lastModified = header.getLastModifiedAt().getSeconds() * 1000L;
              if (nowMs - lastModified < minAgeMs) {
                continue;
              }
            }
          }
          if (blobStore.delete(key)) {
            deleted++;
          }
        }
      }
      token = page.nextToken();
      if (token == null || token.isBlank()) {
        break;
      }
    }

    return new DeleteResult(scanned, deleted);
  }

  private static String decodeSuffix(String prefix, String fullKey) {
    if (fullKey == null || !fullKey.startsWith(prefix)) {
      return null;
    }
    String suffix = fullKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
  }

  private static String normalizeKey(String key) {
    if (key == null) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }
}
