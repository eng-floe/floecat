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
import ai.floedb.floecat.service.catalog.impl.StatsVisibilityGate;
import ai.floedb.floecat.service.metagraph.snapshot.SnapshotRetentionPolicy;
import ai.floedb.floecat.service.repo.impl.SnapshotManifests;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Predicate;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class PointerGc {

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TableRootRepository tableRootRepository;
  @Inject StatsRepository statsRepository;

  @Inject
  SnapshotRetentionPolicy retentionPolicy =
      new SnapshotRetentionPolicy(Clock.systemUTC(), Duration.ZERO, Duration.ofDays(7));

  public record Result(int scanned, int deleted, int missingBlobs, int staleSecondaries) {}

  /**
   * Account directory pointers are global control-plane indexes, not child data owned by one
   * account pass. Keep their historical global sweep so orphaned by-id and by-name rows left by a
   * crash or partial account deletion are still reclaimed.
   */
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
    Result byId =
        scanPrefix(
            Keys.accountPointerByIdPrefix(),
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    Result byName =
        scanPrefix(
            Keys.accountPointerByNamePrefix(),
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    return new Result(
        byId.scanned + byName.scanned,
        byId.deleted + byName.deleted,
        byId.missingBlobs + byName.missingBlobs,
        byId.staleSecondaries + byName.staleSecondaries);
  }

  public Result runForAccount(String accountId, long deadlineMs) {
    return runForAccountInternal(accountId, deadlineMs);
  }

  private Result runForAccountInternal(String accountId, long deadlineMs) {
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
            Keys.tablePointerByIdPrefix(accountId),
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

    collectIds(Keys.tablePointerByIdPrefix(accountId), pageSize, tableIds);

    Result catalogsById =
        scanPrefix(
            Keys.catalogPointerByIdPrefix(accountId),
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
            Keys.namespacePointerByIdPrefix(accountId),
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
            Keys.viewPointerByIdPrefix(accountId),
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
            Keys.connectorPointerByIdPrefix(accountId),
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
            Keys.connectorPointerByNamePrefix(accountId),
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

    Result integrationsById =
        scanPrefix(
            Keys.catalogIntegrationPointerByIdPrefix(accountId),
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += integrationsById.scanned;
    deleted += integrationsById.deleted;
    missingBlobs += integrationsById.missingBlobs;
    staleSecondaries += integrationsById.staleSecondaries;

    Result integrationsByName =
        scanPrefix(
            Keys.catalogIntegrationPointerByNamePrefix(accountId),
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += integrationsByName.scanned;
    deleted += integrationsByName.deleted;
    missingBlobs += integrationsByName.missingBlobs;
    staleSecondaries += integrationsByName.staleSecondaries;

    Result overlaysById =
        scanPrefix(
            Keys.catalogOverlayPointerByIdPrefix(accountId),
            pageSize,
            deadlineMs,
            blobCache,
            p -> true,
            nowMs,
            minAgeMs);
    scanned += overlaysById.scanned;
    deleted += overlaysById.deleted;
    missingBlobs += overlaysById.missingBlobs;
    staleSecondaries += overlaysById.staleSecondaries;

    Result overlaySecondaryPointers =
        scanPrefix(
            Keys.catalogOverlayRootPrefix(accountId),
            pageSize,
            deadlineMs,
            blobCache,
            p -> {
              String key = p.getKey();
              return key != null
                  && (key.contains("/by-name/")
                      || key.contains("/by-integration/")
                      || key.contains("/by-catalog/"));
            },
            nowMs,
            minAgeMs);
    scanned += overlaySecondaryPointers.scanned;
    deleted += overlaySecondaryPointers.deleted;
    missingBlobs += overlaySecondaryPointers.missingBlobs;
    staleSecondaries += overlaySecondaryPointers.staleSecondaries;

    Result catalogsByName =
        scanPrefix(
            Keys.catalogPointerByNamePrefix(accountId),
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
            Keys.catalogRootPrefix(accountId),
            pageSize,
            deadlineMs,
            blobCache,
            p -> {
              String key = p.getKey();
              return key != null
                  && (key.contains(Keys.SEG_NAMESPACE_BY_PATH)
                      || key.contains(Keys.SEG_TABLES_BY_NAME)
                      || key.contains(Keys.SEG_VIEWS_BY_NAME));
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
      String snapshotsById = Keys.snapshotPointerByIdPrefix(accountId, tableId);
      OptionalLong currentSnapshotId = currentSnapshotId(accountId, tableId);
      // A missing or unreadable root is not proof that every historical snapshot is orphaned.
      // Leave only the retention-dependent deletion for a later pass rather than skipping the
      // independent secondary-index sweeps below.
      if (currentSnapshotId.isPresent()) {
        deleted +=
            deleteExpiredSnapshotPointers(
                snapshotsById, pageSize, deadlineMs, currentSnapshotId.getAsLong());
      }
      Result snapshotById =
          scanPrefix(snapshotsById, pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
      scanned += snapshotById.scanned;
      deleted += snapshotById.deleted;
      missingBlobs += snapshotById.missingBlobs;
      staleSecondaries += snapshotById.staleSecondaries;

      String snapshotsByTime = Keys.snapshotPointerByTimePrefix(accountId, tableId);
      Result snapshotByTime =
          scanPrefix(snapshotsByTime, pageSize, deadlineMs, blobCache, p -> true, nowMs, minAgeMs);
      scanned += snapshotByTime.scanned;
      deleted += snapshotByTime.deleted;
      missingBlobs += snapshotByTime.missingBlobs;
      staleSecondaries += snapshotByTime.staleSecondaries;

      String snapshotsRoot = Keys.snapshotRootPrefix(accountId, tableId);
      Result statsPointers =
          scanPrefix(
              snapshotsRoot,
              pageSize,
              deadlineMs,
              blobCache,
              p -> p.getKey() != null && p.getKey().contains(Keys.SEG_STATS),
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
        if (!PointerReferences.isBlobPointer(p)) {
          continue;
        }
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

  /** Removes expired canonical snapshot pointers so CAS GC can reclaim their immutable blobs. */
  private int deleteExpiredSnapshotPointers(
      String prefix, int pageSize, long deadlineMs, long currentSnapshotId) {
    if (!retentionPolicy.isRetentionEnabled()) {
      return 0;
    }
    int deleted = 0;
    String token = "";
    while (System.currentTimeMillis() < deadlineMs) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      if (pointers.isEmpty()) {
        break;
      }
      for (Pointer pointer : pointers) {
        var ingestedAt = snapshotIngestedAt(pointer);
        // Legacy backfill may CAS the timestamp onto the pointer and increment its version.
        Pointer current =
            pointer.hasIngestedAt() ? pointer : pointerStore.get(pointer.getKey()).orElse(pointer);
        boolean eligible =
            snapshotId(pointer.getKey()) != currentSnapshotId
                && retentionPolicy.gcEligible(ingestedAt);
        boolean removed =
            eligible && pointerStore.compareAndDelete(current.getKey(), current.getVersion());
        if (removed) {
          deleted++;
        }
      }
      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }
    return deleted;
  }

  private OptionalLong currentSnapshotId(String accountId, String tableId) {
    var rootPointer = pointerStore.get(Keys.tableRootByTable(accountId, tableId)).orElse(null);
    if (rootPointer == null || rootPointer.getBlobUri().isBlank()) {
      return OptionalLong.empty();
    }
    try {
      var root =
          tableRootRepository == null
              ? ai.floedb.floecat.catalog.rpc.TableRoot.parseFrom(
                  blobStore.get(rootPointer.getBlobUri()))
              : tableRootRepository.getByBlobUri(rootPointer.getBlobUri()).orElse(null);
      if (root == null || !root.hasCurrentSnapshotId()) {
        return OptionalLong.empty();
      }
      long committedCurrent = root.getCurrentSnapshotId();
      if (tableRootRepository == null
          || statsRepository == null
          || !StatsVisibilityGate.gateOnFinalize(statsRepository)
          || !root.hasSnapshotManifestRef()) {
        return OptionalLong.of(committedCurrent);
      }
      var committedEntry =
          SnapshotManifests.findEntry(
                  tableRootRepository, root.getSnapshotManifestRef(), committedCurrent)
              .orElse(null);
      if (committedEntry == null || committedEntry.hasStatsGenerationRef()) {
        return OptionalLong.of(committedCurrent);
      }
      return SnapshotManifests.latestQueryableCurrent(
              tableRootRepository, root.getSnapshotManifestRef(), committedEntry)
          .map(entry -> OptionalLong.of(entry.getSnapshotId()))
          .orElse(OptionalLong.empty());
    } catch (RuntimeException | com.google.protobuf.InvalidProtocolBufferException e) {
      return OptionalLong.empty();
    }
  }

  private long snapshotId(String key) {
    int slash = key == null ? -1 : key.lastIndexOf('/');
    if (slash < 0) {
      return Long.MIN_VALUE;
    }
    try {
      return Long.parseLong(key.substring(slash + 1));
    } catch (NumberFormatException e) {
      return Long.MIN_VALUE;
    }
  }

  private com.google.protobuf.Timestamp snapshotIngestedAt(Pointer pointer) {
    if (pointer != null && pointer.hasIngestedAt()) {
      return pointer.getIngestedAt();
    }
    try {
      var snapshot =
          ai.floedb.floecat.catalog.rpc.Snapshot.parseFrom(blobStore.get(pointer.getBlobUri()));
      if (!snapshot.hasIngestedAt()) {
        return null;
      }
      var ingestedAt = snapshot.getIngestedAt();
      if (pointer != null && pointer.getVersion() > 0L) {
        pointerStore.compareAndSet(
            pointer.getKey(),
            pointer.getVersion(),
            pointer.toBuilder().setIngestedAt(ingestedAt).build());
      }
      return ingestedAt;
    } catch (RuntimeException | com.google.protobuf.InvalidProtocolBufferException e) {
      return null;
    }
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
    // Segment-exact, so a namespace named "markers" is not mistaken for a marker key and
    // silently skipped: the key vocabulary owns this test, not a substring match here.
    return Keys.isIdempotencyOrMarkerKey(key);
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

    String accountId = decode(parts[1]);
    String scope = parts[2];

    if ("account".equals(scope)) {
      return Keys.accountPointerById(accountId);
    }

    if ("catalogs".equals(scope) && parts.length >= 5 && "catalog".equals(parts[4])) {
      return Keys.catalogPointerById(accountId, decode(parts[3]));
    }

    if ("namespaces".equals(scope) && parts.length >= 5 && "namespace".equals(parts[4])) {
      return Keys.namespacePointerById(accountId, decode(parts[3]));
    }

    if ("tables".equals(scope) && parts.length >= 5) {
      String tableId = decode(parts[3]);
      String sub = parts[4];
      if ("table".equals(sub)) {
        return Keys.tablePointerById(accountId, tableId);
      }
      if ("snapshots".equals(sub) && parts.length >= 7 && "snapshot".equals(parts[6])) {
        long snapshotId = parseLong(decode(parts[5]));
        return Keys.snapshotPointerById(accountId, tableId, snapshotId);
      }
      if ("target-stats".equals(sub) || "file-stats".equals(sub)) {
        return null;
      }
    }

    if ("views".equals(scope) && parts.length >= 5 && "view".equals(parts[4])) {
      return Keys.viewPointerById(accountId, decode(parts[3]));
    }

    if ("connectors".equals(scope) && parts.length >= 5 && "connector".equals(parts[4])) {
      return Keys.connectorPointerById(accountId, decode(parts[3]));
    }

    if ("catalog-integrations".equals(scope)
        && parts.length >= 5
        && "integration".equals(parts[4])) {
      return Keys.catalogIntegrationPointerById(accountId, decode(parts[3]));
    }

    if ("catalog-overlays".equals(scope) && parts.length >= 5 && "overlay".equals(parts[4])) {
      return Keys.catalogOverlayPointerById(accountId, decode(parts[3]));
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
    return decode(suffix);
  }

  private static String decode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static long parseLong(String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return 0L;
    }
  }

  private static String encode(String value) {
    return Keys.encodeSegment(value);
  }
}
