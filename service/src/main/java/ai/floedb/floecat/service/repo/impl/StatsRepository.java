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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.ColumnStatsKey;
import ai.floedb.floecat.service.repo.model.FileColumnStatsKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TableStatsKey;
import ai.floedb.floecat.service.repo.util.ColumnStatsNormalizer;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class StatsRepository {

  private static final Logger LOG = Logger.getLogger(StatsRepository.class);
  private final LongAdder columnStatsBatchCalls = new LongAdder();
  private final LongAdder columnStatsBatchLatencyNanos = new LongAdder();
  private final LongAdder columnStatsScanCalls = new LongAdder();
  private final LongAdder columnStatsScanPages = new LongAdder();
  private final LongAdder columnStatsScanItems = new LongAdder();
  private final LongAdder columnStatsScanLatencyNanos = new LongAdder();

  // switch to scan mode when >= columns
  private static final int DEFAULT_SMART_SCAN_THRESHOLD = 64;
  // max columns fetched per prefix page
  private static final int DEFAULT_SCAN_COLUMNS_PER_PAGE = 200;
  // max prefix pages before capping a scan (<=0 disables the cap)
  private static final int DEFAULT_MAX_SCAN_PAGES = 5;

  private final GenericResourceRepository<TableStats, TableStatsKey> tableStatsRepo;
  private final GenericResourceRepository<ColumnStats, ColumnStatsKey> columnStatsRepo;
  private final GenericResourceRepository<FileColumnStats, FileColumnStatsKey> fileStatsRepo;
  private final int smartScanThreshold;
  private final int scanColumnsPerPage;
  private final int maxScanPages;

  private static int normalizeNonPositiveToZero(int v) {
    return v <= 0 ? 0 : v;
  }

  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this(
        pointerStore,
        blobStore,
        DEFAULT_SMART_SCAN_THRESHOLD,
        DEFAULT_SCAN_COLUMNS_PER_PAGE,
        DEFAULT_MAX_SCAN_PAGES);
  }

  @Inject
  public StatsRepository(
      PointerStore pointerStore,
      BlobStore blobStore,
      @ConfigProperty(
              name = "floecat.stats.batch.smart-threshold",
              defaultValue = "" + DEFAULT_SMART_SCAN_THRESHOLD)
          int smartThreshold,
      @ConfigProperty(
              name = "floecat.stats.batch.scan.columns-per-page",
              defaultValue = "" + DEFAULT_SCAN_COLUMNS_PER_PAGE)
          int scanColumnsPerPage,
      @ConfigProperty(
              name = "floecat.stats.batch.max-scan-pages",
              defaultValue = "" + DEFAULT_MAX_SCAN_PAGES)
          int maxScanPages) {
    this.smartScanThreshold = smartThreshold;
    this.scanColumnsPerPage = Math.max(1, scanColumnsPerPage);
    this.maxScanPages = normalizeNonPositiveToZero(maxScanPages);

    this.tableStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE_STATS,
            TableStats::parseFrom,
            TableStats::toByteArray,
            "application/x-protobuf");

    this.columnStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.COLUMN_STATS,
            ColumnStats::parseFrom,
            ColumnStats::toByteArray,
            "application/x-protobuf");

    this.fileStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.FILE_COLUMN_STATS,
            FileColumnStats::parseFrom,
            FileColumnStats::toByteArray,
            "application/x-protobuf");
  }

  /**
   * Test/support constructor that bypasses CDI config injection.
   *
   * <p>Prefer the CDI constructor in production; thresholds are configurable via MicroProfile
   * config keys {@code floecat.stats.batch.smart-threshold}, {@code
   * floecat.stats.batch.scan.columns-per-page}, and {@code floecat.stats.batch.max-scan-pages}.
   */
  public static StatsRepository forTesting(
      PointerStore pointerStore,
      BlobStore blobStore,
      int smartThreshold,
      int scanColumnsPerPage,
      int maxScanPages) {
    return new StatsRepository(
        pointerStore, blobStore, smartThreshold, scanColumnsPerPage, maxScanPages);
  }

  public void putTableStats(ResourceId tableId, long snapshotId, TableStats value) {
    tableStatsRepo.create(value);
  }

  public Optional<TableStats> getTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.getByKey(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public Optional<TableStatsView> getTableStatsView(ResourceId tableId, long snapshotId) {
    return getTableStats(tableId, snapshotId).map(StatsRepository::tableStatsView);
  }

  public boolean deleteTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.delete(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public void putColumnStats(ResourceId tableId, long snapshotId, ColumnStats value) {
    columnStatsRepo.create(value);
  }

  public Optional<ColumnStats> getColumnStats(ResourceId tableId, long snapshotId, long columnId) {
    return columnStatsRepo.getByKey(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  /**
   * Read stats per column in a simple loop. This path is still used for small batches and keeps the
   * existing debug instrumentation for call/latency tracking.
   */
  public List<ColumnStats> getColumnStatsBatch(
      ResourceId tableId, long snapshotId, List<Long> columnIds) {
    long start = System.nanoTime();
    List<ColumnStats> stats = new ArrayList<>(columnIds.size());
    for (Long columnId : columnIds) {
      getColumnStats(tableId, snapshotId, columnId).ifPresent(stats::add);
    }
    long duration = System.nanoTime() - start;
    columnStatsBatchCalls.increment();
    columnStatsBatchLatencyNanos.add(duration);
    if (LOG.isDebugEnabled()) {
      long totalCalls = columnStatsBatchCalls.sum();
      long totalLatency = columnStatsBatchLatencyNanos.sum();
      long avgMs = TimeUnit.NANOSECONDS.toMillis(totalLatency / Math.max(1, totalCalls));
      LOG.debugf(
          "column stats batch read: account=%s table=%s snapshot=%d columns=%d took=%dms"
              + " (totalCalls=%d avg=%dms)",
          tableId.getAccountId(),
          tableId.getId(),
          snapshotId,
          columnIds.size(),
          TimeUnit.NANOSECONDS.toMillis(duration),
          totalCalls,
          avgMs);
    }
    return stats;
  }

  /**
   * Smart batch: for wide requests we scan the snapshot prefix once and filter the results, falling
   * back to the per-column loop for small batches. This greatly reduces round-trips when the
   * planner asks for many columns (the heuristic threshold is configurable via {@code
   * floecat.stats.batch.smart-threshold}).
   */
  public ColumnStatsBatchResult getColumnStatsBatchSmart(
      ResourceId tableId, long snapshotId, List<Long> columnIds) {
    if (columnIds == null || columnIds.isEmpty()) {
      return new ColumnStatsBatchResult(Map.of(), false, 0);
    }
    if (smartScanThreshold > 0 && columnIds.size() >= smartScanThreshold) {
      ColumnStatsScanResult scanResult = scanColumnStats(tableId, snapshotId, columnIds);
      Map<Long, ColumnStats> map = new LinkedHashMap<>(scanResult.found());
      if (scanResult.capped() && map.size() < columnIds.size()) {
        List<Long> missing = new ArrayList<>(columnIds.size() - map.size());
        for (Long columnId : columnIds) {
          if (!map.containsKey(columnId)) {
            missing.add(columnId);
          }
        }
        if (!missing.isEmpty()) {
          for (ColumnStats cs : getColumnStatsBatch(tableId, snapshotId, missing)) {
            map.put(cs.getColumnId(), cs);
          }
        }
      }
      return new ColumnStatsBatchResult(map, scanResult.capped(), scanResult.pagesScanned());
    }
    List<ColumnStats> results = getColumnStatsBatch(tableId, snapshotId, columnIds);
    Map<Long, ColumnStats> map = new LinkedHashMap<>(results.size());
    for (ColumnStats cs : results) {
      map.put(cs.getColumnId(), cs);
    }
    return new ColumnStatsBatchResult(map, false, 0);
  }

  /**
   * Expose the configured maximum number of prefix pages scanned before capping. Non-positive
   * values mean the cap is disabled.
   */
  public int maxScanPages() {
    return maxScanPages;
  }

  private ColumnStatsScanResult scanColumnStats(
      ResourceId tableId, long snapshotId, List<Long> columnIds) {
    long start = System.nanoTime();
    int pagesScanned = 0;
    int itemsExamined = 0;
    boolean capped = false;
    Set<Long> targets = new HashSet<>(columnIds);
    Map<Long, ColumnStats> found = new LinkedHashMap<>();
    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      List<ColumnStats> page = list(tableId, snapshotId, scanColumnsPerPage, token, next);
      pagesScanned++;
      itemsExamined += page.size();
      for (ColumnStats cs : page) {
        long columnId = cs.getColumnId();
        if (targets.contains(columnId) && !found.containsKey(columnId)) {
          found.put(columnId, cs);
        }
      }
      if (found.size() >= targets.size()) {
        break;
      }
      if (maxScanPages > 0 && pagesScanned >= maxScanPages) {
        capped = true;
        break;
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    long duration = System.nanoTime() - start;
    columnStatsScanCalls.increment();
    columnStatsScanLatencyNanos.add(duration);
    columnStatsScanPages.add(pagesScanned);
    columnStatsScanItems.add(itemsExamined);

    if (LOG.isDebugEnabled()) {
      long totalCalls = columnStatsScanCalls.sum();
      long totalPages = columnStatsScanPages.sum();
      long totalItems = columnStatsScanItems.sum();
      long avgPages = Math.round((double) totalPages / Math.max(1, totalCalls));
      long avgItems = Math.round((double) totalItems / Math.max(1, totalCalls));
      LOG.debugf(
          "column stats scan: account=%s table=%s snapshot=%d pages=%d items=%d duration=%dms"
              + " capped=%b found=%d/%d totalCalls=%d avgPages=%d avgItems=%d",
          tableId.getAccountId(),
          tableId.getId(),
          snapshotId,
          pagesScanned,
          itemsExamined,
          TimeUnit.NANOSECONDS.toMillis(duration),
          capped,
          found.size(),
          targets.size(),
          totalCalls,
          avgPages,
          avgItems);
    }
    if (capped) {
      LOG.warnf(
          "column stats scan capped after %d pages for %s snapshot %d (found %d/%d columns)",
          maxScanPages, tableId.getId(), snapshotId, found.size(), targets.size());
    }
    return new ColumnStatsScanResult(found, capped, pagesScanned);
  }

  private static final record ColumnStatsScanResult(
      Map<Long, ColumnStats> found, boolean capped, int pagesScanned) {}

  public static final record ColumnStatsBatchResult(
      Map<Long, ColumnStats> stats, boolean capped, int pagesScanned) {}

  public int putColumnStats(ResourceId tableId, long snapshotId, List<ColumnStats> batch) {
    int created = 0;
    for (ColumnStats cs : batch) {
      ColumnStatsKey key =
          new ColumnStatsKey(
              cs.getTableId().getAccountId(),
              cs.getTableId().getId(),
              cs.getSnapshotId(),
              cs.getColumnId(),
              "");
      if (columnStatsRepo.getByKey(key).isEmpty()) {
        created++;
      }
      columnStatsRepo.create(cs);
    }
    return created;
  }

  public List<ColumnStats> list(
      ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return columnStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  public int count(ResourceId tableId, long snapshotId) {
    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return columnStatsRepo.countByPrefix(prefix);
  }

  public void putFileColumnStats(ResourceId tableId, long snapshotId, FileColumnStats value) {
    fileStatsRepo.create(value);
  }

  public Optional<FileColumnStats> getFileColumnStats(
      ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.getByKey(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }

  public int putFileColumnStats(ResourceId tableId, long snapshotId, List<FileColumnStats> batch) {
    int created = 0;
    for (FileColumnStats fs : batch) {
      FileColumnStatsKey key =
          new FileColumnStatsKey(
              fs.getTableId().getAccountId(),
              fs.getTableId().getId(),
              fs.getSnapshotId(),
              fs.getFilePath(),
              "");
      if (fileStatsRepo.getByKey(key).isEmpty()) {
        created++;
      }
      fileStatsRepo.create(fs);
    }
    return created;
  }

  public List<FileColumnStats> listFileStats(
      ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    String prefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return fileStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  public int countFileStats(ResourceId tableId, long snapshotId) {
    String prefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return fileStatsRepo.countByPrefix(prefix);
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    deleteTableStats(tableId, snapshotId);

    String colPrefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      List<ColumnStats> page = columnStatsRepo.listByPrefix(colPrefix, 200, token, next);
      for (ColumnStats cs : page) {
        String sha = ColumnStatsNormalizer.sha256Hex(cs.toByteArray());
        ColumnStatsKey key =
            new ColumnStatsKey(
                cs.getTableId().getAccountId(),
                cs.getTableId().getId(),
                cs.getSnapshotId(),
                cs.getColumnId(),
                sha);
        columnStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    String filePrefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    token = "";
    next.setLength(0);

    do {
      List<FileColumnStats> page = fileStatsRepo.listByPrefix(filePrefix, 200, token, next);
      for (FileColumnStats fs : page) {
        String sha = ColumnStatsNormalizer.sha256Hex(fs.toByteArray());
        FileColumnStatsKey key =
            new FileColumnStatsKey(
                fs.getTableId().getAccountId(),
                fs.getTableId().getId(),
                fs.getSnapshotId(),
                fs.getFilePath(),
                sha);
        fileStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""), nowTs);
  }

  public MutationMeta metaForTableStatsSafe(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaForSafe(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public MutationMeta metaForColumnStats(ResourceId tableId, long snapshotId, long columnId) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  public MutationMeta metaForColumnStats(
      ResourceId tableId, long snapshotId, long columnId, Timestamp nowTs) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""),
        nowTs);
  }

  public MutationMeta metaForColumnStatsSafe(ResourceId tableId, long snapshotId, long columnId) {
    return columnStatsRepo.metaForSafe(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  public MutationMeta metaForFileColumnStats(ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.metaFor(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }

  public MutationMeta metaForFileColumnStats(
      ResourceId tableId, long snapshotId, String filePath, Timestamp nowTs) {
    return fileStatsRepo.metaFor(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""),
        nowTs);
  }

  public MutationMeta metaForFileColumnStatsSafe(
      ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.metaForSafe(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }

  private static TableStatsView tableStatsView(TableStats stats) {
    return new TableStatsView(
        stats.getTableId(), stats.getSnapshotId(), stats.getRowCount(), stats.getTotalSizeBytes());
  }

  public static final class TableStatsView {
    private final ResourceId tableId;
    private final long snapshotId;
    private final long rowCount;
    private final long totalSizeBytes;

    private TableStatsView(
        ResourceId tableId, long snapshotId, long rowCount, long totalSizeBytes) {
      this.tableId = tableId;
      this.snapshotId = snapshotId;
      this.rowCount = rowCount;
      this.totalSizeBytes = totalSizeBytes;
    }

    public ResourceId tableId() {
      return tableId;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public long rowCount() {
      return rowCount;
    }

    public long totalSizeBytes() {
      return totalSizeBytes;
    }
  }
}
