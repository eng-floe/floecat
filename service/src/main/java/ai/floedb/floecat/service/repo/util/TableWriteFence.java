/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/** Keeps table-scoped writes atomic with the continued existence of their table row. */
public final class TableWriteFence {
  private TableWriteFence() {}

  public record FencedBatch(boolean writable, List<PointerStore.CasOp> operations) {}

  /** A table-scoped write lost because its owning table row was deleted. */
  public static final class TableDeletedException extends BaseResourceRepository.NotFoundException {
    public TableDeletedException(String tableRow) {
      super(ResourceKind.RK_TABLE, "table deleted during scoped publication: " + tableRow);
    }
  }

  /** Distinguishes a permanent table deletion from an ordinary retryable CAS conflict. */
  public static void requireTableRowsPresent(
      PointerStore pointerStore, List<? extends PointerStore.CasOp> operations) {
    for (String row : tableRowsRequiringChecks(operations)) {
      if (pointerStore.get(row).isEmpty()) {
        throw new TableDeletedException(row);
      }
    }
  }

  /**
   * Adds a version check for every table row named by an upsert in {@code operations}, and the
   * purge-marker operations owed by every table row the batch creates or deletes.
   *
   * <p>A batch that creates or updates the table row already fences itself. Deletes do not need a
   * liveness check. This makes a table-row delete serialize against every write below that table: a
   * write that wins first is removed by the following purge, and a write that loses cannot recreate
   * state after the row is gone. The purge marker closes the remaining gap, a recreation of the
   * same table id while that purge is still pending (see {@link #purgeMarkerOps}).
   */
  public static FencedBatch fence(
      PointerStore pointerStore, List<? extends PointerStore.CasOp> operations) {
    List<PointerStore.CasOp> source = operations == null ? List.of() : List.copyOf(operations);
    Set<String> rows = tableRowsRequiringChecks(source);
    List<PointerStore.CasOp> purgeMarkers = purgeMarkerOps(source);
    if (rows.isEmpty() && purgeMarkers.isEmpty()) {
      return new FencedBatch(true, source);
    }
    List<PointerStore.CasOp> fenced =
        new ArrayList<>(source.size() + rows.size() + purgeMarkers.size());
    for (String row : rows) {
      // pointerStore is the raw durable store, never a cached view, so this read is authoritative.
      Optional<Pointer> current = pointerStore.get(row);
      if (current.isEmpty()) {
        return new FencedBatch(false, List.of());
      }
      fenced.add(new PointerStore.CasCheck(row, current.orElseThrow().getVersion()));
    }
    fenced.addAll(source);
    fenced.addAll(purgeMarkers);
    return new FencedBatch(true, List.copyOf(fenced));
  }

  /** Whether {@link #fence} would change {@code operations}, so a single-key write may skip it. */
  public static boolean requiresFence(List<? extends PointerStore.CasOp> operations) {
    return !tableRowsRequiringChecks(operations).isEmpty() || !purgeMarkerOps(operations).isEmpty();
  }

  /**
   * How many operations {@link #fence} adds to {@code operations} when it accepts them.
   *
   * <p>Callers that enforce a backend's per-transaction operation limit must count these, because
   * the fence is applied below them by the store. Counting needs no reads: a fence either adds
   * exactly this many operations or refuses the batch.
   *
   * @throws IllegalArgumentException if the batch deletes a table row and writes that table's state
   */
  public static int addedOperationCount(List<? extends PointerStore.CasOp> operations) {
    return tableRowsRequiringChecks(operations).size() + purgeMarkerOps(operations).size();
  }

  /** Whether {@code pointerKey} is a user table's canonical row. */
  public static boolean isTableRow(String pointerKey) {
    TableRef table = tableOf(pointerKey);
    return table != null && table.row().equals(normalize(pointerKey));
  }

  /**
   * The purge-marker operations owed by the table rows {@code operations} creates or deletes.
   *
   * <p>A row delete writes {@link Keys#tablePurgePointer} in the same batch, so the cleanup that
   * follows it is durable work: the purge removes the marker only once the table-scoped state is
   * gone, and pointer GC retries any marker a failed or interrupted purge left behind. A row create
   * requires the marker absent, so a table id cannot be reused while the state of its previous
   * incarnation is still being purged; that purge would otherwise delete the new table's state.
   *
   * <p>Each delete gives its marker a random version. A purge fences its deletes on the version it
   * started from, so a stalled purge of one deletion can never match the marker of a later one.
   */
  static List<PointerStore.CasOp> purgeMarkerOps(List<? extends PointerStore.CasOp> operations) {
    if (operations == null) {
      return List.of();
    }
    Set<String> batchKeys = new HashSet<>();
    for (PointerStore.CasOp operation : operations) {
      batchKeys.add(normalize(operation.key()));
    }
    Map<String, PointerStore.CasOp> markers = new LinkedHashMap<>();
    for (PointerStore.CasOp operation : operations) {
      if (!isTableRow(operation.key())) {
        continue;
      }
      TableRef table = tableOf(operation.key());
      String marker = table.purgeMarker();
      if (batchKeys.contains(marker)) {
        continue;
      }
      if (operation instanceof PointerStore.CasDelete) {
        long version = ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE);
        markers.put(
            marker,
            new PointerStore.UnconditionalUpsert(
                marker, PointerReferences.opaqueMarkerPointer(marker, table.row(), version)));
      } else if ((operation instanceof PointerStore.CasUpsert upsert
              && upsert.expectedVersion() == 0L)
          || operation instanceof PointerStore.UnconditionalUpsert) {
        markers.putIfAbsent(marker, new PointerStore.CasCheckAbsent(marker));
      }
    }
    return List.copyOf(markers.values());
  }

  /** Table-row keys that must be checked by a backend writing the supplied operations directly. */
  public static Set<String> tableRowsRequiringChecks(
      List<? extends PointerStore.CasOp> operations) {
    Set<String> writtenRows = new LinkedHashSet<>();
    Set<String> deletedRows = new LinkedHashSet<>();
    Set<String> existingChecks = new LinkedHashSet<>();
    Set<String> touchedRows = new LinkedHashSet<>();
    if (operations == null) {
      return Set.of();
    }
    for (PointerStore.CasOp operation : operations) {
      String operationRow = canonicalTableRow(operation.key());
      if (operationRow != null && operationRow.equals(normalize(operation.key()))) {
        if (operation instanceof PointerStore.CasUpsert
            || operation instanceof PointerStore.UnconditionalUpsert) {
          writtenRows.add(operationRow);
        } else if (operation instanceof PointerStore.CasDelete) {
          deletedRows.add(operationRow);
        } else if (operation instanceof PointerStore.CasCheck) {
          existingChecks.add(operationRow);
        }
      }
      if (operation instanceof PointerStore.CasUpsert upsert) {
        collectTouchedRow(upsert.key(), touchedRows);
        if (PointerReferences.isPointerKeyPointer(upsert.next())) {
          collectTouchedRow(upsert.next().getBlobUri(), touchedRows);
        }
      } else if (operation instanceof PointerStore.UnconditionalUpsert upsert) {
        collectTouchedRow(upsert.key(), touchedRows);
        if (PointerReferences.isPointerKeyPointer(upsert.next())) {
          collectTouchedRow(upsert.next().getBlobUri(), touchedRows);
        }
      }
    }
    Set<String> deletedAndTouched = new LinkedHashSet<>(touchedRows);
    deletedAndTouched.retainAll(deletedRows);
    if (!deletedAndTouched.isEmpty()) {
      throw new IllegalArgumentException(
          "a batch cannot delete a table row and write table-scoped state: "
              + deletedAndTouched.iterator().next());
    }
    touchedRows.removeAll(writtenRows);
    touchedRows.removeAll(existingChecks);
    return Set.copyOf(touchedRows);
  }

  private static void collectTouchedRow(String pointerKey, Set<String> rows) {
    String row = canonicalTableRow(pointerKey);
    if (row != null && !row.equals(normalize(pointerKey))) {
      rows.add(row);
    }
  }

  /** The canonical row of the user table {@code pointerKey} belongs to, or null if none. */
  public static String canonicalTableRow(String pointerKey) {
    TableRef table = tableOf(pointerKey);
    return table == null ? null : table.row();
  }

  /**
   * The user table a pointer key belongs to, decoded, with its keys rebuilt through {@link Keys}.
   */
  private record TableRef(String accountId, String tableId) {
    String row() {
      return Keys.tablePointerById(accountId, tableId);
    }

    String purgeMarker() {
      return Keys.tablePurgePointer(accountId, tableId);
    }
  }

  private static TableRef tableOf(String pointerKey) {
    String normalized = normalize(pointerKey);
    if (normalized == null) return null;
    String[] parts = normalized.substring(1).split("/", -1);
    if (parts.length < 4 || !"accounts".equals(parts[0])) return null;
    String account = parts[1];
    if (account.isBlank()
        || "by-id".equals(account)
        || "by-name".equals(account)
        || SystemNodeRegistry.SYSTEM_ACCOUNT.equals(account)) return null;

    String table = null;
    if ("tables".equals(parts[2]) && parts.length >= 5) {
      table = parts[3];
      if ("by-id".equals(table) || "by-name".equals(table) || "purging".equals(table)) {
        if ("by-id".equals(table) && parts.length == 5 && !parts[4].isBlank()) {
          table = parts[4];
        } else {
          return null;
        }
      }
    } else if ("root-resyncs".equals(parts[2])
        && parts.length >= 5
        && "by-table".equals(parts[3])) {
      table = parts[4];
    } else if ("reconcile".equals(parts[2])
        && parts.length >= 7
        && "finalized-snapshots".equals(parts[3])
        && "by-id".equals(parts[4])) {
      table = parts[5];
    }
    if (table == null || table.isBlank()) return null;
    return new TableRef(Keys.decodeSegment(account), Keys.decodeSegment(table));
  }

  private static String normalize(String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank()) return null;
    return pointerKey.startsWith("/") ? pointerKey : "/" + pointerKey;
  }
}
