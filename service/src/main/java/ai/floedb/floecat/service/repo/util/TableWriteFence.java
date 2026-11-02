/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Keeps table-scoped writes atomic with the continued existence of their table row. */
public final class TableWriteFence {
  private TableWriteFence() {}

  public record FencedBatch(boolean writable, List<PointerStore.CasOp> operations) {}

  /**
   * Adds a version check for every table row named by an upsert in {@code operations}.
   *
   * <p>A batch that creates or updates the table row already fences itself. Deletes do not need a
   * liveness check. This makes a table-row delete serialize against every write below that table: a
   * write that wins first is removed by the following purge, and a write that loses cannot recreate
   * state after the row is gone.
   */
  public static FencedBatch fence(
      PointerStore pointerStore, List<? extends PointerStore.CasOp> operations) {
    List<PointerStore.CasOp> source = operations == null ? List.of() : List.copyOf(operations);
    Set<String> rows = tableRowsRequiringChecks(source);
    if (rows.isEmpty()) {
      return new FencedBatch(true, source);
    }
    List<PointerStore.CasOp> fenced = new ArrayList<>(source.size() + rows.size());
    for (String row : rows) {
      // Callers pass the authoritative store view; consistency is selected by composition rather
      // than by choosing a stronger read method at this call site.
      Optional<Pointer> current = pointerStore.get(row);
      if (current.isEmpty()) {
        return new FencedBatch(false, List.of());
      }
      fenced.add(new PointerStore.CasCheck(row, current.orElseThrow().getVersion()));
    }
    fenced.addAll(source);
    return new FencedBatch(true, List.copyOf(fenced));
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

  static String canonicalTableRow(String pointerKey) {
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
      if ("by-id".equals(table) || "by-name".equals(table)) {
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
    return "/accounts/" + account + "/tables/by-id/" + table;
  }

  private static String normalize(String pointerKey) {
    if (pointerKey == null || pointerKey.isBlank()) return null;
    return pointerKey.startsWith("/") ? pointerKey : "/" + pointerKey;
  }
}
