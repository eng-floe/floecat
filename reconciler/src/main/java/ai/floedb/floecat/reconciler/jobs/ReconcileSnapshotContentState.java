/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package ai.floedb.floecat.reconciler.jobs;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Canonical content identity and set-like capture coverage for one materialized snapshot. */
public final class ReconcileSnapshotContentState {
  public static final int FORMAT_VERSION = 3;

  private ReconcileSnapshotContentState() {}

  public static List<String> coverage(CaptureMode captureMode, ReconcileScope scope) {
    if (captureMode == null || captureMode == CaptureMode.METADATA_ONLY) {
      return List.of();
    }
    ReconcileScope effective = scope == null ? ReconcileScope.empty() : scope;
    ReconcileCapturePolicy policy = effective.capturePolicy();
    String semantics = policySemantics(policy);
    LinkedHashSet<String> atoms = new LinkedHashSet<>();
    List<ReconcileScope.ScopedCaptureRequest> requests = effective.destinationCaptureRequests();
    if (requests.isEmpty()) {
      addPolicyAtoms(atoms, policy, "*", semantics);
    } else {
      for (ReconcileScope.ScopedCaptureRequest request : requests) {
        if (request == null) {
          continue;
        }
        String target = request.targetSpec().isBlank() ? "*" : request.targetSpec();
        addNonColumnAtoms(atoms, policy, target, semantics);
        if (isColumnTarget(target)) {
          addMaterializedColumnTargetAtoms(atoms, policy, target, semantics);
          continue;
        }
        Set<String> selectors = new LinkedHashSet<>(request.columnSelectors());
        if (selectors.isEmpty()) {
          selectors.addAll(policy.selectorsForAnyCapture());
        }
        addColumnAtoms(atoms, policy, target, selectors, semantics);
      }
    }
    return atoms.stream().sorted().toList();
  }

  public static List<String> missingCoverage(
      List<String> requestedCoverage, List<String> materializedCoverage) {
    List<String> materialized = materializedCoverage == null ? List.of() : materializedCoverage;
    return requestedCoverage == null
        ? List.of()
        : requestedCoverage.stream()
            .filter(
                requested ->
                    materialized.stream().noneMatch(available -> covers(available, requested)))
            .sorted()
            .toList();
  }

  public static List<String> unionCoverage(List<String> left, List<String> right) {
    LinkedHashSet<String> union = new LinkedHashSet<>();
    if (left != null) {
      union.addAll(left);
    }
    if (right != null) {
      union.addAll(right);
    }
    return union.stream().filter(value -> value != null && !value.isBlank()).sorted().toList();
  }

  /** Expands column coverage with every equivalent selector actually materialized. */
  public static List<String> materializedCoverage(
      List<String> requestedCoverage,
      List<String> realizedStatsSelectors,
      List<String> realizedIndexSelectors) {
    LinkedHashSet<String> materialized = new LinkedHashSet<>();
    if (requestedCoverage != null) {
      materialized.addAll(requestedCoverage);
    }
    for (String encoded : requestedCoverage == null ? List.<String>of() : requestedCoverage) {
      CoverageAtom atom = parseAtom(encoded);
      if (atom == null) {
        continue;
      }
      List<String> realized =
          switch (atom.output()) {
            case COLUMN_STATS -> normalizedSelectors(realizedStatsSelectors);
            case PARQUET_PAGE_INDEX -> normalizedSelectors(realizedIndexSelectors);
            default -> List.of();
          };
      for (String selector : realized) {
        materialized.add(atom(atom.output(), atom.target(), selector, atom.semantics()));
      }
    }
    return materialized.stream()
        .filter(value -> value != null && !value.isBlank())
        .sorted()
        .toList();
  }

  public static boolean containsAll(List<String> available, List<String> requested) {
    return missingCoverage(requested, available).isEmpty();
  }

  private static List<String> normalizedSelectors(List<String> selectors) {
    return selectors == null
        ? List.of()
        : selectors.stream()
            .filter(selector -> selector != null && !selector.isBlank())
            .map(String::trim)
            .distinct()
            .sorted()
            .toList();
  }

  private static boolean covers(String available, String requested) {
    if (java.util.Objects.equals(available, requested)) {
      return true;
    }
    CoverageAtom availableAtom = parseAtom(available);
    CoverageAtom requestedAtom = parseAtom(requested);
    if (availableAtom == null
        || requestedAtom == null
        || availableAtom.output() != requestedAtom.output()
        || !availableAtom.target().equals(requestedAtom.target())
        || !availableAtom.semantics().equals(requestedAtom.semantics())) {
      return false;
    }
    DefaultSelection availableDefault = parseDefaultSelection(availableAtom.selector());
    DefaultSelection requestedDefault = parseDefaultSelection(requestedAtom.selector());
    if (requestedDefault == null) {
      return availableAtom.selector().equals(requestedAtom.selector())
          || (availableDefault != null
              && availableDefault.scope() == ReconcileCapturePolicy.DefaultColumnScope.ALL);
    }
    if (availableDefault == null) {
      return false;
    }
    if (availableDefault.scope() == ReconcileCapturePolicy.DefaultColumnScope.ALL) {
      return true;
    }
    return requestedDefault.scope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
        && availableDefault.scope() == ReconcileCapturePolicy.DefaultColumnScope.FIRST_N
        && availableDefault.limit() >= requestedDefault.limit();
  }

  private static DefaultSelection parseDefaultSelection(String selector) {
    if (selector == null || !selector.startsWith("@default:")) {
      return null;
    }
    String[] parts = selector.split(":", -1);
    if (parts.length != 3) {
      return null;
    }
    try {
      return new DefaultSelection(
          ReconcileCapturePolicy.DefaultColumnScope.valueOf(parts[1]), Integer.parseInt(parts[2]));
    } catch (IllegalArgumentException ignored) {
      return null;
    }
  }

  public static ReconcileScope narrowScope(ReconcileScope scope, List<String> missingCoverage) {
    return narrowScope(scope, missingCoverage, List.of());
  }

  public static ReconcileScope narrowScope(
      ReconcileScope scope, List<String> missingCoverage, List<String> materializedCoverage) {
    ReconcileScope effective = scope == null ? ReconcileScope.empty() : scope;
    Set<String> missing = missingCoverage == null ? Set.of() : new LinkedHashSet<>(missingCoverage);
    if (missing.isEmpty()) {
      return ReconcileScope.of(
          effective.destinationNamespaceIds(),
          effective.destinationTableId(),
          effective.destinationViewId(),
          List.of(),
          ReconcileCapturePolicy.empty(),
          effective.snapshotSelection());
    }
    ReconcileCapturePolicy policy =
        expandMaterializedColumnCoverage(effective.capturePolicy(), missing, materializedCoverage);
    Set<ReconcileCapturePolicy.Output> outputs = new LinkedHashSet<>();
    for (ReconcileCapturePolicy.Output output : policy.outputs()) {
      if (missing.stream().anyMatch(atom -> atom.startsWith(output.name() + "|"))) {
        outputs.add(output);
      }
    }
    boolean recaptureColumnStats = outputs.contains(ReconcileCapturePolicy.Output.COLUMN_STATS);
    boolean recapturePageIndexes =
        outputs.contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX);
    List<ReconcileScope.ScopedCaptureRequest> requests =
        effective.destinationCaptureRequests().stream()
            .filter(
                request ->
                    requestOwnsMissingCoverage(request, policy, missing)
                        || (recaptureColumnStats
                            && requestOwnsOutputCoverage(
                                request, policy, ReconcileCapturePolicy.Output.COLUMN_STATS))
                        || (recapturePageIndexes
                            && requestOwnsOutputCoverage(
                                request, policy, ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)))
            .map(
                request -> {
                  boolean preserveColumnCoverage =
                      (recaptureColumnStats
                              && requestOwnsOutputCoverage(
                                  request, policy, ReconcileCapturePolicy.Output.COLUMN_STATS))
                          || (recapturePageIndexes
                              && requestOwnsOutputCoverage(
                                  request,
                                  policy,
                                  ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
                  return new ReconcileScope.ScopedCaptureRequest(
                      request.tableId(),
                      request.snapshotId(),
                      request.targetSpec(),
                      preserveColumnCoverage
                              || isColumnTarget(request.targetSpec())
                              || request.columnSelectors().isEmpty()
                          ? request.columnSelectors()
                          : request.columnSelectors().stream()
                              .filter(
                                  selector ->
                                      requestSelectorOwnsMissingCoverage(
                                          request, selector, policy, missing))
                              .toList());
                })
            .toList();
    List<ReconcileCapturePolicy.Column> columns =
        policy.columns().stream()
            .filter(
                column ->
                    (recaptureColumnStats && column.captureStats())
                        || (recapturePageIndexes && column.captureIndex()))
            .map(
                column ->
                    new ReconcileCapturePolicy.Column(
                        column.selector(),
                        column.captureStats()
                            && outputs.contains(ReconcileCapturePolicy.Output.COLUMN_STATS),
                        column.captureIndex()
                            && outputs.contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)))
            .filter(ReconcileCapturePolicy.Column::enabled)
            .toList();
    return ReconcileScope.of(
        effective.destinationNamespaceIds(),
        effective.destinationTableId(),
        effective.destinationViewId(),
        requests,
        ReconcileCapturePolicy.of(
            columns,
            outputs,
            policy.defaultColumnScope(),
            policy.maxDefaultColumns(),
            policy.properties()),
        effective.snapshotSelection());
  }

  public static String fingerprint(Map<String, ?> fields) {
    TreeMap<String, String> canonical = new TreeMap<>();
    if (fields != null) {
      fields.forEach(
          (key, value) -> {
            if (key != null && !key.isBlank()) {
              canonical.put(key.trim(), canonicalValue(value));
            }
          });
    }
    StringBuilder payload = new StringBuilder();
    canonical.forEach(
        (key, value) ->
            payload
                .append(key.length())
                .append(':')
                .append(key)
                .append('=')
                .append(value.length())
                .append(':')
                .append(value)
                .append('\n'));
    return sha256(payload.toString());
  }

  public static String executionSemantics(ReconcileExecutionPolicy policy) {
    ReconcileExecutionPolicy effective =
        policy == null ? ReconcileExecutionPolicy.defaults() : policy;
    return fingerprint(
        Map.of(
            "class", effective.executionClass().name(),
            "lane", effective.lane(),
            "attributes", effective.attributes()));
  }

  private static void addPolicyAtoms(
      Set<String> atoms, ReconcileCapturePolicy policy, String target, String semantics) {
    addNonColumnAtoms(atoms, policy, target, semantics);
    Set<String> selectors = policy.selectorsForAnyCapture();
    if (selectors.isEmpty()
        && (policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
            || policy.outputs().contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX))) {
      selectors = Set.of(defaultColumnTarget(policy));
    }
    addColumnAtoms(atoms, policy, target, selectors, semantics);
  }

  private static void addNonColumnAtoms(
      Set<String> atoms, ReconcileCapturePolicy policy, String target, String semantics) {
    for (ReconcileCapturePolicy.Output output : policy.outputs()) {
      if (output == ReconcileCapturePolicy.Output.COLUMN_STATS
          || output == ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX) {
        continue;
      }
      atoms.add(atom(output, target, "", nonColumnPolicySemantics(policy)));
    }
  }

  private static void addColumnAtoms(
      Set<String> atoms,
      ReconcileCapturePolicy policy,
      String target,
      Set<String> selectors,
      String semantics) {
    for (String selector : selectors) {
      if (selector == null || selector.isBlank()) {
        continue;
      }
      boolean stats =
          policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
              && (policy.columns().isEmpty()
                  || policy.columns().stream()
                      .anyMatch(
                          column -> column.selector().equals(selector) && column.captureStats()));
      boolean indexes =
          policy.outputs().contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)
              && (policy.columns().isEmpty()
                  || policy.columns().stream()
                      .anyMatch(
                          column -> column.selector().equals(selector) && column.captureIndex()));
      if (stats) {
        atoms.add(atom(ReconcileCapturePolicy.Output.COLUMN_STATS, target, selector, semantics));
      }
      if (indexes) {
        atoms.add(
            atom(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX, target, selector, semantics));
      }
    }
  }

  private static void addMaterializedColumnTargetAtoms(
      Set<String> atoms, ReconcileCapturePolicy policy, String target, String semantics) {
    String identity = columnTargetIdentity(target);
    boolean unconstrained = policy.columns().isEmpty();
    boolean stats =
        policy.outputs().contains(ReconcileCapturePolicy.Output.COLUMN_STATS)
            && (unconstrained
                || policy.columns().stream()
                    .anyMatch(
                        column ->
                            column.captureStats()
                                && normalizeColumnIdentity(column.selector()).equals(identity)));
    boolean indexes =
        policy.outputs().contains(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)
            && (unconstrained
                || policy.columns().stream()
                    .anyMatch(
                        column ->
                            column.captureIndex()
                                && normalizeColumnIdentity(column.selector()).equals(identity)));
    if (stats) {
      atoms.add(atom(ReconcileCapturePolicy.Output.COLUMN_STATS, target, "", semantics));
    }
    if (indexes) {
      atoms.add(atom(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX, target, "", semantics));
    }
  }

  private static boolean requestOwnsMissingCoverage(
      ReconcileScope.ScopedCaptureRequest request,
      ReconcileCapturePolicy policy,
      Set<String> missing) {
    return requestCoverage(request, policy).stream().anyMatch(missing::contains);
  }

  private static boolean requestSelectorOwnsMissingCoverage(
      ReconcileScope.ScopedCaptureRequest request,
      String selector,
      ReconcileCapturePolicy policy,
      Set<String> missing) {
    ReconcileScope.ScopedCaptureRequest narrowed =
        new ReconcileScope.ScopedCaptureRequest(
            request.tableId(), request.snapshotId(), request.targetSpec(), List.of(selector));
    return requestCoverage(narrowed, policy).stream().anyMatch(missing::contains);
  }

  private static boolean requestOwnsOutputCoverage(
      ReconcileScope.ScopedCaptureRequest request,
      ReconcileCapturePolicy policy,
      ReconcileCapturePolicy.Output output) {
    String prefix = output.name() + "|";
    return requestCoverage(request, policy).stream().anyMatch(atom -> atom.startsWith(prefix));
  }

  private static List<String> requestCoverage(
      ReconcileScope.ScopedCaptureRequest request, ReconcileCapturePolicy policy) {
    return coverage(
        CaptureMode.CAPTURE_ONLY,
        ReconcileScope.of(
            List.of(),
            request.tableId().isBlank() ? null : request.tableId(),
            List.of(request),
            policy));
  }

  private static boolean isColumnTarget(String target) {
    return target != null && target.trim().regionMatches(true, 0, "column:", 0, 7);
  }

  private static String columnTargetIdentity(String target) {
    String normalized = target == null ? "" : target.trim();
    return normalizeColumnIdentity(
        isColumnTarget(normalized) ? normalized.substring(7) : normalized);
  }

  private static String normalizeColumnIdentity(String value) {
    String normalized = value == null ? "" : value.trim();
    if (normalized.regionMatches(true, 0, "column:", 0, 7)) {
      normalized = normalized.substring(7);
    }
    if (normalized.startsWith("#")) {
      normalized = normalized.substring(1);
    }
    return normalized;
  }

  private static String defaultColumnTarget(ReconcileCapturePolicy policy) {
    return "@default:" + policy.defaultColumnScope().name() + ":" + policy.maxDefaultColumns();
  }

  private static String atom(
      ReconcileCapturePolicy.Output output, String target, String selector, String semantics) {
    return output.name() + "|" + encode(target) + "|" + encode(selector) + "|" + semantics;
  }

  private static String policySemantics(ReconcileCapturePolicy policy) {
    return fingerprint(Map.of("properties", policy.properties()));
  }

  private static String nonColumnPolicySemantics(ReconcileCapturePolicy policy) {
    return fingerprint(Map.of("properties", policy.properties()));
  }

  private static ReconcileCapturePolicy expandMaterializedColumnCoverage(
      ReconcileCapturePolicy policy,
      Set<String> missingCoverage,
      List<String> materializedCoverage) {
    boolean statsMissing =
        missingCoverage.stream()
            .anyMatch(
                atom -> atom.startsWith(ReconcileCapturePolicy.Output.COLUMN_STATS.name() + "|"));
    boolean indexesMissing =
        missingCoverage.stream()
            .anyMatch(
                atom ->
                    atom.startsWith(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX.name() + "|"));
    if ((!statsMissing && !indexesMissing)
        || materializedCoverage == null
        || materializedCoverage.isEmpty()) {
      return policy;
    }
    Map<String, ReconcileCapturePolicy.Column> columns = new java.util.LinkedHashMap<>();
    for (ReconcileCapturePolicy.Column column : policy.columns()) {
      columns.put(column.selector(), column);
    }
    for (String encoded : materializedCoverage) {
      CoverageAtom atom = parseAtom(encoded);
      if (atom == null
          || (atom.output() == ReconcileCapturePolicy.Output.COLUMN_STATS && !statsMissing)
          || (atom.output() == ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX && !indexesMissing)
          || (atom.output() != ReconcileCapturePolicy.Output.COLUMN_STATS
              && atom.output() != ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)) {
        continue;
      }
      String selector =
          atom.selector().isBlank() && isColumnTarget(atom.target())
              ? columnTargetIdentity(atom.target())
              : atom.selector();
      if (selector.isBlank() || selector.startsWith("@default:")) {
        continue;
      }
      boolean captureStats = atom.output() == ReconcileCapturePolicy.Output.COLUMN_STATS;
      boolean captureIndex = atom.output() == ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX;
      columns.merge(
          selector,
          new ReconcileCapturePolicy.Column(selector, captureStats, captureIndex),
          (left, right) ->
              new ReconcileCapturePolicy.Column(
                  selector,
                  left.captureStats() || right.captureStats(),
                  left.captureIndex() || right.captureIndex()));
    }
    return ReconcileCapturePolicy.of(
        List.copyOf(columns.values()),
        policy.outputs(),
        policy.defaultColumnScope(),
        policy.maxDefaultColumns(),
        policy.properties());
  }

  private static CoverageAtom parseAtom(String encoded) {
    if (encoded == null || encoded.isBlank()) {
      return null;
    }
    int outputEnd = encoded.indexOf('|');
    if (outputEnd <= 0) {
      return null;
    }
    ReconcileCapturePolicy.Output output;
    try {
      output = ReconcileCapturePolicy.Output.valueOf(encoded.substring(0, outputEnd));
    } catch (IllegalArgumentException ignored) {
      return null;
    }
    DecodedPart target = decodePart(encoded, outputEnd + 1);
    if (target == null
        || target.nextOffset() >= encoded.length()
        || encoded.charAt(target.nextOffset()) != '|') {
      return null;
    }
    DecodedPart selector = decodePart(encoded, target.nextOffset() + 1);
    if (selector == null
        || selector.nextOffset() >= encoded.length()
        || encoded.charAt(selector.nextOffset()) != '|') {
      return null;
    }
    int semanticsOffset = selector.nextOffset() + 1;
    if (semanticsOffset > encoded.length()) {
      return null;
    }
    return new CoverageAtom(
        output, target.value(), selector.value(), encoded.substring(semanticsOffset));
  }

  private static DecodedPart decodePart(String encoded, int offset) {
    int colon = encoded.indexOf(':', offset);
    if (colon < offset) {
      return null;
    }
    int length;
    try {
      length = Integer.parseInt(encoded.substring(offset, colon));
    } catch (NumberFormatException ignored) {
      return null;
    }
    int valueStart = colon + 1;
    int valueEnd = valueStart + length;
    if (length < 0 || valueEnd > encoded.length()) {
      return null;
    }
    return new DecodedPart(encoded.substring(valueStart, valueEnd), valueEnd);
  }

  private record CoverageAtom(
      ReconcileCapturePolicy.Output output, String target, String selector, String semantics) {}

  private record DefaultSelection(ReconcileCapturePolicy.DefaultColumnScope scope, int limit) {}

  private record DecodedPart(String value, int nextOffset) {}

  private static String canonicalValue(Object value) {
    if (value == null) {
      return "";
    }
    if (value instanceof Map<?, ?> map) {
      TreeMap<String, String> sorted = new TreeMap<>();
      map.forEach((key, item) -> sorted.put(String.valueOf(key), canonicalValue(item)));
      return sorted.toString();
    }
    if (value instanceof Set<?> set) {
      return set.stream()
          .map(ReconcileSnapshotContentState::canonicalValue)
          .sorted()
          .toList()
          .toString();
    }
    if (value instanceof Iterable<?> iterable) {
      List<String> values = new ArrayList<>();
      iterable.forEach(item -> values.add(canonicalValue(item)));
      values.sort(Comparator.naturalOrder());
      return values.toString();
    }
    return String.valueOf(value);
  }

  private static String encode(String value) {
    String normalized = value == null ? "" : value.trim();
    return normalized.length() + ":" + normalized;
  }

  private static String sha256(String value) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return java.util.HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
