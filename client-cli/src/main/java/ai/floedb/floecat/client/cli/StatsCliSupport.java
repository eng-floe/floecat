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

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexCoverage;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
import ai.floedb.floecat.catalog.rpc.TableIndexServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.CaptureOutput;
import ai.floedb.floecat.connector.rpc.CapturePolicy;
import ai.floedb.floecat.connector.rpc.DefaultColumnScope;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;

/** CLI support for {@code stats} and {@code analyze} commands. */
final class StatsCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int FILE_STATS_PAGE_SIZE = 100;
  private static final String TABLE_TARGET_SPEC = "table";

  private StatsCliSupport() {}

  /**
   * Dispatches {@code stats} and {@code analyze} commands.
   *
   * @param command either {@code "stats"} or {@code "analyze"}
   * @param args tokens after the top-level command word
   * @param out output stream
   * @param statistics gRPC statistics service stub
   * @param tables gRPC table service stub (used by analyze)
   * @param namespaces gRPC namespace service stub
   * @param reconcileControl gRPC reconcile control stub (used by analyze)
   * @param resolveTableId resolves a table FQ name or UUID to a {@link ResourceId}
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    switch (command) {
      case "stats" -> stats(args, out, statistics, indexes, resolveTableId);
      case "analyze" ->
          analyze(args, out, snapshots, tables, namespaces, reconcileControl, resolveTableId);
      default -> out.println("Unknown stats command: " + command);
    }
  }

  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    handle(
        command,
        args,
        out,
        statistics,
        indexes,
        null,
        tables,
        namespaces,
        reconcileControl,
        resolveTableId);
  }

  private static void stats(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println("usage: stats <table|columns|files|index|indexes> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = CliArgs.tail(args);
    switch (sub) {
      case "table" -> statsTable(tail, out, statistics, resolveTableId);
      case "columns" -> statsColumns(tail, out, statistics, resolveTableId);
      case "files" -> statsFiles(tail, out, statistics, resolveTableId);
      case "index", "indexes" -> statsIndexes(tail, out, indexes, resolveTableId);
      default -> out.println("unknown stats subcommand: " + sub);
    }
  }

  private static void statsTable(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: stats table <id|catalog.ns[.ns...].table> [--snapshot <id>|--current] [--json] (defaults to"
              + " --current)");
      return;
    }
    boolean json = CliArgs.hasFlag(args, "--json");
    String fq = args.get(0);
    ResourceId tableId = resolveTableId.apply(fq);
    var req =
        GetTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()))
            .build();
    GetTargetStatsResponse resp = statistics.getTargetStats(req);
    if (json) {
      CliUtils.printJson(resp, out);
      return;
    }
    if (!resp.hasStats() || !resp.getStats().hasTable()) {
      out.println("No table stats found.");
      return;
    }
    printTableStats(resp.getStats(), out);
  }

  private static void statsColumns(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: stats columns <id|catalog.ns[.ns...].table> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N] [--json]");
      return;
    }
    boolean json = CliArgs.hasFlag(args, "--json");
    String fq = args.get(0);
    boolean hasLimit = CliArgs.hasFlag(args, "--limit");
    int limit =
        hasLimit ? Math.max(1, CliArgs.parseIntFlag(args, "--limit", 0)) : Integer.MAX_VALUE;
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    ResourceId tableId = resolveTableId.apply(fq);
    ListTargetStatsRequest.Builder rb =
        ListTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .addTargetKinds(StatsTargetKind.STK_COLUMN);
    Function<ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse, String> nextToken =
        r -> r.hasPage() ? r.getPage().getNextPageToken() : "";

    if (json) {
      streamJsonRecords(
          out,
          writer -> {
            var remaining = new int[] {limit};
            CliArgs.forEachPageUntil(
                pageSize,
                pr ->
                    statistics.listTargetStats(
                        rb.setPage(
                                PageRequest.newBuilder()
                                    .setPageSize(pr.getPageSize())
                                    .setPageToken(pr.getPageToken())
                                    .build())
                            .build()),
                r -> r.getRecordsList(),
                nextToken,
                (response, records) -> {
                  if (remaining[0] <= 0) {
                    return false;
                  }
                  List<TargetStatsRecord> page =
                      records.stream()
                          .filter(TargetStatsRecord::hasScalar)
                          .limit(remaining[0])
                          .toList();
                  if (!page.isEmpty()) {
                    try {
                      writer.write(page);
                    } catch (InvalidProtocolBufferException e) {
                      throw new JsonStreamRuntimeException(e);
                    }
                    remaining[0] -= page.size();
                  }
                  return remaining[0] > 0;
                });
          });
      return;
    }
    printColumnStatsHeader(out);
    var remaining = new int[] {limit};
    var truncated = new boolean[] {false};
    CliArgs.forEachPageUntil(
        pageSize,
        pr ->
            statistics.listTargetStats(
                rb.setPage(
                        PageRequest.newBuilder()
                            .setPageSize(pr.getPageSize())
                            .setPageToken(pr.getPageToken())
                            .build())
                    .build()),
        r -> r.getRecordsList(),
        nextToken,
        (response, records) -> {
          if (remaining[0] <= 0) {
            return false;
          }
          List<TargetStatsRecord> filtered =
              records.stream().filter(TargetStatsRecord::hasScalar).toList();
          List<TargetStatsRecord> page = filtered.stream().limit(remaining[0]).toList();
          if (!page.isEmpty()) {
            printColumnStatsRows(page, out);
            remaining[0] -= page.size();
          }
          if (remaining[0] <= 0) {
            truncated[0] = filtered.size() > page.size() || !nextToken.apply(response).isBlank();
            return false;
          }
          return true;
        });
    if (hasLimit && truncated[0]) {
      printLimitNotice("column stats", limit, out);
    }
  }

  private static void statsFiles(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: stats files <id|catalog.ns[.ns...].table> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N]");
      return;
    }
    String fq = args.get(0);
    boolean hasLimit = CliArgs.hasFlag(args, "--limit");
    int limit =
        hasLimit ? Math.max(1, CliArgs.parseIntFlag(args, "--limit", 0)) : Integer.MAX_VALUE;
    int pageSize = Math.min(limit, FILE_STATS_PAGE_SIZE);

    ResourceId tableId = resolveTableId.apply(fq);
    ListTargetStatsRequest.Builder rb =
        ListTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .addTargetKinds(StatsTargetKind.STK_FILE);
    Function<ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse, String> nextToken =
        r -> r.hasPage() ? r.getPage().getNextPageToken() : "";

    var remaining = new int[] {limit};
    var printed = new int[] {0};
    var truncated = new boolean[] {false};
    CliArgs.forEachPageUntil(
        pageSize,
        pr ->
            statistics.listTargetStats(
                rb.setPage(
                        PageRequest.newBuilder()
                            .setPageSize(pr.getPageSize())
                            .setPageToken(pr.getPageToken())
                            .build())
                    .build()),
        r -> r.getRecordsList(),
        nextToken,
        (response, records) -> {
          if (remaining[0] <= 0) {
            return false;
          }
          List<TargetStatsRecord> filtered =
              records.stream().filter(TargetStatsRecord::hasFile).toList();
          List<TargetStatsRecord> page = filtered.stream().limit(remaining[0]).toList();
          if (!page.isEmpty()) {
            printFileColumnStatsPage(page, printed[0], printed[0] == 0, out);
            printed[0] += page.size();
            remaining[0] -= page.size();
          }
          if (remaining[0] <= 0) {
            truncated[0] = filtered.size() > page.size() || !nextToken.apply(response).isBlank();
            return false;
          }
          return true;
        });
    if (printed[0] == 0) {
      out.println("No file stats found.");
      return;
    }
    if (hasLimit && truncated[0]) {
      printLimitNotice("file stats", limit, out);
    }
  }

  private static void statsIndexes(
      List<String> args,
      PrintStream out,
      TableIndexServiceGrpc.TableIndexServiceBlockingStub indexes,
      Function<String, ResourceId> resolveTableId) {
    if (indexes == null) {
      throw new IllegalStateException("index service unavailable");
    }
    if (args.isEmpty()) {
      out.println(
          "usage: stats index <id|catalog.ns[.ns...].table> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N] [--json]");
      return;
    }
    boolean json = CliArgs.hasFlag(args, "--json");
    String fq = args.get(0);
    boolean hasLimit = CliArgs.hasFlag(args, "--limit");
    int limit =
        hasLimit ? Math.max(1, CliArgs.parseIntFlag(args, "--limit", 0)) : Integer.MAX_VALUE;
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    ResourceId tableId = resolveTableId.apply(fq);
    ListIndexArtifactsRequest.Builder rb =
        ListIndexArtifactsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")));
    Function<ai.floedb.floecat.catalog.rpc.ListIndexArtifactsResponse, String> nextToken =
        r -> r.hasPage() ? r.getPage().getNextPageToken() : "";

    if (json) {
      streamJsonRecords(
          out,
          writer -> {
            var remaining = new int[] {limit};
            CliArgs.forEachPageUntil(
                pageSize,
                pr ->
                    indexes.listIndexArtifacts(
                        rb.setPage(
                                PageRequest.newBuilder()
                                    .setPageSize(pr.getPageSize())
                                    .setPageToken(pr.getPageToken())
                                    .build())
                            .build()),
                r -> r.getRecordsList(),
                nextToken,
                (response, records) -> {
                  if (remaining[0] <= 0) {
                    return false;
                  }
                  List<IndexArtifactRecord> page = records.stream().limit(remaining[0]).toList();
                  if (!page.isEmpty()) {
                    try {
                      writer.write(page);
                    } catch (InvalidProtocolBufferException e) {
                      throw new JsonStreamRuntimeException(e);
                    }
                    remaining[0] -= page.size();
                  }
                  return remaining[0] > 0;
                });
          });
      return;
    }
    var remaining = new int[] {limit};
    var printed = new int[] {0};
    var truncated = new boolean[] {false};
    CliArgs.forEachPageUntil(
        pageSize,
        pr ->
            indexes.listIndexArtifacts(
                rb.setPage(
                        PageRequest.newBuilder()
                            .setPageSize(pr.getPageSize())
                            .setPageToken(pr.getPageToken())
                            .build())
                    .build()),
        r -> r.getRecordsList(),
        nextToken,
        (response, records) -> {
          if (remaining[0] <= 0) {
            return false;
          }
          List<IndexArtifactRecord> page = records.stream().limit(remaining[0]).toList();
          if (!page.isEmpty()) {
            if (printed[0] == 0) {
              printIndexArtifactsHeader(out);
            }
            printIndexArtifactsRows(page, printed[0], out);
            printed[0] += page.size();
            remaining[0] -= page.size();
          }
          if (remaining[0] <= 0) {
            truncated[0] = records.size() > page.size() || !nextToken.apply(response).isBlank();
            return false;
          }
          return true;
        });
    if (printed[0] == 0) {
      out.println("No index artifacts found.");
      return;
    }
    if (hasLimit && truncated[0]) {
      printLimitNotice("index artifacts", limit, out);
    }
  }

  // analyze runs a synchronous table-scoped CaptureNow call that defaults to stats-only capture.
  private static void analyze(
      List<String> args,
      PrintStream out,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: analyze <tableFQ> [--columns c1,c2,...]"
              + " [--default-cols first-n|all|explicit-only]"
              + " [--max-default-cols <n>]"
              + " [--snapshot <id>|--current]"
              + " [--mode metadata-only|metadata-and-capture|capture-only]"
              + " [--capture stats|table-stats|file-stats|column-stats|index,...]"
              + " [--full] [--wait-seconds <n>]"
              + "  (defaults: --mode capture-only --capture stats)");
      return;
    }

    String fq = args.get(0);
    List<String> columns =
        CliUtils.csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--columns", "")));
    DefaultColumnScope defaultColumnScope =
        CliUtils.parseDefaultColumnScope(
            Quotes.unquote(CliArgs.parseStringFlag(args, "--default-cols", "")));
    int maxDefaultColumns = CliArgs.parseIntFlag(args, "--max-default-cols", 32);
    if (maxDefaultColumns <= 0) {
      throw new IllegalArgumentException("--max-default-cols must be greater than 0");
    }
    String snapshotToken = Quotes.unquote(CliArgs.parseStringFlag(args, "--snapshot", ""));
    String modeToken = Quotes.unquote(CliArgs.parseStringFlag(args, "--mode", ""));
    CaptureMode mode =
        modeToken == null || modeToken.isBlank()
            ? CaptureMode.CM_CAPTURE_ONLY
            : CliUtils.parseCaptureMode(modeToken);
    String captureToken = Quotes.unquote(CliArgs.parseStringFlag(args, "--capture", ""));
    java.util.Set<CaptureOutput> requestedOutputs =
        captureToken == null || captureToken.isBlank()
            ? defaultAnalyzeCaptureOutputs(mode)
            : CliUtils.parseCaptureOutputs(captureToken);
    boolean full = CliArgs.hasFlag(args, "--full");
    int waitSeconds = CliArgs.parseIntFlag(args, "--wait-seconds", 10);
    if (waitSeconds <= 0) {
      throw new IllegalArgumentException("--wait-seconds must be greater than 0");
    }

    ResourceId tableId = resolveTableId.apply(fq);
    var table =
        tables.getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
    if (!table.hasUpstream() || !table.getUpstream().hasConnectorId()) {
      throw new IllegalArgumentException("table has no upstream connector");
    }
    CaptureScope.Builder scope =
        CaptureScope.newBuilder()
            .setConnectorId(table.getUpstream().getConnectorId())
            .setDestinationTableId(tableId.getId());
    Long snapshotId =
        snapshotToken.isBlank()
            ? null
            : resolveSnapshotId(snapshotToken, tableId, snapshotsService);
    if (!columns.isEmpty()) {
      if (mode == CaptureMode.CM_METADATA_ONLY) {
        throw new IllegalArgumentException("--columns requires a stats mode");
      }
    }
    if (snapshotId != null || !columns.isEmpty()) {
      long effectiveSnapshotId =
          snapshotId != null ? snapshotId : resolveSnapshotId("current", tableId, snapshotsService);
      scope.addDestinationCaptureRequests(
          ai.floedb.floecat.reconciler.rpc.ScopedCaptureRequest.newBuilder()
              .setTableId(tableId.getId())
              .setSnapshotId(effectiveSnapshotId)
              .setTargetSpec(TABLE_TARGET_SPEC)
              .addAllColumnSelectors(columns)
              .build());
    }
    CapturePolicy capturePolicy =
        CliUtils.buildCapturePolicy(
            mode, requestedOutputs, columns, defaultColumnScope, maxDefaultColumns);
    if (capturePolicy != null) {
      scope.setCapturePolicy(capturePolicy);
    }

    var response =
        reconcileControl.captureNow(
            CaptureNowRequest.newBuilder()
                .setScope(scope.build())
                .setMode(mode)
                .setFullRescan(full)
                .setMaxWait(Duration.newBuilder().setSeconds(waitSeconds).build())
                .build());
    out.printf(
        "analyze ok table=%s scanned=%d changed=%d errors=%d%n",
        fq, response.getTablesScanned(), response.getTablesChanged(), response.getErrors());
  }

  private static java.util.Set<CaptureOutput> defaultAnalyzeCaptureOutputs(CaptureMode mode) {
    if (mode == CaptureMode.CM_METADATA_ONLY) {
      return java.util.Set.of();
    }
    return java.util.Set.of(
        CaptureOutput.CO_TABLE_STATS, CaptureOutput.CO_FILE_STATS, CaptureOutput.CO_COLUMN_STATS);
  }

  private static long resolveSnapshotId(
      String snapshotToken,
      ResourceId tableId,
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService) {
    if (snapshotToken == null
        || snapshotToken.isBlank()
        || "current".equalsIgnoreCase(snapshotToken)) {
      return resolveCurrentSnapshotId(tableId, snapshotsService);
    }
    try {
      return Long.parseLong(snapshotToken);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("--snapshot must be a numeric id or 'current'", e);
    }
  }

  private static long resolveCurrentSnapshotId(
      ResourceId tableId, SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotsService) {
    if (snapshotsService == null) {
      throw new IllegalStateException("snapshot service unavailable");
    }
    return snapshotsService
        .getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                .build())
        .getSnapshot()
        .getSnapshotId();
  }

  // --- print helpers ---

  private static void printTableStats(TargetStatsRecord record, PrintStream out) {
    TableValueStats s = record.getTable();
    out.println("Table Stats:");
    out.printf("  table_id:        %s%n", CliUtils.rid(record.getTableId()));
    out.printf("  snapshot_id:     %d%n", record.getSnapshotId());
    out.printf("  row_count:       %d%n", s.getRowCount());
    out.printf("  data_file_count: %d%n", s.getDataFileCount());
    out.printf("  total_size:      %d bytes%n", s.getTotalSizeBytes());
    if (s.hasUpstream()) {
      out.printf(
          "  upstream:        system=%s commit=%s created=%s%n",
          s.getUpstream().getSystem().name(),
          s.getUpstream().getCommitRef(),
          CliUtils.ts(s.getUpstream().getFetchedAt()));
    }
  }

  private static void printColumnStats(List<TargetStatsRecord> cols, PrintStream out) {
    printColumnStatsHeader(out);
    printColumnStatsRows(cols, out);
  }

  private static void printColumnStatsHeader(PrintStream out) {
    out.printf(
        "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
        "CID", "NAME", "TYPE", "ROWS", "NULLS", "NaNs", "MIN", "MAX", "NDV", "#THETA SKETCHES");
  }

  private static void printColumnStatsRows(List<TargetStatsRecord> cols, PrintStream out) {
    for (var c : cols) {
      ScalarStats scalar = c.getScalar();
      long nullCount = scalar.hasNullCount() ? scalar.getNullCount() : 0L;
      long nanCount = scalar.hasNanCount() ? scalar.getNanCount() : 0L;
      out.printf(
          "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
          c.getTarget().getColumn().getColumnId(),
          CliUtils.trunc(scalar.getDisplayName(), 28),
          CliUtils.trunc(scalar.getLogicalType(), 12),
          Long.toString(scalar.getRowCount()),
          Long.toString(nullCount),
          Long.toString(nanCount),
          CliUtils.trunc(scalar.getMin(), 24),
          CliUtils.trunc(scalar.getMax(), 24),
          scalar.hasNdv() ? CliUtils.ndvToString(scalar.getNdv()) : "-",
          scalar.hasNdv() ? scalar.getNdv().getSketchesCount() : "-");
    }
  }

  private static void printFileColumnStats(List<TargetStatsRecord> files, PrintStream out) {
    if (files == null || files.isEmpty()) {
      out.println("No file stats found.");
      return;
    }
    printFileColumnStatsPage(files, 0, true, out);
  }

  private static void printFileColumnStatsPage(
      List<TargetStatsRecord> files, int startIndex, boolean includeHeader, PrintStream out) {
    if (includeHeader) {
      out.printf("%-4s %-10s %-12s %-20s %s%n", "IDX", "ROWS", "BYTES", "CONTENT", "PATH");
    }
    for (int i = 0; i < files.size(); i++) {
      FileTargetStats fs = files.get(i).getFile();
      String content = fs.getFileContent().name().replaceFirst("^FC_", "");
      out.printf(
          "%-4d %-10d %-12d %-20s %s%n",
          startIndex + i, fs.getRowCount(), fs.getSizeBytes(), content, fs.getFilePath());
      var cols = fs.getColumnsList();
      if (!cols.isEmpty()) {
        out.println("    columns:");
        for (FileColumnStats c : cols) {
          ScalarStats scalar = c.getScalar();
          String ndv = scalar.hasNdv() ? CliUtils.ndvToString(scalar.getNdv()) : "-";
          long nullCount = scalar.hasNullCount() ? scalar.getNullCount() : 0L;
          long nanCount = scalar.hasNanCount() ? scalar.getNanCount() : 0L;
          String columnName =
              !scalar.getDisplayName().isBlank() ? scalar.getDisplayName() : "#" + c.getColumnId();
          out.printf(
              "      %-8s %-24s %-10s rows=%-8s nulls=%-8d NaNs=%-8d min=%-20s max=%-20s"
                  + " ndv=%s%n",
              c.getColumnId(),
              CliUtils.trunc(columnName, 24),
              CliUtils.trunc(scalar.getLogicalType(), 10),
              Long.toString(scalar.getRowCount()),
              nullCount,
              nanCount,
              CliUtils.trunc(scalar.getMin(), 20),
              CliUtils.trunc(scalar.getMax(), 20),
              ndv);
        }
      }
    }
  }

  private static void printIndexArtifacts(List<IndexArtifactRecord> records, PrintStream out) {
    if (records == null || records.isEmpty()) {
      out.println("No index artifacts found.");
      return;
    }
    printIndexArtifactsHeader(out);
    printIndexArtifactsRows(records, 0, out);
  }

  private static void printIndexArtifactsHeader(PrintStream out) {
    out.printf(
        "%-4s %-10s %-8s %-8s %-8s %-10s %-12s %s%n",
        "IDX", "STATE", "FORMAT", "ROWS", "LIVE", "PAGES", "ROW_GROUPS", "PATH");
  }

  private static void printIndexArtifactsRows(
      List<IndexArtifactRecord> records, int startIndex, PrintStream out) {
    for (int i = 0; i < records.size(); i++) {
      IndexArtifactRecord record = records.get(i);
      IndexCoverage coverage = record.getCoverage();
      out.printf(
          "%-4d %-10s %-8s %-8s %-8s %-10s %-12s %s%n",
          startIndex + i,
          record.getState().name().replaceFirst("^IAS_", ""),
          CliUtils.trunc(record.getArtifactFormat(), 8),
          coverage.hasRowsIndexed() ? Long.toString(coverage.getRowsIndexed()) : "-",
          coverage.hasLiveRowsIndexed() ? Long.toString(coverage.getLiveRowsIndexed()) : "-",
          coverage.hasPagesIndexed() ? Long.toString(coverage.getPagesIndexed()) : "-",
          coverage.hasRowGroupsIndexed() ? Long.toString(coverage.getRowGroupsIndexed()) : "-",
          record.getTarget().hasFile() ? record.getTarget().getFile().getFilePath() : "-");
      out.printf("    uri=%s%n", record.getArtifactUri());
    }
  }

  @FunctionalInterface
  private interface JsonPageEmitter {
    void emit(JsonArrayWriter writer);
  }

  private static void streamJsonRecords(PrintStream out, JsonPageEmitter emitter) {
    try {
      JsonArrayWriter writer = new JsonArrayWriter("records", out);
      emitter.emit(writer);
      writer.finish();
    } catch (JsonStreamRuntimeException e) {
      throw new IllegalArgumentException("failed to render protobuf as json", e.getCause());
    }
  }

  private static final class JsonArrayWriter {
    private final PrintStream out;
    private boolean first = true;

    JsonArrayWriter(String fieldName, PrintStream out) {
      this.out = out;
      out.print("{\"");
      out.print(fieldName);
      out.print("\":[");
    }

    void write(List<? extends MessageOrBuilder> records) throws InvalidProtocolBufferException {
      for (MessageOrBuilder record : records) {
        if (!first) {
          out.print(",");
        }
        out.print(CliUtils.jsonPrinter().print(record));
        first = false;
      }
    }

    void finish() {
      out.println("]}");
    }
  }

  private static final class JsonStreamRuntimeException extends RuntimeException {
    JsonStreamRuntimeException(InvalidProtocolBufferException cause) {
      super(cause);
    }
  }

  private static void printLimitNotice(String subject, int limit, PrintStream out) {
    out.printf("Showing first %d %s. Re-run with --limit <n> to fetch more.%n", limit, subject);
  }
}
