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
import ai.floedb.floecat.catalog.rpc.ListTargetStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListTargetStatsResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTargetKind;
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
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import com.google.protobuf.Duration;
import java.io.PrintStream;
import java.util.List;
import java.util.function.Function;

/** CLI support for {@code stats} and {@code analyze} commands. */
final class StatsCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;
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
      SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    switch (command) {
      case "stats" -> stats(args, out, statistics, resolveTableId);
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
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    handle(
        command, args, out, statistics, null, tables, namespaces, reconcileControl, resolveTableId);
  }

  private static void stats(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println("usage: stats <table|columns|files> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = CliArgs.tail(args);
    switch (sub) {
      case "table" -> statsTable(tail, out, statistics, resolveTableId);
      case "columns" -> statsColumns(tail, out, statistics, resolveTableId);
      case "files" -> statsFiles(tail, out, statistics, resolveTableId);
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
          "usage: stats table <tableFQ> [--snapshot <id>|--current] [--json] (defaults to"
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
          "usage: stats columns <tableFQ> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N] [--json]");
      return;
    }
    boolean json = CliArgs.hasFlag(args, "--json");
    String fq = args.get(0);
    int limit = CliArgs.parseIntFlag(args, "--limit", 2000);
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    ResourceId tableId = resolveTableId.apply(fq);
    ListTargetStatsRequest.Builder rb =
        ListTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .addTargetKinds(StatsTargetKind.STK_COLUMN);

    List<TargetStatsRecord> all =
        CliArgs.collectPages(
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
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    all = all.stream().filter(TargetStatsRecord::hasScalar).toList();
    if (all.size() > limit) {
      all = all.subList(0, limit);
    }
    if (json) {
      CliUtils.printJson(ListTargetStatsResponse.newBuilder().addAllRecords(all).build(), out);
      return;
    }
    printColumnStats(all, out);
  }

  private static void statsFiles(
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: stats files <tableFQ> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N]");
      return;
    }
    String fq = args.get(0);
    int limit = CliArgs.parseIntFlag(args, "--limit", 1000);
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    ResourceId tableId = resolveTableId.apply(fq);
    ListTargetStatsRequest.Builder rb =
        ListTargetStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .addTargetKinds(StatsTargetKind.STK_FILE);

    List<TargetStatsRecord> all =
        CliArgs.collectPages(
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
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    all = all.stream().filter(TargetStatsRecord::hasFile).toList();
    if (all.size() > limit) {
      all = all.subList(0, limit);
    }
    printFileColumnStats(all, out);
  }

  // analyze runs a synchronous table-scoped CaptureNow call for metadata and table stats.
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
              + " [--snapshot <id>|--current]"
              + " [--mode metadata-only|metadata-and-stats|stats-only]"
              + " [--full] [--wait-seconds <n>]");
      return;
    }

    String fq = args.get(0);
    List<String> columns =
        CliUtils.csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--columns", "")));
    String snapshotToken = Quotes.unquote(CliArgs.parseStringFlag(args, "--snapshot", ""));
    CaptureMode mode =
        CliUtils.parseCaptureMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--mode", "")));
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
      scope.addDestinationStatsRequests(
          ai.floedb.floecat.reconciler.rpc.ScopedStatsRequest.newBuilder()
              .setTableId(tableId.getId())
              .setSnapshotId(effectiveSnapshotId)
              .setTargetSpec(TABLE_TARGET_SPEC)
              .addAllColumnSelectors(columns)
              .build());
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
    out.printf(
        "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
        "CID", "NAME", "TYPE", "VALUES", "NULLS", "NaNs", "MIN", "MAX", "NDV", "#THETA SKETCHES");
    for (var c : cols) {
      ScalarStats scalar = c.getScalar();
      long nullCount = scalar.hasNullCount() ? scalar.getNullCount() : 0L;
      long nanCount = scalar.hasNanCount() ? scalar.getNanCount() : 0L;
      out.printf(
          "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
          c.getTarget().getColumn().getColumnId(),
          CliUtils.trunc(scalar.getDisplayName(), 28),
          CliUtils.trunc(scalar.getLogicalType(), 12),
          Long.toString(scalar.getValueCount()),
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
    out.printf("%-4s %-10s %-12s %-20s %s%n", "IDX", "ROWS", "BYTES", "CONTENT", "PATH");
    for (int i = 0; i < files.size(); i++) {
      FileTargetStats fs = files.get(i).getFile();
      String content = fs.getFileContent().name().replaceFirst("^FC_", "");
      out.printf(
          "%-4d %-10d %-12d %-20s %s%n",
          i, fs.getRowCount(), fs.getSizeBytes(), content, fs.getFilePath());
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
              "      %-8s %-24s %-10s values=%-8d nulls=%-8d NaNs=%-8d min=%-20s max=%-20s"
                  + " ndv=%s%n",
              c.getColumnId(),
              CliUtils.trunc(columnName, 24),
              CliUtils.trunc(scalar.getLogicalType(), 10),
              scalar.getValueCount(),
              nullCount,
              nanCount,
              CliUtils.trunc(scalar.getMin(), 20),
              CliUtils.trunc(scalar.getMax(), 20),
              ndv);
        }
      }
    }
  }
}
