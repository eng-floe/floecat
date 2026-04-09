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

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsResponse;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import com.google.protobuf.Duration;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** CLI support for {@code stats} and {@code analyze} commands. */
final class StatsCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private StatsCliSupport() {}

  /**
   * Dispatches {@code stats} and {@code analyze} commands.
   *
   * @param command either {@code "stats"} or {@code "analyze"}
   * @param args tokens after the top-level command word
   * @param out output stream
   * @param statistics gRPC statistics service stub
   * @param tables gRPC table service stub (used by analyze)
   * @param namespaces gRPC namespace service stub (used by analyze)
   * @param reconcileControl gRPC reconcile control stub (used by analyze)
   * @param resolveTableId resolves a table FQ name or UUID to a {@link ResourceId}
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    switch (command) {
      case "stats" -> stats(args, out, statistics, resolveTableId);
      case "analyze" -> analyze(args, out, tables, namespaces, reconcileControl, resolveTableId);
      default -> out.println("Unknown stats command: " + command);
    }
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
        GetTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")))
            .build();
    GetTableStatsResponse resp = statistics.getTableStats(req);
    if (json) {
      CliUtils.printJson(resp, out);
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
    ListColumnStatsRequest.Builder rb =
        ListColumnStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")));

    List<ColumnStats> all =
        CliArgs.collectPages(
            pageSize,
            pr -> statistics.listColumnStats(rb.setPage(pr).build()),
            r -> r.getColumnsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    if (all.size() > limit) all = all.subList(0, limit);
    if (json) {
      CliUtils.printJson(ListColumnStatsResponse.newBuilder().addAllColumns(all).build(), out);
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
    ListFileColumnStatsRequest.Builder rb =
        ListFileColumnStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(
                CliUtils.snapshotFromTokenOrCurrent(
                    CliArgs.parseStringFlag(args, "--snapshot", "current")));

    List<FileColumnStats> all =
        CliArgs.collectPages(
            pageSize,
            pr -> statistics.listFileColumnStats(rb.setPage(pr).build()),
            r -> r.getFileColumnsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    if (all.size() > limit) {
      all = all.subList(0, limit);
    }
    printFileColumnStats(all, out);
  }

  // analyze runs a synchronous table-scoped CaptureNow call for metadata and table stats.
  private static void analyze(
      List<String> args,
      PrintStream out,
      TableServiceGrpc.TableServiceBlockingStub tables,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      Function<String, ResourceId> resolveTableId) {
    if (args.isEmpty()) {
      out.println(
          "usage: analyze <tableFQ> [--columns c1,c2,...]"
              + " [--mode metadata-only|metadata-and-stats|stats-only]"
              + " [--full] [--wait-seconds <n>]");
      return;
    }

    String fq = args.get(0);
    String columnsArg = Quotes.unquote(CliArgs.parseStringFlag(args, "--columns", ""));
    List<String> columns = CliUtils.csvList(columnsArg);
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
    var namespace =
        namespaces
            .getNamespace(
                GetNamespaceRequest.newBuilder().setNamespaceId(table.getNamespaceId()).build())
            .getNamespace();
    var scopePath = new ArrayList<>(namespace.getParentsList());
    if (!namespace.getDisplayName().isBlank()) {
      scopePath.add(namespace.getDisplayName());
    }

    var response =
        reconcileControl.captureNow(
            CaptureNowRequest.newBuilder()
                .setScope(
                    CaptureScope.newBuilder()
                        .setConnectorId(table.getUpstream().getConnectorId())
                        .addDestinationNamespacePaths(
                            NamespacePath.newBuilder().addAllSegments(scopePath).build())
                        .setDestinationTableDisplayName(table.getDisplayName())
                        .addAllDestinationTableColumns(columns)
                        .build())
                .setMode(mode)
                .setFullRescan(full)
                .setMaxWait(Duration.newBuilder().setSeconds(waitSeconds).build())
                .build());
    out.printf(
        "analyze ok table=%s scanned=%d changed=%d errors=%d%n",
        fq, response.getTablesScanned(), response.getTablesChanged(), response.getErrors());
  }

  // --- print helpers ---

  private static void printTableStats(TableStats s, PrintStream out) {
    out.println("Table Stats:");
    out.printf("  table_id:        %s%n", CliUtils.rid(s.getTableId()));
    out.printf("  snapshot_id:     %d%n", s.getSnapshotId());
    out.printf("  row_count:       %d%n", s.getRowCount());
    out.printf("  data_file_count: %d%n", s.getDataFileCount());
    out.printf("  total_size:      %d bytes%n", s.getTotalSizeBytes());
    if (s.hasNdv()) {
      out.printf("  ndv:             %s%n", CliUtils.ndvToString(s.getNdv()));
    }
    if (s.hasUpstream()) {
      out.printf(
          "  upstream:        system=%s commit=%s created=%s%n",
          s.getUpstream().getSystem().name(),
          s.getUpstream().getCommitRef(),
          CliUtils.ts(s.getUpstream().getFetchedAt()));
    }
  }

  private static void printColumnStats(List<ColumnStats> cols, PrintStream out) {
    out.printf(
        "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
        "CID", "NAME", "TYPE", "VALUES", "NULLS", "NaNs", "MIN", "MAX", "NDV", "#THETA SKETCHES");
    for (var c : cols) {
      out.printf(
          "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
          c.getColumnId(),
          CliUtils.trunc(c.getColumnName(), 28),
          CliUtils.trunc(c.getLogicalType(), 12),
          Long.toString(c.getValueCount()),
          Long.toString(c.getNullCount()),
          Long.toString(c.getNanCount()),
          CliUtils.trunc(c.getMin(), 24),
          CliUtils.trunc(c.getMax(), 24),
          c.hasNdv() ? CliUtils.ndvToString(c.getNdv()) : "-",
          c.hasNdv() ? c.getNdv().getSketchesCount() : "-");
    }
  }

  private static void printFileColumnStats(List<FileColumnStats> files, PrintStream out) {
    if (files == null || files.isEmpty()) {
      out.println("No file stats found.");
      return;
    }
    out.printf("%-4s %-10s %-12s %-20s %s%n", "IDX", "ROWS", "BYTES", "CONTENT", "PATH");
    for (int i = 0; i < files.size(); i++) {
      FileColumnStats fs = files.get(i);
      String content = fs.getFileContent().name().replaceFirst("^FC_", "");
      out.printf(
          "%-4d %-10d %-12d %-20s %s%n",
          i, fs.getRowCount(), fs.getSizeBytes(), content, fs.getFilePath());
      var cols = fs.getColumnsList();
      if (!cols.isEmpty()) {
        out.println("    columns:");
        for (ColumnStats c : cols) {
          String ndv = c.hasNdv() ? CliUtils.ndvToString(c.getNdv()) : "-";
          out.printf(
              "      %-8s %-24s %-10s values=%-8d nulls=%-8d NaNs=%-8d min=%-20s max=%-20s"
                  + " ndv=%s%n",
              c.getColumnId(),
              CliUtils.trunc(c.getColumnName(), 24),
              CliUtils.trunc(c.getLogicalType(), 10),
              c.getValueCount(),
              c.getNullCount(),
              c.getNanCount(),
              CliUtils.trunc(c.getMin(), 20),
              CliUtils.trunc(c.getMax(), 20),
              ndv);
        }
      }
    }
  }
}
