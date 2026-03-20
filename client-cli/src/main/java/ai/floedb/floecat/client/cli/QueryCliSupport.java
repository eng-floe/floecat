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

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.client.cli.util.NameRefUtil;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryResponse;
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.FetchScanBundleRequest;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.query.rpc.RenewQueryResponse;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableObligations;
import com.google.protobuf.Timestamp;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** CLI support for the {@code query} command. */
final class QueryCliSupport {

  private QueryCliSupport() {}

  /**
   * Dispatches the {@code query} command.
   *
   * @param command the top-level command token ("query")
   * @param args tokens after the command
   * @param out output stream
   * @param queries QueryService stub
   * @param queryScan QueryScanService stub
   * @param querySchema QuerySchemaService stub
   * @param directory DirectoryService stub (used for catalog resolution in begin)
   * @param getCurrentCatalog supplier for the current default catalog
   * @param getCurrentAccountId supplier for the current account id
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      QueryServiceGrpc.QueryServiceBlockingStub queries,
      QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan,
      QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentCatalog,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println("usage: query <begin|renew|end|get|fetch-scan> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = args.subList(1, args.size());
    switch (sub) {
      case "begin" ->
          queryBegin(
              tail,
              out,
              queries,
              queryScan,
              querySchema,
              directory,
              getCurrentCatalog,
              getCurrentAccountId);
      case "renew" -> queryRenew(tail, out, queries);
      case "end" -> queryEnd(tail, out, queries);
      case "get" -> queryGet(tail, out, queries);
      case "fetch-scan" -> queryFetchScan(tail, out, queryScan, getCurrentAccountId);
      default -> out.println("usage: query <begin|renew|end|get|fetch-scan> ...");
    }
  }

  private static void queryBegin(
      List<String> args,
      PrintStream out,
      QueryServiceGrpc.QueryServiceBlockingStub queries,
      QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan,
      QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentCatalog,
      Supplier<String> getCurrentAccountId) {
    String currentCatalog = getCurrentCatalog.get();
    if (currentCatalog == null || currentCatalog.isBlank()) {
      out.println("query begin: no default catalog set (use `catalog use <name>`)");
      return;
    }
    if (args.isEmpty()) {
      out.println(
          "usage: query begin [--ttl <seconds>] [--as-of-default <timestamp>] (table <fq> "
              + "[--snapshot <id|current>] [--as-of <timestamp>] | table-id <uuid> "
              + "[--snapshot <id|current>] [--as-of <timestamp>] | view-id <uuid> | namespace"
              + " <fq>)+");
      return;
    }

    List<QueryInput> inputs = new ArrayList<>();
    int ttlSeconds = 0;
    Timestamp asOfDefault = null;

    int i = 0;
    while (i < args.size()) {
      String token = args.get(i);
      switch (token) {
        case "--ttl" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: --ttl requires a value");
            return;
          }
          try {
            ttlSeconds = Integer.parseInt(args.get(i + 1));
          } catch (NumberFormatException e) {
            out.println("query begin: invalid ttl value '" + args.get(i + 1) + "'");
            return;
          }
          i += 2;
        }
        case "--as-of-default" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: --as-of-default requires a value");
            return;
          }
          try {
            asOfDefault = parseTimestampFlexible(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return;
          }
          i += 2;
        }
        case "table" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: table requires a fully qualified name");
            return;
          }
          NameRef nr;
          try {
            nr = NameRefUtil.nameRefForTable(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return;
          }
          if (nr.getCatalog().isBlank()) {
            nr = NameRef.newBuilder(nr).setCatalog(currentCatalog).build();
          }
          QueryInput.Builder input = QueryInput.newBuilder().setName(nr);
          int next = parseQueryInputOptions(args, i + 2, input, true, out);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "namespace" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: namespace requires a fully qualified name");
            return;
          }
          NameRef nr;
          try {
            nr = NamespaceCliSupport.nameRefForNamespace(args.get(i + 1), true);
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return;
          }
          if (nr.getCatalog().isBlank()) {
            nr = NameRef.newBuilder(nr).setCatalog(currentCatalog).build();
          }
          QueryInput.Builder input = QueryInput.newBuilder().setName(nr);
          int next = parseQueryInputOptions(args, i + 2, input, false, out);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "table-id" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: table-id requires a value");
            return;
          }
          ResourceId rid = tableRid(args.get(i + 1), getCurrentAccountId);
          QueryInput.Builder input = QueryInput.newBuilder().setTableId(rid);
          int next = parseQueryInputOptions(args, i + 2, input, true, out);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "view-id" -> {
          if (i + 1 >= args.size()) {
            out.println("query begin: view-id requires a value");
            return;
          }
          ResourceId rid = viewRid(args.get(i + 1), getCurrentAccountId);
          QueryInput.Builder input = QueryInput.newBuilder().setViewId(rid);
          int next = parseQueryInputOptions(args, i + 2, input, false, out);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        default -> {
          out.println("query begin: unknown argument '" + token + "'");
          return;
        }
      }
    }

    if (inputs.isEmpty()) {
      out.println("query begin: at least one table, view, or namespace input is required");
      return;
    }

    ResourceId queryDefaultCatalogId =
        CatalogCliSupport.resolveCatalogId(currentCatalog, directory, getCurrentAccountId);

    BeginQueryRequest.Builder req =
        BeginQueryRequest.newBuilder().setDefaultCatalogId(queryDefaultCatalogId);
    if (ttlSeconds > 0) {
      req.setTtlSeconds(ttlSeconds);
    }
    if (asOfDefault != null) {
      req.setAsOfDefault(asOfDefault);
    }

    BeginQueryResponse resp = queries.beginQuery(req.build());
    String queryId = resp.getQuery().getQueryId();

    DescribeInputsRequest.Builder descReq =
        DescribeInputsRequest.newBuilder().setQueryId(queryId).addAllInputs(inputs);
    DescribeInputsResponse descResp = querySchema.describeInputs(descReq.build());

    printDescribeInputs(descResp, inputs, out);
    printQueryBegin(resp, out);
  }

  private static void queryRenew(
      List<String> args, PrintStream out, QueryServiceGrpc.QueryServiceBlockingStub queries) {
    if (args.isEmpty()) {
      out.println("usage: query renew <query_id> [--ttl <seconds>]");
      return;
    }
    String queryId = args.get(0);
    int ttlSeconds = 0;

    int i = 1;
    while (i < args.size()) {
      String token = args.get(i);
      if ("--ttl".equals(token)) {
        if (i + 1 >= args.size()) {
          out.println("query renew: --ttl requires a value");
          return;
        }
        try {
          ttlSeconds = Integer.parseInt(args.get(i + 1));
        } catch (NumberFormatException e) {
          out.println("query renew: invalid ttl value '" + args.get(i + 1) + "'");
          return;
        }
        i += 2;
      } else {
        out.println("query renew: unknown argument '" + token + "'");
        return;
      }
    }

    RenewQueryRequest.Builder req = RenewQueryRequest.newBuilder().setQueryId(queryId);
    if (ttlSeconds > 0) {
      req.setTtlSeconds(ttlSeconds);
    }

    RenewQueryResponse resp = queries.renewQuery(req.build());
    out.println("query id: " + resp.getQueryId());
    out.println("expires: " + ts(resp.getExpiresAt()));
  }

  private static void queryEnd(
      List<String> args, PrintStream out, QueryServiceGrpc.QueryServiceBlockingStub queries) {
    if (args.isEmpty()) {
      out.println("usage: query end <query_id> [--commit|--abort]");
      return;
    }
    String queryId = args.get(0);
    Boolean commit = null;

    for (int i = 1; i < args.size(); i++) {
      String token = args.get(i);
      switch (token) {
        case "--commit" -> {
          if (commit != null && !commit) {
            out.println("query end: cannot specify both --commit and --abort");
            return;
          }
          commit = true;
        }
        case "--abort" -> {
          if (commit != null && commit) {
            out.println("query end: cannot specify both --commit and --abort");
            return;
          }
          commit = false;
        }
        default -> {
          out.println("query end: unknown argument '" + token + "'");
          return;
        }
      }
    }

    boolean commitFlag = commit != null ? commit : false;
    EndQueryResponse resp =
        queries.endQuery(
            EndQueryRequest.newBuilder().setQueryId(queryId).setCommit(commitFlag).build());
    out.println("query id: " + resp.getQueryId());
  }

  private static void queryGet(
      List<String> args, PrintStream out, QueryServiceGrpc.QueryServiceBlockingStub queries) {
    if (args.isEmpty()) {
      out.println("usage: query get <query_id>");
      return;
    }
    String queryId = args.get(0);
    GetQueryResponse resp =
        queries.getQuery(GetQueryRequest.newBuilder().setQueryId(queryId).build());
    if (!resp.hasQuery()) {
      out.println("query get: no query details returned");
      return;
    }
    printQueryDescriptor(resp.getQuery(), out);
  }

  private static void queryFetchScan(
      List<String> args,
      PrintStream out,
      QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan,
      Supplier<String> getCurrentAccountId) {
    if (args.size() < 2) {
      out.println("usage: query fetch-scan <query_id> <table_id>");
      return;
    }

    String queryId = args.get(0);
    ResourceId tableId;
    try {
      tableId = tableRid(args.get(1), getCurrentAccountId);
    } catch (IllegalArgumentException e) {
      out.println("query fetch-scan: " + e.getMessage());
      return;
    }

    var resp =
        queryScan.fetchScanBundle(
            FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(tableId).build());

    if (!resp.hasBundle()) {
      out.println("query fetch-scan: no bundle returned");
      return;
    }

    out.println("query id: " + queryId);
    out.println("table id: " + rid(tableId));
    printQueryFiles("data_files", resp.getBundle().getDataFilesList(), out);
    printQueryFiles("delete_files", resp.getBundle().getDeleteFilesList(), out);
  }

  // --- input option parsing ---

  private static int parseQueryInputOptions(
      List<String> args,
      int start,
      QueryInput.Builder input,
      boolean allowSnapshot,
      PrintStream out) {
    SnapshotRef snapshotRef = null;
    boolean snapshotSet = false;

    int i = start;
    while (i < args.size()) {
      String token = args.get(i);
      if (isQueryInputStartToken(token) || isQueryGlobalFlag(token)) {
        break;
      }
      switch (token) {
        case "--snapshot" -> {
          if (!allowSnapshot) {
            out.println("query begin: snapshots are not supported for this input type");
            return -1;
          }
          if (i + 1 >= args.size()) {
            out.println("query begin: --snapshot requires a value");
            return -1;
          }
          if (snapshotSet) {
            out.println("query begin: multiple snapshot selectors provided for one input");
            return -1;
          }
          try {
            snapshotRef = snapshotFromTokenOrCurrent(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return -1;
          }
          snapshotSet = true;
          i += 2;
        }
        case "--as-of" -> {
          if (!allowSnapshot) {
            out.println("query begin: --as-of is not supported for this input type");
            return -1;
          }
          if (i + 1 >= args.size()) {
            out.println("query begin: --as-of requires a value");
            return -1;
          }
          if (snapshotSet) {
            out.println("query begin: multiple snapshot selectors provided for one input");
            return -1;
          }
          try {
            Timestamp ts = parseTimestampFlexible(args.get(i + 1));
            snapshotRef = SnapshotRef.newBuilder().setAsOf(ts).build();
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return -1;
          }
          snapshotSet = true;
          i += 2;
        }
        default -> {
          out.println("query begin: unknown flag '" + token + "'");
          return -1;
        }
      }
    }

    if (snapshotSet && snapshotRef != null) {
      input.setSnapshot(snapshotRef);
    }
    return i;
  }

  private static boolean isQueryInputStartToken(String token) {
    return switch (token) {
      case "table", "table-id", "view-id", "namespace" -> true;
      default -> false;
    };
  }

  private static boolean isQueryGlobalFlag(String token) {
    return "--ttl".equals(token) || "--as-of-default".equals(token);
  }

  // --- print helpers ---

  private static void printQueryBegin(BeginQueryResponse resp, PrintStream out) {
    if (!resp.hasQuery()) {
      out.println("query begin: no query returned");
      return;
    }
    printQueryDescriptor(resp.getQuery(), out);
  }

  private static void printQueryDescriptor(QueryDescriptor query, PrintStream out) {
    out.println("query id: " + query.getQueryId());
    if (!query.getAccountId().isBlank()) {
      out.println("account: " + query.getAccountId());
    }
    out.println("status: " + query.getQueryStatus().name().toLowerCase(Locale.ROOT));
    out.println("created: " + ts(query.getCreatedAt()));
    out.println("expires: " + ts(query.getExpiresAt()));
    printQuerySnapshots(query.getSnapshots(), out);
    printQueryExpansion(query.getExpansion(), out);
    printQueryObligations(query.getObligationsList(), out);
  }

  private static void printQuerySnapshots(SnapshotSet snapshots, PrintStream out) {
    if (snapshots == null || snapshots.getPinsCount() == 0) {
      out.println("snapshots: <none>");
      return;
    }
    out.println("snapshots:");
    for (SnapshotPin pin : snapshots.getPinsList()) {
      StringBuilder line = new StringBuilder("  - table=");
      line.append(pin.hasTableId() ? rid(pin.getTableId()) : "<unknown>");
      if (pin.getSnapshotId() > 0) {
        line.append(" snapshot=").append(pin.getSnapshotId());
      }
      if (pin.hasAsOf()) {
        line.append(" as_of=").append(ts(pin.getAsOf()));
      }
      out.println(line.toString());
    }
  }

  private static void printQueryExpansion(ExpansionMap expansion, PrintStream out) {
    if (expansion == null) {
      out.println("expansion: <none>");
      return;
    }
    boolean hasViews = expansion.getViewsCount() > 0;
    boolean hasReadVia = expansion.getReadViaViewTablesCount() > 0;
    if (!hasViews && !hasReadVia) {
      out.println("expansion: <none>");
      return;
    }
    out.println("expansion:");
    if (hasViews) {
      out.println("  views:");
      expansion
          .getViewsList()
          .forEach(
              view -> {
                StringBuilder line =
                    new StringBuilder("    - view=")
                        .append(view.hasViewId() ? rid(view.getViewId()) : "<unknown>");
                if (!view.getCanonicalSql().isBlank()) {
                  line.append(" sql=").append(trunc(view.getCanonicalSql(), 80));
                }
                out.println(line.toString());
                if (!view.getBaseTableIdsList().isEmpty()) {
                  String bases =
                      view.getBaseTableIdsList().stream()
                          .map(QueryCliSupport::rid)
                          .collect(Collectors.joining(", "));
                  out.println("      bases: " + bases);
                }
              });
    }
    if (hasReadVia) {
      String via =
          expansion.getReadViaViewTablesList().stream()
              .map(QueryCliSupport::rid)
              .collect(Collectors.joining(", "));
      out.println("  read_via_view_tables: " + via);
    }
  }

  private static void printQueryObligations(List<TableObligations> obligations, PrintStream out) {
    if (obligations == null || obligations.isEmpty()) {
      return;
    }
    out.println("obligations:");
    for (TableObligations obligation : obligations) {
      out.println("  - table=" + rid(obligation.getTableId()));
      if (!obligation.getRowFilterExpression().isBlank()) {
        out.println("    row_filter: " + obligation.getRowFilterExpression());
      }
      if (!obligation.getMasksList().isEmpty()) {
        out.println("    masks:");
        obligation
            .getMasksList()
            .forEach(
                mask ->
                    out.println(
                        "      " + mask.getColumn() + " => " + trunc(mask.getExpression(), 100)));
      }
    }
  }

  private static void printQueryFiles(String label, List<ScanFile> files, PrintStream out) {
    if (files == null || files.isEmpty()) {
      out.println(label + ": <none>");
      return;
    }
    out.println(label + ":");
    for (ScanFile file : files) {
      String content = file.getFileContent().name().toLowerCase(Locale.ROOT);
      out.printf(
          "  - path=%s format=%s size=%dB records=%d content=%s%n",
          file.getFilePath(),
          file.getFileFormat(),
          file.getFileSizeInBytes(),
          file.getRecordCount(),
          content);
      if (!file.getDeleteFileIndicesList().isEmpty()) {
        out.printf("    delete_file_indices: %s%n", file.getDeleteFileIndicesList());
      }
      if (!file.getPartitionDataJson().isBlank()) {
        out.printf(
            "    partition_data: %s (spec_id=%d)%n",
            file.getPartitionDataJson(), file.getPartitionSpecId());
      }
      if (!file.getColumnsList().isEmpty()) {
        out.println("    columns:");
        file.getColumnsList()
            .forEach(
                col -> {
                  String ndv = col.hasNdv() ? ndvToString(col.getNdv()) : "-";
                  out.printf(
                      "      %s (%s) nulls=%d ndv=%s%n",
                      col.getColumnName(), col.getLogicalType(), col.getNullCount(), ndv);
                });
      }
    }
  }

  private static void printDescribeInputs(
      DescribeInputsResponse resp, List<QueryInput> inputs, PrintStream out) {
    out.println("resolved inputs:");
    for (int i = 0; i < resp.getSchemasCount(); i++) {
      SchemaDescriptor schema = resp.getSchemas(i);
      QueryInput input = inputs.get(i);

      out.println();
      out.println("input " + (i + 1) + ": " + input.toString());
      out.println("schema:");
      for (var col : schema.getColumnsList()) {
        out.printf(
            "  - %-20s %-12s field_id=%d nullable=%s physical=%s partition=%s%n",
            col.getName(),
            col.getLogicalType(),
            col.getFieldId(),
            col.getNullable(),
            col.getPhysicalPath(),
            col.getPartitionKey());
      }
    }
    out.println();
  }

  // --- utility helpers ---

  private static ResourceId tableRid(String id, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_TABLE)
        .setId(id)
        .build();
  }

  private static ResourceId viewRid(String id, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_OVERLAY)
        .setId(id)
        .build();
  }

  private static SnapshotRef snapshotFromTokenOrCurrent(String tokenOrNull) {
    if (tokenOrNull == null || tokenOrNull.isBlank() || tokenOrNull.equalsIgnoreCase("current")) {
      return SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
    }
    try {
      return SnapshotRef.newBuilder().setSnapshotId(Long.parseLong(tokenOrNull.trim())).build();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid snapshot selector '" + tokenOrNull + "'");
    }
  }

  private static Timestamp parseTimestampFlexible(String value) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("timestamp value is blank");
    }
    String trimmed = value.trim();
    try {
      long numeric = Long.parseLong(trimmed);
      if (Math.abs(numeric) >= 1_000_000_000_000L) {
        long seconds = Math.floorDiv(numeric, 1000L);
        long millisPart = Math.floorMod(numeric, 1000L);
        int nanos = (int) (millisPart * 1_000_000L);
        return Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
      } else {
        return Timestamp.newBuilder().setSeconds(numeric).build();
      }
    } catch (NumberFormatException ignored) {
      try {
        Instant instant = Instant.parse(trimmed);
        return Timestamp.newBuilder()
            .setSeconds(instant.getEpochSecond())
            .setNanos(instant.getNano())
            .build();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "invalid timestamp '"
                + value
                + "' (expected epoch seconds/millis or ISO-8601 instant)");
      }
    }
  }

  private static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private static String trunc(String s, int n) {
    if (s == null) {
      return "-";
    }
    return s.length() <= n ? s : (s.substring(0, n - 1) + "\u2026");
  }

  private static String ndvToString(Ndv n) {
    if (n.hasExact()) {
      return Long.toString(n.getExact());
    }
    if (n.hasApprox()) {
      NdvApprox approx = n.getApprox();
      return approx.getEstimate() == Math.rint(approx.getEstimate())
          ? Long.toString(Math.round(approx.getEstimate()))
          : String.format(Locale.ROOT, "%.3f", approx.getEstimate());
    }
    return "-";
  }

  private static String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }
}
