package ai.floedb.metacat.client.cli;

import static java.lang.System.out;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Ndv;
import ai.floedb.metacat.catalog.rpc.NdvApprox;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveViewRequest;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

@TopCommand
@CommandLine.Command(
    name = "metacat-shell",
    mixinStandardHelpOptions = true,
    version = "metacat-shell 0.1",
    description = "Interactive CLI to browse catalogs/namespaces/tables and view stats")
@jakarta.inject.Singleton
public class Shell implements Runnable {

  @Inject
  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @Inject
  @GrpcClient("namespace-service")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @Inject
  @GrpcClient("table-service")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @Inject
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Inject
  @GrpcClient("table-statistics-service")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;

  @Inject
  @GrpcClient("snapshot-service")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Override
  public void run() {
    out.println("Metacat Shell (type 'help' for commands, 'quit' to exit).");

    try {
      Terminal terminal = TerminalBuilder.builder().system(true).build();

      Path histFile = Paths.get(System.getProperty("user.home"), ".metacat_shell_history");

      var parser = new DefaultParser();
      parser.setEofOnUnclosedBracket(
          DefaultParser.Bracket.CURLY, DefaultParser.Bracket.ROUND, DefaultParser.Bracket.SQUARE);
      parser.setEscapeChars(null);

      Completer completer =
          new StringsCompleter(
              "catalogs",
              "namespaces",
              "tables",
              "resolve",
              "describe",
              "snapshots",
              "stats",
              "help",
              "quit",
              "exit");

      LineReader reader =
          LineReaderBuilder.builder()
              .terminal(terminal)
              .appName("metacat-shell")
              .parser(parser)
              .completer(completer)
              .variable(LineReader.HISTORY_FILE, histFile)
              .option(LineReader.Option.HISTORY_TIMESTAMPED, true)
              .build();

      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      reader.getHistory().save();
                    } catch (Throwable ignore) {
                    }
                  }));

      while (true) {
        String line;
        try {
          line = reader.readLine("metacat> ");
        } catch (UserInterruptException e) {
          continue;
        } catch (EndOfFileException e) {
          break;
        }
        if (line == null) break;
        line = line.trim();
        if (line.isEmpty()) continue;

        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) break;
        if (line.equalsIgnoreCase("help")) {
          printHelp();
          continue;
        }

        try {
          dispatch(line);
        } catch (Exception e) {
          out.println(
              "! " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
      }
    } catch (Exception e) {
      out.println("Fatal: " + e);
    }
  }

  private void printHelp() {
    out.println(
        """
        Commands:
          catalogs
          namespaces <catalogName|catalogId>
          tables <catalog.NS1/NS2[/.../NSk][.prefix]>
          resolve table <fq> | resolve view <fq> | resolve catalog <name> | resolve namespace <fq>
          describe table <fq>
          snapshots <tableFQ>
          stats table <tableFQ> [--snapshot <id>|--current]
          stats columns <tableFQ> [--snapshot <id>|--current] [--limit N]
          help
          quit
        """);
  }

  private void dispatch(String line) {
    var tok = Arrays.stream(line.split("\\s+")).filter(s -> !s.isBlank()).toList();
    if (tok.isEmpty()) return;
    String cmd = tok.get(0);

    switch (cmd) {
      case "catalogs" -> cmdCatalogs();
      case "namespaces" -> cmdNamespaces(tail(tok));
      case "tables" -> cmdTables(tail(tok));
      case "resolve" -> cmdResolve(tail(tok));
      case "describe" -> cmdDescribe(tail(tok));
      case "snapshots" -> cmdSnapshots(tail(tok));
      case "stats" -> cmdStats(tail(tok));
      default -> out.println("Unknown command. Type 'help'.");
    }
  }

  private List<String> tail(List<String> t) {
    return t.size() <= 1 ? List.of() : t.subList(1, t.size());
  }

  private void cmdCatalogs() {
    var resp =
        catalogs.listCatalogs(
            ListCatalogsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(500).build())
                .build());
    printCatalogs(resp.getCatalogsList());
  }

  private void cmdNamespaces(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: namespaces <catalogName|catalogId>");
      return;
    }
    ResourceId catId = guessResourceId(args.get(0));
    if (catId == null) {
      var r =
          directory.resolveCatalog(
              ResolveCatalogRequest.newBuilder().setRef(nameCatalog(args.get(0))).build());
      catId = r.getResourceId();
    }
    var resp =
        namespaces.listNamespaces(
            ListNamespacesRequest.newBuilder()
                .setCatalogId(catId)
                .setPage(PageRequest.newBuilder().setPageSize(1000).build())
                .build());
    printNamespaces(resp.getNamespacesList());
  }

  private void cmdTables(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: tables <catalog.db.schema[.prefix]>");
      return;
    }
    var prefix = nameRefForTablePrefix(args.get(0));
    var resp =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefix)
                .setPage(PageRequest.newBuilder().setPageSize(1000).build())
                .build());
    printResolvedTables(resp.getTablesList());
  }

  private void cmdResolve(List<String> args) {
    if (args.size() < 2) {
      out.println("usage: resolve table|view|catalog|namespace <fq-or-name>");
      return;
    }
    String kind = args.get(0), s = args.get(1);
    switch (kind) {
      case "table" ->
          out.println(
              "table id: "
                  + rid(
                      directory
                          .resolveTable(
                              ResolveTableRequest.newBuilder().setRef(nameRefForTable(s)).build())
                          .getResourceId()));

      case "view" ->
          out.println(
              "view id: "
                  + rid(
                      directory
                          .resolveView(
                              ResolveViewRequest.newBuilder().setRef(nameRefForTable(s)).build())
                          .getResourceId()));

      case "namespace" ->
          out.println(
              "namespace id: "
                  + rid(
                      directory
                          .resolveNamespace(
                              ResolveNamespaceRequest.newBuilder()
                                  .setRef(nameRefForNamespace(s))
                                  .build())
                          .getResourceId()));
      case "catalog" ->
          out.println(
              "catalog id: "
                  + rid(
                      directory
                          .resolveCatalog(
                              ResolveCatalogRequest.newBuilder().setRef(nameCatalog(s)).build())
                          .getResourceId()));
      default -> out.println("unknown kind: " + kind);
    }
  }

  private void cmdDescribe(List<String> args) {
    if (args.size() < 2 || !"table".equals(args.get(0))) {
      out.println("usage: describe table <fq>");
      return;
    }
    String fq = args.get(1);
    var r = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameFromFq(fq)).build());
    var t = tables.getTable(GetTableRequest.newBuilder().setTableId(r.getResourceId()).build());
    printTable(t.getTable());
  }

  private void cmdSnapshots(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: snapshots <tableFQ>");
      return;
    }
    var r =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(nameFromFq(args.get(0))).build());
    var resp =
        snapshots.listSnapshots(
            ListSnapshotsRequest.newBuilder()
                .setTableId(r.getResourceId())
                .setPage(PageRequest.newBuilder().setPageSize(1000).build())
                .build());
    printSnapshots(resp.getSnapshotsList());
  }

  private void cmdStats(List<String> args) {
    if (args.isEmpty()) {
      printHelp();
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "table" -> statsTable(args.subList(1, args.size()));
      case "columns" -> statsColumns(args.subList(1, args.size()));
      default -> out.println("unknown stats subcommand: " + sub);
    }
  }

  private void statsTable(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: stats table <tableFQ> [--snapshot <id>|--current]");
      return;
    }
    String fq = args.get(0);
    var r = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameFromFq(fq)).build());
    var req =
        GetTableStatsRequest.newBuilder()
            .setTableId(r.getResourceId())
            .setSnapshot(parseSnapshotSelector(args))
            .build();
    var resp = stats.getTableStats(req);
    printTableStats(resp.getStats());
  }

  private void statsColumns(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: stats columns <tableFQ> [--snapshot <id>|--current] [--limit N]");
      return;
    }
    String fq = args.get(0);
    int limit = parseIntFlag(args, "--limit", 2000);
    var r = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameFromFq(fq)).build());
    var req =
        ListColumnStatsRequest.newBuilder()
            .setTableId(r.getResourceId())
            .setSnapshot(parseSnapshotSelector(args))
            .setPage(PageRequest.newBuilder().setPageSize(limit).build())
            .build();
    printColumnStats(stats.listColumnStats(req).getColumnsList());
  }

  private ResourceId guessResourceId(String s) {
    if (s != null && s.contains("-") && s.length() >= 36) {
      return ResourceId.newBuilder().setId(s).build();
    }
    return null;
  }

  private NameRef nameCatalog(String name) {
    return NameRef.newBuilder().setCatalog(name).build();
  }

  private NameRef nameFromFq(String fq) {
    List<String> parts = Arrays.asList(fq.split("\\."));
    List<String> namespace = Arrays.asList(parts.get(1).split("/"));

    return NameRef.newBuilder()
        .setCatalog(parts.get(0))
        .addAllPath(namespace)
        .setName(parts.get(2))
        .build();
  }

  private SnapshotRef parseSnapshotSelector(List<String> args) {
    int idx = args.indexOf("--snapshot");
    boolean current = args.contains("--current");
    SnapshotRef.Builder b = SnapshotRef.newBuilder();
    if (idx >= 0 && idx + 1 < args.size()) {
      b.setSnapshotId(Long.parseLong(args.get(idx + 1)));
    } else if (current) {
      b.setSpecial(SpecialSnapshot.SS_CURRENT);
    } else {
      b.setSpecial(SpecialSnapshot.SS_CURRENT);
    }
    return b.build();
  }

  private int parseIntFlag(List<String> args, String flag, int dflt) {
    int i = args.indexOf(flag);
    if (i >= 0 && i + 1 < args.size()) {
      try {
        return Integer.parseInt(args.get(i + 1));
      } catch (Exception ignore) {
      }
    }
    return dflt;
  }

  private String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }

  private void printCatalogs(List<Catalog> cats) {
    out.printf("%-40s  %-24s  %s%n", "CATALOG_ID", "CREATED_AT", "DISPLAY_NAME");
    for (var c : cats) {
      out.printf(
          "%-40s  %-24s  %s%n", rid(c.getResourceId()), ts(c.getCreatedAt()), c.getDisplayName());
    }
  }

  private void printNamespaces(List<Namespace> nss) {
    out.printf("%-40s  %-24s  %s%n", "NAMESPACE_ID", "CREATED_AT", "DISPLAY_NAME");
    for (var ns : nss) {
      out.printf(
          "%-40s  %-24s  %s%n",
          rid(ns.getResourceId()), ts(ns.getCreatedAt()), ns.getDisplayName());
    }
  }

  private void printResolvedTables(List<ResolveFQTablesResponse.Entry> entries) {
    out.printf("%-40s  %s%n", "TABLE_ID", "NAME(parts)");
    for (var e : entries) {
      String catalog = e.getName().getCatalog();
      String namespace = String.join("/", e.getName().getPathList());
      String table = e.getName().getName();
      out.printf("%-40s  %s%n", rid(e.getResourceId()), catalog + "." + namespace + "." + table);
    }
  }

  private void printTable(Table t) {
    out.println("Table:");
    out.printf("  id:           %s%n", rid(t.getResourceId()));
    out.printf("  name:         %s%n", t.getDisplayName());
    out.printf("  format:       %s%n", t.getFormat().name());
    out.printf("  root_uri:     %s%n", t.getRootUri());
    out.printf("  created_at:   %s%n", ts(t.getCreatedAt()));
    if (!t.getPartitionKeysList().isEmpty()) {
      out.printf("  partitions:   %s%n", String.join(", ", t.getPartitionKeysList()));
    }
    if (!t.getPropertiesMap().isEmpty()) {
      out.println("  properties:");
      t.getPropertiesMap().forEach((k, v) -> out.printf("    %s = %s%n", k, v));
    }
  }

  private void printSnapshots(List<Snapshot> snaps) {
    out.printf("%-20s  %-24s  %-20s%n", "SNAPSHOT_ID", "UPSTREAM_CREATED_AT", "PARENT_ID");
    for (var s : snaps) {
      out.printf(
          "%-20d  %-24s  %-20d%n",
          s.getSnapshotId(), ts(s.getUpstreamCreatedAt()), s.getParentSnapshotId());
    }
  }

  private void printTableStats(TableStats s) {
    out.println("Table Stats:");
    out.printf("  table_id:        %s%n", rid(s.getTableId()));
    out.printf("  snapshot_id:     %d%n", s.getSnapshotId());
    out.printf("  row_count:       %d%n", s.getRowCount());
    out.printf("  data_file_count: %d%n", s.getDataFileCount());
    out.printf("  total_size:      %d bytes%n", s.getTotalSizeBytes());
    if (s.hasNdv()) {
      out.printf("  ndv:             %s%n", ndvToString(s.getNdv()));
    }
    if (s.hasUpstream()) {
      out.printf(
          "  upstream:        system=%s commit=%s fetched=%s%n",
          s.getUpstream().getSystem().name(),
          s.getUpstream().getCommitRef(),
          ts(s.getUpstream().getFetchedAt()));
    }
  }

  private void printColumnStats(List<ColumnStats> cols) {
    out.printf(
        "%-8s %-28s %-12s %-10s %-10s %-24s %-24s %-12s%n",
        "CID", "NAME", "TYPE", "NULLS", "NaNs", "MIN", "MAX", "NDV");
    for (var c : cols) {
      out.printf(
          "%-8s %-28s %-12s %-10s %-10s %-24s %-24s %-12s%n",
          c.getColumnId(),
          trunc(c.getColumnName(), 28),
          trunc(c.getLogicalType(), 12),
          Long.toString(c.getNullCount()),
          Long.toString(c.getNanCount()),
          trunc(c.getMin(), 24),
          trunc(c.getMax(), 24),
          c.hasNdv() ? ndvToString(c.getNdv()) : "-");
    }
  }

  private String ts(com.google.protobuf.Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) return "-";
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private String trunc(String s, int n) {
    if (s == null) return "-";
    return s.length() <= n ? s : (s.substring(0, n - 1) + "…");
  }

  private String ndvToString(Ndv n) {
    if (n.hasExact()) {
      return Long.toString(n.getExact());
    }

    if (n.hasApprox()) {
      NdvApprox ndvAprox = n.getApprox();
      double estimate = ndvAprox.getEstimate();
      return String.valueOf(estimate);
    }

    return "-";
  }

  private NameRef nameRefForTable(String fq) {
    ParsedFq p = parseFqFlexible(fq);
    if (p.object.isEmpty()) {
      throw new IllegalArgumentException("Missing table/view name: use catalog.NS/.../NSk.<table>");
    }
    return NameRef.newBuilder()
        .setCatalog(p.catalog)
        .addAllPath(p.nsParts)
        .setName(p.object.get())
        .build();
  }

  private NameRef nameRefForNamespace(String fqNs) {
    ParsedFq p = parseFqFlexible(fqNs);
    if (p.object.isPresent()) {
      throw new IllegalArgumentException("Namespace must not include a table name");
    }
    if (p.nsParts.isEmpty()) {
      throw new IllegalArgumentException("Namespace path is empty");
    }
    List<String> parents = p.nsParts.subList(0, p.nsParts.size() - 1);
    String leaf = p.nsParts.get(p.nsParts.size() - 1);
    return NameRef.newBuilder().setCatalog(p.catalog).addAllPath(parents).setName(leaf).build();
  }

  private NameRef nameRefForTablePrefix(String fqPrefix) {
    ParsedFq p = parseFqFlexible(fqPrefix);
    NameRef.Builder b = NameRef.newBuilder().setCatalog(p.catalog).addAllPath(p.nsParts);
    p.object.ifPresent(b::setName);
    return b.build();
  }

  private static final class ParsedFq {
    final String catalog;
    final List<String> nsParts;
    final Optional<String> object;

    ParsedFq(String catalog, List<String> nsParts, String object) {
      this.catalog = catalog;
      this.nsParts = nsParts;
      this.object = Optional.ofNullable(object).filter(s -> !s.isBlank());
    }
  }

  private ParsedFq parseFqFlexible(String s) {
    s = s.trim();
    if (s.isEmpty() || !s.contains(".")) {
      throw new IllegalArgumentException(
          "Fully qualified name must contain a catalog and namespace");
    }

    if (s.contains("/")) {
      String[] parts = s.split("\\.", 3);
      if (parts.length < 2) {
        throw new IllegalArgumentException("Expected form: catalog.NS1/NS2/...[/NSk][.object]");
      }
      String catalog = parts[0];
      List<String> ns = parts[1].isBlank() ? List.of() : Arrays.asList(parts[1].split("/"));
      String object = (parts.length == 3) ? parts[2] : null;
      return new ParsedFq(catalog, ns, object);
    } else {
      String[] parts = s.split("\\.");
      if (parts.length < 2) {
        throw new IllegalArgumentException("Expected form: catalog.ns[.ns...][.object]");
      }
      String catalog = parts[0];
      String object = (parts.length >= 3) ? parts[parts.length - 1] : null;
      List<String> ns =
          Arrays.asList(parts).subList(1, object == null ? parts.length : parts.length - 1);
      return new ParsedFq(catalog, ns, object);
    }
  }
}
