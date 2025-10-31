package ai.floedb.metacat.client.cli;

import static java.lang.System.out;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
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
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.Precondition;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.connector.rpc.AuthConfig;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorKind;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.metacat.connector.rpc.GetReconcileJobRequest;
import ai.floedb.metacat.connector.rpc.ListConnectorsRequest;
import ai.floedb.metacat.connector.rpc.ReconcilePolicy;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.connector.rpc.ValidateConnectorRequest;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
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
    description = "Interactive CLI to browse and manage catalogs/namespaces/tables/connectors")
@jakarta.inject.Singleton
public class Shell implements Runnable {

  @Inject
  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @Inject
  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @Inject
  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @Inject
  @GrpcClient("metacat")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @Inject
  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @Inject
  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Inject
  @GrpcClient("metacat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  private volatile String currentTenantId =
      System.getenv().getOrDefault("METACAT_TENANT", "").trim();

  @Override
  public void run() {
    out.println("Metacat Shell (type 'help' for commands, 'quit' to exit).");
    try {
      Terminal terminal = TerminalBuilder.builder().system(true).build();
      Path historyPath = Paths.get(System.getProperty("user.home"), ".metacat_shell_history");
      var parser = new DefaultParser();
      parser.setEofOnUnclosedBracket(
          DefaultParser.Bracket.CURLY, DefaultParser.Bracket.ROUND, DefaultParser.Bracket.SQUARE);
      parser.setEscapeChars(null);
      Completer completer =
          new StringsCompleter(
              "catalogs",
              "catalog",
              "namespaces",
              "namespace",
              "tables",
              "table",
              "connectors",
              "connector",
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
              .variable(LineReader.HISTORY_FILE, historyPath)
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
        if (line == null) {
          break;
        }
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }
        if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
          break;
        }
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
        tenant <id>
        catalogs
        catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--opt k=v ...]
        catalog get <display_name|id>
        catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--opt k=v ...] [--etag <etag>]
        catalog delete <display_name|id> [--require-empty] [--etag <etag>]
        namespaces <catalog | catalog.ns[.ns...]> | --id UUID> [--prefix P] [--recursive]
        namespace create <catalogName|catalogId> <display_name|a.b.c> [--desc <text>] [--path ns1.ns2...] [--ann k=v ...] [--policy <id>]
        namespace get <id|catalog.ns[.ns...][.name]>
        namespace update <id|fq> [--display <name>] [--path ns1.ns2...] [--catalog <catalogName|id>] [--etag <etag>]
        namespace delete <id|fq> [--require-empty] [--etag <etag>]
        tables <catalog.ns[.ns...][.prefix]>
        table create <catalogName|id> <namespaceFQ|id> <name> [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--prop k=v ...]
        table get <id|catalog.ns[.ns...].table>
        table update <id|fq> [--catalog <catalogName|id>] [--namespace <namespaceFQ|id>] [--name <name>] [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--prop k=v ...] [--etag <etag>]
        table delete <id|fq> [--purge-stats] [--purge-snaps] [--etag <etag>]
        resolve table <fq> | resolve view <fq> | resolve catalog <name> | resolve namespace <fq>
        describe table <fq>
        snapshots <tableFQ>
        stats table <tableFQ> [--snapshot <id>|--current]
        stats columns <tableFQ> [--snapshot <id>|--current] [--limit N]
        connectors
        connector list [--kind <KIND>]
        connector get <display_name|id>
        connector create <display_name> <kind> <uri> [--target-catalog <display>] [--target-tenant <tenant>]
            [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
            [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
            [--policy-not-before-epoch <sec>] [--opt k=v ...]
        connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>] [--target-catalog <display>] [--target-tenant <tenant>]
            [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
            [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
            [--policy-not-before-epoch <sec>] [--opt k=v ...] [--etag <etag>]
        connector delete <display_name|id>  [--etag <etag>]
        connector validate <kind> <uri> [--target-catalog <display>] [--target-tenant <tenant>]
            [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
            [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
            [--policy-not-before-epoch <sec>] [--opt k=v ...]
        connector trigger <display_name|id> [--full]
        connector job <jobId>
        help
        quit
        """);
  }

  private void dispatch(String inputLine) {
    var tokens = tokenize(inputLine);
    if (tokens.isEmpty()) {
      return;
    }
    String command = tokens.get(0);

    switch (command) {
      case "tenant", "help", "quit", "exit" -> {}
      default -> ensureTenantSet();
    }

    switch (command) {
      case "tenant" -> cmdTenant(tail(tokens));
      case "catalogs" -> cmdCatalogs();
      case "catalog" -> cmdCatalogCrud(tail(tokens));
      case "namespaces" -> cmdNamespaces(tail(tokens));
      case "namespace" -> cmdNamespaceCrud(tail(tokens));
      case "tables" -> cmdTables(tail(tokens));
      case "table" -> cmdTableCrud(tail(tokens));
      case "connectors" -> cmdConnectorsList();
      case "connector" -> cmdConnectorCrud(tail(tokens));
      case "resolve" -> cmdResolve(tail(tokens));
      case "describe" -> cmdDescribe(tail(tokens));
      case "snapshots" -> cmdSnapshots(tail(tokens));
      case "stats" -> cmdStats(tail(tokens));
      default -> out.println("Unknown command. Type 'help'.");
    }
  }

  private void cmdTenant(List<String> args) {
    if (args.isEmpty()) {
      out.println(
          currentTenantId == null || currentTenantId.isBlank()
              ? "tenant: <not set>"
              : ("tenant: " + currentTenantId));
      return;
    }
    String t = args.get(0).trim();
    if (t.isEmpty()) {
      out.println("usage: tenant <tenantId>");
      return;
    }
    currentTenantId = t;
    out.println("tenant set: " + currentTenantId);
  }

  private List<String> tail(List<String> list) {
    return list.size() <= 1 ? List.of() : list.subList(1, list.size());
  }

  private void cmdCatalogs() {
    var response =
        catalogs.listCatalogs(
            ListCatalogsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(500).build())
                .build());
    printCatalogs(response.getCatalogsList());
  }

  private void cmdCatalogCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: catalog <create|get|update|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "create" -> {
        if (args.size() < 2) {
          out.println(
              "usage: catalog create <display_name> [--desc <text>] [--connector <id>] [--policy"
                  + " <id>] [--opt k=v ...]");
          return;
        }
        String display = args.get(1);
        String desc = parseStringFlag(args, "--desc", null);
        String connectorRef = parseStringFlag(args, "--connector", null);
        String policyRef = parseStringFlag(args, "--policy", null);
        Map<String, String> options = parseKeyValueList(args, "--opt");
        var spec =
            CatalogSpec.newBuilder()
                .setDisplayName(display)
                .setDescription(nvl(desc, ""))
                .setConnectorRef(nvl(connectorRef, ""))
                .putAllOptions(options)
                .setPolicyRef(nvl(policyRef, ""))
                .build();
        var resp =
            catalogs.createCatalog(
                ai.floedb.metacat.catalog.rpc.CreateCatalogRequest.newBuilder()
                    .setSpec(spec)
                    .setIdempotency(newIdem())
                    .build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: catalog get <display_name|id>");
          return;
        }
        var resp =
            catalogs.getCatalog(
                ai.floedb.metacat.catalog.rpc.GetCatalogRequest.newBuilder()
                    .setCatalogId(resolveCatalogId(args.get(1)))
                    .build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: catalog update <display_name|id> [--display <name>] [--desc <text>]"
                  + " [--connector <id>] [--policy <id>] [--opt k=v ...] [--etag <etag>]");
          return;
        }
        String id = args.get(1);
        String display = parseStringFlag(args, "--display", null);
        String desc = parseStringFlag(args, "--desc", null);
        String connectorRef = parseStringFlag(args, "--connector", null);
        String policyRef = parseStringFlag(args, "--policy", null);
        Map<String, String> options = parseKeyValueList(args, "--opt");
        var spec =
            CatalogSpec.newBuilder()
                .setDisplayName(nvl(display, ""))
                .setDescription(nvl(desc, ""))
                .setConnectorRef(nvl(connectorRef, ""))
                .putAllOptions(options)
                .setPolicyRef(nvl(policyRef, ""))
                .build();
        var req =
            ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(id))
                .setSpec(spec)
                .setPrecondition(preconditionFromEtag(args))
                .build();
        var resp = catalogs.updateCatalog(req);
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: catalog delete <display_name|id> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");
        var req =
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(args.get(1)))
                .setRequireEmpty(requireEmpty)
                .setPrecondition(preconditionFromEtag(args))
                .build();
        catalogs.deleteCatalog(req);
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdNamespaces(List<String> args) {
    if (args.isEmpty()) {
      out.println(
          "usage: namespaces <catalog | catalog.ns[.ns...] | --id UUID> [--prefix P]"
              + " [--recursive]");
      return;
    }

    String tokenArg = args.get(0).trim();
    String namePrefix = parseStringFlag(args, "--prefix", "");
    boolean recursive = args.contains("--recursive");
    boolean childrenOnly = !recursive;

    ListNamespacesRequest.Builder rb =
        ListNamespacesRequest.newBuilder()
            .setChildrenOnly(childrenOnly)
            .setRecursive(recursive)
            .setNamePrefix(nvl(namePrefix, ""))
            .setPage(PageRequest.newBuilder().setPageSize(1000).build());

    String explicitId = parseStringFlag(args, "--id", "");
    if (!explicitId.isBlank()) {
      rb.setNamespaceId(resolveNamespaceId(explicitId));
    } else if (looksLikeUuid(tokenArg)) {
      rb.setNamespaceId(resolveNamespaceId(tokenArg));
    } else if (tokenArg.contains(".") || tokenArg.contains("/")) {
      ParsedFq p = parseFqFlexible(tokenArg, false);
      rb.setCatalogId(resolveCatalogId(p.catalog)).addAllPath(p.nsParts);
    } else {
      rb.setCatalogId(resolveCatalogId(tokenArg));
    }

    List<Namespace> all = new ArrayList<>();
    String pageToken = "";
    do {
      var pageReq =
          rb.setPage(PageRequest.newBuilder().setPageSize(1000).setPageToken(pageToken)).build();
      var resp = namespaces.listNamespaces(pageReq);

      all.addAll(resp.getNamespacesList());

      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
      if (pageToken == null) {
        pageToken = "";
      }
    } while (!pageToken.isBlank());

    printNamespaces(all);
  }

  private void cmdNamespaceCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: namespace <create|get|update|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "create" -> {
        if (args.size() < 3) {
          out.println(
              "usage: namespace create <display_name|catalog_id> <display_name> [--desc <text>]"
                  + " [--path ns1/ns2/..] [--ann k=v ...] [--policy <id>]");
          return;
        }
        ResourceId catalogId = resolveCatalogId(args.get(1));
        String displayOrPath = args.get(2);
        String desc = parseStringFlag(args, "--desc", "");
        String path = parseStringFlag(args, "--path", "");
        Map<String, String> annotations = parseKeyValueList(args, "--ann");
        String policy = parseStringFlag(args, "--policy", "");
        String display;
        List<String> parents;
        if (path != null && !path.isBlank()) {
          display = displayOrPath;
          parents = pathToList(path);
        } else if (displayOrPath.contains("/") || displayOrPath.contains(".")) {
          List<String> parts = pathToList(displayOrPath);
          display = parts.get(parts.size() - 1);
          parents = parts.subList(0, parts.size() - 1);
        } else {
          display = displayOrPath;
          parents = List.of();
        }
        var spec =
            NamespaceSpec.newBuilder()
                .setCatalogId(catalogId)
                .setDisplayName(display)
                .setDescription(desc)
                .addAllPath(parents)
                .putAllAnnotations(annotations)
                .setPolicyRef(policy)
                .build();
        var resp =
            namespaces.createNamespace(
                ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest.newBuilder()
                    .setSpec(spec)
                    .setIdempotency(newIdem())
                    .build());
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: namespace get <id|fq>");
          return;
        }
        ResourceId nsId =
            looksLikeUuid(args.get(1))
                ? namespaceRid(args.get(1))
                : resolveNamespaceId(args.get(1));
        var resp =
            namespaces.getNamespace(
                ai.floedb.metacat.catalog.rpc.GetNamespaceRequest.newBuilder()
                    .setNamespaceId(nsId)
                    .build());
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: namespace update <id|fq> [--display <name>] [--path ns1/ns2/..] [--catalog"
                  + " <catalogId>] [--etag <etag>]");
          return;
        }
        ResourceId nsId =
            looksLikeUuid(args.get(1))
                ? namespaceRid(args.get(1))
                : resolveNamespaceId(args.get(1));
        String display = parseStringFlag(args, "--display", "");
        String path = parseStringFlag(args, "--path", "");
        String catalogId = parseStringFlag(args, "--catalog", "");
        var req =
            ai.floedb.metacat.catalog.rpc.UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setDisplayName(display)
                .addAllPath(pathToList(path))
                .setCatalogId(
                    catalogId.isBlank()
                        ? ResourceId.getDefaultInstance()
                        : resourceId(catalogId, ResourceKind.RK_CATALOG))
                .setPrecondition(preconditionFromEtag(args))
                .build();
        var resp = namespaces.updateNamespace(req);
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: namespace delete <id|fq> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");
        ResourceId nsId =
            looksLikeUuid(args.get(1))
                ? namespaceRid(args.get(1))
                : resolveNamespaceId(args.get(1));
        var req =
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(nsId)
                .setRequireEmpty(requireEmpty)
                .setPrecondition(preconditionFromEtag(args))
                .build();
        namespaces.deleteNamespace(req);
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdTables(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: tables <catalog.db.schema[.prefix]>");
      return;
    }
    var prefix = nameRefForTablePrefix(args.get(0));
    var resolved =
        directory.resolveFQTables(
            ResolveFQTablesRequest.newBuilder()
                .setPrefix(prefix)
                .setPage(PageRequest.newBuilder().setPageSize(1000).build())
                .build());
    printResolvedTables(resolved.getTablesList());
  }

  private void cmdTableCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: table <create|get|update|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "create" -> {
        if (args.size() < 4) {
          out.println(
              "usage: table create <catalogId> <namespaceId> <name> [--desc <text>] [--root <uri>]"
                  + " [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--prop k=v"
                  + " ...]");
          return;
        }
        ResourceId catalogId = resourceId(args.get(1), ResourceKind.RK_CATALOG);
        ResourceId namespaceId = resourceId(args.get(2), ResourceKind.RK_NAMESPACE);
        String name = args.get(3);
        String desc = parseStringFlag(args, "--desc", "");
        String root = parseStringFlag(args, "--root", "");
        String schema = parseStringFlag(args, "--schema", "");
        List<String> parts = csvToList(parseStringFlag(args, "--parts", ""));
        String formatStr = parseStringFlag(args, "--format", "");
        Map<String, String> props = parseKeyValueList(args, "--prop");
        var spec =
            TableSpec.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName(name)
                .setDescription(desc)
                .setRootUri(root)
                .setSchemaJson(schema)
                .addAllPartitionKeys(parts)
                .setFormat(parseFormat(formatStr))
                .putAllProperties(props)
                .build();
        var resp =
            tables.createTable(
                ai.floedb.metacat.catalog.rpc.CreateTableRequest.newBuilder()
                    .setSpec(spec)
                    .setIdempotency(newIdem())
                    .build());
        printTable(resp.getTable());
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: table get <id|catalog.ns[.ns...].table>");
          return;
        }
        ResourceId tableId;
        if (looksLikeUuid(args.get(1))) {
          tableId = tableRid(args.get(1));
        } else {
          var r =
              directory.resolveTable(
                  ResolveTableRequest.newBuilder().setRef(nameRefForTable(args.get(1))).build());
          tableId = r.getResourceId();
        }
        var resp = tables.getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
        printTable(resp.getTable());
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: table update <id|catalog.ns[.ns...].table> \" [--catalog <catalogId>]"
                  + " [--namespace <namespaceId>] [--name <name>] [--desc <text>] [--root <uri>]"
                  + " [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--prop k=v"
                  + " ...] [--etag <etag>]");
          return;
        }
        ResourceId tableId =
            looksLikeUuid(args.get(1))
                ? tableRid(args.get(1))
                : directory
                    .resolveTable(
                        ResolveTableRequest.newBuilder()
                            .setRef(nameRefForTable(args.get(1)))
                            .build())
                    .getResourceId();
        String catalogStr = parseStringFlag(args, "--catalog", "");
        String nsStr = parseStringFlag(args, "--namespace", "");
        String name = parseStringFlag(args, "--name", "");
        String desc = parseStringFlag(args, "--desc", "");
        String root = parseStringFlag(args, "--root", "");
        String schema = parseStringFlag(args, "--schema", "");
        List<String> parts = csvToList(parseStringFlag(args, "--parts", ""));
        String formatStr = parseStringFlag(args, "--format", "");
        Map<String, String> props = parseKeyValueList(args, "--prop");
        var spec =
            TableSpec.newBuilder()
                .setCatalogId(
                    catalogStr.isBlank()
                        ? ResourceId.getDefaultInstance()
                        : resourceId(catalogStr, ResourceKind.RK_CATALOG))
                .setNamespaceId(
                    nsStr.isBlank()
                        ? ResourceId.getDefaultInstance()
                        : resourceId(nsStr, ResourceKind.RK_NAMESPACE))
                .setDisplayName(name)
                .setDescription(desc)
                .setRootUri(root)
                .setSchemaJson(schema)
                .addAllPartitionKeys(parts)
                .setFormat(parseFormat(formatStr))
                .putAllProperties(props)
                .build();
        var req =
            ai.floedb.metacat.catalog.rpc.UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(spec)
                .setPrecondition(preconditionFromEtag(args))
                .build();
        var resp = tables.updateTable(req);
        printTable(resp.getTable());
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println(
              "usage: table delete <id|catalog.ns[.ns...].table> [--purge-stats] [--purge-snaps]"
                  + " [--etag <etag>]");
          return;
        }
        ResourceId tableId =
            looksLikeUuid(args.get(1))
                ? tableRid(args.get(1))
                : directory
                    .resolveTable(
                        ResolveTableRequest.newBuilder()
                            .setRef(nameRefForTable(args.get(1)))
                            .build())
                    .getResourceId();
        var req =
            DeleteTableRequest.newBuilder()
                .setTableId(tableId)
                .setPurgeStats(args.contains("--purge-stats"))
                .setPurgeSnapshots(args.contains("--purge-snaps"))
                .setPrecondition(preconditionFromEtag(args))
                .build();
        tables.deleteTable(req);
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdConnectorsList() {
    var resp =
        connectors.listConnectors(
            ListConnectorsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(500).build())
                .build());
    printConnectors(resp.getConnectorsList());
  }

  private void cmdConnectorCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: connector <list|get|create|update|delete|validate|trigger|job> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "list" -> {
        String kind = parseStringFlag(args, "--kind", "");
        var req =
            ListConnectorsRequest.newBuilder()
                .setPage(PageRequest.newBuilder().setPageSize(500).build())
                .setKind(kind)
                .build();
        var resp = connectors.listConnectors(req);
        printConnectors(resp.getConnectorsList());
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: connector get <display_name|id>");
          return;
        }
        var resp =
            connectors.getConnector(
                ai.floedb.metacat.connector.rpc.GetConnectorRequest.newBuilder()
                    .setConnectorId(resolveConnectorId(args.get(1)))
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "create" -> {
        if (args.size() < 4) {
          out.println(
              "usage: connector create <display_name> <kind> <uri> [--target-catalog <display>]"
                  + " [--target-tenant <tenant>] [--auth-scheme <scheme>] [--auth k=v ...] [--head"
                  + " k=v ...] [--secret <ref>] [--policy-enabled] [--policy-interval-sec <n>]"
                  + " [--policy-max-par <n>] [--policy-not-before-epoch <sec>] [--opt k=v ...]");
          return;
        }
        String display = args.get(1);
        ConnectorKind kind = parseConnectorKind(args.get(2));
        String uri = args.get(3);
        String targetCatalog = parseStringFlag(args, "--target-catalog", "");
        String targetTenant = parseStringFlag(args, "--target-tenant", "");
        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> options = parseKeyValueList(args, "--opt");
        var auth =
            AuthConfig.newBuilder()
                .setScheme(authScheme)
                .putAllProps(authProps)
                .putAllHeaderHints(headerHints)
                .setSecretRef(secretRef)
                .build();
        var policy =
            ReconcilePolicy.newBuilder()
                .setEnabled(policyEnabled)
                .setMaxParallel(maxPar)
                .setNotBefore(notBeforeSec == 0 ? nullTs() : tsSeconds(notBeforeSec))
                .setInterval(intervalSec == 0 ? nullDur() : durSeconds(intervalSec))
                .build();
        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName(display)
                .setKind(kind)
                .setTargetCatalogDisplayName(targetCatalog)
                .setTargetTenantId(targetTenant)
                .setUri(uri)
                .putAllOptions(options)
                .setAuth(auth)
                .setPolicy(policy)
                .build();
        var resp =
            connectors.createConnector(
                ai.floedb.metacat.connector.rpc.CreateConnectorRequest.newBuilder()
                    .setSpec(spec)
                    .setIdempotency(newIdem())
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri"
                  + " <uri>] [--target-catalog <display>] [--target-tenant <tenant>] [--auth-scheme"
                  + " <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]"
                  + " [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par"
                  + " <n>] [--policy-not-before-epoch <sec>] [--opt k=v ...] [--etag <etag>]");
          return;
        }
        ResourceId cid = resolveConnectorId(args.get(1));
        String display = parseStringFlag(args, "--display", "");
        String kindStr = parseStringFlag(args, "--kind", "");
        String uri = parseStringFlag(args, "--uri", "");
        String targetCatalog = parseStringFlag(args, "--target-catalog", "");
        String targetTenant = parseStringFlag(args, "--target-tenant", "");
        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        String policyEnabledStr = parseStringFlag(args, "--policy-enabled", "");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> options = parseKeyValueList(args, "--opt");
        var auth =
            AuthConfig.newBuilder()
                .setScheme(authScheme)
                .putAllProps(authProps)
                .putAllHeaderHints(headerHints)
                .setSecretRef(secretRef)
                .build();
        var policy =
            ReconcilePolicy.newBuilder()
                .setEnabled(
                    policyEnabledStr.isBlank() ? false : Boolean.parseBoolean(policyEnabledStr))
                .setMaxParallel(maxPar)
                .setNotBefore(notBeforeSec == 0 ? nullTs() : tsSeconds(notBeforeSec))
                .setInterval(intervalSec == 0 ? nullDur() : durSeconds(intervalSec))
                .build();
        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName(display)
                .setKind(
                    kindStr.isBlank() ? ConnectorKind.CK_UNSPECIFIED : parseConnectorKind(kindStr))
                .setTargetCatalogDisplayName(targetCatalog)
                .setTargetTenantId(targetTenant)
                .setUri(uri)
                .putAllOptions(options)
                .setAuth(auth)
                .setPolicy(policy)
                .build();
        var resp =
            connectors.updateConnector(
                ai.floedb.metacat.connector.rpc.UpdateConnectorRequest.newBuilder()
                    .setConnectorId(cid)
                    .setSpec(spec)
                    .setPrecondition(preconditionFromEtag(args))
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: connector delete <display_name|id> [--etag <etag>]");
          return;
        }
        var req =
            DeleteConnectorRequest.newBuilder()
                .setConnectorId(resolveConnectorId(args.get(1)))
                .setPrecondition(preconditionFromEtag(args))
                .build();
        connectors.deleteConnector(req);
        out.println("ok");
      }
      case "validate" -> {
        if (args.size() < 3) {
          out.println(
              "usage: connector validate <kind> <uri> [--target-catalog <display>] [--target-tenant"
                  + " <tenant>] [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]"
                  + " [--secret <ref>] [--policy-enabled] [--policy-interval-sec <n>]"
                  + " [--policy-max-par <n>] [--policy-not-before-epoch <sec>] [--opt k=v ...]");
          return;
        }
        ConnectorKind kind = parseConnectorKind(args.get(1));
        String uri = args.get(2);
        String targetCatalog = parseStringFlag(args, "--target-catalog", "");
        String targetTenant = parseStringFlag(args, "--target-tenant", "");
        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> options = parseKeyValueList(args, "--opt");
        var auth =
            AuthConfig.newBuilder()
                .setScheme(authScheme)
                .putAllProps(authProps)
                .putAllHeaderHints(headerHints)
                .setSecretRef(secretRef)
                .build();
        var policy =
            ReconcilePolicy.newBuilder()
                .setEnabled(policyEnabled)
                .setMaxParallel(maxPar)
                .setNotBefore(notBeforeSec == 0 ? nullTs() : tsSeconds(notBeforeSec))
                .setInterval(intervalSec == 0 ? nullDur() : durSeconds(intervalSec))
                .build();
        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName("")
                .setKind(kind)
                .setTargetCatalogDisplayName(targetCatalog)
                .setTargetTenantId(targetTenant)
                .setUri(uri)
                .putAllOptions(options)
                .setAuth(auth)
                .setPolicy(policy)
                .build();
        var resp =
            connectors.validateConnector(
                ValidateConnectorRequest.newBuilder().setSpec(spec).build());
        out.printf(
            "ok=%s summary=%s capabilities=%s%n",
            resp.getOk(),
            resp.getSummary(),
            resp.getCapabilitiesMap().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(",")));
      }
      case "trigger" -> {
        if (args.size() < 2) {
          out.println("usage: connector trigger <display_name|id> [--full]");
          return;
        }
        boolean full = args.contains("--full");
        var resp =
            connectors.triggerReconcile(
                TriggerReconcileRequest.newBuilder()
                    .setConnectorId(resolveConnectorId(args.get(1)))
                    .setFullRescan(full)
                    .build());
        out.println(resp.getJobId());
      }
      case "job" -> {
        if (args.size() < 2) {
          out.println("usage: connector job <jobId>");
          return;
        }
        var resp =
            connectors.getReconcileJob(
                GetReconcileJobRequest.newBuilder().setJobId(args.get(1)).build());
        out.printf(
            "job_id=%s connector_id=%s state=%s message=%s started=%s finished=%s scanned=%d"
                + " changed=%d errors=%d%n",
            resp.getJobId(),
            resp.getConnectorId(),
            resp.getState().name(),
            resp.getMessage(),
            ts(resp.getStartedAt()),
            ts(resp.getFinishedAt()),
            resp.getTablesScanned(),
            resp.getTablesChanged(),
            resp.getErrors());
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdResolve(List<String> args) {
    if (args.size() < 2) {
      out.println("usage: resolve table|view|catalog|namespace <fq-or-name>");
      return;
    }
    String kind = args.get(0);
    String s = args.get(1);
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
    var resp = statistics.getTableStats(req);
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
    printColumnStats(statistics.listColumnStats(req).getColumnsList());
  }

  private boolean looksLikeUuid(String s) {
    if (s == null) {
      return false;
    }
    String t = s.trim();
    return t.matches(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  private ResourceId resourceId(String id, ai.floedb.metacat.common.rpc.ResourceKind kind) {
    if (currentTenantId == null || currentTenantId.isBlank()) {
      throw new IllegalStateException("No tenant set. Use: tenant <tenantId>");
    }
    return ResourceId.newBuilder().setTenantId(currentTenantId).setKind(kind).setId(id).build();
  }

  private ResourceId catalogRid(String id) {
    return resourceId(id, ai.floedb.metacat.common.rpc.ResourceKind.RK_CATALOG);
  }

  private ResourceId namespaceRid(String id) {
    return resourceId(id, ai.floedb.metacat.common.rpc.ResourceKind.RK_NAMESPACE);
  }

  private ResourceId tableRid(String id) {
    return resourceId(id, ai.floedb.metacat.common.rpc.ResourceKind.RK_TABLE);
  }

  private ResourceId connectorRid(String id) {
    return resourceId(id, ai.floedb.metacat.common.rpc.ResourceKind.RK_CONNECTOR);
  }

  private ResourceId resolveConnectorId(String token) {
    if (looksLikeUuid(token)) {
      return connectorRid(token);
    }

    var req =
        ListConnectorsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(1000).build())
            .build();
    var resp = connectors.listConnectors(req);
    var exact =
        resp.getConnectorsList().stream().filter(c -> token.equals(c.getDisplayName())).toList();
    if (exact.size() == 1) return exact.get(0).getResourceId();

    var ci =
        resp.getConnectorsList().stream()
            .filter(c -> token.equalsIgnoreCase(c.getDisplayName()))
            .toList();
    if (ci.size() == 1) return ci.get(0).getResourceId();

    if (exact.isEmpty() && ci.isEmpty()) {
      throw new IllegalArgumentException("Connector not found: " + token);
    }
    String alts =
        (exact.isEmpty() ? ci : exact)
            .stream()
                .map(c -> c.getDisplayName() + " (" + rid(c.getResourceId()) + ")")
                .collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Connector name is ambiguous: " + token + ". Candidates: " + alts);
  }

  private ResourceId resolveCatalogId(String token) {
    if (looksLikeUuid(token)) {
      return catalogRid(token);
    }
    return directory
        .resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(nameCatalog(token)).build())
        .getResourceId();
  }

  private ResourceId resolveNamespaceId(String tokenOrFq) {
    if (looksLikeUuid(tokenOrFq)) {
      return namespaceRid(tokenOrFq);
    }
    return directory
        .resolveNamespace(
            ResolveNamespaceRequest.newBuilder().setRef(nameRefForNamespace(tokenOrFq)).build())
        .getResourceId();
  }

  private NameRef nameCatalog(String name) {
    return NameRef.newBuilder().setCatalog(name).build();
  }

  private NameRef nameFromFq(String fq) {
    ParsedFq p = parseFqFlexible(fq, /* expectObject= */ true);
    if (p.object.isEmpty()) {
      throw new IllegalArgumentException("Missing table name in FQ: " + fq);
    }
    return NameRef.newBuilder()
        .setCatalog(p.catalog)
        .addAllPath(p.nsParts)
        .setName(p.object.get())
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

  private long parseLongFlag(List<String> args, String flag, long dflt) {
    int i = args.indexOf(flag);
    if (i >= 0 && i + 1 < args.size()) {
      try {
        return Long.parseLong(args.get(i + 1));
      } catch (Exception ignore) {
      }
    }
    return dflt;
  }

  private String parseStringFlag(List<String> args, String flag, String dflt) {
    int i = args.indexOf(flag);
    if (i >= 0 && i + 1 < args.size()) {
      return args.get(i + 1);
    }
    return dflt;
  }

  private Map<String, String> parseKeyValueList(List<String> args, String flag) {
    Map<String, String> outMap = new LinkedHashMap<>();
    for (int i = 0; i < args.size(); i++) {
      if (flag.equals(args.get(i)) && i + 1 < args.size()) {
        int j = i + 1;
        while (j < args.size() && !args.get(j).startsWith("--")) {
          String kv = args.get(j);
          int eq = kv.indexOf('=');
          if (eq > 0) {
            String k = kv.substring(0, eq);
            String v = kv.substring(eq + 1);
            outMap.put(k, v);
          }
          j++;
        }
      }
    }
    return outMap;
  }

  private List<String> csvToList(String s) {
    if (s == null || s.isBlank()) {
      return List.of();
    }
    return Arrays.stream(s.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
  }

  private List<String> pathToList(String s) {
    if (s == null || s.isBlank()) {
      return List.of();
    }
    return Arrays.stream(s.split("[./]")).map(String::trim).filter(x -> !x.isEmpty()).toList();
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

  private void printNamespaces(List<Namespace> rows) {
    out.printf("%-36s  %-5s  %-24s  %s%n", "NAMESPACE_ID", "TYPE", "CREATED_AT", "PATH");

    for (var ns : rows) {
      String type = ns.hasResourceId() ? "real" : "virt";
      String created = ns.hasCreatedAt() ? ts(ns.getCreatedAt()) : "-";
      String path =
          String.join(".", ns.getParentsList().isEmpty() ? List.of() : ns.getParentsList())
              + (ns.getDisplayName().isBlank()
                  ? ""
                  : (ns.getParentsList().isEmpty() ? "" : ".") + ns.getDisplayName());

      String id = ns.hasResourceId() ? ns.getResourceId().getId() : "<virtual>";
      out.printf("%-36s  %-5s  %-24s  %s%n", id, type, created, path);
    }
  }

  private void printResolvedTables(List<ResolveFQTablesResponse.Entry> entries) {
    out.printf("%-40s  %s%n", "TABLE_ID", "NAME(parts)");
    for (var e : entries) {
      String catalog = e.getName().getCatalog();
      String namespace = String.join(".", e.getName().getPathList());
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
          "  upstream:        system=%s commit=%s created=%s%n",
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

  private void printConnectors(List<Connector> list) {
    out.printf(
        "%-40s %-8s %-20s %-24s %-24s %s%n",
        "CONNECTOR_ID", "KIND", "DISPLAY", "CREATED_AT", "UPDATED_AT", "URI");
    for (var c : list) {
      out.printf(
          "%-40s %-8s %-20s %-24s %-24s %s%n",
          rid(c.getResourceId()),
          c.getKind().name(),
          c.getDisplayName(),
          ts(c.getCreatedAt()),
          ts(c.getUpdatedAt()),
          c.getUri());
    }
  }

  private String ts(com.google.protobuf.Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private com.google.protobuf.Timestamp tsSeconds(long epochSeconds) {
    return com.google.protobuf.Timestamp.newBuilder().setSeconds(epochSeconds).build();
  }

  private com.google.protobuf.Timestamp nullTs() {
    return com.google.protobuf.Timestamp.getDefaultInstance();
  }

  private com.google.protobuf.Duration nullDur() {
    return com.google.protobuf.Duration.getDefaultInstance();
  }

  private com.google.protobuf.Duration durSeconds(long seconds) {
    return com.google.protobuf.Duration.newBuilder().setSeconds(seconds).build();
  }

  private String trunc(String s, int n) {
    if (s == null) {
      return "-";
    }
    return s.length() <= n ? s : (s.substring(0, n - 1) + "…");
  }

  private String ndvToString(Ndv n) {
    if (n.hasExact()) {
      return Long.toString(n.getExact());
    }
    if (n.hasApprox()) {
      NdvApprox approx = n.getApprox();
      double estimate = approx.getEstimate();
      return String.valueOf(estimate);
    }
    return "-";
  }

  private NameRef nameRefForTable(String fq) {
    ParsedFq p = parseFqFlexible(fq, true);
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
    ParsedFq p = parseFqFlexible(fqNs, false);
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
    ParsedFq p = parseFqFlexible(fqPrefix, false);
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

  private ParsedFq parseFqFlexible(String s, boolean expectObject) {
    s = s.trim();
    if (s.isEmpty() || !s.contains(".")) {
      throw new IllegalArgumentException(
          "Fully qualified name must contain a catalog and namespace");
    }

    String[] head = s.split("\\.", 2);
    if (head.length < 2) {
      throw new IllegalArgumentException("Expected form: catalog.ns[.ns...][.object]");
    }
    String catalog = head[0];
    String remainder = head[1];

    List<String> segs =
        Arrays.stream(remainder.split("[./]")).map(String::trim).filter(x -> !x.isEmpty()).toList();
    if (segs.isEmpty()) {
      throw new IllegalArgumentException("Namespace path is empty");
    }

    if (expectObject && segs.size() >= 2) {
      String object = segs.get(segs.size() - 1);
      List<String> ns = segs.subList(0, segs.size() - 1);
      return new ParsedFq(catalog, ns, object);
    } else {
      return new ParsedFq(catalog, segs, null);
    }
  }

  private Precondition preconditionFromEtag(List<String> args) {
    String etag = parseStringFlag(args, "--etag", "");
    if (etag == null || etag.isBlank()) {
      return Precondition.getDefaultInstance();
    }
    return Precondition.newBuilder().setExpectedEtag(etag).build();
  }

  private IdempotencyKey newIdem() {
    return IdempotencyKey.newBuilder().setKey(UUID.randomUUID().toString()).build();
  }

  private String nvl(String s, String d) {
    return s == null ? d : s;
  }

  private TableFormat parseFormat(String s) {
    if (s == null || s.isBlank()) {
      return TableFormat.TF_UNSPECIFIED;
    }
    String u = s.trim().toUpperCase(Locale.ROOT);
    if (u.equals("ICEBERG")) {
      return TableFormat.TF_ICEBERG;
    }
    if (u.equals("DELTA")) {
      return TableFormat.TF_DELTA;
    }
    return TableFormat.TF_UNSPECIFIED;
  }

  private ConnectorKind parseConnectorKind(String s) {
    if (s == null || s.isBlank()) {
      return ConnectorKind.CK_UNSPECIFIED;
    }
    String u = s.trim().toUpperCase(Locale.ROOT);
    return switch (u) {
      case "ICEBERG" -> ConnectorKind.CK_ICEBERG;
      case "DELTA" -> ConnectorKind.CK_DELTA;
      case "GLUE" -> ConnectorKind.CK_GLUE;
      case "UNITY" -> ConnectorKind.CK_UNITY;
      default -> ConnectorKind.CK_UNSPECIFIED;
    };
  }

  private List<String> tokenize(String line) {
    List<String> out = new ArrayList<>();
    StringBuilder cur = new StringBuilder();
    boolean inSingle = false, inDouble = false, escaping = false;
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (escaping) {
        cur.append(ch);
        escaping = false;
        continue;
      }
      if (ch == '\\') {
        escaping = true;
        continue;
      }
      if (ch == '\'' && !inDouble) {
        inSingle = !inSingle;
        continue;
      }
      if (ch == '"' && !inSingle) {
        inDouble = !inDouble;
        continue;
      }
      if (!inSingle && !inDouble && Character.isWhitespace(ch)) {
        if (cur.length() > 0) {
          out.add(cur.toString());
          cur.setLength(0);
        }
        continue;
      }
      cur.append(ch);
    }
    if (inSingle || inDouble) {
      throw new IllegalArgumentException("Unclosed quote in command");
    }
    if (cur.length() > 0) out.add(cur.toString());
    return out;
  }

  private void ensureTenantSet() {
    if (currentTenantId == null || currentTenantId.isBlank()) {
      throw new IllegalStateException("No tenant set. Use: tenant <tenantId>");
    }
  }
}
