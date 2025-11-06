package ai.floedb.metacat.client.cli;

import static java.lang.System.out;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.CatalogSpec;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.GetCatalogRequest;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.metacat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.metacat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.LookupTableRequest;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
import ai.floedb.metacat.catalog.rpc.Ndv;
import ai.floedb.metacat.catalog.rpc.NdvApprox;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.metacat.catalog.rpc.ResolveFQTablesResponse.Entry;
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
import ai.floedb.metacat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.metacat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
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
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.metacat.connector.rpc.DestinationTarget;
import ai.floedb.metacat.connector.rpc.GetConnectorRequest;
import ai.floedb.metacat.connector.rpc.GetReconcileJobRequest;
import ai.floedb.metacat.connector.rpc.ListConnectorsRequest;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import ai.floedb.metacat.connector.rpc.ReconcilePolicy;
import ai.floedb.metacat.connector.rpc.SourceSelector;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.metacat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanRequest;
import ai.floedb.metacat.planning.rpc.BeginPlanResponse;
import ai.floedb.metacat.planning.rpc.EndPlanRequest;
import ai.floedb.metacat.planning.rpc.EndPlanResponse;
import ai.floedb.metacat.planning.rpc.ExpansionMap;
import ai.floedb.metacat.planning.rpc.GetPlanRequest;
import ai.floedb.metacat.planning.rpc.GetPlanResponse;
import ai.floedb.metacat.planning.rpc.PlanDescriptor;
import ai.floedb.metacat.planning.rpc.PlanFile;
import ai.floedb.metacat.planning.rpc.PlanInput;
import ai.floedb.metacat.planning.rpc.PlanningGrpc;
import ai.floedb.metacat.planning.rpc.RenewPlanRequest;
import ai.floedb.metacat.planning.rpc.RenewPlanResponse;
import ai.floedb.metacat.planning.rpc.SnapshotPin;
import ai.floedb.metacat.planning.rpc.SnapshotSet;
import ai.floedb.metacat.planning.rpc.TableObligations;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Inject
  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @Inject
  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Inject
  @GrpcClient("metacat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject
  @GrpcClient("metacat")
  PlanningGrpc.PlanningBlockingStub planning;

  private static final int DEFAULT_PAGE_SIZE = 1000;

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
              "plan",
              "tenant",
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
          Throwable root = e;
          while (root.getCause() != null) {
            root = root.getCause();
          }

          out.println(
              "! "
                  + e.getClass().getSimpleName()
                  + ": "
                  + (e.getMessage() == null ? "-" : e.getMessage()));
          if (root != e && root.getMessage() != null) {
            out.println(
                "! caused by: " + root.getClass().getSimpleName() + ": " + root.getMessage());
          }
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
         catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...]
         catalog get <display_name|id>
         catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]
         catalog delete <display_name|id> [--require-empty] [--etag <etag>]
         namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P] [--recursive]
         namespace create <catalog.ns[.ns...]> [--desc <text>] [--props k=v ...] [--policy <id>]
         namespace get <id | catalog.ns[.ns...]>
         namespace update <id|catalog.ns[.ns...]>
             [--display <name>] [--desc <text>]
             [--policy <ref>] [--props k=v ...]
             [--path a.b[.c]] [--catalog <id|name>]
             [--etag <etag>]
         namespace delete <id|fq> [--require-empty] [--etag <etag>]
         tables <catalog.ns[.ns...][.prefix]>
         table create <catalog.ns[.ns...].name> [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--props k=v ...]
             [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]
         table get <id|catalog.ns[.ns...].table>
         table update <id|fq> [--catalog <catalogName|id>] [--namespace <namespaceFQ|id>] [--name <name>] [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] [--format ICEBERG|DELTA] [--props k=v ...] [--etag <etag>]
             [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]
         table delete <id|fq> [--purge-stats] [--purge-snaps] [--etag <etag>]
         resolve table <fq> | resolve view <fq> | resolve catalog <name> | resolve namespace <fq>
         describe table <fq>
         snapshots <tableFQ>
         stats table <tableFQ> [--snapshot <id>|--current] (defaults to --current)
         stats columns <tableFQ> [--snapshot <id>|--current] [--limit N] defaults to --current
         plan begin [--ttl <seconds>] [--as-of-default <timestamp>] (table <catalog.ns....table> [--snapshot <id|current>] [--as-of <timestamp>] | table-id <uuid> [--snapshot <id|current>] [--as-of <timestamp>] | view-id <uuid> | namespace <catalog.ns[.ns...]>)+
         plan renew <plan_id> [--ttl <seconds>]
         plan end <plan_id> [--commit|--abort]
         plan get <plan_id>
         connectors
         connector list [--kind <KIND>] [--page-size <N>]
         connector get <display_name|id>
         connector create <display_name> <source_type (ICEBERG|DELTA|GLUE|UNITY)> <uri> <source_namespace (a[.b[.c]...])> <destination_catalog (name)>
             [--source-table <name>] [--source-cols c1,#id2,...]
             [--dest-ns <a.b[.c]>] [--dest-table <name>]
             [--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...]
             [--head k=v ...] [--secret <ref>]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
         connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
             [--dest-tenant <tenant>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
             [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
         connector delete <display_name|id>  [--etag <etag>]
         connector validate <kind> <uri>
             [--dest-tenant <tenant>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
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
      case "plan" -> cmdPlan(tail(tokens));
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
    ListCatalogsRequest.Builder rb =
        ListCatalogsRequest.newBuilder()
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    List<Catalog> all = new ArrayList<>();
    String pageToken = "";
    do {
      var pageReq =
          rb.setPage(
                  PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).setPageToken(pageToken))
              .build();
      var resp = catalogs.listCatalogs(pageReq);

      all.addAll(resp.getCatalogsList());

      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
      if (pageToken == null) {
        pageToken = "";
      }
    } while (!pageToken.isBlank());

    printCatalogs(all);
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
                  + " <id>] [--props k=v ...]");
          return;
        }
        String display = args.get(1);
        String desc = parseStringFlag(args, "--desc", null);
        String connectorRef = parseStringFlag(args, "--connector", null);
        String policyRef = parseStringFlag(args, "--policy", null);
        Map<String, String> properties = parseKeyValueList(args, "--props");
        var spec =
            CatalogSpec.newBuilder()
                .setDisplayName(display)
                .setDescription(nvl(desc, ""))
                .setConnectorRef(nvl(connectorRef, ""))
                .putAllProperties(properties)
                .setPolicyRef(nvl(policyRef, ""))
                .build();
        var resp =
            catalogs.createCatalog(
                CreateCatalogRequest.newBuilder().setSpec(spec).setIdempotency(newIdem()).build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: catalog get <display_name|id>");
          return;
        }
        var resp =
            catalogs.getCatalog(
                GetCatalogRequest.newBuilder().setCatalogId(resolveCatalogId(args.get(1))).build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: catalog update <display_name|id> [--display <name>] [--desc <text>]"
                  + " [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]");
          return;
        }
        String id = args.get(1);
        String display = parseStringFlag(args, "--display", null);
        String desc = parseStringFlag(args, "--desc", null);
        String connectorRef = parseStringFlag(args, "--connector", null);
        String policyRef = parseStringFlag(args, "--policy", null);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var sb = CatalogSpec.newBuilder();
        LinkedHashSet<String> mask = new java.util.LinkedHashSet<>();

        if (display != null) {
          sb.setDisplayName(display);
          mask.add("display_name");
        }

        if (desc != null) {
          sb.setDescription(desc);
          mask.add("description");
        }

        if (connectorRef != null) {
          sb.setConnectorRef(connectorRef);
          mask.add("connector_ref");
        }

        if (policyRef != null) {
          sb.setPolicyRef(policyRef);
          mask.add("policy_ref");
        }

        if (!properties.isEmpty()) {
          sb.putAllProperties(properties);
          mask.add("properties");
        }

        var req =
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(id))
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build())
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
          "usage: namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P]"
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
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

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
          rb.setPage(
                  PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).setPageToken(pageToken))
              .build();
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
        if (args.size() < 2) {
          out.println(
              "usage: namespace create <catalog.ns[.ns...]> [--desc <text>] [--props k=v ...]"
                  + " [--policy <id>]");
          return;
        }
        String fq = args.get(1);
        ParsedFq p = parseFqFlexible(fq, false);
        if (p.nsParts.isEmpty()) {
          throw new IllegalArgumentException("Namespace path is empty in: " + fq);
        }

        ResourceId catalogId = resolveCatalogId(p.catalog);
        String display = p.nsParts.get(p.nsParts.size() - 1);
        List<String> parents = p.nsParts.subList(0, p.nsParts.size() - 1);

        String desc = parseStringFlag(args, "--desc", "");
        Map<String, String> properties = parseKeyValueList(args, "--props");
        String policy = parseStringFlag(args, "--policy", "");

        var spec =
            NamespaceSpec.newBuilder()
                .setCatalogId(catalogId)
                .setDisplayName(display)
                .setDescription(desc)
                .addAllPath(parents)
                .putAllProperties(properties)
                .setPolicyRef(policy)
                .build();

        var resp =
            namespaces.createNamespace(
                CreateNamespaceRequest.newBuilder()
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
            namespaces.getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(nsId).build());
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: namespace update <id|catalog.ns[.ns...]> "
                  + "[--display <name>] [--desc <text>] "
                  + "[--policy <ref>] [--props k=v ...] "
                  + "[--path a.b[.c]] [--catalog <id|name>] "
                  + "[--etag <etag>]");
          return;
        }

        ResourceId namespaceId =
            looksLikeUuid(args.get(1))
                ? namespaceRid(args.get(1))
                : directory
                    .resolveNamespace(
                        ResolveNamespaceRequest.newBuilder()
                            .setRef(nameRefForNamespace(args.get(1)))
                            .build())
                    .getResourceId();

        String display = parseStringFlag(args, "--display", null);
        String desc = parseStringFlag(args, "--desc", null);
        String policyRef = parseStringFlag(args, "--policy", null);
        String pathStr = parseStringFlag(args, "--path", null);
        String catalogStr = parseStringFlag(args, "--catalog", null);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        if (display != null && pathStr != null) {
          out.println("Error: cannot combine --path with --display in the same update.");
          return;
        }

        NamespaceSpec.Builder sb = NamespaceSpec.newBuilder();
        LinkedHashSet<String> mask = new LinkedHashSet<>();

        if (display != null) {
          sb.setDisplayName(display);
          mask.add("display_name");
        }

        if (desc != null) {
          sb.setDescription(desc);
          mask.add("description");
        }

        if (policyRef != null) {
          sb.setPolicyRef(policyRef);
          mask.add("policy_ref");
        }

        if (pathStr != null) {
          var pathList = pathStr.isBlank() ? List.<String>of() : pathToList(pathStr);
          sb.clearPath().addAllPath(pathList);
          mask.add("path");
        }

        if (catalogStr != null) {
          ResourceId cid =
              looksLikeUuid(catalogStr) ? catalogRid(catalogStr) : resolveCatalogId(catalogStr);
          sb.setCatalogId(cid);
          mask.add("catalog_id");
        }

        if (!properties.isEmpty()) {
          sb.putAllProperties(properties);
          mask.add("properties");
        }

        if (mask.isEmpty()) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }

        var req =
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build())
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

    ResolveFQTablesRequest.Builder rb =
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefix)
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    List<Entry> all = new ArrayList<>();
    String pageToken = "";
    do {
      var pageReq =
          rb.setPage(
                  PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).setPageToken(pageToken))
              .build();
      var resp = directory.resolveFQTables(pageReq);

      all.addAll(resp.getTablesList());

      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
      if (pageToken == null) {
        pageToken = "";
      }
    } while (!pageToken.isBlank());

    printResolvedTables(all);
  }

  private void cmdTableCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: table <create|get|update|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "create" -> {
        if (args.size() < 2) {
          out.println(
              "usage: table create <catalog.ns[.ns...].name> "
                  + " [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>]"
                  + "[--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...] "
                  + "[--format ICEBERG|DELTA] [--props k=v ...]");
          return;
        }

        var ref = nameRefForTable(args.get(1));
        ResourceId catalogId = resolveCatalogId(ref.getCatalog());
        String nsFq =
            ref.getCatalog()
                + (ref.getPathList().isEmpty() ? "" : "." + String.join(".", ref.getPathList()));
        ResourceId namespaceId = resolveNamespaceId(nsFq);
        String name = ref.getName();

        String desc = parseStringFlag(args, "--desc", "");
        String root = parseStringFlag(args, "--root", "");
        String schema = parseStringFlag(args, "--schema", "");
        List<String> parts = csvToList(parseStringFlag(args, "--parts", ""));
        String formatStr = parseStringFlag(args, "--format", "");
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = parseStringFlag(args, "--up-connector", "");
        String upNs = parseStringFlag(args, "--up-ns", "");
        String upTable = parseStringFlag(args, "--up-table", "");

        var ub =
            UpstreamRef.newBuilder()
                .setFormat(parseFormat(formatStr))
                .setUri(root)
                .addAllPartitionKeys(parts);

        if (!upConnector.isBlank()) {
          ub.setConnectorId(resolveConnectorId(upConnector));
        }
        if (!upNs.isBlank()) {
          ub.clearNamespacePath().addAllNamespacePath(pathToList(upNs));
        }
        if (!upTable.isBlank()) {
          ub.setTableDisplayName(upTable);
        }

        var upstream = ub.build();

        var spec =
            TableSpec.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName(name)
                .setDescription(desc)
                .setUpstream(upstream)
                .setSchemaJson(schema)
                .putAllProperties(props)
                .build();

        var resp =
            tables.createTable(
                CreateTableRequest.newBuilder().setSpec(spec).setIdempotency(newIdem()).build());
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
              "usage: table update <id|catalog.ns[.ns...].table> [--catalog"
                  + " <catalogId|catalogName>] [--namespace <namespaceId|catalog.ns[.ns...]>]"
                  + " [--up-connector <id|name>] [--up-ns <a.b[.c]>] [--up-table <name>] [--name"
                  + " <name>] [--desc <text>] [--root <uri>] [--schema <json>] [--parts k1,k2,...]"
                  + " [--format ICEBERG|DELTA] [--props k=v ...] [--etag <etag>]");
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

        String catalogStr = parseStringFlag(args, "--catalog", null);
        String nsStr = parseStringFlag(args, "--namespace", null);
        String name = parseStringFlag(args, "--name", null);
        String desc = parseStringFlag(args, "--desc", null);
        String root = parseStringFlag(args, "--root", null);
        String schema = parseStringFlag(args, "--schema", null);
        List<String> parts = csvToList(parseStringFlag(args, "--parts", ""));
        String formatStr = parseStringFlag(args, "--format", "");
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = parseStringFlag(args, "--up-connector", null);
        String upNs = parseStringFlag(args, "--up-ns", null);
        String upTable = parseStringFlag(args, "--up-table", null);

        boolean changingCatalog = catalogStr != null && !catalogStr.isBlank();
        boolean changingNs = nsStr != null && !nsStr.isBlank();
        if (changingCatalog ^ changingNs) {
          out.println(
              "Error: moving a table across catalogs requires both --catalog and --namespace.");
          return;
        }

        TableSpec.Builder sb = TableSpec.newBuilder();
        UpstreamRef.Builder ub = UpstreamRef.newBuilder();

        ArrayList<String> maskPaths = new ArrayList<>();
        boolean touchUpstream = false;

        if (catalogStr != null && !catalogStr.isBlank()) {
          ResourceId cid =
              looksLikeUuid(catalogStr) ? catalogRid(catalogStr) : resolveCatalogId(catalogStr);
          sb.setCatalogId(cid);
          maskPaths.add("catalog_id");
        }

        if (nsStr != null && !nsStr.isBlank()) {
          ResourceId nid = looksLikeUuid(nsStr) ? namespaceRid(nsStr) : resolveNamespaceId(nsStr);
          sb.setNamespaceId(nid);
          maskPaths.add("namespace_id");
        }

        if (name != null) {
          sb.setDisplayName(name);
          maskPaths.add("display_name");
        }

        if (desc != null) {
          sb.setDescription(desc);
          maskPaths.add("description");
        }

        if (schema != null) {
          sb.setSchemaJson(schema);
          maskPaths.add("schema_json");
        }

        if (!props.isEmpty()) {
          sb.putAllProperties(props);
          maskPaths.add("properties");
        }

        if (root != null) {
          ub.setUri(root);
          maskPaths.add("upstream.uri");
          touchUpstream = true;
        }

        if (!parts.isEmpty()) {
          ub.addAllPartitionKeys(parts);
          maskPaths.add("upstream.partition_keys");
          touchUpstream = true;
        }

        if (formatStr != null && !formatStr.isBlank()) {
          ub.setFormat(parseFormat(formatStr));
          maskPaths.add("upstream.format");
          touchUpstream = true;
        }

        if (upConnector != null && !upConnector.isBlank()) {
          ub.setConnectorId(resolveConnectorId(upConnector));
          maskPaths.add("upstream.connector_id");
          touchUpstream = true;
        }

        if (upNs != null && !upNs.isBlank()) {
          ub.clearNamespacePath().addAllNamespacePath(pathToList(upNs));
          maskPaths.add("upstream.namespace_path");
          touchUpstream = true;
        }

        if (upTable != null && !upTable.isBlank()) {
          ub.setTableDisplayName(upTable);
          maskPaths.add("upstream.table_display_name");
          touchUpstream = true;
        }

        if (touchUpstream) {
          sb.setUpstream(ub.build());
        }

        if (maskPaths.isEmpty()) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }

        FieldMask mask = FieldMask.newBuilder().addAllPaths(maskPaths).build();

        var req =
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(sb.build())
                .setPrecondition(preconditionFromEtag(args))
                .setUpdateMask(mask)
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
    var all = listAllConnectors(null, DEFAULT_PAGE_SIZE);
    printConnectors(all);
  }

  private List<Connector> listAllConnectors(ConnectorKind kind, int pageSize) {
    List<Connector> out = new ArrayList<>();
    String pageToken = "";
    do {
      var req =
          ListConnectorsRequest.newBuilder()
              .setPage(
                  PageRequest.newBuilder().setPageSize(pageSize).setPageToken(pageToken).build())
              .build();

      var resp = connectors.listConnectors(req);
      List<Connector> connectorsList = resp.getConnectorsList();

      for (Connector c : connectorsList) {
        if (kind == null || c.getKind() == kind) {
          out.add(c);
        }
      }

      pageToken = resp.getPage().getNextPageToken();
    } while (!pageToken.isEmpty());
    return out;
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
        int pageSize = parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);

        ConnectorKind connectorKind = ConnectorKind.CK_UNSPECIFIED;
        if (!kind.isBlank()) {
          switch (kind.toLowerCase(Locale.ROOT)) {
            case "iceberg" -> {
              connectorKind = ConnectorKind.CK_ICEBERG;
            }
            case "delta" -> {
              connectorKind = ConnectorKind.CK_DELTA;
            }
            case "glue" -> {
              connectorKind = ConnectorKind.CK_GLUE;
            }
            case "unity" -> {
              connectorKind = ConnectorKind.CK_UNITY;
            }
          }
        }

        var all = listAllConnectors(connectorKind, pageSize);
        printConnectors(all);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: connector get <display_name|id>");
          return;
        }
        var resp =
            connectors.getConnector(
                GetConnectorRequest.newBuilder()
                    .setConnectorId(resolveConnectorId(args.get(1)))
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "create" -> {
        if (args.size() < 4) {
          out.println(
              "usage: connector create "
                  + "<display_name> <source_type (ICEBERG|DELTA|GLUE|UNITY)> <uri> "
                  + "<source_namespace (a[.b[.c]...])> <destination_catalog (name)> "
                  + "[--source-table <name>] [--source-cols c1,#id2,...] "
                  + "[--dest-ns <a.b[.c]>] [--dest-table <name>] "
                  + "[--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...] "
                  + "[--head k=v ...] [--secret <ref>] "
                  + "[--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>] "
                  + "[--policy-not-before-epoch <sec>] [--props k=v ...]");
          return;
        }

        String display = args.get(1);
        ConnectorKind kind = parseConnectorKind(args.get(2));
        String uri = args.get(3);
        String sourceNamespace = args.get(4);
        String destCatalog = args.get(5);

        String sourceTable = parseStringFlag(args, "--source-table", "");
        List<String> sourceCols = csvAllowHashes(parseStringFlag(args, "--source-cols", ""));

        String destNamespace = parseStringFlag(args, "--dest-ns", "");
        String destTable = parseStringFlag(args, "--dest-table", "");

        String description = parseStringFlag(args, "--desc", "");
        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var auth =
            AuthConfig.newBuilder()
                .setScheme(authScheme)
                .putAllProperties(authProps)
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

        var src = SourceSelector.newBuilder();
        boolean setSource = false;
        if (!sourceNamespace.isBlank()) {
          src.setNamespace(toNsPath(sourceNamespace));
          setSource = true;
        }
        if (!sourceTable.isBlank()) {
          src.setTable(sourceTable);
          setSource = true;
        }
        if (!sourceCols.isEmpty()) {
          src.addAllColumns(sourceCols);
          setSource = true;
        }

        var dst = DestinationTarget.newBuilder();
        boolean setDestination = false;
        if (!destCatalog.isBlank()) {
          dst.setCatalogDisplayName(destCatalog);
          setDestination = true;
        }
        if (!destNamespace.isBlank()) {
          dst.setNamespace(toNsPath(destNamespace));
          setDestination = true;
        }
        if (!destTable.isBlank()) {
          dst.setTableDisplayName(destTable);
          setDestination = true;
        }

        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName(display)
                .setDescription(description)
                .setKind(kind)
                .setUri(uri)
                .putAllProperties(properties)
                .setAuth(auth)
                .setPolicy(policy);

        if (setSource) {
          spec.setSource(src.build());
        } else {
        }

        if (setDestination) {
          spec.setDestination(dst.build());
        }

        var resp =
            connectors.createConnector(
                CreateConnectorRequest.newBuilder()
                    .setSpec(spec.build())
                    .setIdempotency(newIdem())
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri"
                  + " <uri>] [--source-ns <a.b[.c]>] [--source-table <name>] [--source-cols"
                  + " c1,#id2,...] [--dest-catalog <name>] [--dest-ns <a.b[.c]>] [--dest-table"
                  + " <name>] [--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v"
                  + " ...] [--secret <ref>] [--policy-enabled true|false] [--policy-interval-sec"
                  + " <n>] [--policy-max-par <n>] [--policy-not-before-epoch <sec>] [--props k=v"
                  + " ...] [--etag <etag>]");
          return;
        }

        ResourceId connectorId = resolveConnectorId(args.get(1));

        String display = parseStringFlag(args, "--display", "");
        String kindStr = parseStringFlag(args, "--kind", "");
        String uri = parseStringFlag(args, "--uri", "");
        String description = parseStringFlag(args, "--desc", "");

        String sourceNs = parseStringFlag(args, "--source-ns", "");
        String sourceTable = parseStringFlag(args, "--source-table", "");
        List<String> sourceCols = csvAllowHashes(parseStringFlag(args, "--source-cols", ""));

        String destCatalog = parseStringFlag(args, "--dest-catalog", "");
        String destNs = parseStringFlag(args, "--dest-ns", "");
        String destTable = parseStringFlag(args, "--dest-table", "");

        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        String policyEnabledStr = parseStringFlag(args, "--policy-enabled", "");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var spec = ConnectorSpec.newBuilder();
        var mask = new LinkedHashSet<String>();

        if (!display.isBlank()) {
          spec.setDisplayName(display);
          mask.add("display_name");
        }
        if (!description.isBlank()) {
          spec.setDescription(description);
          mask.add("description");
        }
        if (!kindStr.isBlank()) {
          spec.setKind(parseConnectorKind(kindStr));
          mask.add("kind");
        }
        if (!uri.isBlank()) {
          spec.setUri(uri);
          mask.add("uri");
        }
        if (!properties.isEmpty()) {
          spec.putAllProperties(properties);
          mask.add("properties");
        }

        boolean authSet =
            !authScheme.isBlank()
                || !authProps.isEmpty()
                || !headerHints.isEmpty()
                || !secretRef.isBlank();
        if (authSet) {
          var ab = AuthConfig.newBuilder();
          if (!authScheme.isBlank()) {
            ab.setScheme(authScheme);
            mask.add("auth.scheme");
          }
          if (!secretRef.isBlank()) {
            ab.setSecretRef(secretRef);
            mask.add("auth.secret_ref");
          }
          if (!authProps.isEmpty()) {
            ab.putAllProperties(authProps);
            mask.add("auth.properties");
          }
          if (!headerHints.isEmpty()) {
            ab.putAllHeaderHints(headerHints);
            mask.add("auth.header_hints");
          }
          spec.setAuth(ab);
        }

        boolean policySet =
            !policyEnabledStr.isBlank() || intervalSec != 0L || maxPar != 0 || notBeforeSec != 0L;
        if (policySet) {
          var pb = ReconcilePolicy.newBuilder();
          if (!policyEnabledStr.isBlank()) {
            pb.setEnabled(Boolean.parseBoolean(policyEnabledStr));
            mask.add("policy.enabled");
          }
          if (intervalSec != 0L) {
            pb.setInterval(durSeconds(intervalSec));
            mask.add("policy.interval");
          }
          if (maxPar != 0) {
            pb.setMaxParallel(maxPar);
            mask.add("policy.max_parallel");
          }
          if (notBeforeSec != 0L) {
            pb.setNotBefore(tsSeconds(notBeforeSec));
            mask.add("policy.not_before");
          }
          spec.setPolicy(pb);
        }

        boolean sourceSet = !sourceNs.isBlank() || !sourceTable.isBlank() || !sourceCols.isEmpty();
        if (sourceSet) {
          var src = SourceSelector.newBuilder();
          if (!sourceNs.isBlank()) {
            src.setNamespace(toNsPath(sourceNs));
            mask.add("source.namespace");
          }
          if (!sourceTable.isBlank()) {
            src.setTable(sourceTable);
            mask.add("source.table");
          }
          if (!sourceCols.isEmpty()) {
            src.addAllColumns(sourceCols);
            mask.add("source.columns");
          }
          spec.setSource(src);
        }

        boolean destSet = !destCatalog.isBlank() || !destNs.isBlank() || !destTable.isBlank();
        if (destSet) {
          var dst = DestinationTarget.newBuilder();
          if (!destCatalog.isBlank()) {
            dst.setCatalogDisplayName(destCatalog);
            mask.add("destination.catalog_display_name");
          }
          if (!destNs.isBlank()) {
            dst.setNamespace(toNsPath(destNs));
            mask.add("destination.namespace");
          }
          if (!destTable.isBlank()) {
            dst.setTableDisplayName(destTable);
            mask.add("destination.table_display_name");
          }
          spec.setDestination(dst);
        }

        if (mask.isEmpty()) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }

        var req =
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(spec.build())
                .setPrecondition(preconditionFromEtag(args))
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build())
                .build();

        var resp = connectors.updateConnector(req);
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
              "usage: connector validate <kind> <uri>"
                  + " [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]"
                  + " [--source-ns <a.b[.c]>] [--source-table <name>] [--source-cols c1,#id2,...]"
                  + " [--dest-catalog <name>] [--dest-ns <a.b[.c]>] [--dest-table <name>]"
                  + " [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]"
                  + " [--policy-not-before-epoch <sec>] [--props k=v ...]");
          return;
        }

        ConnectorKind kind = parseConnectorKind(args.get(1));
        String uri = args.get(2);

        String sourceNs = parseStringFlag(args, "--source-ns", "");
        String sourceTable = parseStringFlag(args, "--source-table", "");
        List<String> sourceCols = csvAllowHashes(parseStringFlag(args, "--source-cols", ""));

        String destCatalog = parseStringFlag(args, "--dest-catalog", "");
        String destNs = parseStringFlag(args, "--dest-ns", "");
        String destTable = parseStringFlag(args, "--dest-table", "");

        String authScheme = parseStringFlag(args, "--auth-scheme", "");
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = parseStringFlag(args, "--secret", "");
        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var auth =
            AuthConfig.newBuilder()
                .setScheme(authScheme)
                .putAllProperties(authProps)
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
                .setUri(uri)
                .putAllProperties(properties)
                .setAuth(auth)
                .setPolicy(policy);

        boolean sourceSet = !sourceNs.isBlank() || !sourceTable.isBlank() || !sourceCols.isEmpty();
        if (sourceSet) {
          var src = SourceSelector.newBuilder();
          if (!sourceNs.isBlank()) src.setNamespace(toNsPath(sourceNs));
          if (!sourceTable.isBlank()) src.setTable(sourceTable);
          if (!sourceCols.isEmpty()) src.addAllColumns(sourceCols);
          spec.setSource(src);
        }

        boolean destSet = !destCatalog.isBlank() || !destNs.isBlank() || !destTable.isBlank();
        if (destSet) {
          var dst = DestinationTarget.newBuilder();
          if (!destCatalog.isBlank()) dst.setCatalogDisplayName(destCatalog);
          if (!destNs.isBlank()) dst.setNamespace(toNsPath(destNs));
          if (!destTable.isBlank()) dst.setTableDisplayName(destTable);
          spec.setDestination(dst);
        }

        var resp =
            connectors.validateConnector(
                ValidateConnectorRequest.newBuilder().setSpec(spec.build()).build());

        out.printf(
            "ok=%s summary=%s capabilities=%s%n",
            resp.getOk(),
            resp.getSummary(),
            resp.getCapabilitiesMap().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(java.util.stream.Collectors.joining(",")));
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
                .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build())
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
      out.println(
          "usage: stats table <tableFQ> [--snapshot <id>|--current] (defaults to --current)");
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
      out.println(
          "usage: stats columns <tableFQ> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N]");
      return;
    }

    String fq = args.get(0);
    int limit = parseIntFlag(args, "--limit", 2000);
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    var r = directory.resolveTable(ResolveTableRequest.newBuilder().setRef(nameFromFq(fq)).build());

    ListColumnStatsRequest.Builder rb =
        ListColumnStatsRequest.newBuilder()
            .setTableId(r.getResourceId())
            .setSnapshot(parseSnapshotSelector(args))
            .setPage(PageRequest.newBuilder().setPageSize(limit).build());

    List<ColumnStats> all = new ArrayList<>();
    String pageToken = "";
    do {
      var pageReq =
          rb.setPage(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(pageToken))
              .build();
      var resp = statistics.listColumnStats(pageReq);

      all.addAll(resp.getColumnsList());
      if (all.size() >= limit) {
        all = all.subList(0, limit);
        break;
      }

      pageToken = resp.hasPage() ? resp.getPage().getNextPageToken() : "";
      if (pageToken == null) {
        pageToken = "";
      }
    } while (!pageToken.isBlank());

    printColumnStats(all);
  }

  private void cmdPlan(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: plan <begin|renew|end|get> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = args.subList(1, args.size());
    switch (sub) {
      case "begin" -> planBegin(tail);
      case "renew" -> planRenew(tail);
      case "end" -> planEnd(tail);
      case "get" -> planGet(tail);
      default -> out.println("usage: plan <begin|renew|end|get> ...");
    }
  }

  private void planBegin(List<String> args) {
    if (args.isEmpty()) {
      out.println(
          "usage: plan begin [--ttl <seconds>] [--as-of-default <timestamp>] (table <fq> "
              + "[--snapshot <id|current>] [--as-of <timestamp>] | table-id <uuid> "
              + "[--snapshot <id|current>] [--as-of <timestamp>] | view-id <uuid> | namespace"
              + " <fq>)+");
      return;
    }

    List<PlanInput> inputs = new ArrayList<>();
    int ttlSeconds = 0;
    Timestamp asOfDefault = null;

    int i = 0;
    while (i < args.size()) {
      String token = args.get(i);
      switch (token) {
        case "--ttl" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: --ttl requires a value");
            return;
          }
          try {
            ttlSeconds = Integer.parseInt(args.get(i + 1));
          } catch (NumberFormatException e) {
            out.println("plan begin: invalid ttl value '" + args.get(i + 1) + "'");
            return;
          }
          i += 2;
        }
        case "--as-of-default" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: --as-of-default requires a value");
            return;
          }
          try {
            asOfDefault = parseTimestampFlexible(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("plan begin: " + e.getMessage());
            return;
          }
          i += 2;
        }
        case "table" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: table requires a fully qualified name");
            return;
          }
          NameRef nr;
          try {
            nr = nameRefForTable(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("plan begin: " + e.getMessage());
            return;
          }
          PlanInput.Builder input = PlanInput.newBuilder().setName(nr);
          int next = parsePlanInputOptions(args, i + 2, input, true);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "namespace" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: namespace requires a fully qualified name");
            return;
          }
          NameRef nr;
          try {
            nr = nameRefForNamespaceTarget(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("plan begin: " + e.getMessage());
            return;
          }
          PlanInput.Builder input = PlanInput.newBuilder().setName(nr);
          int next = parsePlanInputOptions(args, i + 2, input, false);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "table-id" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: table-id requires a value");
            return;
          }
          ResourceId rid = tableRid(args.get(i + 1));
          PlanInput.Builder input = PlanInput.newBuilder().setTableId(rid);
          int next = parsePlanInputOptions(args, i + 2, input, true);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        case "view-id" -> {
          if (i + 1 >= args.size()) {
            out.println("plan begin: view-id requires a value");
            return;
          }
          ResourceId rid = viewRid(args.get(i + 1));
          PlanInput.Builder input = PlanInput.newBuilder().setViewId(rid);
          int next = parsePlanInputOptions(args, i + 2, input, false);
          if (next < 0) {
            return;
          }
          inputs.add(input.build());
          i = next;
        }
        default -> {
          out.println("plan begin: unknown argument '" + token + "'");
          return;
        }
      }
    }

    if (inputs.isEmpty()) {
      out.println("plan begin: at least one table, view, or namespace input is required");
      return;
    }

    BeginPlanRequest.Builder req = BeginPlanRequest.newBuilder().addAllInputs(inputs);
    if (ttlSeconds > 0) {
      req.setTtlSeconds(ttlSeconds);
    }
    if (asOfDefault != null) {
      req.setAsOfDefault(asOfDefault);
    }

    BeginPlanResponse resp = planning.beginPlan(req.build());
    printPlanBegin(resp);
  }

  private void planRenew(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: plan renew <plan_id> [--ttl <seconds>]");
      return;
    }
    String planId = args.get(0);
    int ttlSeconds = 0;

    int i = 1;
    while (i < args.size()) {
      String token = args.get(i);
      if ("--ttl".equals(token)) {
        if (i + 1 >= args.size()) {
          out.println("plan renew: --ttl requires a value");
          return;
        }
        try {
          ttlSeconds = Integer.parseInt(args.get(i + 1));
        } catch (NumberFormatException e) {
          out.println("plan renew: invalid ttl value '" + args.get(i + 1) + "'");
          return;
        }
        i += 2;
      } else {
        out.println("plan renew: unknown argument '" + token + "'");
        return;
      }
    }

    RenewPlanRequest.Builder req = RenewPlanRequest.newBuilder().setPlanId(planId);
    if (ttlSeconds > 0) {
      req.setTtlSeconds(ttlSeconds);
    }

    RenewPlanResponse resp = planning.renewPlan(req.build());
    out.println("plan id: " + resp.getPlanId());
    out.println("expires: " + ts(resp.getExpiresAt()));
  }

  private void planEnd(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: plan end <plan_id> [--commit|--abort]");
      return;
    }
    String planId = args.get(0);
    Boolean commit = null;

    for (int i = 1; i < args.size(); i++) {
      String token = args.get(i);
      switch (token) {
        case "--commit" -> {
          if (commit != null && !commit) {
            out.println("plan end: cannot specify both --commit and --abort");
            return;
          }
          commit = true;
        }
        case "--abort" -> {
          if (commit != null && commit) {
            out.println("plan end: cannot specify both --commit and --abort");
            return;
          }
          commit = false;
        }
        default -> {
          out.println("plan end: unknown argument '" + token + "'");
          return;
        }
      }
    }

    boolean commitFlag = commit != null ? commit : false;
    EndPlanResponse resp =
        planning.endPlan(
            EndPlanRequest.newBuilder().setPlanId(planId).setCommit(commitFlag).build());
    out.println("plan id: " + resp.getPlanId());
  }

  private void planGet(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: plan get <plan_id>");
      return;
    }
    String planId = args.get(0);
    GetPlanResponse resp = planning.getPlan(GetPlanRequest.newBuilder().setPlanId(planId).build());
    if (!resp.hasPlan()) {
      out.println("plan get: no plan details returned");
      return;
    }
    printPlanDescriptor(resp.getPlan());
  }

  private int parsePlanInputOptions(
      List<String> args, int start, PlanInput.Builder input, boolean allowSnapshot) {
    SnapshotRef snapshotRef = null;
    boolean snapshotSet = false;

    int i = start;
    while (i < args.size()) {
      String token = args.get(i);
      if (isPlanInputStartToken(token) || isPlanGlobalFlag(token)) {
        break;
      }
      switch (token) {
        case "--snapshot" -> {
          if (!allowSnapshot) {
            out.println("plan begin: snapshots are not supported for this input type");
            return -1;
          }
          if (i + 1 >= args.size()) {
            out.println("plan begin: --snapshot requires a value");
            return -1;
          }
          if (snapshotSet) {
            out.println("plan begin: multiple snapshot selectors provided for one input");
            return -1;
          }
          try {
            snapshotRef = snapshotFromToken(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("plan begin: " + e.getMessage());
            return -1;
          }
          snapshotSet = true;
          i += 2;
        }
        case "--as-of" -> {
          if (!allowSnapshot) {
            out.println("plan begin: --as-of is not supported for this input type");
            return -1;
          }
          if (i + 1 >= args.size()) {
            out.println("plan begin: --as-of requires a value");
            return -1;
          }
          if (snapshotSet) {
            out.println("plan begin: multiple snapshot selectors provided for one input");
            return -1;
          }
          try {
            Timestamp ts = parseTimestampFlexible(args.get(i + 1));
            snapshotRef = SnapshotRef.newBuilder().setAsOf(ts).build();
          } catch (IllegalArgumentException e) {
            out.println("plan begin: " + e.getMessage());
            return -1;
          }
          snapshotSet = true;
          i += 2;
        }
        default -> {
          out.println("plan begin: unknown flag '" + token + "'");
          return -1;
        }
      }
    }

    if (snapshotSet && snapshotRef != null) {
      input.setSnapshot(snapshotRef);
    }
    return i;
  }

  private boolean isPlanInputStartToken(String token) {
    return switch (token) {
      case "table", "table-id", "view-id", "namespace" -> true;
      default -> false;
    };
  }

  private boolean isPlanGlobalFlag(String token) {
    return "--ttl".equals(token) || "--as-of-default".equals(token);
  }

  private void printPlanBegin(BeginPlanResponse resp) {
    if (!resp.hasPlan()) {
      out.println("plan begin: no plan returned");
      return;
    }
    printPlanDescriptor(resp.getPlan());
  }

  private void printPlanDescriptor(PlanDescriptor plan) {
    out.println("plan id: " + plan.getPlanId());
    if (!plan.getTenantId().isBlank()) {
      out.println("tenant: " + plan.getTenantId());
    }
    out.println("status: " + plan.getPlanStatus().name().toLowerCase(Locale.ROOT));
    out.println("created: " + ts(plan.getCreatedAt()));
    out.println("expires: " + ts(plan.getExpiresAt()));
    printPlanSnapshots(plan.getSnapshots());
    printPlanExpansion(plan.getExpansion());
    printPlanObligations(plan.getObligationsList());
    printPlanFiles("data_files", plan.getDataFilesList());
    printPlanFiles("delete_files", plan.getDeleteFilesList());
  }

  private void printPlanSnapshots(SnapshotSet snapshots) {
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

  private void printPlanExpansion(ExpansionMap expansion) {
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
                          .map(this::rid)
                          .collect(Collectors.joining(", "));
                  out.println("      bases: " + bases);
                }
              });
    }
    if (hasReadVia) {
      String via =
          expansion.getReadViaViewTablesList().stream()
              .map(this::rid)
              .collect(Collectors.joining(", "));
      out.println("  read_via_view_tables: " + via);
    }
  }

  private void printPlanObligations(List<TableObligations> obligations) {
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

  private void printPlanFiles(String label, List<PlanFile> files) {
    if (files == null || files.isEmpty()) {
      out.println(label + ": <none>");
      return;
    }
    out.println(label + ":");
    for (PlanFile file : files) {
      String content = file.getFileContent().name().toLowerCase(Locale.ROOT);
      out.printf(
          "  - path=%s format=%s size=%dB records=%d content=%s%n",
          file.getFilePath(),
          file.getFileFormat(),
          file.getFileSizeInBytes(),
          file.getRecordCount(),
          content);
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

  private boolean looksLikeUuid(String s) {
    if (s == null) {
      return false;
    }
    String t = s.trim();
    return t.matches(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  private ResourceId resourceId(String id, ResourceKind kind) {
    if (currentTenantId == null || currentTenantId.isBlank()) {
      throw new IllegalStateException("No tenant set. Use: tenant <tenantId>");
    }
    return ResourceId.newBuilder().setTenantId(currentTenantId).setKind(kind).setId(id).build();
  }

  private ResourceId catalogRid(String id) {
    return resourceId(id, ResourceKind.RK_CATALOG);
  }

  private ResourceId namespaceRid(String id) {
    return resourceId(id, ResourceKind.RK_NAMESPACE);
  }

  private ResourceId tableRid(String id) {
    return resourceId(id, ResourceKind.RK_TABLE);
  }

  private ResourceId connectorRid(String id) {
    return resourceId(id, ResourceKind.RK_CONNECTOR);
  }

  private ResourceId viewRid(String id) {
    return resourceId(id, ResourceKind.RK_OVERLAY);
  }

  private ResourceId resolveConnectorId(String token) {
    if (looksLikeUuid(token)) {
      return connectorRid(token);
    }

    var all = listAllConnectors(null, DEFAULT_PAGE_SIZE);

    var exact = all.stream().filter(c -> token.equals(c.getDisplayName())).toList();
    if (exact.size() == 1) {
      return exact.get(0).getResourceId();
    }

    var ci = all.stream().filter(c -> token.equalsIgnoreCase(c.getDisplayName())).toList();
    if (ci.size() == 1) {
      return ci.get(0).getResourceId();
    }

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
    ParsedFq p = parseFqFlexible(fq, true);
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

  private Timestamp parseTimestampFlexible(String value) {
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

  private SnapshotRef snapshotFromToken(String token) {
    if (token == null || token.isBlank()) {
      throw new IllegalArgumentException("snapshot selector is blank");
    }
    String trimmed = token.trim();
    if ("current".equalsIgnoreCase(trimmed)) {
      return SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
    }
    try {
      long id = Long.parseLong(trimmed);
      return SnapshotRef.newBuilder().setSnapshotId(id).build();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid snapshot selector '" + token + "'");
    }
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
    out.printf(
        "%-40s  %-24s  %-24s  %s%n", "CATALOG_ID", "CREATED_AT", "DISPLAY_NAME", "DESCRIPTION");
    for (var c : cats) {
      out.printf(
          "%-40s  %-24s  %-24s  %s%n",
          rid(c.getResourceId()),
          ts(c.getCreatedAt()),
          c.getDisplayName(),
          c.hasDescription() ? c.getDescription() : "");
    }
  }

  private void printNamespaces(List<Namespace> rows) {
    out.printf(
        "%-36s  %-5s  %-24s  %-24s  %s%n",
        "NAMESPACE_ID", "TYPE", "CREATED_AT", "PATH", "DESCRIPTION");

    for (var ns : rows) {
      String type = ns.hasResourceId() ? "real" : "virt";
      String created = ns.hasCreatedAt() ? ts(ns.getCreatedAt()) : "-";
      String path =
          String.join(".", ns.getParentsList().isEmpty() ? List.of() : ns.getParentsList())
              + (ns.getDisplayName().isBlank()
                  ? ""
                  : (ns.getParentsList().isEmpty() ? "" : ".") + ns.getDisplayName());

      String id = ns.hasResourceId() ? ns.getResourceId().getId() : "<virtual>";
      String desc = ns.hasDescription() ? ns.getDescription() : "";
      out.printf("%-36s  %-5s  %-24s  %-24s  %s%n", id, type, created, path, desc);
    }
  }

  private void printResolvedTables(List<ResolveFQTablesResponse.Entry> entries) {
    out.printf("%-40s  %s%n", "TABLE_ID", "NAME");
    for (var e : entries) {
      String catalog = e.getName().getCatalog();
      String namespace = String.join(".", e.getName().getPathList());
      String table = e.getName().getName();
      String fq =
          namespace.isEmpty() ? (catalog + "." + table) : (catalog + "." + namespace + "." + table);
      out.printf("%-40s  %s%n", rid(e.getResourceId()), fq);
    }
  }

  private void printTable(Table t) {
    UpstreamRef upstream = t.getUpstream();
    out.println("Table:");
    out.printf("  id:           %s%n", rid(t.getResourceId()));
    out.printf("  name:         %s%n", t.getDisplayName());
    out.printf("  format:       %s%n", upstream.getFormat().name());
    out.printf("  root_uri:     %s%n", upstream.getUri());
    out.printf("  created_at:   %s%n", ts(t.getCreatedAt()));
    out.printf(
        "  connector_id: %s%n",
        upstream.hasConnectorId() ? upstream.getConnectorId().getId() : "-");
    if (!upstream.getPartitionKeysList().isEmpty()) {
      out.printf("  partitions:   %s%n", String.join(", ", upstream.getPartitionKeysList()));
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
    final int W_ID = 36;
    final int W_KIND = 10;
    final int W_DISPLAY = 28;
    final int W_TS = 24;
    final int W_DESTCAT = 24;
    final int W_STATE = 10;
    final int W_URI = 80;

    out.printf(
        "%-" + W_ID + "s  %-" + W_KIND + "s  %-" + W_DISPLAY + "s  %-" + W_TS + "s  %-" + W_TS
            + "s  %-" + W_DESTCAT + "s  %-" + W_STATE + "s  %s%n",
        "CONNECTOR_ID",
        "KIND",
        "DISPLAY",
        "CREATED_AT",
        "UPDATED_AT",
        "DEST_CATALOG",
        "STATE",
        "URI");

    for (var c : list) {
      String id = rid(c.getResourceId());
      String kind = c.getKind().name().replaceFirst("^CK_", "");
      String display = c.getDisplayName();
      String created = ts(c.getCreatedAt());
      String updated = ts(c.getUpdatedAt());

      String destCat = "";
      if (c.hasDestination()) {
        var resp =
            directory.lookupCatalog(
                LookupCatalogRequest.newBuilder()
                    .setResourceId(c.getDestination().getCatalogId())
                    .build());
        destCat = resp.getDisplayName();
      }

      String state = c.getState().name();
      String uri = c.getUri();

      out.printf(
          "%-" + W_ID + "s  %-" + W_KIND + "s  %-" + W_DISPLAY + "s  %-" + W_TS + "s  %-" + W_TS
              + "s  %-" + W_DESTCAT + "s  %-" + W_STATE + "s  %s%n",
          trunc(id, W_ID),
          trunc(kind, W_KIND),
          trunc(display, W_DISPLAY),
          trunc(created, W_TS),
          trunc(updated, W_TS),
          trunc(destCat, W_DESTCAT),
          trunc(state, W_STATE),
          (W_URI > 0 ? trunc(uri, W_URI) : uri));

      if (c.hasDestination()) {
        String destNs = "";
        String destTbl = "";

        if (c.getDestination().hasNamespaceId()) {
          var nsId = c.getDestination().getNamespaceId();
          var resp =
              directory.lookupNamespace(
                  LookupNamespaceRequest.newBuilder().setResourceId(nsId).build());
          var ref = resp.getRef();
          if (ref.getPathList().isEmpty()) {
            destNs = ref.getName();
          } else {
            destNs = String.join(".", ref.getPathList()) + "." + ref.getName();
          }
        }

        if (c.getDestination().hasTableId()) {
          var tblId = c.getDestination().getTableId();
          var resp =
              directory.lookupTable(LookupTableRequest.newBuilder().setResourceId(tblId).build());
          var ref = resp.getName();
          destTbl = ref.getName();
        }

        if (!destTbl.isEmpty()) {
          out.println("  destination: " + destCat + "." + destNs + "." + destTbl);
        } else if (!destNs.isEmpty()) {
          out.println("  destination: " + destCat + "." + destNs);
        }
      }

      if (c.hasSource()) {
        var s = c.getSource();
        String sNs = s.hasNamespace() ? String.join(".", s.getNamespace().getSegmentsList()) : "";
        String sTbl = s.getTable();
        String sCols = String.join(",", s.getColumnsList());
        boolean anyS = !sNs.isEmpty() || (sTbl != null && !sTbl.isBlank()) || !sCols.isEmpty();
        if (anyS) {
          out.println(
              "  source:"
                  + (sNs.isEmpty() ? "" : sNs)
                  + (sTbl == null || sTbl.isBlank() ? "" : "." + sTbl)
                  + (sCols.isEmpty() ? "" : " cols=[" + sCols + "]"));
        }
      }

      if (c.hasPolicy()) {
        var p = c.getPolicy();
        boolean anyP =
            p.getEnabled() || p.getMaxParallel() > 0 || p.hasInterval() || p.hasNotBefore();
        if (anyP) {
          String interval = p.hasInterval() ? (p.getInterval().getSeconds() + "s") : "";
          String notBefore = p.hasNotBefore() ? ts(p.getNotBefore()) : "";
          out.println(
              "  policy:"
                  + " enabled="
                  + p.getEnabled()
                  + (interval.isEmpty() ? "" : " interval=" + interval)
                  + (p.getMaxParallel() > 0 ? " max_par=" + p.getMaxParallel() : "")
                  + (notBefore.isEmpty() ? "" : " not_before=" + notBefore));
        }
      }

      if (c.hasAuth()) {
        var a = c.getAuth();
        boolean anyA =
            (a.getScheme() != null && !a.getScheme().isBlank())
                || (a.getSecretRef() != null && !a.getSecretRef().isBlank());
        if (anyA) {
          out.println(
              "  auth:"
                  + (a.getScheme().isBlank() ? "" : " scheme=" + a.getScheme())
                  + (a.getSecretRef().isBlank() ? "" : " secret_ref=" + a.getSecretRef()));
        }
      }
    }
  }

  private String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private Timestamp tsSeconds(long epochSeconds) {
    return Timestamp.newBuilder().setSeconds(epochSeconds).build();
  }

  private Timestamp nullTs() {
    return Timestamp.getDefaultInstance();
  }

  private Duration nullDur() {
    return Duration.getDefaultInstance();
  }

  private Duration durSeconds(long seconds) {
    return Duration.newBuilder().setSeconds(seconds).build();
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

  private NameRef nameRefForNamespaceTarget(String fqNs) {
    ParsedFq p = parseFqFlexible(fqNs, false);
    if (p.nsParts.isEmpty()) {
      throw new IllegalArgumentException("Namespace path is empty");
    }
    return NameRef.newBuilder().setCatalog(p.catalog).addAllPath(p.nsParts).build();
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

  private List<String> csvAllowHashes(String s) {
    if (s == null || s.isBlank()) return List.of();
    return Arrays.stream(s.split(",")).map(String::trim).filter(x -> !x.isEmpty()).toList();
  }

  private NamespacePath toNsPath(String path) {
    var segs =
        Arrays.stream(path.split("[./]")).map(String::trim).filter(x -> !x.isEmpty()).toList();
    return NamespacePath.newBuilder().addAllSegments(segs).build();
  }
}
