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

import static java.lang.System.out;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.client.cli.util.AuthHeaderInterceptor;
import ai.floedb.floecat.client.cli.util.OidcClientCredentialsTokenProvider;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
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
    name = "floecat-shell",
    mixinStandardHelpOptions = true,
    version = "floecat-shell 0.1",
    description = "Interactive CLI to browse and manage catalogs/namespaces/tables/connectors")
@jakarta.inject.Singleton
public class Shell implements Runnable {

  @CommandLine.Option(
      names = {"--host"},
      description = "gRPC host (default: localhost)")
  String grpcHost;

  @CommandLine.Option(
      names = {"--port"},
      description = "gRPC port (default: 9100)")
  Integer grpcPort;

  @CommandLine.Option(
      names = {"--token"},
      description = "Authorization bearer token (or set FLOECAT_TOKEN)")
  String authToken;

  @CommandLine.Option(
      names = {"--session-token"},
      description = "Session token header value (or set FLOECAT_SESSION_TOKEN)")
  String sessionToken;

  @CommandLine.Option(
      names = {"--account-id"},
      description = "Default account id (or set FLOECAT_ACCOUNT)")
  String accountId;

  @CommandLine.Option(
      names = {"--auth-header"},
      description = "Authorization header name (default: authorization)")
  String authHeaderName;

  @CommandLine.Option(
      names = {"--session-header"},
      description = "Session header name (default: x-floe-session)")
  String sessionHeaderName;

  @CommandLine.Option(
      names = {"--account-header"},
      description = "Account header name for dev mode (default: x-floe-account)")
  String accountHeaderName;

  @CommandLine.Option(
      names = {"--oidc-token-url"},
      description = "OIDC token endpoint URL (or set FLOECAT_OIDC_TOKEN_URL)")
  String oidcTokenUrl;

  @CommandLine.Option(
      names = {"--oidc-issuer"},
      description = "OIDC issuer URL (or set FLOECAT_OIDC_ISSUER)")
  String oidcIssuer;

  @CommandLine.Option(
      names = {"--oidc-client-id"},
      description = "OIDC client id (or set FLOECAT_OIDC_CLIENT_ID)")
  String oidcClientId;

  @CommandLine.Option(
      names = {"--oidc-client-secret"},
      description = "OIDC client secret (or set FLOECAT_OIDC_CLIENT_SECRET)")
  String oidcClientSecret;

  @CommandLine.Option(
      names = {"--oidc-refresh-skew-seconds"},
      description =
          "Token refresh lead time in seconds (default: FLOECAT_OIDC_REFRESH_SKEW_SECONDS or 30)")
  Integer oidcRefreshSkewSeconds;

  @Inject
  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;

  @Inject
  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;

  @Inject
  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub tables;

  @Inject
  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @Inject
  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @Inject
  @GrpcClient("floecat")
  TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService;

  @Inject
  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Inject
  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub viewService;

  @Inject
  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

  @Inject
  @GrpcClient("floecat")
  ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl;

  @Inject
  @GrpcClient("floecat")
  QueryServiceGrpc.QueryServiceBlockingStub queries;

  @Inject
  @GrpcClient("floecat")
  QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan;

  @Inject
  @GrpcClient("floecat")
  QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema;

  @Inject
  @GrpcClient("floecat")
  AccountServiceGrpc.AccountServiceBlockingStub accounts;

  @Inject
  @GrpcClient("floecat")
  io.grpc.Channel floecatChannel;

  private ManagedChannel overrideChannel;
  private CliCommandExecutor executor;

  private final boolean debugErrors =
      Boolean.getBoolean("floecat.shell.debug") || System.getenv("FLOECAT_SHELL_DEBUG") != null;

  private volatile String currentAccountId = "";

  private volatile String currentCatalog =
      System.getenv().getOrDefault("FLOECAT_CATALOG", "").trim();

  private volatile OidcClientCredentialsTokenProvider oidcTokenProvider;

  @Override
  public void run() {
    initAuthConfig();
    out.println("Floecat Shell (type 'help' for commands, 'quit' to exit).");
    try {
      configureGrpcChannel();
      applyAuthInterceptors();
      maybeWarmUpConnection();
      executor =
          CliCommandExecutor.builder()
              .out(out)
              .accounts(accounts)
              .catalogs(catalogs)
              .directory(directory)
              .namespaces(namespaces)
              .tables(tables)
              .viewService(viewService)
              .connectors(connectors)
              .reconcileControl(reconcileControl)
              .snapshots(snapshots)
              .statistics(statistics)
              .constraintsService(constraintsService)
              .queries(queries)
              .queryScan(queryScan)
              .querySchema(querySchema)
              .getAccountId(() -> currentAccountId)
              .setAccountId(id -> currentAccountId = id)
              .getCatalog(() -> currentCatalog)
              .setCatalog(name -> currentCatalog = name)
              .build();
      Terminal terminal = TerminalBuilder.builder().system(true).build();
      Path historyPath = Paths.get(System.getProperty("user.home"), ".floecat_shell_history");
      var parser = new DefaultParser();
      parser.setEofOnUnclosedBracket(
          DefaultParser.Bracket.CURLY, DefaultParser.Bracket.ROUND, DefaultParser.Bracket.SQUARE);
      parser.setEofOnEscapedNewLine(true);
      parser.setEofOnUnclosedQuote(true);
      parser.setEscapeChars(new char[] {'\\'});
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
              "constraints",
              "analyze",
              "query",
              "account",
              "help",
              "quit",
              "exit");
      LineReader reader =
          LineReaderBuilder.builder()
              .terminal(terminal)
              .appName("floecat-shell")
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
                    if (overrideChannel != null) {
                      overrideChannel.shutdownNow();
                    }
                  }));
      while (true) {
        String line;
        try {
          line = reader.readLine("floecat> ");
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
          printError(e);
        }
      }
    } catch (Exception e) {
      out.println("Fatal: " + e);
    }
  }

  private void printError(Throwable t) {
    var chain = new ArrayList<Throwable>();
    Throwable cur = t;
    while (cur != null && !chain.contains(cur)) {
      chain.add(cur);
      cur = cur.getCause();
    }

    Throwable root = chain.get(chain.size() - 1);
    String msg = messageFor(root);
    if (msg == null || msg.isBlank()) {
      msg = messageFor(t);
    }
    if (msg == null || msg.isBlank()) {
      msg = "An error occurred";
    }

    var lines = splitErrorLines(msg);
    if (lines.isEmpty()) {
      lines = List.of("An error occurred");
    }
    System.out.println("! " + lines.get(0));
    for (int i = 1; i < lines.size(); i++) {
      System.out.println("!   " + lines.get(i));
    }

    for (int i = chain.size() - 2; i >= 0; i--) {
      var c = chain.get(i);
      String rendered = renderThrowable(c);
      if (rendered != null && !rendered.isBlank()) {
        System.out.println("! caused by: " + rendered);
      }
    }

    if (debugErrors) {
      System.out.println("! [debug] exception: " + t.getClass().getName());
      for (int i = chain.size() - 2; i >= 0; i--) {
        System.out.println("! [debug] caused by: " + chain.get(i).getClass().getName());
      }
      t.printStackTrace(System.out);
    }
  }

  private static String messageFor(Throwable t) {
    if (t == null) {
      return "";
    }
    if (t instanceof StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return desc;
    }
    return t.getMessage();
  }

  private static String renderThrowable(Throwable t) {
    if (t == null) {
      return "";
    }
    if (t instanceof StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + desc;
    }
    String msg = t.getMessage();
    String cls = t.getClass().getSimpleName();
    if (msg == null || msg.isBlank()) {
      return cls;
    }
    return cls + ": " + msg;
  }

  private static List<String> splitErrorLines(String msg) {
    if (msg == null) {
      return List.of();
    }
    String normalized = msg.replace("\r\n", "\n").replace("\r", "\n");
    if (normalized.contains("\n")) {
      var out = new ArrayList<String>();
      for (String line : normalized.split("\n")) {
        if (!line.isBlank()) {
          out.add(stripBullet(line.trim()));
        }
      }
      return out;
    }
    if (normalized.contains(" | ")) {
      var out = new ArrayList<String>();
      for (String part : normalized.split("\\s\\|\\s")) {
        if (!part.isBlank()) {
          out.add(stripBullet(part.trim()));
        }
      }
      return out;
    }
    return List.of(stripBullet(normalized.trim()));
  }

  private static String stripBullet(String line) {
    if (line.startsWith("- ")) {
      return line.substring(2).trim();
    }
    if (line.startsWith("* ")) {
      return line.substring(2).trim();
    }
    return line;
  }

  void printHelp() {
    out.println(
"""
         Options:
         --host <host>   gRPC host (default: localhost)
         --port <port>   gRPC port (default: 9100)
         --token <jwt>   Authorization token (default: FLOECAT_TOKEN)
         --session-token <jwt>  Session token (default: FLOECAT_SESSION_TOKEN)
         --account-id <id>      Default account id (default: FLOECAT_ACCOUNT)
         --auth-header <name>   Authorization header name
         --session-header <name>  Session header name
         --account-header <name>  Account header name for dev mode
         --oidc-token-url <url> OIDC token endpoint (default: FLOECAT_OIDC_TOKEN_URL)
         --oidc-issuer <url>    OIDC issuer base URL (default: FLOECAT_OIDC_ISSUER)
         --oidc-client-id <id>  OIDC client id (default: FLOECAT_OIDC_CLIENT_ID)
         --oidc-client-secret <secret> OIDC client secret (default: FLOECAT_OIDC_CLIENT_SECRET)
         --oidc-refresh-skew-seconds <n> Refresh token n seconds before expiry

         Commands:
         account <id|display_name>
         account list
         account get <id|display_name>
         account create <display_name> [--desc <text>]
         account delete <id> (or omit id to use current account)
         catalogs
         catalog create <display_name> [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...]
         catalog get <display_name|id>
         catalog update <display_name|id> [--display <name>] [--desc <text>] [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]
         catalog delete <display_name|id> [--require-empty] [--etag <etag>]
         namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P] [--recursive]
         namespace create <catalog|catalog.ns[.ns...]> [--display <leaf>] [--path a.b[.c]] [--desc <text>] [--props k=v ...] [--policy <id>]
             # Examples:
             #   namespace create cat1 --display "a.b.c"
             #   namespace create cat1 --path a --display "b.c"
             #   namespace create cat1.a.b
             #   namespace create cat1.a\\.b.c      (treats 'a.b' as one namespace)
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
         table delete <id|fq> [--etag <etag>]
         views <catalog.ns[.ns...]>
         view create <catalog.ns[.ns...].name> [--sql <text>] [--desc <text>] [--props k=v ...]
         view get <id|catalog.ns[.ns...].name>
         view update <id|fq> [--display <name>] [--namespace <catalog.ns[.ns...]>] [--sql <text>] [--desc <text>] [--props k=v ...]
         view delete <id|fq>
         resolve table <fq> | resolve view <fq> | resolve catalog <name> | resolve namespace <fq>
         describe table <fq>
         snapshots <tableFQ>
         snapshot get <id|catalog.ns[.ns...].table> <snapshot_id>
         snapshot delete <id|catalog.ns[.ns...].table> <snapshot_id>
         stats table <tableFQ> [--snapshot <id>|--current] [--json] (defaults to --current)
         stats columns <tableFQ> [--snapshot <id>|--current] [--limit N] [--json] defaults to --current
         stats files <tableFQ> [--snapshot <id>|--current] [--limit N] defaults to --current
         constraints get <id|catalog.ns[.ns...].table> [--snapshot <id>] [--json] (defaults to current snapshot)
         constraints list <id|catalog.ns[.ns...].table> [--limit N] [--json]
         constraints put <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--idempotency <key>] [--json]      (replace bundle)
         constraints update <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]   (server-side merge by constraint name)
         constraints add <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <snapshot_constraints_json> [--etag <etag>|--version <n>] [--json]      (server-side append-only; errors on duplicate names)
         constraints delete <id|catalog.ns[.ns...].table> [--snapshot <id>]
         constraints add-one <id|catalog.ns[.ns...].table> [--snapshot <id>] --file <constraint_definition_json> [--etag <etag>|--version <n>] [--json]
         constraints delete-one <id|catalog.ns[.ns...].table> <constraint_name> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         constraints add-pk <id|catalog.ns[.ns...].table> <constraint_name> <columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         constraints add-unique <id|catalog.ns[.ns...].table> <constraint_name> <columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         constraints add-not-null <id|catalog.ns[.ns...].table> <constraint_name> <column_name> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         constraints add-check <id|catalog.ns[.ns...].table> <constraint_name> <check_expression> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         constraints add-fk <id|catalog.ns[.ns...].table> <constraint_name> <local_columns_csv> <referenced_table> <referenced_columns_csv> [--snapshot <id>] [--etag <etag>|--version <n>] [--json]
         analyze <tableFQ> [--columns c1,c2,...] [--snapshot <id>|--current] [--mode metadata-only|metadata-and-capture|capture-only]
             [--capture stats|table-stats|file-stats|column-stats|index,...]
             # Defaults to --mode capture-only --capture stats.
             [--full] [--wait-seconds <n>]
             # Runs synchronous table-scoped capture_now.
         query begin [--ttl <seconds>] [--as-of-default <timestamp>] (table <catalog.ns....table> [--snapshot <id|current>] [--as-of <timestamp>] | table-id <uuid> [--snapshot <id|current>] [--as-of <timestamp>] | view-id <uuid> | namespace <catalog.ns[.ns...]>)+
         query renew <query_id> [--ttl <seconds>]
         query end <query_id> [--commit|--abort]
         query get <query_id>
         connectors
         connector list [--kind <KIND>] [--page-size <N>]
         connector get <display_name|id>
         connector create <display_name> <source_type (ICEBERG|DELTA|GLUE|UNITY)> <uri> <source_namespace (a[.b[.c]...])> <destination_catalog (name)>
             [--source-table <name>] [--source-cols c1,#id2,...]
             [--dest-ns <a.b[.c]>] [--dest-table <name>]
             [--desc <text>] [--auth-scheme <scheme>] [--auth k=v ...]
             [--head k=v ...]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-mode incremental|full] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
         connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
             [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]
             [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-mode incremental|full] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
         connector delete <display_name|id>  [--etag <etag>]
         connector validate <kind> <uri>
             [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-mode incremental|full] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
         connector trigger <display_name|id> [--full]
             [--mode metadata-only|metadata-and-capture|capture-only]
             [--capture stats|table-stats|file-stats|column-stats|index,...]
             # --capture is required for capture modes.
             [--dest-ns <a.b[.c]>] [--dest-table <name>] [--snapshot <id>|--current] [--columns c1,#id2,...]
         connector job <jobId>
         connector jobs [--connector <display_name|id>] [--state <queued|running|cancelling|cancelled|succeeded|failed>[,...]] [--page-size <N>]
         connector cancel <jobId> [--reason <text>]
         connector settings get
         connector settings update [--auto-enabled true|false] [--default-interval-sec <n>] [--default-mode incremental|full] [--finished-job-retention-sec <n>]
         help
         quit
""");
  }

  private void maybeWarmUpConnection() {
    if (!isInteractiveSession()) {
      return;
    }
    warmUpConnectionAsync();
  }

  private boolean isInteractiveSession() {
    return System.console() != null;
  }

  private void warmUpConnectionAsync() {
    ManagedChannel ch = managedChannelForWarmUp();
    if (ch == null) {
      return;
    }

    // Connect in background so the prompt appears immediately, but the connection is ready
    // by the time the user types their first command.
    Thread warmUp =
        new Thread(
            () -> {
              ch.getState(true);
              long deadline = System.nanoTime() + 2_000_000_000L;
              while (System.nanoTime() < deadline) {
                ConnectivityState state = ch.getState(false);
                if (state == ConnectivityState.READY
                    || state == ConnectivityState.TRANSIENT_FAILURE) {
                  break;
                }
                try {
                  Thread.sleep(10);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            },
            "grpc-warmup");
    warmUp.setDaemon(true);
    warmUp.start();
  }

  private ManagedChannel managedChannelForWarmUp() {
    if (overrideChannel != null) {
      return overrideChannel;
    }
    if (floecatChannel instanceof ManagedChannel managed) {
      return managed;
    }
    return null;
  }

  private void configureGrpcChannel() {
    if (grpcHost == null && grpcPort == null) {
      return;
    }
    String host = grpcHost == null || grpcHost.isBlank() ? "localhost" : grpcHost.trim();
    int port = grpcPort == null ? 9100 : grpcPort;
    overrideChannel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    catalogs = CatalogServiceGrpc.newBlockingStub(overrideChannel);
    namespaces = NamespaceServiceGrpc.newBlockingStub(overrideChannel);
    tables = TableServiceGrpc.newBlockingStub(overrideChannel);
    directory = DirectoryServiceGrpc.newBlockingStub(overrideChannel);
    statistics = TableStatisticsServiceGrpc.newBlockingStub(overrideChannel);
    constraintsService = TableConstraintsServiceGrpc.newBlockingStub(overrideChannel);
    snapshots = SnapshotServiceGrpc.newBlockingStub(overrideChannel);
    viewService = ViewServiceGrpc.newBlockingStub(overrideChannel);
    connectors = ConnectorsGrpc.newBlockingStub(overrideChannel);
    reconcileControl = ReconcileControlGrpc.newBlockingStub(overrideChannel);
    queries = QueryServiceGrpc.newBlockingStub(overrideChannel);
    queryScan = QueryScanServiceGrpc.newBlockingStub(overrideChannel);
    querySchema = QuerySchemaServiceGrpc.newBlockingStub(overrideChannel);
    accounts = AccountServiceGrpc.newBlockingStub(overrideChannel);
  }

  private void initAuthConfig() {
    if (authToken == null || authToken.isBlank()) {
      authToken = System.getenv().getOrDefault("FLOECAT_TOKEN", "").trim();
    }
    if (sessionToken == null || sessionToken.isBlank()) {
      sessionToken = System.getenv().getOrDefault("FLOECAT_SESSION_TOKEN", "").trim();
    }
    if (accountId == null || accountId.isBlank()) {
      accountId = System.getenv().getOrDefault("FLOECAT_ACCOUNT", "").trim();
    }
    if (authHeaderName == null || authHeaderName.isBlank()) {
      authHeaderName = "authorization";
    }
    if (sessionHeaderName == null || sessionHeaderName.isBlank()) {
      sessionHeaderName = "x-floe-session";
    }
    if (accountHeaderName == null || accountHeaderName.isBlank()) {
      accountHeaderName = "x-floe-account";
    }
    if (currentAccountId == null || currentAccountId.isBlank()) {
      currentAccountId = accountId;
    }

    if (oidcTokenUrl == null || oidcTokenUrl.isBlank()) {
      oidcTokenUrl = System.getenv().getOrDefault("FLOECAT_OIDC_TOKEN_URL", "").trim();
    }
    if (oidcIssuer == null || oidcIssuer.isBlank()) {
      oidcIssuer = System.getenv().getOrDefault("FLOECAT_OIDC_ISSUER", "").trim();
    }
    if (oidcClientId == null || oidcClientId.isBlank()) {
      oidcClientId = System.getenv().getOrDefault("FLOECAT_OIDC_CLIENT_ID", "").trim();
    }
    if (oidcClientSecret == null || oidcClientSecret.isBlank()) {
      oidcClientSecret = System.getenv().getOrDefault("FLOECAT_OIDC_CLIENT_SECRET", "").trim();
    }
    if (oidcRefreshSkewSeconds == null) {
      String skew = System.getenv().getOrDefault("FLOECAT_OIDC_REFRESH_SKEW_SECONDS", "30").trim();
      if (!skew.isBlank()) {
        try {
          oidcRefreshSkewSeconds = Integer.parseInt(skew);
        } catch (NumberFormatException ignored) {
          oidcRefreshSkewSeconds = 30;
        }
      }
    }
    if (oidcRefreshSkewSeconds == null || oidcRefreshSkewSeconds < 0) {
      oidcRefreshSkewSeconds = 30;
    }

    if (authToken == null || authToken.isBlank()) {
      String effectiveTokenUrl = effectiveOidcTokenUrl();
      if (!effectiveTokenUrl.isBlank()
          && oidcClientId != null
          && !oidcClientId.isBlank()
          && oidcClientSecret != null
          && !oidcClientSecret.isBlank()) {
        oidcTokenProvider =
            new OidcClientCredentialsTokenProvider(
                effectiveTokenUrl,
                oidcClientId,
                oidcClientSecret,
                oidcRefreshSkewSeconds,
                debugErrors);
      }
    }
  }

  private String effectiveOidcTokenUrl() {
    if (oidcTokenUrl != null && !oidcTokenUrl.isBlank()) {
      return oidcTokenUrl.trim();
    }
    if (oidcIssuer == null || oidcIssuer.isBlank()) {
      return "";
    }
    return oidcIssuer.replaceFirst("/+$", "") + "/protocol/openid-connect/token";
  }

  private void applyAuthInterceptors() {
    var authInterceptor =
        new AuthHeaderInterceptor(
            this::resolveAuthorizationToken,
            () -> sessionToken == null ? "" : sessionToken.trim(),
            () -> currentAccountId == null ? "" : currentAccountId.trim(),
            authHeaderName,
            sessionHeaderName,
            accountHeaderName);
    catalogs = catalogs.withInterceptors(authInterceptor);
    namespaces = namespaces.withInterceptors(authInterceptor);
    tables = tables.withInterceptors(authInterceptor);
    directory = directory.withInterceptors(authInterceptor);
    statistics = statistics.withInterceptors(authInterceptor);
    constraintsService = constraintsService.withInterceptors(authInterceptor);
    snapshots = snapshots.withInterceptors(authInterceptor);
    viewService = viewService.withInterceptors(authInterceptor);
    connectors = connectors.withInterceptors(authInterceptor);
    reconcileControl = reconcileControl.withInterceptors(authInterceptor);
    queries = queries.withInterceptors(authInterceptor);
    queryScan = queryScan.withInterceptors(authInterceptor);
    querySchema = querySchema.withInterceptors(authInterceptor);
    accounts = accounts.withInterceptors(authInterceptor);
  }

  void dispatch(String inputLine) {
    var tokens = CliArgs.tokenize(inputLine);
    if (tokens.isEmpty()) {
      return;
    }
    String command = tokens.get(0);
    switch (command) {
      case "account", "help", "quit", "exit" -> {}
      default -> ensureAccountSet();
    }
    executor.execute(inputLine);
  }

  private void ensureAccountSet() {
    if (currentAccountId == null || currentAccountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
  }

  private String resolveAuthorizationToken() {
    String staticToken = authToken == null ? "" : authToken.trim();
    if (!staticToken.isBlank()) {
      return staticToken;
    }
    if (oidcTokenProvider == null) {
      return "";
    }
    return oidcTokenProvider.resolveToken();
  }
}
