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

import ai.floedb.floecat.account.rpc.Account;
import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.account.rpc.AccountSpec;
import ai.floedb.floecat.account.rpc.CreateAccountRequest;
import ai.floedb.floecat.account.rpc.DeleteAccountRequest;
import ai.floedb.floecat.account.rpc.GetAccountRequest;
import ai.floedb.floecat.account.rpc.ListAccountsRequest;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableStatsRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListFileColumnStatsRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupTableRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.NdvApprox;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.client.cli.util.CsvListParserUtil;
import ai.floedb.floecat.client.cli.util.FQNameParserUtil;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.QueryInput;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.ReconcilePolicy;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.query.rpc.BeginQueryRequest;
import ai.floedb.floecat.query.rpc.BeginQueryResponse;
import ai.floedb.floecat.query.rpc.DataFile;
import ai.floedb.floecat.query.rpc.DataFileBatch;
import ai.floedb.floecat.query.rpc.DeleteFileBatch;
import ai.floedb.floecat.query.rpc.DescribeInputsRequest;
import ai.floedb.floecat.query.rpc.DescribeInputsResponse;
import ai.floedb.floecat.query.rpc.EndQueryRequest;
import ai.floedb.floecat.query.rpc.EndQueryResponse;
import ai.floedb.floecat.query.rpc.ExpansionMap;
import ai.floedb.floecat.query.rpc.GetQueryRequest;
import ai.floedb.floecat.query.rpc.GetQueryResponse;
import ai.floedb.floecat.query.rpc.InitScanRequest;
import ai.floedb.floecat.query.rpc.InitScanResponse;
import ai.floedb.floecat.query.rpc.QueryDescriptor;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.RenewQueryRequest;
import ai.floedb.floecat.query.rpc.RenewQueryResponse;
import ai.floedb.floecat.query.rpc.ScanHandle;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.query.rpc.SnapshotPin;
import ai.floedb.floecat.query.rpc.SnapshotSet;
import ai.floedb.floecat.query.rpc.TableInfo;
import ai.floedb.floecat.query.rpc.TableObligations;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.picocli.runtime.annotations.TopCommand;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
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
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Inject
  @GrpcClient("floecat")
  ViewServiceGrpc.ViewServiceBlockingStub viewService;

  @Inject
  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connectors;

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

  private ManagedChannel overrideChannel;

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private final boolean debugErrors =
      Boolean.getBoolean("floecat.shell.debug") || System.getenv("FLOECAT_SHELL_DEBUG") != null;

  private volatile String currentAccountId = "";

  private volatile String currentCatalog =
      System.getenv().getOrDefault("FLOECAT_CATALOG", "").trim();

  @Override
  public void run() {
    initAuthConfig();
    out.println("Floecat Shell (type 'help' for commands, 'quit' to exit).");
    try {
      configureGrpcChannel();
      applyAuthInterceptors();
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

  private void printHelp() {
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

         Commands:
         account <id>
         account list
         account get <id>
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
         stats table <tableFQ> [--snapshot <id>|--current] (defaults to --current)
         stats columns <tableFQ> [--snapshot <id>|--current] [--limit N] defaults to --current
         stats files <tableFQ> [--snapshot <id>|--current] [--limit N] defaults to --current
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
             [--head k=v ...] [--secret <ref>]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
         connector update <display_name|id> [--display <name>] [--kind <kind>] [--uri <uri>]
             [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
             [--policy-enabled true|false] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...] [--etag <etag>]
         connector delete <display_name|id>  [--etag <etag>]
         connector validate <kind> <uri>
             [--dest-account <account>] [--dest-catalog <display>] [--dest-ns <a.b[.c]> ...] [--dest-table <name>] [--dest-cols c1,#id2,...]
             [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...] [--secret <ref>]
             [--policy-enabled] [--policy-interval-sec <n>] [--policy-max-par <n>]
             [--policy-not-before-epoch <sec>] [--props k=v ...]
         connector trigger <display_name|id> [--full]
         connector job <jobId>
         help
         quit
""");
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
    snapshots = SnapshotServiceGrpc.newBlockingStub(overrideChannel);
    viewService = ViewServiceGrpc.newBlockingStub(overrideChannel);
    connectors = ConnectorsGrpc.newBlockingStub(overrideChannel);
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
    if (currentAccountId == null || currentAccountId.isBlank()) {
      currentAccountId = accountId;
    }
  }

  private void applyAuthInterceptors() {
    ClientInterceptor authInterceptor = new AuthHeaderInterceptor();
    catalogs = catalogs.withInterceptors(authInterceptor);
    namespaces = namespaces.withInterceptors(authInterceptor);
    tables = tables.withInterceptors(authInterceptor);
    directory = directory.withInterceptors(authInterceptor);
    statistics = statistics.withInterceptors(authInterceptor);
    snapshots = snapshots.withInterceptors(authInterceptor);
    viewService = viewService.withInterceptors(authInterceptor);
    connectors = connectors.withInterceptors(authInterceptor);
    queries = queries.withInterceptors(authInterceptor);
    queryScan = queryScan.withInterceptors(authInterceptor);
    querySchema = querySchema.withInterceptors(authInterceptor);
    accounts = accounts.withInterceptors(authInterceptor);
  }

  private void dispatch(String inputLine) {
    var tokens = tokenize(inputLine);
    if (tokens.isEmpty()) {
      return;
    }
    String command = tokens.get(0);

    switch (command) {
      case "account", "help", "quit", "exit" -> {}
      default -> ensureAccountSet();
    }

    switch (command) {
      case "account" -> cmdAccount(tail(tokens));
      case "catalogs" -> cmdCatalogs();
      case "catalog" -> {
        if (tokens.size() >= 3 && "use".equals(tokens.get(1))) {
          cmdUseCatalog(tokens.subList(2, tokens.size()));
        } else {
          cmdCatalogCrud(tail(tokens));
        }
      }
      case "namespaces" -> cmdNamespaces(tail(tokens));
      case "namespace" -> cmdNamespaceCrud(tail(tokens));
      case "tables" -> cmdTables(tail(tokens));
      case "table" -> cmdTableCrud(tail(tokens));
      case "views" -> cmdViews(tail(tokens));
      case "view" -> cmdViewCrud(tail(tokens));
      case "connectors" -> cmdConnectorsList();
      case "connector" -> cmdConnectorCrud(tail(tokens));
      case "resolve" -> cmdResolve(tail(tokens));
      case "describe" -> cmdDescribe(tail(tokens));
      case "snapshots" -> cmdSnapshots(tail(tokens));
      case "snapshot" -> cmdSnapshotCrud(tail(tokens));
      case "stats" -> cmdStats(tail(tokens));
      case "query" -> cmdQuery(tail(tokens));
      default -> out.println("Unknown command. Type 'help'.");
    }
  }

  private void cmdUseCatalog(List<String> args) {
    if (args.size() != 1) {
      out.println("usage: catalog use <catalog-name>");
      return;
    }
    String name = Quotes.unquote(args.get(0));
    if (name.isBlank()) {
      out.println("catalog name cannot be empty");
      return;
    }

    // Validate catalog eagerly
    ResourceId cid = resolveCatalogId(name);

    currentCatalog = name;
    out.println("catalog set: " + name + " (" + cid.getId() + ")");
  }

  private void cmdAccount(List<String> args) {
    if (args.isEmpty()) {
      out.println(
          currentAccountId == null || currentAccountId.isBlank()
              ? "account: <not set>"
              : ("account: " + currentAccountId));
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "list" -> cmdAccountList();
      case "get" -> cmdAccountGet(tail(args));
      case "create" -> cmdAccountCreate(tail(args));
      case "delete" -> cmdAccountDelete(tail(args));
      default -> {
        String t = sub.trim();
        if (t.isEmpty()) {
          out.println("usage: account <accountId>");
          return;
        }
        currentAccountId = t;
        out.println("account set: " + currentAccountId);
      }
    }
  }

  private void cmdAccountList() {
    List<Account> all =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> accounts.listAccounts(ListAccountsRequest.newBuilder().setPage(pr).build()),
            r -> r.getAccountsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printAccounts(all);
  }

  private void cmdAccountGet(List<String> args) {
    if (args.size() < 1) {
      out.println("usage: account get <id>");
      return;
    }
    String id = Quotes.unquote(args.get(0));
    var resp =
        accounts.getAccount(
            GetAccountRequest.newBuilder()
                .setAccountId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT))
                .build());
    printAccounts(List.of(resp.getAccount()));
  }

  private void cmdAccountCreate(List<String> args) {
    if (args.size() < 1) {
      out.println("usage: account create <display_name> [--desc <text>]");
      return;
    }
    String display = Quotes.unquote(args.get(0));
    String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
    var spec =
        AccountSpec.newBuilder().setDisplayName(display).setDescription(nvl(desc, "")).build();
    var resp = accounts.createAccount(CreateAccountRequest.newBuilder().setSpec(spec).build());
    printAccounts(List.of(resp.getAccount()));
  }

  private void cmdAccountDelete(List<String> args) {
    String id =
        args.isEmpty() ? (currentAccountId == null ? "" : currentAccountId.trim()) : args.get(0);
    id = Quotes.unquote(id);
    if (id.isBlank()) {
      out.println("usage: account delete <id>");
      return;
    }
    accounts.deleteAccount(
        DeleteAccountRequest.newBuilder()
            .setAccountId(ResourceId.newBuilder().setId(id).setKind(ResourceKind.RK_ACCOUNT))
            .build());
    if (currentAccountId != null && currentAccountId.trim().equals(id)) {
      currentAccountId = null;
    }
    out.println("account deleted: " + id);
  }

  private List<String> tail(List<String> list) {
    return list.size() <= 1 ? List.of() : list.subList(1, list.size());
  }

  private void cmdCatalogs() {
    List<Catalog> all =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> catalogs.listCatalogs(ListCatalogsRequest.newBuilder().setPage(pr).build()),
            r -> r.getCatalogsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
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
        String display = Quotes.unquote(args.get(1));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
        String connectorRef = Quotes.unquote(parseStringFlag(args, "--connector", null));
        String policyRef = Quotes.unquote(parseStringFlag(args, "--policy", null));
        Map<String, String> properties = parseKeyValueList(args, "--props");
        var spec =
            CatalogSpec.newBuilder()
                .setDisplayName(display)
                .setDescription(nvl(desc, ""))
                .setConnectorRef(nvl(connectorRef, ""))
                .putAllProperties(properties)
                .setPolicyRef(nvl(policyRef, ""))
                .build();
        var resp = catalogs.createCatalog(CreateCatalogRequest.newBuilder().setSpec(spec).build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: catalog get <display_name|id>");
          return;
        }
        var resp =
            catalogs.getCatalog(
                GetCatalogRequest.newBuilder()
                    .setCatalogId(resolveCatalogId(Quotes.unquote(args.get(1))))
                    .build());
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: catalog update <display_name|id> [--display <name>] [--desc <text>]"
                  + " [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]");
          return;
        }
        String id = Quotes.unquote(args.get(1));
        String display = Quotes.unquote(parseStringFlag(args, "--display", null));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
        String connectorRef = Quotes.unquote(parseStringFlag(args, "--connector", null));
        String policyRef = Quotes.unquote(parseStringFlag(args, "--policy", null));
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var sb = CatalogSpec.newBuilder();
        LinkedHashSet<String> mask = new LinkedHashSet<>();

        if (display != null) {
          sb.setDisplayName(Quotes.unquote(display));
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

        var updateCatalogBuilder =
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(id))
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build());
        var catalogPrecondition = preconditionFromEtag(args);
        if (catalogPrecondition != null) {
          updateCatalogBuilder.setPrecondition(catalogPrecondition);
        }
        var req = updateCatalogBuilder.build();

        var resp = catalogs.updateCatalog(req);
        printCatalogs(List.of(resp.getCatalog()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: catalog delete <display_name|id> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");
        var deleteCatalogBuilder =
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(Quotes.unquote(args.get(1))))
                .setRequireEmpty(requireEmpty);
        var deleteCatalogPrecondition = preconditionFromEtag(args);
        if (deleteCatalogPrecondition != null) {
          deleteCatalogBuilder.setPrecondition(deleteCatalogPrecondition);
        }
        var req = deleteCatalogBuilder.build();
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

    String raw = args.get(0).trim();
    String explicitId = Quotes.unquote(parseStringFlag(args, "--id", ""));
    String namePrefix = Quotes.unquote(parseStringFlag(args, "--prefix", ""));
    boolean recursive = args.contains("--recursive");
    boolean childrenOnly = !recursive;

    ListNamespacesRequest.Builder rb =
        ListNamespacesRequest.newBuilder()
            .setChildrenOnly(childrenOnly)
            .setRecursive(recursive)
            .setNamePrefix(nvl(namePrefix, ""))
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    if (!explicitId.isBlank()) {
      rb.setNamespaceId(resolveNamespaceIdFlexible(explicitId));
    } else if (looksLikeQuotedOrRawUuid(raw)) {
      rb.setNamespaceId(resolveNamespaceIdFlexible(raw));
    } else {
      try {
        NameRef ref = nameRefForNamespace(raw, true);
        rb.setCatalogId(resolveCatalogId(ref.getCatalog())).addAllPath(ref.getPathList());
      } catch (IllegalArgumentException ex) {
        rb.setCatalogId(resolveCatalogId(raw));
      }
    }

    List<Namespace> all =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> namespaces.listNamespaces(rb.setPage(pr).build()),
            r -> r.getNamespacesList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
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
              "usage: namespace create <catalog|catalog.ns[.ns...]> "
                  + "[--display <leaf>] [--path a.b[.c]] "
                  + "[--desc <text>] [--props k=v ...] [--policy <id>]");
          return;
        }

        String token = args.get(1).trim();
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", ""));
        Map<String, String> properties = parseKeyValueList(args, "--props");
        String policy = Quotes.unquote(parseStringFlag(args, "--policy", ""));

        String leafOpt = Quotes.unquote(parseStringFlag(args, "--display", null));
        String pathOpt = parseStringFlag(args, "--path", null);

        String catalog;
        List<String> parents;
        String leaf;

        if (leafOpt != null || pathOpt != null) {
          int firstDot = token.indexOf('.');
          catalog = Quotes.unquote((firstDot > 0) ? token.substring(0, firstDot) : token);
          if (catalog.isBlank()) {
            out.println("Error: catalog is required before --path/--display.");
            return;
          }
          parents =
              (pathOpt == null || pathOpt.isBlank())
                  ? List.of()
                  : splitPathRespectingQuotesAndEscapes(pathOpt);
          if (leafOpt == null || leafOpt.isBlank()) {
            out.println("Error: --display is required when using --path/explicit mode.");
            return;
          }
          leaf = leafOpt;
        } else {
          NameRef nr = nameRefForNamespace(token, true);
          catalog = nr.getCatalog();
          if (nr.getPathList().isEmpty()) {
            out.println("Namespace path is empty.");
            return;
          }
          parents = nr.getPathList().subList(0, nr.getPathCount() - 1);
          leaf = nr.getPathList().get(nr.getPathCount() - 1);
        }

        var spec =
            NamespaceSpec.newBuilder()
                .setCatalogId(resolveCatalogId(catalog))
                .addAllPath(parents)
                .setDisplayName(leaf)
                .setDescription(desc)
                .putAllProperties(properties)
                .setPolicyRef(policy)
                .build();

        var resp =
            namespaces.createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build());
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: namespace get <id|fq>");
          return;
        }
        ResourceId nsId =
            looksLikeQuotedOrRawUuid(args.get(1))
                ? namespaceRid(Quotes.unquote(args.get(1)))
                : resolveNamespaceIdFlexible(args.get(1));
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
            looksLikeQuotedOrRawUuid(args.get(1))
                ? namespaceRid(Quotes.unquote(args.get(1)))
                : directory
                    .resolveNamespace(
                        ResolveNamespaceRequest.newBuilder()
                            .setRef(nameRefForNamespace(args.get(1), false))
                            .build())
                    .getResourceId();

        String display = Quotes.unquote(parseStringFlag(args, "--display", null));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
        String policyRef = Quotes.unquote(parseStringFlag(args, "--policy", null));
        String pathStr = parseStringFlag(args, "--path", null);
        String catalogStr = Quotes.unquote(parseStringFlag(args, "--catalog", null));
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
          var pathList =
              pathStr.isBlank() ? List.<String>of() : splitPathRespectingQuotesAndEscapes(pathStr);
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

        var updateNamespaceBuilder =
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build());
        var nsPrecondition = preconditionFromEtag(args);
        if (nsPrecondition != null) {
          updateNamespaceBuilder.setPrecondition(nsPrecondition);
        }
        var req = updateNamespaceBuilder.build();

        var resp = namespaces.updateNamespace(req);
        printNamespaces(List.of(resp.getNamespace()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: namespace delete <id|fq> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");

        ResourceId nsId = resolveNamespaceIdFlexible(args.get(1));

        var deleteNamespaceBuilder =
            DeleteNamespaceRequest.newBuilder().setNamespaceId(nsId).setRequireEmpty(requireEmpty);
        var deleteNamespacePrecondition = preconditionFromEtag(args);
        if (deleteNamespacePrecondition != null) {
          deleteNamespaceBuilder.setPrecondition(deleteNamespacePrecondition);
        }
        var req = deleteNamespaceBuilder.build();
        namespaces.deleteNamespace(req);
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdTables(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: tables <catalog.ns[.ns...][.prefix]>");
      return;
    }
    var prefix = nameRefForTablePrefix(args.get(0));

    ResolveFQTablesRequest.Builder rb =
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefix)
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    List<ResolveFQTablesResponse.Entry> all =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> directory.resolveFQTables(rb.setPage(pr).build()),
            ResolveFQTablesResponse::getTablesList,
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
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

        NameRef nsRef =
            NameRef.newBuilder().setCatalog(ref.getCatalog()).addAllPath(ref.getPathList()).build();

        ResourceId namespaceId =
            directory
                .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(nsRef).build())
                .getResourceId();

        String name = ref.getName();

        String desc = Quotes.unquote(parseStringFlag(args, "--desc", ""));
        String root = Quotes.unquote(parseStringFlag(args, "--root", ""));
        String schema = Quotes.unquote(parseStringFlag(args, "--schema", ""));
        List<String> parts = csvList(Quotes.unquote(parseStringFlag(args, "--parts", "")));
        String formatStr = Quotes.unquote(parseStringFlag(args, "--format", ""));
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = Quotes.unquote(parseStringFlag(args, "--up-connector", ""));
        String upNs = Quotes.unquote(parseStringFlag(args, "--up-ns", ""));
        String upTable = Quotes.unquote(parseStringFlag(args, "--up-table", ""));

        var ub =
            UpstreamRef.newBuilder()
                .setFormat(parseFormat(formatStr))
                .setUri(root)
                .addAllPartitionKeys(parts);

        if (!upConnector.isBlank()) {
          ub.setConnectorId(resolveConnectorId(upConnector));
        }
        if (!upNs.isBlank()) {
          ub.clearNamespacePath().addAllNamespacePath(splitPathRespectingQuotesAndEscapes(upNs));
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

        var resp = tables.createTable(CreateTableRequest.newBuilder().setSpec(spec).build());
        printTable(resp.getTable());
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: table get <id|catalog.ns[.ns...].table>");
          return;
        }
        ResourceId tableId = resolveTableIdFlexible(args.get(1));
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

        ResourceId tableId = resolveTableIdFlexible(args.get(1));

        String catalogStr = Quotes.unquote(parseStringFlag(args, "--catalog", null));
        String nsStr = Quotes.unquote(parseStringFlag(args, "--namespace", null));
        String name = Quotes.unquote(parseStringFlag(args, "--name", null));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
        String root = Quotes.unquote(parseStringFlag(args, "--root", null));
        String schema = Quotes.unquote(parseStringFlag(args, "--schema", null));
        List<String> parts = csvList(Quotes.unquote(parseStringFlag(args, "--parts", "")));
        String formatStr = Quotes.unquote(parseStringFlag(args, "--format", ""));
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = Quotes.unquote(parseStringFlag(args, "--up-connector", null));
        String upNs = Quotes.unquote(parseStringFlag(args, "--up-ns", null));
        String upTable = Quotes.unquote(parseStringFlag(args, "--up-table", null));

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
          ub.clearNamespacePath().addAllNamespacePath(splitPathRespectingQuotesAndEscapes(upNs));
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

        var updateTableBuilder =
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(sb.build())
                .setUpdateMask(mask);
        var tablePrecondition = preconditionFromEtag(args);
        if (tablePrecondition != null) {
          updateTableBuilder.setPrecondition(tablePrecondition);
        }
        var req = updateTableBuilder.build();

        var resp = tables.updateTable(req);
        printTable(resp.getTable());
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: table delete <id|catalog.ns[.ns...].table> [--etag <etag>]");
          return;
        }
        ResourceId tableId = resolveTableIdFlexible(args.get(1));
        var deleteTableBuilder = DeleteTableRequest.newBuilder().setTableId(tableId);
        var deleteTablePrecondition = preconditionFromEtag(args);
        if (deleteTablePrecondition != null) {
          deleteTableBuilder.setPrecondition(deleteTablePrecondition);
        }
        var req = deleteTableBuilder.build();
        tables.deleteTable(req);
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  private void cmdViews(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: views <catalog.ns[.ns...]>");
      return;
    }
    ResourceId namespaceId = resolveNamespaceIdFlexible(args.get(0));
    List<View> all =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr ->
                viewService.listViews(
                    ListViewsRequest.newBuilder().setNamespaceId(namespaceId).setPage(pr).build()),
            ListViewsResponse::getViewsList,
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printViews(all);
  }

  private void cmdViewCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: view <create|get|update|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "create" -> {
        if (args.size() < 2) {
          out.println(
              "usage: view create <catalog.ns[.ns...].name> [--sql <text>] [--desc <text>] [--props"
                  + " k=v ...]");
          return;
        }
        NameRef ref = nameRefForTable(args.get(1));
        ResourceId catalogId = resolveCatalogId(ref.getCatalog());
        ResourceId namespaceId =
            directory
                .resolveNamespace(
                    ResolveNamespaceRequest.newBuilder()
                        .setRef(
                            NameRef.newBuilder()
                                .setCatalog(ref.getCatalog())
                                .addAllPath(ref.getPathList())
                                .build())
                        .build())
                .getResourceId();
        String sql = Quotes.unquote(parseStringFlag(args, "--sql", ""));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", ""));
        Map<String, String> props = parseKeyValueList(args, "--props");

        ViewSpec.Builder spec =
            ViewSpec.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName(ref.getName())
                .setSql(nvl(sql, ""));
        if (desc != null && !desc.isBlank()) {
          spec.setDescription(desc);
        }
        if (!props.isEmpty()) {
          spec.putAllProperties(props);
        }
        var resp = viewService.createView(CreateViewRequest.newBuilder().setSpec(spec).build());
        printView(resp.getView());
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: view get <id|catalog.ns[.ns...].name>");
          return;
        }
        ResourceId viewId = resolveViewIdFlexible(args.get(1));
        var resp = viewService.getView(GetViewRequest.newBuilder().setViewId(viewId).build());
        printView(resp.getView());
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: view update <id|fq> [--display <name>] [--namespace <catalog.ns[.ns...]>]"
                  + " [--sql <text>] [--desc <text>] [--props k=v ...]");
          return;
        }
        ResourceId viewId = resolveViewIdFlexible(args.get(1));
        String display = Quotes.unquote(parseStringFlag(args, "--display", null));
        String ns = Quotes.unquote(parseStringFlag(args, "--namespace", null));
        String sql = Quotes.unquote(parseStringFlag(args, "--sql", null));
        String desc = Quotes.unquote(parseStringFlag(args, "--desc", null));
        Map<String, String> props = parseKeyValueList(args, "--props");

        ViewSpec.Builder spec = ViewSpec.newBuilder();
        FieldMask.Builder mask = FieldMask.newBuilder();
        if (display != null) {
          spec.setDisplayName(display);
          mask.addPaths("display_name");
        }
        if (ns != null) {
          ResourceId namespaceId = resolveNamespaceIdFlexible(ns);
          spec.setNamespaceId(namespaceId);
          mask.addPaths("namespace_id");
        }
        if (sql != null) {
          spec.setSql(sql);
          mask.addPaths("sql");
        }
        if (desc != null) {
          spec.setDescription(desc);
          mask.addPaths("description");
        }
        if (!props.isEmpty()) {
          spec.putAllProperties(props);
          mask.addPaths("properties");
        }
        if (mask.getPathsCount() == 0) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }
        var resp =
            viewService.updateView(
                UpdateViewRequest.newBuilder()
                    .setViewId(viewId)
                    .setSpec(spec)
                    .setUpdateMask(mask)
                    .build());
        printView(resp.getView());
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: view delete <id|catalog.ns[.ns...].name>");
          return;
        }
        ResourceId viewId = resolveViewIdFlexible(args.get(1));
        viewService.deleteView(DeleteViewRequest.newBuilder().setViewId(viewId).build());
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
    List<Connector> all =
        collectPages(
            pageSize,
            pr -> connectors.listConnectors(ListConnectorsRequest.newBuilder().setPage(pr).build()),
            r -> r.getConnectorsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    return (kind == null) ? all : all.stream().filter(c -> c.getKind() == kind).toList();
  }

  private void cmdConnectorCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: connector <list|get|create|update|delete|validate|trigger|job> ...");
      return;
    }
    String sub = args.get(0);

    switch (sub) {
      case "list" -> {
        String kindStr = Quotes.unquote(parseStringFlag(args, "--kind", ""));
        int pageSize = parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);
        ConnectorKind filter = parseConnectorKind(kindStr);
        filter = (filter == ConnectorKind.CK_UNSPECIFIED && kindStr.isBlank()) ? null : filter;

        var all = listAllConnectors(filter, pageSize);
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
                    .setConnectorId(resolveConnectorId(Quotes.unquote(args.get(1))))
                    .build());
        printConnectors(List.of(resp.getConnector()));
      }
      case "create" -> {
        if (args.size() < 6) {
          out.println(
              "usage: connector create <display_name> <kind (ICEBERG|DELTA|GLUE|UNITY)> <uri>"
                  + " <source_namespace (a[.b[.c]...])> <destination_catalog (name)>"
                  + " [--source-table <name>] [--source-cols c1,#id2,...] [--dest-ns <a.b[.c]>]"
                  + " [--dest-table <name>] [--desc <text>] [--auth-scheme <scheme>] [--auth k=v"
                  + " ...] [--head k=v ...] [--secret <ref>] [--policy-enabled] (if provided,"
                  + " policy.enabled=true) [--policy-interval-sec <n>] [--policy-max-par <n>]"
                  + " [--policy-not-before-epoch <sec>] [--props k=v ...]  (e.g."
                  + " stats.ndv.enabled=false,stats.ndv.sample_fraction=0.1)");
          return;
        }

        String display = Quotes.unquote(args.get(1));
        ConnectorKind kind = parseConnectorKind(Quotes.unquote(args.get(2)));
        String uri = Quotes.unquote(args.get(3));
        String sourceNamespace = Quotes.unquote(args.get(4));
        String destCatalog = Quotes.unquote(args.get(5));

        String sourceTable = Quotes.unquote(parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(parseStringFlag(args, "--source-cols", "")));
        String destNamespace = Quotes.unquote(parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(parseStringFlag(args, "--dest-table", ""));
        if (sourceTable.isBlank() && !destTable.isBlank()) {
          sourceTable = destTable;
        } else if (!sourceTable.isBlank() && destTable.isBlank()) {
          destTable = sourceTable;
        }

        String description = Quotes.unquote(parseStringFlag(args, "--desc", ""));
        String authScheme = Quotes.unquote(parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = Quotes.unquote(parseStringFlag(args, "--secret", ""));

        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = parseLongFlag(args, "--policy-interval-sec", 0L);
        int maxPar = parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var auth = buildAuth(authScheme, authProps, headerHints, secretRef);
        var policy = buildPolicy(policyEnabled, intervalSec, maxPar, notBeforeSec);

        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName(display)
                .setDescription(description)
                .setKind(kind)
                .setUri(uri)
                .putAllProperties(properties)
                .setAuth(auth)
                .setPolicy(policy);

        boolean haveSource =
            !sourceNamespace.isBlank() || !sourceTable.isBlank() || !sourceCols.isEmpty();
        if (haveSource) {
          spec.setSource(buildSource(sourceNamespace, sourceTable, sourceCols));
        }

        boolean haveDest =
            !destCatalog.isBlank() || !destNamespace.isBlank() || !destTable.isBlank();
        if (haveDest) {
          spec.setDestination(buildDest(destCatalog, destNamespace, destTable));
        }

        var resp =
            connectors.createConnector(
                CreateConnectorRequest.newBuilder().setSpec(spec.build()).build());
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

        ResourceId connectorId = resolveConnectorId(Quotes.unquote(args.get(1)));

        String display = Quotes.unquote(parseStringFlag(args, "--display", ""));
        String kindStr = Quotes.unquote(parseStringFlag(args, "--kind", ""));
        String uri = Quotes.unquote(parseStringFlag(args, "--uri", ""));
        String description = Quotes.unquote(parseStringFlag(args, "--desc", ""));

        String sourceNs = Quotes.unquote(parseStringFlag(args, "--source-ns", ""));
        String sourceTable = Quotes.unquote(parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(parseStringFlag(args, "--source-cols", "")));

        String destCatalog = Quotes.unquote(parseStringFlag(args, "--dest-catalog", ""));
        String destNs = Quotes.unquote(parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(parseStringFlag(args, "--dest-table", ""));
        if (sourceTable.isBlank() && !destTable.isBlank()) {
          sourceTable = destTable;
        } else if (!sourceTable.isBlank() && destTable.isBlank()) {
          destTable = sourceTable;
        }

        String authScheme = Quotes.unquote(parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = Quotes.unquote(parseStringFlag(args, "--secret", ""));
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
          var ab = buildAuth(authScheme, authProps, headerHints, secretRef);
          spec.setAuth(ab);
          if (!authScheme.isBlank()) mask.add("auth.scheme");
          if (!secretRef.isBlank()) mask.add("auth.secret_ref");
          if (!authProps.isEmpty()) mask.add("auth.properties");
          if (!headerHints.isEmpty()) mask.add("auth.header_hints");
        }

        boolean policySet =
            !policyEnabledStr.isBlank() || intervalSec != 0L || maxPar != 0 || notBeforeSec != 0L;
        if (policySet) {
          var pb =
              buildPolicy(
                  !policyEnabledStr.isBlank() && Boolean.parseBoolean(policyEnabledStr),
                  intervalSec,
                  maxPar,
                  notBeforeSec);
          spec.setPolicy(pb);
          if (!policyEnabledStr.isBlank()) mask.add("policy.enabled");
          if (intervalSec != 0L) mask.add("policy.interval");
          if (maxPar != 0) mask.add("policy.max_parallel");
          if (notBeforeSec != 0L) mask.add("policy.not_before");
        }

        boolean sourceSet = !sourceNs.isBlank() || !sourceTable.isBlank() || !sourceCols.isEmpty();
        if (sourceSet) {
          spec.setSource(buildSource(sourceNs, sourceTable, sourceCols));
          if (!sourceNs.isBlank()) mask.add("source.namespace");
          if (!sourceTable.isBlank()) mask.add("source.table");
          if (!sourceCols.isEmpty()) mask.add("source.columns");
        }

        boolean destSet = !destCatalog.isBlank() || !destNs.isBlank() || !destTable.isBlank();
        if (destSet) {
          spec.setDestination(buildDest(destCatalog, destNs, destTable));
          if (!destCatalog.isBlank()) mask.add("destination.catalog_display_name");
          if (!destNs.isBlank()) mask.add("destination.namespace");
          if (!destTable.isBlank()) mask.add("destination.table_display_name");
        }

        if (mask.isEmpty()) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }

        var updateConnectorBuilder =
            UpdateConnectorRequest.newBuilder()
                .setConnectorId(connectorId)
                .setSpec(spec.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build());
        var connectorPrecondition = preconditionFromEtag(args);
        if (connectorPrecondition != null) {
          updateConnectorBuilder.setPrecondition(connectorPrecondition);
        }
        var req = updateConnectorBuilder.build();

        var resp = connectors.updateConnector(req);
        printConnectors(List.of(resp.getConnector()));
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: connector delete <display_name|id> [--etag <etag>]");
          return;
        }
        var deleteConnectorBuilder =
            DeleteConnectorRequest.newBuilder()
                .setConnectorId(resolveConnectorId(Quotes.unquote(args.get(1))));
        var deleteConnectorPrecondition = preconditionFromEtag(args);
        if (deleteConnectorPrecondition != null) {
          deleteConnectorBuilder.setPrecondition(deleteConnectorPrecondition);
        }
        var req = deleteConnectorBuilder.build();
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
                  + " [--props k=v ...]");
          return;
        }

        ConnectorKind kind = parseConnectorKind(Quotes.unquote(args.get(1)));
        String uri = Quotes.unquote(args.get(2));

        String sourceNs = Quotes.unquote(parseStringFlag(args, "--source-ns", ""));
        String sourceTable = Quotes.unquote(parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(parseStringFlag(args, "--source-cols", "")));

        String destCatalog = Quotes.unquote(parseStringFlag(args, "--dest-catalog", ""));
        String destNs = Quotes.unquote(parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(parseStringFlag(args, "--dest-table", ""));

        String authScheme = Quotes.unquote(parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String secretRef = Quotes.unquote(parseStringFlag(args, "--secret", ""));

        Map<String, String> properties = parseKeyValueList(args, "--props");

        var auth = buildAuth(authScheme, authProps, headerHints, secretRef);

        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName("")
                .setKind(kind)
                .setUri(uri)
                .putAllProperties(properties)
                .setAuth(auth);

        boolean sourceSet = !sourceNs.isBlank() || !sourceTable.isBlank() || !sourceCols.isEmpty();
        if (sourceSet) spec.setSource(buildSource(sourceNs, sourceTable, sourceCols));

        boolean destSet = !destCatalog.isBlank() || !destNs.isBlank() || !destTable.isBlank();
        if (destSet) spec.setDestination(buildDest(destCatalog, destNs, destTable));

        var resp =
            connectors.validateConnector(
                ValidateConnectorRequest.newBuilder().setSpec(spec.build()).build());

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
                    .setConnectorId(resolveConnectorId(Quotes.unquote(args.get(1))))
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
                GetReconcileJobRequest.newBuilder().setJobId(Quotes.unquote(args.get(1))).build());
        out.printf(
            "job_id=%s connector_id=%s state=%s started=%s finished=%s scanned=%d"
                + " changed=%d errors=%d%n",
            resp.getJobId(),
            resp.getConnectorId(),
            resp.getState().name(),
            ts(resp.getStartedAt()),
            ts(resp.getFinishedAt()),
            resp.getTablesScanned(),
            resp.getTablesChanged(),
            resp.getErrors());
        if (resp.getMessage() != null && !resp.getMessage().isBlank()) {
          var lines = splitErrorLines(resp.getMessage());
          if (lines.isEmpty()) {
            out.println("message: " + resp.getMessage());
          } else if (lines.size() == 1) {
            out.println("message: " + lines.get(0));
          } else {
            out.println("message:");
            for (String line : lines) {
              out.println("  - " + line);
            }
          }
        }
      }

      default -> out.println("unknown subcommand");
    }
  }

  private AuthConfig buildAuth(
      String scheme, Map<String, String> props, Map<String, String> heads, String secret) {
    return AuthConfig.newBuilder()
        .setScheme(nvl(scheme, ""))
        .putAllProperties(props)
        .putAllHeaderHints(heads)
        .setSecretRef(nvl(secret, ""))
        .build();
  }

  private ReconcilePolicy buildPolicy(
      boolean enabled, long intervalSec, int maxPar, long notBeforeSec) {
    ReconcilePolicy.Builder b = ReconcilePolicy.newBuilder().setEnabled(enabled);
    if (maxPar > 0) {
      b.setMaxParallel(maxPar);
    }
    if (intervalSec > 0) {
      b.setInterval(durSeconds(intervalSec));
    }
    if (notBeforeSec > 0) {
      b.setNotBefore(tsSeconds(notBeforeSec));
    }
    return b.build();
  }

  private SourceSelector buildSource(String ns, String table, List<String> cols) {
    var b = SourceSelector.newBuilder();
    if (!nvl(ns, "").isBlank()) {
      b.setNamespace(toNsPath(ns));
    }
    if (!nvl(table, "").isBlank()) {
      b.setTable(table);
    }
    if (cols != null && !cols.isEmpty()) {
      b.addAllColumns(cols);
    }
    return b.build();
  }

  private DestinationTarget buildDest(String cat, String ns, String table) {
    var b = DestinationTarget.newBuilder();
    if (!nvl(cat, "").isBlank()) {
      b.setCatalogDisplayName(cat);
    }
    if (!nvl(ns, "").isBlank()) {
      b.setNamespace(toNsPath(ns));
    }
    if (!nvl(table, "").isBlank()) {
      b.setTableDisplayName(table);
    }
    return b.build();
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
                                  .setRef(nameRefForNamespace(s, false))
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
    var r =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(nameRefForTable(fq)).build());
    var t = tables.getTable(GetTableRequest.newBuilder().setTableId(r.getResourceId()).build());
    printTable(t.getTable());
  }

  private void cmdSnapshots(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: snapshots <tableFQ>");
      return;
    }
    var tableId =
        directory
            .resolveTable(
                ResolveTableRequest.newBuilder().setRef(nameRefForTable(args.get(0))).build())
            .getResourceId();
    var snaps =
        collectPages(
            DEFAULT_PAGE_SIZE,
            pr ->
                snapshots.listSnapshots(
                    ListSnapshotsRequest.newBuilder().setTableId(tableId).setPage(pr).build()),
            r -> r.getSnapshotsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printSnapshots(snaps);
  }

  private void cmdSnapshotCrud(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: snapshot <get|delete> ...");
      return;
    }
    String sub = args.get(0);
    switch (sub) {
      case "get" -> {
        if (args.size() < 3) {
          out.println("usage: snapshot get <id|catalog.ns[.ns...].table> <snapshot_id>");
          return;
        }
        ResourceId tableId = resolveTableIdFlexible(args.get(1));
        long snapshotId = Long.parseLong(args.get(2));
        var resp =
            snapshots.getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                    .build());
        Snapshot snapshot = resp.getSnapshot();
        printSnapshotDetail(snapshot);
      }
      case "delete" -> {
        if (args.size() < 3) {
          out.println("usage: snapshot delete <id|catalog.ns[.ns...].table> <snapshot_id>");
          return;
        }
        ResourceId tableId = resolveTableIdFlexible(args.get(1));
        long snapshotId = Long.parseLong(args.get(2));
        var deleteSnapshotBuilder =
            DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
        var snapshotPrecondition = preconditionFromEtag(args);
        if (snapshotPrecondition != null) {
          deleteSnapshotBuilder.setPrecondition(snapshotPrecondition);
        }
        var req = deleteSnapshotBuilder.build();
        try {
          snapshots.deleteSnapshot(req);
          out.println("ok");
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.FAILED_PRECONDITION) {
            out.println(
                "! Precondition failed (etag/version mismatch). Retry with --etag from "
                    + "`snapshot get`.");
          } else {
            throw e;
          }
        }
      }
      default -> out.println("unknown subcommand");
    }
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
      case "files" -> statsFiles(args.subList(1, args.size()));
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
    var r =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(nameRefForTable(fq)).build());
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

    var resp =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(nameRefForTable(fq)).build());

    ListColumnStatsRequest.Builder rb =
        ListColumnStatsRequest.newBuilder()
            .setTableId(resp.getResourceId())
            .setSnapshot(parseSnapshotSelector(args));

    List<ColumnStats> all =
        collectPages(
            pageSize,
            pr -> statistics.listColumnStats(rb.setPage(pr).build()),
            r -> r.getColumnsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    if (all.size() > limit) all = all.subList(0, limit);
    printColumnStats(all);
  }

  private void statsFiles(List<String> args) {
    if (args.isEmpty()) {
      out.println(
          "usage: stats files <tableFQ> [--snapshot <id>|--current] (defaults to --current)"
              + " [--limit N]");
      return;
    }

    String fq = args.get(0);
    int limit = parseIntFlag(args, "--limit", 1000);
    int pageSize = Math.min(limit, DEFAULT_PAGE_SIZE);

    var resolved =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(nameRefForTable(fq)).build());

    ListFileColumnStatsRequest.Builder rb =
        ListFileColumnStatsRequest.newBuilder()
            .setTableId(resolved.getResourceId())
            .setSnapshot(parseSnapshotSelector(args));

    List<FileColumnStats> all =
        collectPages(
            pageSize,
            pr -> statistics.listFileColumnStats(rb.setPage(pr).build()),
            r -> r.getFileColumnsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");

    if (all.size() > limit) {
      all = all.subList(0, limit);
    }

    printFileColumnStats(all);
  }

  private void cmdQuery(List<String> args) {
    if (args.isEmpty()) {
      out.println("usage: query <begin|renew|end|get|fetch-scan> ...");
      return;
    }
    String sub = args.get(0);
    List<String> tail = args.subList(1, args.size());
    switch (sub) {
      case "begin" -> queryBegin(tail);
      case "renew" -> queryRenew(tail);
      case "end" -> queryEnd(tail);
      case "get" -> queryGet(tail);
      case "fetch-scan" -> queryFetchScan(tail);
      default -> out.println("usage: query <begin|renew|end|get|fetch-scan> ...");
    }
  }

  private void queryBegin(List<String> args) {
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
            nr = nameRefForTable(args.get(i + 1));
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return;
          }
          if (nr.getCatalog().isBlank()) {
            nr = NameRef.newBuilder(nr).setCatalog(currentCatalog).build();
          }
          QueryInput.Builder input = QueryInput.newBuilder().setName(nr);
          int next = parseQueryInputOptions(args, i + 2, input, true);
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
            nr = nameRefForNamespace(args.get(i + 1), true);
          } catch (IllegalArgumentException e) {
            out.println("query begin: " + e.getMessage());
            return;
          }
          if (nr.getCatalog().isBlank()) {
            nr = NameRef.newBuilder(nr).setCatalog(currentCatalog).build();
          }
          QueryInput.Builder input = QueryInput.newBuilder().setName(nr);
          int next = parseQueryInputOptions(args, i + 2, input, false);
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
          ResourceId rid = tableRid(args.get(i + 1));
          QueryInput.Builder input = QueryInput.newBuilder().setTableId(rid);
          int next = parseQueryInputOptions(args, i + 2, input, true);
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
          ResourceId rid = viewRid(args.get(i + 1));
          QueryInput.Builder input = QueryInput.newBuilder().setViewId(rid);
          int next = parseQueryInputOptions(args, i + 2, input, false);
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

    ResourceId queryDefaultCatalogId = resolveCatalogId(currentCatalog);

    BeginQueryRequest.Builder req =
        BeginQueryRequest.newBuilder().setDefaultCatalogId(queryDefaultCatalogId);
    if (ttlSeconds > 0) {
      req.setTtlSeconds(ttlSeconds);
    }
    if (asOfDefault != null) {
      req.setAsOfDefault(asOfDefault);
    }

    // First, begin the query context
    BeginQueryResponse resp = queries.beginQuery(req.build());

    String queryId = resp.getQuery().getQueryId();

    // Now resolve inputs using QuerySchemaService
    DescribeInputsRequest.Builder descReq =
        DescribeInputsRequest.newBuilder().setQueryId(queryId).addAllInputs(inputs);

    DescribeInputsResponse descResp = querySchema.describeInputs(descReq.build());

    // Print the resolved schemas BEFORE printing query descriptor
    printDescribeInputs(descResp, inputs);

    printQueryBegin(resp);
  }

  private void queryRenew(List<String> args) {
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

  private void queryEnd(List<String> args) {
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

  private void queryGet(List<String> args) {
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
    printQueryDescriptor(resp.getQuery());
  }

  private void queryFetchScan(List<String> args) {
    if (args.size() < 2) {
      out.println("usage: query fetch-scan <query_id> <table_id>");
      return;
    }

    String queryId = args.get(0);
    ResourceId tableId;
    try {
      tableId = tableRid(args.get(1));
    } catch (IllegalArgumentException e) {
      out.println("query fetch-scan: " + e.getMessage());
      return;
    }

    InitScanRequest initReq =
        InitScanRequest.newBuilder().setQueryId(queryId).setTableId(tableId).build();
    InitScanResponse initResp = queryScan.initScan(initReq);
    TableInfo tableInfo = initResp.getTableInfo();
    ScanHandle handle = initResp.getHandle();

    List<ScanFile> deleteFiles = new ArrayList<>();
    var deleteStream = queryScan.streamDeleteFiles(handle);
    while (deleteStream.hasNext()) {
      DeleteFileBatch batch = deleteStream.next();
      batch.getItemsList().forEach(deleteFile -> deleteFiles.add(deleteFile.getFile()));
    }

    List<DataFile> dataFiles = new ArrayList<>();
    var dataStream = queryScan.streamDataFiles(handle);
    while (dataStream.hasNext()) {
      DataFileBatch batch = dataStream.next();
      dataFiles.addAll(batch.getItemsList());
    }

    try {
      queryScan.closeScan(handle);
    } catch (Exception ignored) {
      // best effort cleanup
    }

    out.println("query id: " + queryId);
    out.println("table id: " + rid(tableId));
    if (initResp.hasTableInfo()) {
      out.println("schema: " + tableInfo.getSchemaJson());
      out.println("metadata-location: " + tableInfo.getMetadataLocation());
    }
    printQueryFiles("data_files", dataFiles.stream().map(DataFile::getFile).toList());
    printQueryFiles("delete_files", deleteFiles);
  }

  private int parseQueryInputOptions(
      List<String> args, int start, QueryInput.Builder input, boolean allowSnapshot) {
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

  private boolean isQueryInputStartToken(String token) {
    return switch (token) {
      case "table", "table-id", "view-id", "namespace" -> true;
      default -> false;
    };
  }

  private boolean isQueryGlobalFlag(String token) {
    return "--ttl".equals(token) || "--as-of-default".equals(token);
  }

  private void printQueryBegin(BeginQueryResponse resp) {
    if (!resp.hasQuery()) {
      out.println("query begin: no query returned");
      return;
    }
    printQueryDescriptor(resp.getQuery());
  }

  private void printQueryDescriptor(QueryDescriptor query) {
    out.println("query id: " + query.getQueryId());
    if (!query.getAccountId().isBlank()) {
      out.println("account: " + query.getAccountId());
    }
    out.println("status: " + query.getQueryStatus().name().toLowerCase(Locale.ROOT));
    out.println("created: " + ts(query.getCreatedAt()));
    out.println("expires: " + ts(query.getExpiresAt()));
    printQuerySnapshots(query.getSnapshots());
    printQueryExpansion(query.getExpansion());
    printQueryObligations(query.getObligationsList());
  }

  private void printQuerySnapshots(SnapshotSet snapshots) {
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

  private void printQueryExpansion(ExpansionMap expansion) {
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

  private void printQueryObligations(List<TableObligations> obligations) {
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

  private void printQueryFiles(String label, List<ScanFile> files) {
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

  private boolean looksLikeUuid(String s) {
    if (s == null) {
      return false;
    }
    String t = s.trim();
    return t.matches(
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  private ResourceId resourceId(String id, ResourceKind kind) {
    if (currentAccountId == null || currentAccountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder().setAccountId(currentAccountId).setKind(kind).setId(id).build();
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
    String t = Quotes.unquote(token);
    if (looksLikeUuid(t)) {
      return connectorRid(t);
    }

    var all = listAllConnectors(null, DEFAULT_PAGE_SIZE);

    var exact = all.stream().filter(c -> t.equals(c.getDisplayName())).toList();
    if (exact.size() == 1) {
      return exact.get(0).getResourceId();
    }

    var ci = all.stream().filter(c -> t.equalsIgnoreCase(c.getDisplayName())).toList();
    if (ci.size() == 1) {
      return ci.get(0).getResourceId();
    }

    if (exact.isEmpty() && ci.isEmpty()) {
      throw new IllegalArgumentException("Connector not found: " + t);
    }

    String alts =
        (exact.isEmpty() ? ci : exact)
            .stream()
                .map(c -> c.getDisplayName() + " (" + rid(c.getResourceId()) + ")")
                .collect(Collectors.joining(", "));
    throw new IllegalArgumentException(
        "Connector name is ambiguous: " + t + ". Candidates: " + alts);
  }

  private ResourceId resolveCatalogId(String token) {
    String t = Quotes.unquote(token);
    if (looksLikeUuid(t)) {
      return catalogRid(t);
    }
    return directory
        .resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(nameCatalog(t)).build())
        .getResourceId();
  }

  private ResourceId resolveNamespaceId(String tokenOrFq) {
    String t = tokenOrFq == null ? "" : tokenOrFq.trim();
    String tu = Quotes.unquote(t);
    if (looksLikeUuid(tu)) {
      return namespaceRid(tu);
    }
    return directory
        .resolveNamespace(
            ResolveNamespaceRequest.newBuilder().setRef(nameRefForNamespace(tu, false)).build())
        .getResourceId();
  }

  private NameRef nameCatalog(String name) {
    return NameRef.newBuilder().setCatalog(Quotes.unquote(name)).build();
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

  private SnapshotRef snapshotFromTokenOrCurrent(String tokenOrNull) {
    if (tokenOrNull == null || tokenOrNull.isBlank() || tokenOrNull.equalsIgnoreCase("current"))
      return SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build();
    try {
      return SnapshotRef.newBuilder().setSnapshotId(Long.parseLong(tokenOrNull.trim())).build();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("invalid snapshot selector '" + tokenOrNull + "'");
    }
  }

  private SnapshotRef parseSnapshotSelector(List<String> args) {
    int idx = args.indexOf("--snapshot");
    return snapshotFromTokenOrCurrent(
        idx >= 0 && idx + 1 < args.size() ? args.get(idx + 1) : "current");
  }

  private static List<String> csvList(String s) {
    try {
      return CsvListParserUtil.items(s).stream()
          .map(Quotes::unquote)
          .filter(t -> t != null && !t.isBlank())
          .toList();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid CSV list: " + s, e);
    }
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
          Quotes.quoteIfNeeded(c.getDisplayName()),
          c.hasDescription() ? c.getDescription() : "");
    }
  }

  private void printAccounts(List<Account> rows) {
    out.printf(
        "%-40s  %-24s  %-24s  %s%n", "ACCOUNT_ID", "CREATED_AT", "DISPLAY_NAME", "DESCRIPTION");
    for (var a : rows) {
      out.printf(
          "%-40s  %-24s  %-24s  %s%n",
          rid(a.getResourceId()),
          ts(a.getCreatedAt()),
          Quotes.quoteIfNeeded(a.getDisplayName()),
          a.hasDescription() ? a.getDescription() : "");
    }
  }

  private static String joinFqQuoted(String catalog, List<String> nsParts, String obj) {
    StringBuilder sb = new StringBuilder();
    sb.append(Quotes.quoteIfNeeded(catalog));
    for (String ns : nsParts) {
      sb.append('.').append(Quotes.quoteIfNeeded(ns));
    }
    if (obj != null) {
      sb.append('.').append(Quotes.quoteIfNeeded(obj));
    }
    return sb.toString();
  }

  private String parentsAsList(List<String> parents) {
    return "["
        + parents.stream().map(Quotes::quoteIfNeeded).collect(Collectors.joining(", "))
        + "]";
  }

  private void printNamespaces(List<Namespace> rows) {
    out.printf(
        "%-36s  %-24s  %-26s  %-16s  %s%n",
        "NAMESPACE_ID", "CREATED_AT", "PARENTS", "LEAF", "DESCRIPTION");

    for (var ns : rows) {
      var parentsList = ns.getParentsList();
      var leaf = ns.getDisplayName();
      out.printf(
          "%-36s  %-24s  %-26s  %-16s  %s%n",
          rid(ns.getResourceId()),
          ts(ns.getCreatedAt()),
          parentsAsList(parentsList),
          Quotes.quoteIfNeeded(leaf),
          ns.hasDescription() ? ns.getDescription() : "");
    }
  }

  private void printResolvedTables(List<ResolveFQTablesResponse.Entry> entries) {
    out.printf("%-40s  %s%n", "TABLE_ID", "NAME");
    for (var e : entries) {
      String catalog = e.getName().getCatalog();
      List<String> path = e.getName().getPathList();
      String table = e.getName().getName();
      String fq = joinFqQuoted(catalog, path, table);
      out.printf("%-40s  %s%n", rid(e.getResourceId()), fq);
    }
  }

  private void printViews(List<View> views) {
    out.printf("%-40s  %-24s  %s%n", "VIEW_ID", "CREATED_AT", "DISPLAY_NAME");
    for (var view : views) {
      out.printf(
          "%-40s  %-24s  %s%n",
          rid(view.getResourceId()),
          ts(view.getCreatedAt()),
          Quotes.quoteIfNeeded(view.getDisplayName()));
    }
  }

  private void printTable(Table t) {
    UpstreamRef upstream = t.hasUpstream() ? t.getUpstream() : UpstreamRef.getDefaultInstance();
    out.println("Table:");
    out.printf("  id:           %s%n", rid(t.getResourceId()));
    out.printf("  name:         %s%n", t.getDisplayName());
    out.printf("  description:  %s%n", t.hasDescription() ? t.getDescription() : "");
    out.printf("  schema:       %s%n", t.getSchemaJson());
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

  private void printView(View view) {
    out.println("View:");
    out.printf("  id:           %s%n", rid(view.getResourceId()));
    out.printf("  name:         %s%n", view.getDisplayName());
    out.printf("  description:  %s%n", view.hasDescription() ? view.getDescription() : "");
    out.printf("  sql:          %s%n", view.getSql());
    out.printf("  created_at:   %s%n", ts(view.getCreatedAt()));
    if (!view.getPropertiesMap().isEmpty()) {
      out.println("  properties:");
      view.getPropertiesMap().forEach((k, v) -> out.printf("    %s = %s%n", k, v));
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

  private void printSnapshotDetail(Snapshot snapshot) {
    try {
      JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
      out.println(printer.print(snapshot));
      printDecodedFormatMetadata(snapshot, printer);
    } catch (InvalidProtocolBufferException e) {
      out.println(snapshot.toString());
    }
  }

  private void printDecodedFormatMetadata(Snapshot snapshot, JsonFormat.Printer printer) {
    if (snapshot == null || snapshot.getFormatMetadataCount() == 0) {
      return;
    }
    ByteString icebergRaw = snapshot.getFormatMetadataOrDefault("iceberg", ByteString.EMPTY);
    if (icebergRaw == null || icebergRaw.isEmpty()) {
      return;
    }
    out.println("decoded_format_metadata:");
    try {
      IcebergMetadata metadata = IcebergMetadata.parseFrom(icebergRaw);
      out.println("  iceberg: " + printer.print(metadata));
    } catch (InvalidProtocolBufferException e) {
      out.println("  iceberg: <failed to parse: " + e.getMessage() + ">");
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
        "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
        "CID", "NAME", "TYPE", "VALUES", "NULLS", "NaNs", "MIN", "MAX", "NDV", "#THETA SKETCHES");

    for (var c : cols) {
      out.printf(
          "%-8s %-28s %-12s %-12s %-10s %-10s %-24s %-24s %-24s %-24s%n",
          c.getColumnId(),
          trunc(c.getColumnName(), 28),
          trunc(c.getLogicalType(), 12),
          Long.toString(c.getValueCount()),
          Long.toString(c.getNullCount()),
          Long.toString(c.getNanCount()),
          trunc(c.getMin(), 24),
          trunc(c.getMax(), 24),
          c.hasNdv() ? ndvToString(c.getNdv()) : "-",
          c.hasNdv() ? c.getNdv().getSketchesCount() : "-");
    }
  }

  private void printFileColumnStats(List<FileColumnStats> files) {
    if (files == null || files.isEmpty()) {
      out.println("No file stats found.");
      return;
    }

    out.printf("%-4s %-10s %-12s %s%n", "IDX", "ROWS", "BYTES", "PATH");
    for (int i = 0; i < files.size(); i++) {
      FileColumnStats fs = files.get(i);
      out.printf("%-4d %-10d %-12d %s%n", i, fs.getRowCount(), fs.getSizeBytes(), fs.getFilePath());

      var cols = fs.getColumnsList();
      if (!cols.isEmpty()) {
        out.println("    columns:");
        for (ColumnStats c : cols) {
          String ndv = c.hasNdv() ? ndvToString(c.getNdv()) : "-";
          out.printf(
              "      %-8s %-24s %-10s values=%-8d nulls=%-8d NaNs=%-8d min=%-20s max=%-20s"
                  + " ndv=%s%n",
              c.getColumnId(),
              trunc(c.getColumnName(), 24),
              trunc(c.getLogicalType(), 10),
              c.getValueCount(),
              c.getNullCount(),
              c.getNanCount(),
              trunc(c.getMin(), 20),
              trunc(c.getMax(), 20),
              ndv);
        }
      }
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
        String destCatDisplay = "";
        List<String> destNsParts = List.of();
        String destTableDisplay = null;

        if (c.getDestination().hasCatalogId()) {
          var resp =
              directory.lookupCatalog(
                  LookupCatalogRequest.newBuilder()
                      .setResourceId(c.getDestination().getCatalogId())
                      .build());
          destCatDisplay = resp.getDisplayName();
        }

        if (c.getDestination().hasNamespaceId()) {
          var nsResp =
              directory.lookupNamespace(
                  LookupNamespaceRequest.newBuilder()
                      .setResourceId(c.getDestination().getNamespaceId())
                      .build());
          var ref = nsResp.getRef();
          ArrayList<String> parts = new ArrayList<>(ref.getPathList());
          if (!ref.getName().isBlank()) parts.add(ref.getName());
          destNsParts = parts;
        }

        if (c.getDestination().hasTableId()) {
          var tblResp =
              directory.lookupTable(
                  LookupTableRequest.newBuilder()
                      .setResourceId(c.getDestination().getTableId())
                      .build());
          destTableDisplay = tblResp.getName().getName();
        }

        if (destCatDisplay.isBlank() && c.getDestination().hasCatalogId()) {
          destCatDisplay = c.getDestination().getCatalogId().getId();
        }

        boolean anyDest =
            !destCatDisplay.isBlank()
                || !destNsParts.isEmpty()
                || (destTableDisplay != null && !destTableDisplay.isBlank());
        if (anyDest) {
          String fq = joinFqQuoted(destCatDisplay, destNsParts, destTableDisplay);
          out.println("  destination: " + fq);
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

      if (!c.getPropertiesMap().isEmpty()) {
        String propsStr =
            c.getPropertiesMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));
        out.println("  properties: " + propsStr);
      }
    }
  }

  private void printDescribeInputs(DescribeInputsResponse resp, List<QueryInput> inputs) {
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

  private String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) {
      return "-";
    }
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private Timestamp tsSeconds(long epochSeconds) {
    return Timestamp.newBuilder().setSeconds(epochSeconds).build();
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
      return approx.getEstimate() == Math.rint(approx.getEstimate())
          ? Long.toString(Math.round(approx.getEstimate()))
          : String.format(Locale.ROOT, "%.3f", approx.getEstimate());
    }
    return "-";
  }

  private static NameRef nameRefForTable(String fq) {
    if (fq == null) throw new IllegalArgumentException("Fully qualified name is required");
    fq = fq.trim();
    if (fq.isEmpty()) {
      throw new IllegalArgumentException(
          "Fully qualified name must contain a catalog and namespace");
    }

    List<String> segs = splitPathRespectingQuotesAndEscapes(fq);

    if (segs.size() < 3) {
      throw new IllegalArgumentException(
          "Invalid table path: at least a catalog, one namespace, and a table name are required "
              + "(e.g. catalog.namespace.table)");
    }

    String catalog = Quotes.unquote(segs.get(0));
    String object = Quotes.unquote(segs.get(segs.size() - 1));
    List<String> ns = segs.subList(1, segs.size() - 1).stream().map(Quotes::unquote).toList();

    return NameRef.newBuilder().setCatalog(catalog).addAllPath(ns).setName(object).build();
  }

  private NameRef nameRefForNamespace(String fqNs, boolean includeLeafInPath) {
    if (fqNs == null) {
      throw new IllegalArgumentException("Fully qualified name is required");
    }

    fqNs = fqNs.trim();
    if (fqNs.isEmpty()) {
      throw new IllegalArgumentException("Namespace path is empty");
    }

    List<String> segs = splitPathRespectingQuotesAndEscapes(fqNs);
    if (segs.size() < 2) {
      throw new IllegalArgumentException(
          "Invalid namespace path: at least a catalog and one namespace are required "
              + "(e.g. catalog.namespace)");
    }

    String catalog = Quotes.unquote(segs.get(0));
    List<String> path = segs.subList(1, segs.size()).stream().map(Quotes::unquote).toList();

    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog);
    if (includeLeafInPath) {
      b.addAllPath(path);
    } else {
      b.addAllPath(path.subList(0, path.size() - 1)).setName(path.get(path.size() - 1));
    }
    return b.build();
  }

  private static NameRef nameRefForTablePrefix(String s) {
    if (s == null) {
      throw new IllegalArgumentException("Fully qualified name is required");
    }

    s = s.trim();
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Namespace path is empty");
    }

    List<String> segs = splitPathRespectingQuotesAndEscapes(s);
    if (segs.size() < 2) {
      throw new IllegalArgumentException(
          "Invalid namespace path: at least a catalog and one namespace are required "
              + "(e.g. catalog.namespace)");
    }

    String catalog = Quotes.unquote(segs.get(0));
    List<String> rest = segs.subList(1, segs.size()).stream().map(Quotes::unquote).toList();

    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog);

    if (rest.size() >= 2) {
      b.addAllPath(rest.subList(0, rest.size() - 1));
      b.setName(rest.get(rest.size() - 1));
    } else {
      b.addAllPath(rest);
    }

    return b.build();
  }

  private Precondition preconditionFromEtag(List<String> args) {
    String etag = parseStringFlag(args, "--etag", "");
    if (etag == null || etag.isBlank()) {
      return null;
    }
    return Precondition.newBuilder().setExpectedEtag(etag).build();
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
    boolean inSingle = false;
    boolean inDouble = false;
    boolean escaping = false;

    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);

      if (escaping) {
        cur.append('\\').append(ch);
        escaping = false;
        continue;
      }

      if (ch == '\\') {
        escaping = true;
        continue;
      }

      if (ch == '\'' && !inDouble) {
        inSingle = !inSingle;
        cur.append(ch);
        continue;
      }

      if (ch == '"' && !inSingle) {
        inDouble = !inDouble;
        cur.append(ch);
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

    if (escaping) {
      cur.append('\\');
    }
    if (cur.length() > 0) {
      out.add(cur.toString());
    }
    return out;
  }

  private void ensureAccountSet() {
    if (currentAccountId == null || currentAccountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
  }

  private static List<String> splitPathRespectingQuotesAndEscapes(String input) {
    return FQNameParserUtil.segments(input);
  }

  private NamespacePath toNsPath(String path) {
    return NamespacePath.newBuilder()
        .addAllSegments(splitPathRespectingQuotesAndEscapes(path))
        .build();
  }

  private <T> T parseFlag(List<String> args, String flag, T dflt, Function<String, T> fn) {
    int i = args.indexOf(flag);
    if (i >= 0 && i + 1 < args.size()) {
      try {
        return fn.apply(args.get(i + 1));
      } catch (Exception ignore) {
      }
    }
    return dflt;
  }

  private String parseStringFlag(List<String> a, String f, String d) {
    return parseFlag(a, f, d, s -> s);
  }

  private int parseIntFlag(List<String> a, String f, int d) {
    return parseFlag(a, f, d, Integer::parseInt);
  }

  private long parseLongFlag(List<String> a, String f, long d) {
    return parseFlag(a, f, d, Long::parseLong);
  }

  private <T, R> List<T> collectPages(
      int pageSize,
      Function<PageRequest, R> fetch,
      Function<R, List<T>> items,
      Function<R, String> next) {

    List<T> all = new ArrayList<>();
    String token = "";
    do {
      R resp =
          fetch.apply(PageRequest.newBuilder().setPageSize(pageSize).setPageToken(token).build());
      all.addAll(items.apply(resp));
      token = Optional.ofNullable(next.apply(resp)).orElse("");
    } while (!token.isBlank());
    return all;
  }

  private boolean looksLikeQuotedOrRawUuid(String s) {
    return looksLikeUuid(Quotes.unquote(nvl(s, "")));
  }

  private ResourceId resolveNamespaceIdFlexible(String tok) {
    String u = Quotes.unquote(nvl(tok, ""));
    if (looksLikeUuid(u)) {
      return namespaceRid(u);
    }

    NameRef ref = nameRefForNamespace(tok, false);
    return directory
        .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private ResourceId resolveTableIdFlexible(String tok) {
    String u = Quotes.unquote(nvl(tok, ""));
    if (looksLikeUuid(u)) {
      return tableRid(u);
    }

    NameRef ref = nameRefForTable(tok);
    return directory
        .resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private ResourceId resolveViewIdFlexible(String tok) {
    String u = Quotes.unquote(nvl(tok, ""));
    if (looksLikeUuid(u)) {
      return viewRid(u);
    }
    NameRef ref = nameRefForTable(tok);
    return directory
        .resolveView(ResolveViewRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private final class AuthHeaderInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, io.grpc.Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<>(
          next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          String token = authToken == null ? "" : authToken.trim();
          if (!token.isBlank()) {
            String headerValue = token;
            if (!token.regionMatches(true, 0, "bearer ", 0, 7)) {
              headerValue = "Bearer " + token;
            }
            headers.put(
                Metadata.Key.of(authHeaderName, Metadata.ASCII_STRING_MARSHALLER), headerValue);
          }

          String session = sessionToken == null ? "" : sessionToken.trim();
          if (!session.isBlank()) {
            headers.put(
                Metadata.Key.of(sessionHeaderName, Metadata.ASCII_STRING_MARSHALLER), session);
          }

          super.start(responseListener, headers);
        }
      };
    }
  }

  static final class Quotes {
    static boolean needsQuotes(String s) {
      if (s == null || s.isEmpty()) {
        return true;
      }

      for (int i = 0; i < s.length(); i++) {
        char c = s.charAt(i);
        if (Character.isWhitespace(c) || c == '.' || c == '"' || c == '\'' || c == '\\')
          return true;
      }
      return false;
    }

    static String quoteIfNeeded(String s) {
      if (s == null) {
        return null;
      }

      if (!needsQuotes(s)) {
        return s;
      }

      return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    static String unquote(String s) {
      if (s == null) {
        return null;
      }

      s = s.trim();
      if (s.isEmpty()) {
        return s;
      }

      if (!((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'")))) {
        return s;
      }

      char q = s.charAt(0);
      String body = s.substring(1, s.length() - 1);
      StringBuilder out = new StringBuilder(body.length());
      for (int i = 0; i < body.length(); i++) {
        char c = body.charAt(i);
        if (c == '\\' && i + 1 < body.length()) {
          char n = body.charAt(++i);
          if (n == '\\' || n == q) {
            out.append(n);
            continue;
          }
          out.append('\\').append(n);
          continue;
        }
        out.append(c);
      }
      return out.toString();
    }
  }
}
