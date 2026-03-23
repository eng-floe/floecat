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

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableConstraintsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import java.io.PrintStream;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Standalone command executor that can be embedded in non-REPL contexts — for example, to allow
 * another application to run floecat commands against an active gRPC connection without a JLine
 * terminal. Tokenizes a command line and dispatches to the appropriate {@code *CliSupport} handler.
 * Never throws — all errors are written to {@code out}.
 *
 * <h2>Thread safety</h2>
 *
 * This class is <em>thread-safe</em>. All fields are final and set at construction; the executor
 * holds no mutable state of its own. The injected gRPC stubs are also thread-safe by contract. A
 * single instance may be shared across threads and reused across any number of {@link
 * #execute(String)} calls.
 *
 * <h2>Session state</h2>
 *
 * The executor reads session state ({@code getAccountId}, {@code getCatalog}) but delegates
 * mutation to the injected {@code setAccountId} and {@code setCatalog} callbacks. Callers control
 * the mutation policy:
 *
 * <ul>
 *   <li>Pass real consumers (e.g., {@code id -> this.accountId = id}) to allow full session
 *       management — appropriate for the interactive REPL.
 *   <li>Pass no-op consumers ({@code id -> {}}) to silently ignore state changes — appropriate when
 *       the embedding context owns the session state.
 *   <li>Pass error-throwing consumers to surface unsupported operations explicitly.
 * </ul>
 *
 * @see CliArgs#tokenize(String)
 */
public final class CliCommandExecutor {

  private final PrintStream out;
  private final AccountServiceGrpc.AccountServiceBlockingStub accounts;
  private final CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;
  private final TableServiceGrpc.TableServiceBlockingStub tables;
  private final ViewServiceGrpc.ViewServiceBlockingStub viewService;
  private final ConnectorsGrpc.ConnectorsBlockingStub connectors;
  private final ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl;
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;
  private final TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;
  private final TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService;
  private final QueryServiceGrpc.QueryServiceBlockingStub queries;
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan;
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema;
  private final Supplier<String> getAccountId;
  private final Consumer<String> setAccountId;
  private final Supplier<String> getCatalog;
  private final Consumer<String> setCatalog;

  private CliCommandExecutor(Builder builder) {
    this.out = builder.out;
    this.accounts = builder.accounts;
    this.catalogs = builder.catalogs;
    this.directory = builder.directory;
    this.namespaces = builder.namespaces;
    this.tables = builder.tables;
    this.viewService = builder.viewService;
    this.connectors = builder.connectors;
    this.reconcileControl = builder.reconcileControl;
    this.snapshots = builder.snapshots;
    this.statistics = builder.statistics;
    this.constraintsService = builder.constraintsService;
    this.queries = builder.queries;
    this.queryScan = builder.queryScan;
    this.querySchema = builder.querySchema;
    this.getAccountId = builder.getAccountId;
    this.setAccountId = builder.setAccountId;
    this.getCatalog = builder.getCatalog;
    this.setCatalog = builder.setCatalog;
  }

  /** Returns a new {@link Builder}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Convenience factory: creates all gRPC stubs from a single channel and builds the executor with
   * default session state (empty account id and catalog, no-op setters). Suitable for embedding
   * contexts that manage session state externally.
   *
   * <pre>{@code
   * ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
   * CliCommandExecutor executor = CliCommandExecutor.fromChannel(channel, System.out);
   * executor.execute("catalogs");
   * }</pre>
   *
   * @param channel the gRPC channel to use for all service stubs
   * @param out the output stream for command results and errors
   */
  public static CliCommandExecutor fromChannel(io.grpc.Channel channel, PrintStream out) {
    return builder()
        .out(out)
        .accounts(AccountServiceGrpc.newBlockingStub(channel))
        .catalogs(CatalogServiceGrpc.newBlockingStub(channel))
        .directory(DirectoryServiceGrpc.newBlockingStub(channel))
        .namespaces(NamespaceServiceGrpc.newBlockingStub(channel))
        .tables(TableServiceGrpc.newBlockingStub(channel))
        .viewService(ViewServiceGrpc.newBlockingStub(channel))
        .connectors(ConnectorsGrpc.newBlockingStub(channel))
        .reconcileControl(ReconcileControlGrpc.newBlockingStub(channel))
        .snapshots(SnapshotServiceGrpc.newBlockingStub(channel))
        .statistics(TableStatisticsServiceGrpc.newBlockingStub(channel))
        .constraintsService(TableConstraintsServiceGrpc.newBlockingStub(channel))
        .queries(QueryServiceGrpc.newBlockingStub(channel))
        .queryScan(QueryScanServiceGrpc.newBlockingStub(channel))
        .querySchema(QuerySchemaServiceGrpc.newBlockingStub(channel))
        .build();
  }

  /**
   * Fluent builder for {@link CliCommandExecutor}.
   *
   * <p>All gRPC stubs and the output stream are required. Session-state callbacks ({@code
   * setAccountId}, {@code setCatalog}) default to no-ops so that an embedding context can omit them
   * when it owns session state externally.
   */
  public static final class Builder {

    private PrintStream out;
    private AccountServiceGrpc.AccountServiceBlockingStub accounts;
    private CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;
    private DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
    private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;
    private TableServiceGrpc.TableServiceBlockingStub tables;
    private ViewServiceGrpc.ViewServiceBlockingStub viewService;
    private ConnectorsGrpc.ConnectorsBlockingStub connectors;
    private ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl;
    private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;
    private TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;
    private TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService;
    private QueryServiceGrpc.QueryServiceBlockingStub queries;
    private QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan;
    private QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema;
    private Supplier<String> getAccountId = () -> "";
    private Consumer<String> setAccountId = id -> {};
    private Supplier<String> getCatalog = () -> "";
    private Consumer<String> setCatalog = name -> {};

    private Builder() {}

    public Builder out(PrintStream out) {
      this.out = out;
      return this;
    }

    public Builder accounts(AccountServiceGrpc.AccountServiceBlockingStub accounts) {
      this.accounts = accounts;
      return this;
    }

    public Builder catalogs(CatalogServiceGrpc.CatalogServiceBlockingStub catalogs) {
      this.catalogs = catalogs;
      return this;
    }

    public Builder directory(DirectoryServiceGrpc.DirectoryServiceBlockingStub directory) {
      this.directory = directory;
      return this;
    }

    public Builder namespaces(NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces) {
      this.namespaces = namespaces;
      return this;
    }

    public Builder tables(TableServiceGrpc.TableServiceBlockingStub tables) {
      this.tables = tables;
      return this;
    }

    public Builder viewService(ViewServiceGrpc.ViewServiceBlockingStub viewService) {
      this.viewService = viewService;
      return this;
    }

    public Builder connectors(ConnectorsGrpc.ConnectorsBlockingStub connectors) {
      this.connectors = connectors;
      return this;
    }

    public Builder reconcileControl(
        ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl) {
      this.reconcileControl = reconcileControl;
      return this;
    }

    public Builder snapshots(SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots) {
      this.snapshots = snapshots;
      return this;
    }

    public Builder statistics(
        TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics) {
      this.statistics = statistics;
      return this;
    }

    public Builder constraintsService(
        TableConstraintsServiceGrpc.TableConstraintsServiceBlockingStub constraintsService) {
      this.constraintsService = constraintsService;
      return this;
    }

    public Builder queries(QueryServiceGrpc.QueryServiceBlockingStub queries) {
      this.queries = queries;
      return this;
    }

    public Builder queryScan(QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan) {
      this.queryScan = queryScan;
      return this;
    }

    public Builder querySchema(QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema) {
      this.querySchema = querySchema;
      return this;
    }

    /** Supplier for the current account id. Defaults to {@code () -> ""}. */
    public Builder getAccountId(Supplier<String> getAccountId) {
      this.getAccountId = getAccountId;
      return this;
    }

    /**
     * Callback invoked when {@code account set <id>} is executed. Defaults to a no-op — pass a real
     * consumer ({@code id -> this.accountId = id}) when full session management is needed.
     */
    public Builder setAccountId(Consumer<String> setAccountId) {
      this.setAccountId = setAccountId;
      return this;
    }

    /** Supplier for the current catalog name. Defaults to {@code () -> ""}. */
    public Builder getCatalog(Supplier<String> getCatalog) {
      this.getCatalog = getCatalog;
      return this;
    }

    /**
     * Callback invoked when {@code catalog use <name>} is executed. Defaults to a no-op — pass a
     * real consumer ({@code name -> this.catalog = name}) when full session management is needed.
     */
    public Builder setCatalog(Consumer<String> setCatalog) {
      this.setCatalog = setCatalog;
      return this;
    }

    /** Builds the executor. Throws {@link NullPointerException} if any required field is null. */
    public CliCommandExecutor build() {
      Objects.requireNonNull(out, "out");
      Objects.requireNonNull(accounts, "accounts");
      Objects.requireNonNull(catalogs, "catalogs");
      Objects.requireNonNull(directory, "directory");
      Objects.requireNonNull(namespaces, "namespaces");
      Objects.requireNonNull(tables, "tables");
      Objects.requireNonNull(viewService, "viewService");
      Objects.requireNonNull(connectors, "connectors");
      Objects.requireNonNull(reconcileControl, "reconcileControl");
      Objects.requireNonNull(snapshots, "snapshots");
      Objects.requireNonNull(statistics, "statistics");
      Objects.requireNonNull(constraintsService, "constraintsService");
      Objects.requireNonNull(queries, "queries");
      Objects.requireNonNull(queryScan, "queryScan");
      Objects.requireNonNull(querySchema, "querySchema");
      return new CliCommandExecutor(this);
    }
  }

  /**
   * Executes a single command line. Never throws; errors are written to the output stream.
   *
   * @param commandLine the raw command string (tokenized internally by {@link CliArgs#tokenize});
   *     must not be {@code null}
   * @return {@code true} if a command was dispatched and completed without error, or if the input
   *     was blank (nothing went wrong); {@code false} if the command was unknown, a gRPC error
   *     occurred, or the input could not be tokenized
   */
  public boolean execute(String commandLine) {
    Objects.requireNonNull(commandLine, "commandLine");
    List<String> tokens;
    try {
      tokens = CliArgs.tokenize(commandLine);
    } catch (IllegalArgumentException e) {
      out.println("error: " + e.getMessage());
      return false;
    }
    if (tokens.isEmpty()) {
      return true;
    }
    String command = tokens.get(0);
    try {
      return dispatch(command, tokens);
    } catch (Exception e) {
      out.println("error: " + e.getMessage());
      return false;
    }
  }

  private boolean dispatch(String command, List<String> tokens) {
    switch (command) {
      case "account" ->
          AccountCliSupport.handle(CliArgs.tail(tokens), out, accounts, getAccountId, setAccountId);
      case "catalogs", "catalog" ->
          CatalogCliSupport.handle(
              command, CliArgs.tail(tokens), out, catalogs, directory, getAccountId, setCatalog);
      case "namespaces", "namespace" ->
          NamespaceCliSupport.handle(
              command, CliArgs.tail(tokens), out, namespaces, directory, getAccountId);
      case "tables", "table", "resolve", "describe" ->
          TableCliSupport.handle(
              command,
              CliArgs.tail(tokens),
              out,
              tables,
              directory,
              getAccountId,
              tok -> ConnectorCliSupport.resolveConnectorId(tok, connectors, getAccountId));
      case "views", "view" ->
          ViewCliSupport.handle(
              command, CliArgs.tail(tokens), out, viewService, directory, getAccountId);
      case "connectors", "connector" ->
          ConnectorCliSupport.handle(
              command,
              CliArgs.tail(tokens),
              out,
              connectors,
              reconcileControl,
              directory,
              getAccountId);
      case "snapshots", "snapshot" ->
          SnapshotCliSupport.handle(
              command,
              CliArgs.tail(tokens),
              out,
              snapshots,
              tok -> TableCliSupport.resolveTableId(tok, directory, getAccountId));
      case "stats" ->
          StatsCliSupport.handle(
              "stats",
              CliArgs.tail(tokens),
              out,
              statistics,
              tables,
              namespaces,
              reconcileControl,
              tok -> TableCliSupport.resolveTableId(tok, directory, getAccountId));
      case "constraints" ->
          ConstraintsCliSupport.handle(
              CliArgs.tail(tokens),
              out,
              constraintsService,
              snapshots,
              tok -> TableCliSupport.resolveTableId(tok, directory, getAccountId),
              CliArgs::parseStringFlag,
              CliArgs::parseIntFlag,
              CliArgs::hasFlag,
              msg -> CliUtils.printJson(msg, out));
      case "analyze" ->
          StatsCliSupport.handle(
              "analyze",
              CliArgs.tail(tokens),
              out,
              statistics,
              tables,
              namespaces,
              reconcileControl,
              tok -> TableCliSupport.resolveTableId(tok, directory, getAccountId));
      case "query" ->
          QueryCliSupport.handle(
              command,
              CliArgs.tail(tokens),
              out,
              queries,
              queryScan,
              querySchema,
              directory,
              getCatalog,
              getAccountId);
      default -> {
        out.println("Unknown command. Type 'help'.");
        return false;
      }
    }
    return true;
  }
}
