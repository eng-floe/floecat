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
import ai.floedb.floecat.catalog.rpc.LookupCatalogRequest;
import ai.floedb.floecat.catalog.rpc.LookupNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.LookupTableRequest;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.ReconcileMode;
import ai.floedb.floecat.connector.rpc.ReconcilePolicy;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.floecat.reconciler.rpc.CancelReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureMode;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcileJobResponse;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.GetReconcilerSettingsResponse;
import ai.floedb.floecat.reconciler.rpc.JobState;
import ai.floedb.floecat.reconciler.rpc.ListReconcileJobsRequest;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsRequest;
import ai.floedb.floecat.reconciler.rpc.UpdateReconcilerSettingsResponse;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** CLI support for the {@code connectors} and {@code connector} commands. */
final class ConnectorCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private ConnectorCliSupport() {}

  /**
   * Dispatches {@code connectors} and {@code connector} subcommands.
   *
   * @param command the top-level command token ("connectors" or "connector")
   * @param args tokens after the command
   * @param out output stream
   * @param connectors gRPC connectors stub
   * @param reconcileControl gRPC reconcile-control stub
   * @param directory gRPC directory stub (used for display lookups in output)
   * @param getCurrentAccountId returns the currently selected account ID
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      ConnectorsGrpc.ConnectorsBlockingStub connectors,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if ("connectors".equals(command)) {
      var all = listAllConnectors(null, DEFAULT_PAGE_SIZE, connectors);
      printConnectors(all, out, directory);
    } else {
      connectorCrud(args, out, connectors, reconcileControl, directory, getCurrentAccountId);
    }
  }

  // --- connector CRUD ---

  private static void connectorCrud(
      List<String> args,
      PrintStream out,
      ConnectorsGrpc.ConnectorsBlockingStub connectors,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println(
          "usage: connector"
              + " <list|get|create|update|delete|validate|trigger|job|jobs|cancel|settings> ...");
      return;
    }
    String sub = args.get(0);

    switch (sub) {
      case "list" -> {
        String kindStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--kind", ""));
        int pageSize = CliArgs.parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);
        ConnectorKind filter = parseConnectorKind(kindStr);
        filter = (filter == ConnectorKind.CK_UNSPECIFIED && kindStr.isBlank()) ? null : filter;
        var all = listAllConnectors(filter, pageSize, connectors);
        printConnectors(all, out, directory);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: connector get <display_name|id>");
          return;
        }
        var resp =
            connectors.getConnector(
                GetConnectorRequest.newBuilder()
                    .setConnectorId(
                        resolveConnectorId(
                            Quotes.unquote(args.get(1)), connectors, getCurrentAccountId))
                    .build());
        printConnectors(List.of(resp.getConnector()), out, directory);
      }
      case "create" -> {
        if (args.size() < 6) {
          out.println(
              "usage: connector create <display_name> <kind (ICEBERG|DELTA|GLUE|UNITY)> <uri>"
                  + " <source_namespace (a[.b[.c]...])> <destination_catalog (name)>"
                  + " [--source-table <name>] [--source-cols c1,#id2,...] [--dest-ns <a.b[.c]>]"
                  + " [--dest-table <name>] [--desc <text>] [--auth-scheme <scheme>] [--auth k=v"
                  + " ...] [--head k=v ...] [--cred-type <type>] [--cred k=v ...]"
                  + " [--cred-head k=v ...] [--policy-enabled] (if provided,"
                  + " policy.enabled=true) [--policy-interval-sec <n>] [--policy-mode"
                  + " incremental|full] [--policy-max-par <n>]"
                  + " [--policy-not-before-epoch <sec>] [--props k=v ...]  (e.g."
                  + " stats.ndv.enabled=false,stats.ndv.sample_fraction=0.1)");
          return;
        }

        String display = Quotes.unquote(args.get(1));
        ConnectorKind kind = parseConnectorKind(Quotes.unquote(args.get(2)));
        String uri = Quotes.unquote(args.get(3));
        String sourceNamespace = Quotes.unquote(args.get(4));
        String destCatalog = Quotes.unquote(args.get(5));

        String sourceTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--source-cols", "")));
        String destNamespace = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-table", ""));
        if (sourceTable.isBlank() && !destTable.isBlank()) {
          sourceTable = destTable;
        } else if (!sourceTable.isBlank() && destTable.isBlank()) {
          destTable = sourceTable;
        }

        String description = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));
        String authScheme = Quotes.unquote(CliArgs.parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String credType = Quotes.unquote(CliArgs.parseStringFlag(args, "--cred-type", ""));
        Map<String, String> credProps = parseKeyValueList(args, "--cred");
        Map<String, String> credHeaders = parseKeyValueList(args, "--cred-head");

        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = CliArgs.parseLongFlag(args, "--policy-interval-sec", 0L);
        ReconcileMode policyMode =
            parseReconcileMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--policy-mode", "")));
        int maxPar = CliArgs.parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = CliArgs.parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var credentials = AuthCredentialParser.buildCredentials(credType, credProps, credHeaders);
        var auth = buildAuth(authScheme, authProps, headerHints, credentials);
        var policy = buildPolicy(policyEnabled, intervalSec, policyMode, maxPar, notBeforeSec);

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
        printConnectors(List.of(resp.getConnector()), out, directory);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: connector update <display_name|id> [--display <name>] [--kind <kind>]"
                  + " [--uri <uri>] [--source-ns <a.b[.c]>] [--source-table <name>]"
                  + " [--source-cols c1,#id2,...] [--dest-catalog <name>] [--dest-ns <a.b[.c]>]"
                  + " [--dest-table <name>] [--desc <text>] [--auth-scheme <scheme>] [--auth k=v"
                  + " ...] [--head k=v ...] [--cred-type <type>] [--cred k=v ...]"
                  + " [--cred-head k=v ...] [--policy-enabled true|false]"
                  + " [--policy-interval-sec <n>] [--policy-mode incremental|full]"
                  + " [--policy-max-par <n>] [--policy-not-before-epoch <sec>] [--props k=v ...]"
                  + " [--etag <etag>]");
          return;
        }

        ResourceId connectorId =
            resolveConnectorId(Quotes.unquote(args.get(1)), connectors, getCurrentAccountId);

        String display = Quotes.unquote(CliArgs.parseStringFlag(args, "--display", ""));
        String kindStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--kind", ""));
        String uri = Quotes.unquote(CliArgs.parseStringFlag(args, "--uri", ""));
        String description = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));

        String sourceNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--source-ns", ""));
        String sourceTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--source-cols", "")));

        String destCatalog = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-catalog", ""));
        String destNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-table", ""));
        if (sourceTable.isBlank() && !destTable.isBlank()) {
          sourceTable = destTable;
        } else if (!sourceTable.isBlank() && destTable.isBlank()) {
          destTable = sourceTable;
        }

        String authScheme = Quotes.unquote(CliArgs.parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String credType = Quotes.unquote(CliArgs.parseStringFlag(args, "--cred-type", ""));
        Map<String, String> credProps = parseKeyValueList(args, "--cred");
        Map<String, String> credHeaders = parseKeyValueList(args, "--cred-head");
        String policyEnabledStr = CliArgs.parseStringFlag(args, "--policy-enabled", "");
        long intervalSec = CliArgs.parseLongFlag(args, "--policy-interval-sec", 0L);
        ReconcileMode policyMode =
            parseReconcileMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--policy-mode", "")));
        int maxPar = CliArgs.parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = CliArgs.parseLongFlag(args, "--policy-not-before-epoch", 0L);
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

        var credentials = AuthCredentialParser.buildCredentials(credType, credProps, credHeaders);
        boolean authSet =
            !authScheme.isBlank()
                || !authProps.isEmpty()
                || !headerHints.isEmpty()
                || credentials != null;
        if (authSet) {
          var ab = buildAuth(authScheme, authProps, headerHints, credentials);
          spec.setAuth(ab);
          if (!authScheme.isBlank()) mask.add("auth.scheme");
          if (!authProps.isEmpty()) mask.add("auth.properties");
          if (!headerHints.isEmpty()) mask.add("auth.header_hints");
          if (credentials != null) mask.add("auth.credentials");
        }

        boolean policySet =
            !policyEnabledStr.isBlank()
                || intervalSec != 0L
                || policyMode != ReconcileMode.RM_UNSPECIFIED
                || maxPar != 0
                || notBeforeSec != 0L;
        if (policySet) {
          var pb =
              buildPolicy(
                  !policyEnabledStr.isBlank() && Boolean.parseBoolean(policyEnabledStr),
                  intervalSec,
                  policyMode,
                  maxPar,
                  notBeforeSec);
          spec.setPolicy(pb);
          if (!policyEnabledStr.isBlank()) mask.add("policy.enabled");
          if (intervalSec != 0L) mask.add("policy.interval");
          if (policyMode != ReconcileMode.RM_UNSPECIFIED) mask.add("policy.mode");
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

        var resp = connectors.updateConnector(updateConnectorBuilder.build());
        printConnectors(List.of(resp.getConnector()), out, directory);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: connector delete <display_name|id> [--etag <etag>]");
          return;
        }
        var deleteConnectorBuilder =
            DeleteConnectorRequest.newBuilder()
                .setConnectorId(
                    resolveConnectorId(
                        Quotes.unquote(args.get(1)), connectors, getCurrentAccountId));
        var deleteConnectorPrecondition = preconditionFromEtag(args);
        if (deleteConnectorPrecondition != null) {
          deleteConnectorBuilder.setPrecondition(deleteConnectorPrecondition);
        }
        connectors.deleteConnector(deleteConnectorBuilder.build());
        out.println("ok");
      }
      case "validate" -> {
        if (args.size() < 3) {
          out.println(
              "usage: connector validate <kind> <uri>"
                  + " [--auth-scheme <scheme>] [--auth k=v ...] [--head k=v ...]"
                  + " [--cred-type <type>] [--cred k=v ...] [--cred-head k=v ...]"
                  + " [--source-ns <a.b[.c]>] [--source-table <name>] [--source-cols c1,#id2,...]"
                  + " [--dest-catalog <name>] [--dest-ns <a.b[.c]>] [--dest-table <name>]"
                  + " [--policy-enabled] [--policy-interval-sec <n>] [--policy-mode"
                  + " incremental|full] [--policy-max-par <n>] [--policy-not-before-epoch <sec>]"
                  + " [--props k=v ...]");
          return;
        }

        ConnectorKind kind = parseConnectorKind(Quotes.unquote(args.get(1)));
        String uri = Quotes.unquote(args.get(2));

        String sourceNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--source-ns", ""));
        String sourceTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--source-table", ""));
        List<String> sourceCols =
            csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--source-cols", "")));

        String destCatalog = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-catalog", ""));
        String destNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-table", ""));

        String authScheme = Quotes.unquote(CliArgs.parseStringFlag(args, "--auth-scheme", ""));
        Map<String, String> authProps = parseKeyValueList(args, "--auth");
        Map<String, String> headerHints = parseKeyValueList(args, "--head");
        String credType = Quotes.unquote(CliArgs.parseStringFlag(args, "--cred-type", ""));
        Map<String, String> credProps = parseKeyValueList(args, "--cred");
        Map<String, String> credHeaders = parseKeyValueList(args, "--cred-head");
        boolean policyEnabled = args.contains("--policy-enabled");
        long intervalSec = CliArgs.parseLongFlag(args, "--policy-interval-sec", 0L);
        ReconcileMode policyMode =
            parseReconcileMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--policy-mode", "")));
        int maxPar = CliArgs.parseIntFlag(args, "--policy-max-par", 0);
        long notBeforeSec = CliArgs.parseLongFlag(args, "--policy-not-before-epoch", 0L);
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var credentials = AuthCredentialParser.buildCredentials(credType, credProps, credHeaders);
        var auth = buildAuth(authScheme, authProps, headerHints, credentials);

        var spec =
            ConnectorSpec.newBuilder()
                .setDisplayName("")
                .setKind(kind)
                .setUri(uri)
                .putAllProperties(properties)
                .setAuth(auth);
        boolean policySet =
            policyEnabled
                || intervalSec > 0L
                || policyMode != ReconcileMode.RM_UNSPECIFIED
                || maxPar > 0
                || notBeforeSec > 0L;
        if (policySet) {
          spec.setPolicy(buildPolicy(policyEnabled, intervalSec, policyMode, maxPar, notBeforeSec));
        }

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
          out.println(
              "usage: connector trigger <display_name|id> [--full]"
                  + " [--mode metadata-only|metadata-and-stats|stats-only]"
                  + " [--dest-ns <a.b[.c]>] [--dest-table <name>] [--dest-cols c1,#id2,...]"
                  + " [--snapshot-ids id1,id2,...]");
          return;
        }
        boolean full = args.contains("--full");
        CaptureMode mode =
            parseCaptureMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--mode", "")));
        String destNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-ns", ""));
        String destTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-table", ""));
        List<String> destColumns =
            csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--dest-cols", "")));
        List<Long> snapshotIds =
            parseSnapshotIds(Quotes.unquote(CliArgs.parseStringFlag(args, "--snapshot-ids", "")));
        ResourceId connectorId =
            resolveConnectorId(Quotes.unquote(args.get(1)), connectors, getCurrentAccountId);
        CaptureScope.Builder scope = CaptureScope.newBuilder().setConnectorId(connectorId);
        if (!destNs.isBlank()) {
          scope.addDestinationNamespacePaths(toNsPath(destNs));
        }
        if (!destTable.isBlank()) {
          scope.setDestinationTableDisplayName(destTable);
        }
        if (!destColumns.isEmpty()) {
          scope.addAllDestinationTableColumns(destColumns);
        }
        if (!snapshotIds.isEmpty()) {
          scope.addAllDestinationSnapshotIds(snapshotIds);
        }
        var resp =
            reconcileControl.startCapture(
                StartCaptureRequest.newBuilder()
                    .setScope(scope.build())
                    .setMode(mode)
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
            reconcileControl.getReconcileJob(
                GetReconcileJobRequest.newBuilder().setJobId(Quotes.unquote(args.get(1))).build());
        printReconcileJob(resp, out);
      }
      case "jobs" -> {
        int pageSize = CliArgs.parseIntFlag(args, "--page-size", DEFAULT_PAGE_SIZE);
        String connectorRef = Quotes.unquote(CliArgs.parseStringFlag(args, "--connector", ""));
        String stateArg = Quotes.unquote(CliArgs.parseStringFlag(args, "--state", ""));
        var req = ListReconcileJobsRequest.newBuilder();
        req.setPage(
            ai.floedb.floecat.common.rpc.PageRequest.newBuilder().setPageSize(pageSize).build());
        if (!connectorRef.isBlank()) {
          req.setConnectorId(
              rid(resolveConnectorId(connectorRef, connectors, getCurrentAccountId)));
        }
        for (String token : csvList(stateArg)) {
          JobState state = parseJobState(token);
          if (state != JobState.JS_UNSPECIFIED) {
            req.addStates(state);
          }
        }
        var resp = reconcileControl.listReconcileJobs(req.build());
        if (resp.getJobsList().isEmpty()) {
          out.println("no reconcile jobs");
          return;
        }
        for (GetReconcileJobResponse job : resp.getJobsList()) {
          printReconcileJobSummary(job, out);
        }
        if (resp.hasPage() && !resp.getPage().getNextPageToken().isBlank()) {
          out.println("next_page_token=" + resp.getPage().getNextPageToken());
        }
      }
      case "cancel" -> {
        if (args.size() < 2) {
          out.println("usage: connector cancel <jobId> [--reason <text>]");
          return;
        }
        String reason = Quotes.unquote(CliArgs.parseStringFlag(args, "--reason", ""));
        var resp =
            reconcileControl.cancelReconcileJob(
                CancelReconcileJobRequest.newBuilder()
                    .setJobId(Quotes.unquote(args.get(1)))
                    .setReason(reason)
                    .build());
        out.println("cancelled=" + resp.getCancelled());
        if (resp.hasJob()) {
          printReconcileJob(resp.getJob(), out);
        }
      }
      case "settings" -> reconcilerSettings(args.subList(1, args.size()), out, reconcileControl);
      default -> out.println("unknown subcommand");
    }
  }

  // --- reconciler settings ---

  private static void reconcilerSettings(
      List<String> args,
      PrintStream out,
      ReconcileControlGrpc.ReconcileControlBlockingStub reconcileControl) {
    if (args.isEmpty()) {
      out.println(
          "usage: connector settings <get|update> [--auto-enabled true|false]"
              + " [--default-interval-sec <n>] [--default-mode incremental|full]"
              + " [--finished-job-retention-sec <n>]");
      return;
    }
    switch (args.get(0)) {
      case "get" ->
          printReconcilerSettings(
              reconcileControl.getReconcilerSettings(
                  GetReconcilerSettingsRequest.newBuilder().build()),
              out);
      case "update" -> {
        String autoEnabled = CliArgs.parseStringFlag(args, "--auto-enabled", "");
        long defaultIntervalSec = CliArgs.parseLongFlag(args, "--default-interval-sec", 0L);
        ReconcileMode defaultMode =
            parseReconcileMode(Quotes.unquote(CliArgs.parseStringFlag(args, "--default-mode", "")));
        long finishedJobRetentionSec =
            CliArgs.parseLongFlag(args, "--finished-job-retention-sec", 0L);

        var req = UpdateReconcilerSettingsRequest.newBuilder();
        if (!autoEnabled.isBlank()) {
          req.setAutoEnabled(Boolean.parseBoolean(autoEnabled));
        }
        if (defaultIntervalSec > 0L) {
          req.setDefaultInterval(durSeconds(defaultIntervalSec));
        }
        if (defaultMode != ReconcileMode.RM_UNSPECIFIED) {
          req.setDefaultMode(defaultMode);
        }
        if (finishedJobRetentionSec > 0L) {
          req.setFinishedJobRetention(durSeconds(finishedJobRetentionSec));
        }
        if (!req.hasAutoEnabled()
            && !req.hasDefaultInterval()
            && !req.hasDefaultMode()
            && !req.hasFinishedJobRetention()) {
          out.println("Nothing to update. Provide one or more flags to change.");
          return;
        }
        printReconcilerSettings(reconcileControl.updateReconcilerSettings(req.build()), out);
      }
      default -> out.println("usage: connector settings <get|update> ...");
    }
  }

  // --- resolution helpers ---

  /**
   * Resolves a connector token (UUID or display name) to a {@link ResourceId}. Package-private so
   * Shell can pass it as a callback to {@link TableCliSupport}.
   */
  static ResourceId resolveConnectorId(
      String token,
      ConnectorsGrpc.ConnectorsBlockingStub connectors,
      Supplier<String> getCurrentAccountId) {
    String t = Quotes.unquote(token);
    if (looksLikeUuid(t)) {
      return rid(t, ResourceKind.RK_CONNECTOR, getCurrentAccountId);
    }

    var all = listAllConnectors(null, DEFAULT_PAGE_SIZE, connectors);

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

  private static List<Connector> listAllConnectors(
      ConnectorKind kind, int pageSize, ConnectorsGrpc.ConnectorsBlockingStub connectors) {
    List<Connector> all =
        CliArgs.collectPages(
            pageSize,
            pr -> connectors.listConnectors(ListConnectorsRequest.newBuilder().setPage(pr).build()),
            r -> r.getConnectorsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    return (kind == null) ? all : all.stream().filter(c -> c.getKind() == kind).toList();
  }

  private static ResourceId rid(
      String id, ResourceKind kind, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder().setAccountId(accountId).setKind(kind).setId(id).build();
  }

  private static String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }

  private static boolean looksLikeUuid(String s) {
    if (s == null) return false;
    return s.trim()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  // --- spec builders ---

  private static AuthConfig buildAuth(
      String scheme, Map<String, String> props, Map<String, String> heads, AuthCredentials creds) {
    var b =
        AuthConfig.newBuilder()
            .setScheme(scheme == null ? "" : scheme)
            .putAllProperties(props)
            .putAllHeaderHints(heads);
    if (creds != null) {
      b.setCredentials(creds);
    }
    return b.build();
  }

  private static ReconcilePolicy buildPolicy(
      boolean enabled, long intervalSec, ReconcileMode mode, int maxPar, long notBeforeSec) {
    ReconcilePolicy.Builder b = ReconcilePolicy.newBuilder().setEnabled(enabled);
    if (maxPar > 0) b.setMaxParallel(maxPar);
    if (intervalSec > 0) b.setInterval(durSeconds(intervalSec));
    if (mode != null && mode != ReconcileMode.RM_UNSPECIFIED) b.setMode(mode);
    if (notBeforeSec > 0) b.setNotBefore(Timestamp.newBuilder().setSeconds(notBeforeSec).build());
    return b.build();
  }

  private static SourceSelector buildSource(String ns, String table, List<String> cols) {
    var b = SourceSelector.newBuilder();
    if (ns != null && !ns.isBlank()) b.setNamespace(toNsPath(ns));
    if (table != null && !table.isBlank()) b.setTable(table);
    if (cols != null && !cols.isEmpty()) b.addAllColumns(cols);
    return b.build();
  }

  private static DestinationTarget buildDest(String cat, String ns, String table) {
    var b = DestinationTarget.newBuilder();
    if (cat != null && !cat.isBlank()) b.setCatalogDisplayName(cat);
    if (ns != null && !ns.isBlank()) b.setNamespace(toNsPath(ns));
    if (table != null && !table.isBlank()) b.setTableDisplayName(table);
    return b.build();
  }

  private static NamespacePath toNsPath(String path) {
    return NamespacePath.newBuilder()
        .addAllSegments(ai.floedb.floecat.client.cli.util.FQNameParserUtil.segments(path))
        .build();
  }

  // --- output helpers ---

  private static void printConnectors(
      List<Connector> list,
      PrintStream out,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory) {
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
        boolean hasCredentials =
            a.hasCredentials()
                && a.getCredentials().getCredentialCase()
                    != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET;
        boolean anyA = (a.getScheme() != null && !a.getScheme().isBlank()) || hasCredentials;
        if (anyA) {
          out.println(
              "  auth:"
                  + (a.getScheme().isBlank() ? "" : " scheme=" + a.getScheme())
                  + (hasCredentials ? " credentials=present" : ""));
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

  private static void printReconcileJobSummary(GetReconcileJobResponse job, PrintStream out) {
    out.printf(
        "job_id=%s connector_id=%s state=%s mode=%s duration_ms=%d scanned=%d changed=%d"
            + " snapshots=%d stats=%d errors=%d%n",
        job.getJobId(),
        job.getConnectorId(),
        job.getState().name(),
        job.getFullRescan() ? "full" : "incremental",
        job.getDurationMs(),
        job.getTablesScanned(),
        job.getTablesChanged(),
        job.getSnapshotsProcessed(),
        job.getStatsProcessed(),
        job.getErrors());
    if (job.getMessage() != null && !job.getMessage().isBlank()) {
      out.println("message: " + job.getMessage());
    }
  }

  private static void printReconcileJob(GetReconcileJobResponse job, PrintStream out) {
    out.printf(
        "job_id=%s connector_id=%s state=%s mode=%s started=%s finished=%s duration_ms=%d"
            + " scanned=%d changed=%d snapshots=%d stats=%d errors=%d%n",
        job.getJobId(),
        job.getConnectorId(),
        job.getState().name(),
        job.getFullRescan() ? "full" : "incremental",
        ts(job.getStartedAt()),
        ts(job.getFinishedAt()),
        job.getDurationMs(),
        job.getTablesScanned(),
        job.getTablesChanged(),
        job.getSnapshotsProcessed(),
        job.getStatsProcessed(),
        job.getErrors());
    if (job.getMessage() != null && !job.getMessage().isBlank()) {
      var lines = splitErrorLines(job.getMessage());
      if (lines.isEmpty()) {
        out.println("message: " + job.getMessage());
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

  private static void printReconcilerSettings(
      GetReconcilerSettingsResponse settings, PrintStream out) {
    out.printf(
        "auto_enabled=%s default_interval_sec=%d default_mode=%s finished_job_retention_sec=%d%n",
        settings.getAutoEnabled(),
        durationSeconds(settings.getDefaultInterval()),
        settings.getDefaultMode().name(),
        durationSeconds(settings.getFinishedJobRetention()));
  }

  private static void printReconcilerSettings(
      UpdateReconcilerSettingsResponse settings, PrintStream out) {
    out.printf(
        "auto_enabled=%s default_interval_sec=%d default_mode=%s finished_job_retention_sec=%d%n",
        settings.getAutoEnabled(),
        durationSeconds(settings.getDefaultInterval()),
        settings.getDefaultMode().name(),
        durationSeconds(settings.getFinishedJobRetention()));
  }

  // --- parse helpers ---

  static ConnectorKind parseConnectorKind(String s) {
    if (s == null || s.isBlank()) return ConnectorKind.CK_UNSPECIFIED;
    return switch (s.trim().toUpperCase(Locale.ROOT)) {
      case "ICEBERG" -> ConnectorKind.CK_ICEBERG;
      case "DELTA" -> ConnectorKind.CK_DELTA;
      case "GLUE" -> ConnectorKind.CK_GLUE;
      case "UNITY" -> ConnectorKind.CK_UNITY;
      default -> ConnectorKind.CK_UNSPECIFIED;
    };
  }

  private static ReconcileMode parseReconcileMode(String s) {
    if (s == null || s.isBlank()) return ReconcileMode.RM_UNSPECIFIED;
    return switch (s.trim().toUpperCase(Locale.ROOT)) {
      case "INCREMENTAL", "RM_INCREMENTAL" -> ReconcileMode.RM_INCREMENTAL;
      case "FULL", "RM_FULL" -> ReconcileMode.RM_FULL;
      default -> ReconcileMode.RM_UNSPECIFIED;
    };
  }

  private static CaptureMode parseCaptureMode(String s) {
    if (s == null || s.isBlank()) return CaptureMode.CM_METADATA_AND_STATS;
    return switch (s.trim().toUpperCase(Locale.ROOT).replace('-', '_')) {
      case "METADATA_ONLY", "CM_METADATA_ONLY" -> CaptureMode.CM_METADATA_ONLY;
      case "METADATA_AND_STATS", "CM_METADATA_AND_STATS" -> CaptureMode.CM_METADATA_AND_STATS;
      case "STATS_ONLY", "CM_STATS_ONLY" -> CaptureMode.CM_STATS_ONLY;
      default -> throw new IllegalArgumentException("invalid capture mode: " + s);
    };
  }

  private static JobState parseJobState(String s) {
    if (s == null || s.isBlank()) return JobState.JS_UNSPECIFIED;
    return switch (s.trim().toUpperCase(Locale.ROOT)) {
      case "QUEUED", "JS_QUEUED" -> JobState.JS_QUEUED;
      case "RUNNING", "JS_RUNNING" -> JobState.JS_RUNNING;
      case "SUCCEEDED", "SUCCESS", "JS_SUCCEEDED" -> JobState.JS_SUCCEEDED;
      case "FAILED", "JS_FAILED" -> JobState.JS_FAILED;
      case "CANCELLING", "JS_CANCELLING" -> JobState.JS_CANCELLING;
      case "CANCELLED", "JS_CANCELLED" -> JobState.JS_CANCELLED;
      default -> JobState.JS_UNSPECIFIED;
    };
  }

  private static ai.floedb.floecat.common.rpc.Precondition preconditionFromEtag(List<String> args) {
    String etag = CliArgs.parseStringFlag(args, "--etag", "");
    if (etag == null || etag.isBlank()) return null;
    return ai.floedb.floecat.common.rpc.Precondition.newBuilder().setExpectedEtag(etag).build();
  }

  private static List<Long> parseSnapshotIds(String s) {
    if (s == null || s.isBlank()) return List.of();
    var result = new ArrayList<Long>();
    for (String token : csvList(s)) {
      result.add(Long.parseUnsignedLong(token));
    }
    return result;
  }

  private static List<String> csvList(String s) {
    try {
      return ai.floedb.floecat.client.cli.util.CsvListParserUtil.items(s).stream()
          .map(Quotes::unquote)
          .filter(t -> t != null && !t.isBlank())
          .toList();
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid CSV list: " + s, e);
    }
  }

  private static Map<String, String> parseKeyValueList(List<String> args, String flag) {
    Map<String, String> out = new LinkedHashMap<>();
    for (int i = 0; i < args.size(); i++) {
      if (flag.equals(args.get(i)) && i + 1 < args.size()) {
        int j = i + 1;
        while (j < args.size() && !args.get(j).startsWith("--")) {
          String kv = args.get(j);
          int eq = kv.indexOf('=');
          if (eq > 0) {
            out.put(kv.substring(0, eq), kv.substring(eq + 1));
          }
          j++;
        }
      }
    }
    return out;
  }

  // --- misc helpers ---

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

  private static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) return "-";
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  private static Duration durSeconds(long seconds) {
    return Duration.newBuilder().setSeconds(seconds).build();
  }

  private static long durationSeconds(Duration d) {
    return d == null ? 0L : d.getSeconds();
  }

  private static String trunc(String s, int n) {
    if (s == null) return "-";
    return s.length() <= n ? s : (s.substring(0, n - 1) + "…");
  }

  private static List<String> splitErrorLines(String msg) {
    if (msg == null) return List.of();
    String normalized = msg.replace("\r\n", "\n").replace("\r", "\n");
    if (normalized.contains("\n")) {
      var out = new ArrayList<String>();
      for (String line : normalized.split("\n")) {
        if (!line.isBlank()) out.add(stripBullet(line.trim()));
      }
      return out;
    }
    if (normalized.contains(" | ")) {
      var out = new ArrayList<String>();
      for (String part : normalized.split("\\s\\|\\s")) {
        if (!part.isBlank()) out.add(stripBullet(part.trim()));
      }
      return out;
    }
    return List.of(stripBullet(normalized.trim()));
  }

  private static String stripBullet(String line) {
    if (line.startsWith("- ")) return line.substring(2).trim();
    if (line.startsWith("* ")) return line.substring(2).trim();
    return line;
  }
}
