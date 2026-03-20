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

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesRequest;
import ai.floedb.floecat.catalog.rpc.ResolveFQTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.client.cli.util.CsvListParserUtil;
import ai.floedb.floecat.client.cli.util.FQNameParserUtil;
import ai.floedb.floecat.client.cli.util.NameRefUtil;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * CLI support for the {@code tables}, {@code table}, {@code resolve}, and {@code describe}
 * commands.
 */
final class TableCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private TableCliSupport() {}

  /**
   * Dispatches table-related commands.
   *
   * @param command the top-level command token ("tables", "table", "resolve", or "describe")
   * @param args tokens after the command
   * @param out output stream
   * @param tables gRPC table service stub
   * @param directory gRPC directory service stub
   * @param getCurrentAccountId returns the currently selected account ID
   * @param resolveConnectorId resolves a connector name/id to a ResourceId
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      TableServiceGrpc.TableServiceBlockingStub tables,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId,
      Function<String, ResourceId> resolveConnectorId) {
    switch (command) {
      case "tables" -> tablesList(args, out, directory, getCurrentAccountId);
      case "table" ->
          tableCrud(args, out, tables, directory, getCurrentAccountId, resolveConnectorId);
      case "resolve" -> tableResolve(args, out, directory, getCurrentAccountId);
      case "describe" -> tableDescribe(args, out, tables, directory);
    }
  }

  // --- tables list ---

  private static void tablesList(
      List<String> args,
      PrintStream out,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println("usage: tables <catalog.ns[.ns...][.prefix]>");
      return;
    }
    NameRef prefix = nameRefForTablePrefix(args.get(0));
    ResolveFQTablesRequest.Builder rb =
        ResolveFQTablesRequest.newBuilder()
            .setPrefix(prefix)
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    List<ResolveFQTablesResponse.Entry> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> directory.resolveFQTables(rb.setPage(pr).build()),
            ResolveFQTablesResponse::getTablesList,
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printResolvedTables(all, out);
  }

  // --- table CRUD ---

  private static void tableCrud(
      List<String> args,
      PrintStream out,
      TableServiceGrpc.TableServiceBlockingStub tables,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId,
      Function<String, ResourceId> resolveConnectorId) {
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

        var ref = NameRefUtil.nameRefForTable(args.get(1));
        ResourceId catalogId =
            CatalogCliSupport.resolveCatalogId(ref.getCatalog(), directory, getCurrentAccountId);

        NameRef nsRef =
            NameRef.newBuilder().setCatalog(ref.getCatalog()).addAllPath(ref.getPathList()).build();
        ResourceId namespaceId =
            directory
                .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(nsRef).build())
                .getResourceId();

        String name = ref.getName();
        String desc = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));
        String root = Quotes.unquote(CliArgs.parseStringFlag(args, "--root", ""));
        String schema = Quotes.unquote(CliArgs.parseStringFlag(args, "--schema", ""));
        List<String> parts = csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--parts", "")));
        String formatStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--format", ""));
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-connector", ""));
        String upNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-ns", ""));
        String upTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-table", ""));

        var ub =
            UpstreamRef.newBuilder()
                .setFormat(parseFormat(formatStr))
                .setUri(root)
                .addAllPartitionKeys(parts);

        if (!upConnector.isBlank()) {
          ub.setConnectorId(resolveConnectorId.apply(upConnector));
        }
        if (!upNs.isBlank()) {
          ub.clearNamespacePath().addAllNamespacePath(splitPath(upNs));
        }
        if (!upTable.isBlank()) {
          ub.setTableDisplayName(upTable);
        }

        var spec =
            TableSpec.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName(name)
                .setDescription(desc)
                .setUpstream(ub.build())
                .setSchemaJson(schema)
                .putAllProperties(props)
                .build();

        var resp = tables.createTable(CreateTableRequest.newBuilder().setSpec(spec).build());
        printTable(resp.getTable(), out);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: table get <id|catalog.ns[.ns...].table>");
          return;
        }
        ResourceId tableId = resolveTableId(args.get(1), directory, getCurrentAccountId);
        var resp = tables.getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
        printTable(resp.getTable(), out);
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

        ResourceId tableId = resolveTableId(args.get(1), directory, getCurrentAccountId);

        String catalogStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--catalog", null));
        String nsStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--namespace", null));
        String name = Quotes.unquote(CliArgs.parseStringFlag(args, "--name", null));
        String desc = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
        String root = Quotes.unquote(CliArgs.parseStringFlag(args, "--root", null));
        String schema = Quotes.unquote(CliArgs.parseStringFlag(args, "--schema", null));
        List<String> parts = csvList(Quotes.unquote(CliArgs.parseStringFlag(args, "--parts", "")));
        String formatStr = Quotes.unquote(CliArgs.parseStringFlag(args, "--format", ""));
        Map<String, String> props = parseKeyValueList(args, "--props");

        String upConnector = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-connector", null));
        String upNs = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-ns", null));
        String upTable = Quotes.unquote(CliArgs.parseStringFlag(args, "--up-table", null));

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
              looksLikeUuid(catalogStr)
                  ? rid(catalogStr, ResourceKind.RK_CATALOG, getCurrentAccountId)
                  : CatalogCliSupport.resolveCatalogId(catalogStr, directory, getCurrentAccountId);
          sb.setCatalogId(cid);
          maskPaths.add("catalog_id");
        }
        if (nsStr != null && !nsStr.isBlank()) {
          ResourceId nid =
              looksLikeUuid(nsStr)
                  ? rid(nsStr, ResourceKind.RK_NAMESPACE, getCurrentAccountId)
                  : NamespaceCliSupport.resolveNamespaceIdFlexible(
                      nsStr, directory, getCurrentAccountId);
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
          ub.setConnectorId(resolveConnectorId.apply(upConnector));
          maskPaths.add("upstream.connector_id");
          touchUpstream = true;
        }
        if (upNs != null && !upNs.isBlank()) {
          ub.clearNamespacePath().addAllNamespacePath(splitPath(upNs));
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

        var updateBuilder =
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(maskPaths).build());
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          updateBuilder.setPrecondition(precondition);
        }
        var resp = tables.updateTable(updateBuilder.build());
        printTable(resp.getTable(), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: table delete <id|catalog.ns[.ns...].table> [--etag <etag>]");
          return;
        }
        ResourceId tableId = resolveTableId(args.get(1), directory, getCurrentAccountId);
        var deleteBuilder = DeleteTableRequest.newBuilder().setTableId(tableId);
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          deleteBuilder.setPrecondition(precondition);
        }
        tables.deleteTable(deleteBuilder.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  // --- resolve ---

  private static void tableResolve(
      List<String> args,
      PrintStream out,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
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
                              ResolveTableRequest.newBuilder()
                                  .setRef(NameRefUtil.nameRefForTable(s))
                                  .build())
                          .getResourceId()));
      case "view" ->
          out.println(
              "view id: "
                  + rid(
                      directory
                          .resolveView(
                              ai.floedb.floecat.catalog.rpc.ResolveViewRequest.newBuilder()
                                  .setRef(NameRefUtil.nameRefForTable(s))
                                  .build())
                          .getResourceId()));
      case "namespace" ->
          out.println(
              "namespace id: "
                  + rid(
                      directory
                          .resolveNamespace(
                              ResolveNamespaceRequest.newBuilder()
                                  .setRef(NamespaceCliSupport.nameRefForNamespace(s, false))
                                  .build())
                          .getResourceId()));
      case "catalog" ->
          out.println(
              "catalog id: "
                  + rid(
                      directory
                          .resolveCatalog(
                              ResolveCatalogRequest.newBuilder()
                                  .setRef(
                                      NameRef.newBuilder().setCatalog(Quotes.unquote(s)).build())
                                  .build())
                          .getResourceId()));
      default -> out.println("unknown kind: " + kind);
    }
  }

  // --- describe ---

  private static void tableDescribe(
      List<String> args,
      PrintStream out,
      TableServiceGrpc.TableServiceBlockingStub tables,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory) {
    if (args.size() < 2 || !"table".equals(args.get(0))) {
      out.println("usage: describe table <fq>");
      return;
    }
    String fq = args.get(1);
    var r =
        directory.resolveTable(
            ResolveTableRequest.newBuilder().setRef(NameRefUtil.nameRefForTable(fq)).build());
    var t = tables.getTable(GetTableRequest.newBuilder().setTableId(r.getResourceId()).build());
    printTable(t.getTable(), out);
  }

  // --- resolution helpers (package-private for use by Shell and other support classes) ---

  static ResourceId resolveTableId(
      String tok,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    String u = Quotes.unquote(tok == null ? "" : tok);
    if (looksLikeUuid(u)) {
      return rid(u, ResourceKind.RK_TABLE, getCurrentAccountId);
    }
    NameRef ref = NameRefUtil.nameRefForTable(tok);
    return directory
        .resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private static NameRef nameRefForTablePrefix(String s) {
    if (s == null) throw new IllegalArgumentException("Fully qualified name is required");
    s = s.trim();
    if (s.isEmpty()) throw new IllegalArgumentException("Namespace path is empty");

    List<String> segs = FQNameParserUtil.segments(s);
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

  // --- output helpers ---

  private static void printResolvedTables(
      List<ResolveFQTablesResponse.Entry> entries, PrintStream out) {
    out.printf("%-40s  %s%n", "TABLE_ID", "NAME");
    for (var e : entries) {
      String catalog = e.getName().getCatalog();
      List<String> path = e.getName().getPathList();
      String table = e.getName().getName();
      String fq = joinFqQuoted(catalog, path, table);
      out.printf("%-40s  %s%n", rid(e.getResourceId()), fq);
    }
  }

  static void printTable(Table t, PrintStream out) {
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

  // --- misc helpers ---

  private static List<String> splitPath(String input) {
    return FQNameParserUtil.segments(input);
  }

  private static List<String> csvList(String s) {
    if (s == null || s.isBlank()) return List.of();
    try {
      return CsvListParserUtil.items(s).stream()
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

  private static Precondition preconditionFromEtag(List<String> args) {
    String etag = CliArgs.parseStringFlag(args, "--etag", "");
    if (etag == null || etag.isBlank()) return null;
    return Precondition.newBuilder().setExpectedEtag(etag).build();
  }

  private static TableFormat parseFormat(String s) {
    if (s == null || s.isBlank()) return TableFormat.TF_UNSPECIFIED;
    String u = s.trim().toUpperCase(Locale.ROOT);
    if (u.equals("ICEBERG")) return TableFormat.TF_ICEBERG;
    if (u.equals("DELTA")) return TableFormat.TF_DELTA;
    return TableFormat.TF_UNSPECIFIED;
  }
}
