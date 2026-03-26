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

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.client.cli.util.CliUtils;
import ai.floedb.floecat.client.cli.util.NameRefUtil;
import ai.floedb.floecat.client.cli.util.Quotes;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.FieldMask;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** CLI support for the {@code views} and {@code view} commands. */
final class ViewCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private ViewCliSupport() {}

  /**
   * Dispatches {@code views} and {@code view} subcommands.
   *
   * @param command the top-level command token ("views" or "view")
   * @param args tokens after the command
   * @param out output stream
   * @param viewService gRPC view service stub
   * @param directory gRPC directory service stub
   * @param getCurrentAccountId returns the currently selected account ID
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      ViewServiceGrpc.ViewServiceBlockingStub viewService,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if ("views".equals(command)) {
      viewsList(args, out, viewService, directory, getCurrentAccountId);
    } else {
      viewCrud(args, out, viewService, directory, getCurrentAccountId);
    }
  }

  // --- views list ---

  private static void viewsList(
      List<String> args,
      PrintStream out,
      ViewServiceGrpc.ViewServiceBlockingStub viewService,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println("usage: views <catalog.ns[.ns...]>");
      return;
    }
    ResourceId namespaceId =
        NamespaceCliSupport.resolveNamespaceIdFlexible(args.get(0), directory, getCurrentAccountId);
    List<View> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr ->
                viewService.listViews(
                    ListViewsRequest.newBuilder().setNamespaceId(namespaceId).setPage(pr).build()),
            ListViewsResponse::getViewsList,
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printViews(all, out);
  }

  // --- view CRUD ---

  private static void viewCrud(
      List<String> args,
      PrintStream out,
      ViewServiceGrpc.ViewServiceBlockingStub viewService,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
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
        NameRef ref = NameRefUtil.nameRefForTable(args.get(1));
        ResourceId catalogId =
            CatalogCliSupport.resolveCatalogId(ref.getCatalog(), directory, getCurrentAccountId);
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
        String sql = Quotes.unquote(CliArgs.parseStringFlag(args, "--sql", ""));
        String desc = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));
        Map<String, String> props = CliUtils.parseKeyValueList(args, "--props");

        ViewSpec.Builder spec =
            ViewSpec.newBuilder()
                .setCatalogId(catalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName(ref.getName())
                .setSql(CliUtils.nvl(sql, ""));
        if (desc != null && !desc.isBlank()) {
          spec.setDescription(desc);
        }
        if (!props.isEmpty()) {
          spec.putAllProperties(props);
        }
        var resp = viewService.createView(CreateViewRequest.newBuilder().setSpec(spec).build());
        printView(resp.getView(), out);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: view get <id|catalog.ns[.ns...].name>");
          return;
        }
        ResourceId viewId = resolveViewId(args.get(1), directory, getCurrentAccountId);
        var resp = viewService.getView(GetViewRequest.newBuilder().setViewId(viewId).build());
        printView(resp.getView(), out);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: view update <id|fq> [--display <name>] [--namespace <catalog.ns[.ns...]>]"
                  + " [--sql <text>] [--desc <text>] [--props k=v ...]");
          return;
        }
        ResourceId viewId = resolveViewId(args.get(1), directory, getCurrentAccountId);
        String display = Quotes.unquote(CliArgs.parseStringFlag(args, "--display", null));
        String ns = Quotes.unquote(CliArgs.parseStringFlag(args, "--namespace", null));
        String sql = Quotes.unquote(CliArgs.parseStringFlag(args, "--sql", null));
        String desc = Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
        Map<String, String> props = CliUtils.parseKeyValueList(args, "--props");

        ViewSpec.Builder spec = ViewSpec.newBuilder();
        FieldMask.Builder mask = FieldMask.newBuilder();
        if (display != null) {
          spec.setDisplayName(display);
          mask.addPaths("display_name");
        }
        if (ns != null) {
          ResourceId namespaceId =
              NamespaceCliSupport.resolveNamespaceIdFlexible(ns, directory, getCurrentAccountId);
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
        printView(resp.getView(), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: view delete <id|catalog.ns[.ns...].name>");
          return;
        }
        ResourceId viewId = resolveViewId(args.get(1), directory, getCurrentAccountId);
        viewService.deleteView(DeleteViewRequest.newBuilder().setViewId(viewId).build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  // --- resolution helpers ---

  static ResourceId resolveViewId(
      String tok,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    String u = Quotes.unquote(tok == null ? "" : tok);
    if (CliUtils.looksLikeUuid(u)) {
      return rid(u, ResourceKind.RK_OVERLAY, getCurrentAccountId);
    }
    NameRef ref = NameRefUtil.nameRefForTable(tok);
    return directory
        .resolveView(ResolveViewRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private static ResourceId rid(
      String id, ResourceKind kind, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder().setAccountId(accountId).setKind(kind).setId(id).build();
  }

  // --- output helpers ---

  private static void printViews(List<View> views, PrintStream out) {
    out.printf("%-40s  %-24s  %s%n", "VIEW_ID", "CREATED_AT", "DISPLAY_NAME");
    for (var view : views) {
      out.printf(
          "%-40s  %-24s  %s%n",
          CliUtils.rid(view.getResourceId()),
          CliUtils.ts(view.getCreatedAt()),
          Quotes.quoteIfNeeded(view.getDisplayName()));
    }
  }

  static void printView(View view, PrintStream out) {
    out.println("View:");
    out.printf("  id:           %s%n", CliUtils.rid(view.getResourceId()));
    out.printf("  name:         %s%n", view.getDisplayName());
    out.printf("  description:  %s%n", view.hasDescription() ? view.getDescription() : "");
    out.printf("  sql:          %s%n", view.getSql());
    out.printf("  created_at:   %s%n", CliUtils.ts(view.getCreatedAt()));
    if (!view.getPropertiesMap().isEmpty()) {
      out.println("  properties:");
      view.getPropertiesMap().forEach((k, v) -> out.printf("    %s = %s%n", k, v));
    }
  }
}
