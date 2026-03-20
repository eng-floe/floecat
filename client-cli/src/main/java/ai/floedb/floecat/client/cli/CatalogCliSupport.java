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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import java.io.PrintStream;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** CLI support for the {@code catalogs} and {@code catalog} commands. */
final class CatalogCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private CatalogCliSupport() {}

  /**
   * Dispatches {@code catalogs} and {@code catalog} subcommands.
   *
   * @param command the top-level command token ("catalogs" or "catalog")
   * @param args tokens after the command
   * @param out output stream
   * @param catalogs gRPC catalog service stub
   * @param directory gRPC directory service stub (for resolving catalog names)
   * @param getCurrentAccountId returns the currently selected account ID
   * @param setCurrentCatalog called when the user selects a catalog (catalog use)
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      CatalogServiceGrpc.CatalogServiceBlockingStub catalogs,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId,
      Consumer<String> setCurrentCatalog) {
    if ("catalogs".equals(command)) {
      catalogList(out, catalogs, directory, getCurrentAccountId);
      return;
    }
    // command == "catalog"
    if (!args.isEmpty() && "use".equals(args.get(0))) {
      catalogUse(
          CliArgs.tail(args), out, catalogs, directory, getCurrentAccountId, setCurrentCatalog);
    } else {
      catalogCrud(args, out, catalogs, directory, getCurrentAccountId);
    }
  }

  // --- list ---

  private static void catalogList(
      PrintStream out,
      CatalogServiceGrpc.CatalogServiceBlockingStub catalogs,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    List<Catalog> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> catalogs.listCatalogs(ListCatalogsRequest.newBuilder().setPage(pr).build()),
            r -> r.getCatalogsList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printCatalogs(all, out);
  }

  // --- use ---

  private static void catalogUse(
      List<String> args,
      PrintStream out,
      CatalogServiceGrpc.CatalogServiceBlockingStub catalogs,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId,
      Consumer<String> setCurrentCatalog) {
    if (args.size() != 1) {
      out.println("usage: catalog use <catalog-name>");
      return;
    }
    String name = Shell.Quotes.unquote(args.get(0));
    if (name.isBlank()) {
      out.println("catalog name cannot be empty");
      return;
    }
    ResourceId cid = resolveCatalogId(name, directory, getCurrentAccountId);
    setCurrentCatalog.accept(name);
    out.println("catalog set: " + name + " (" + cid.getId() + ")");
  }

  // --- CRUD ---

  private static void catalogCrud(
      List<String> args,
      PrintStream out,
      CatalogServiceGrpc.CatalogServiceBlockingStub catalogs,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
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
        String display = Shell.Quotes.unquote(args.get(1));
        String desc = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
        String connectorRef =
            Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--connector", null));
        String policyRef = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--policy", null));
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
        printCatalogs(List.of(resp.getCatalog()), out);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: catalog get <display_name|id>");
          return;
        }
        var resp =
            catalogs.getCatalog(
                GetCatalogRequest.newBuilder()
                    .setCatalogId(
                        resolveCatalogId(
                            Shell.Quotes.unquote(args.get(1)), directory, getCurrentAccountId))
                    .build());
        printCatalogs(List.of(resp.getCatalog()), out);
      }
      case "update" -> {
        if (args.size() < 2) {
          out.println(
              "usage: catalog update <display_name|id> [--display <name>] [--desc <text>]"
                  + " [--connector <id>] [--policy <id>] [--props k=v ...] [--etag <etag>]");
          return;
        }
        String id = Shell.Quotes.unquote(args.get(1));
        String display = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--display", null));
        String desc = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
        String connectorRef =
            Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--connector", null));
        String policyRef = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--policy", null));
        Map<String, String> properties = parseKeyValueList(args, "--props");

        var sb = CatalogSpec.newBuilder();
        LinkedHashSet<String> mask = new LinkedHashSet<>();

        if (display != null) {
          sb.setDisplayName(Shell.Quotes.unquote(display));
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

        var updateBuilder =
            UpdateCatalogRequest.newBuilder()
                .setCatalogId(resolveCatalogId(id, directory, getCurrentAccountId))
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build());
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          updateBuilder.setPrecondition(precondition);
        }
        var resp = catalogs.updateCatalog(updateBuilder.build());
        printCatalogs(List.of(resp.getCatalog()), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: catalog delete <display_name|id> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");
        var deleteBuilder =
            DeleteCatalogRequest.newBuilder()
                .setCatalogId(
                    resolveCatalogId(
                        Shell.Quotes.unquote(args.get(1)), directory, getCurrentAccountId))
                .setRequireEmpty(requireEmpty);
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          deleteBuilder.setPrecondition(precondition);
        }
        catalogs.deleteCatalog(deleteBuilder.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  // --- resolution helpers ---

  static ResourceId resolveCatalogId(
      String token,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    String t = Shell.Quotes.unquote(token);
    if (looksLikeUuid(t)) {
      return catalogRid(t, getCurrentAccountId);
    }
    return directory
        .resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(nameCatalog(t)).build())
        .getResourceId();
  }

  private static ResourceId catalogRid(String id, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_CATALOG)
        .setId(id)
        .build();
  }

  private static NameRef nameCatalog(String name) {
    return NameRef.newBuilder().setCatalog(Shell.Quotes.unquote(name)).build();
  }

  private static boolean looksLikeUuid(String s) {
    if (s == null) return false;
    return s.trim()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  // --- output helpers ---

  private static void printCatalogs(List<Catalog> cats, PrintStream out) {
    out.printf(
        "%-40s  %-24s  %-24s  %s%n", "CATALOG_ID", "CREATED_AT", "DISPLAY_NAME", "DESCRIPTION");
    for (var c : cats) {
      out.printf(
          "%-40s  %-24s  %-24s  %s%n",
          rid(c.getResourceId()),
          ts(c.getCreatedAt()),
          Shell.Quotes.quoteIfNeeded(c.getDisplayName()),
          c.hasDescription() ? c.getDescription() : "");
    }
  }

  private static String rid(ResourceId id) {
    String s = (id == null) ? null : id.getId();
    return (s == null || s.isBlank()) ? "<no-id>" : s;
  }

  private static String ts(Timestamp t) {
    if (t == null || (t.getSeconds() == 0 && t.getNanos() == 0)) return "-";
    return Instant.ofEpochSecond(t.getSeconds(), t.getNanos()).toString();
  }

  // --- misc helpers ---

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

  private static String nvl(String s, String d) {
    return s == null ? d : s;
  }
}
