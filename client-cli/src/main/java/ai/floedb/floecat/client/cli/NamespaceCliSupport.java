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

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.client.cli.util.FQNameParserUtil;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** CLI support for the {@code namespaces} and {@code namespace} commands. */
final class NamespaceCliSupport {

  private static final int DEFAULT_PAGE_SIZE = 1000;

  private NamespaceCliSupport() {}

  /**
   * Dispatches {@code namespaces} and {@code namespace} subcommands.
   *
   * @param command the top-level command token ("namespaces" or "namespace")
   * @param args tokens after the command
   * @param out output stream
   * @param namespaces gRPC namespace service stub
   * @param directory gRPC directory service stub (for resolving names)
   * @param getCurrentAccountId returns the currently selected account ID
   */
  static void handle(
      String command,
      List<String> args,
      PrintStream out,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if ("namespaces".equals(command)) {
      namespaceList(args, out, namespaces, directory, getCurrentAccountId);
    } else {
      namespaceCrud(args, out, namespaces, directory, getCurrentAccountId);
    }
  }

  // --- list ---

  private static void namespaceList(
      List<String> args,
      PrintStream out,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    if (args.isEmpty()) {
      out.println(
          "usage: namespaces (<catalog | catalog.ns[.ns...]> | <UUID>) [--id <UUID>] [--prefix P]"
              + " [--recursive]");
      return;
    }

    String raw = args.get(0).trim();
    String explicitId = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--id", ""));
    String namePrefix = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--prefix", ""));
    boolean recursive = args.contains("--recursive");
    boolean childrenOnly = !recursive;

    ListNamespacesRequest.Builder rb =
        ListNamespacesRequest.newBuilder()
            .setChildrenOnly(childrenOnly)
            .setRecursive(recursive)
            .setNamePrefix(nvl(namePrefix, ""))
            .setPage(PageRequest.newBuilder().setPageSize(DEFAULT_PAGE_SIZE).build());

    if (explicitId != null && !explicitId.isBlank()) {
      rb.setNamespaceId(resolveNamespaceIdFlexible(explicitId, directory, getCurrentAccountId));
    } else if (looksLikeQuotedOrRawUuid(raw)) {
      rb.setNamespaceId(resolveNamespaceIdFlexible(raw, directory, getCurrentAccountId));
    } else {
      try {
        NameRef ref = nameRefForNamespace(raw, true);
        rb.setCatalogId(
                CatalogCliSupport.resolveCatalogId(
                    ref.getCatalog(), directory, getCurrentAccountId))
            .addAllPath(ref.getPathList());
      } catch (IllegalArgumentException ex) {
        rb.setCatalogId(CatalogCliSupport.resolveCatalogId(raw, directory, getCurrentAccountId));
      }
    }

    List<Namespace> all =
        CliArgs.collectPages(
            DEFAULT_PAGE_SIZE,
            pr -> namespaces.listNamespaces(rb.setPage(pr).build()),
            r -> r.getNamespacesList(),
            r -> r.hasPage() ? r.getPage().getNextPageToken() : "");
    printNamespaces(all, out);
  }

  // --- CRUD ---

  private static void namespaceCrud(
      List<String> args,
      PrintStream out,
      NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
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
        String desc = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", ""));
        Map<String, String> properties = parseKeyValueList(args, "--props");
        String policy = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--policy", ""));

        String leafOpt = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--display", null));
        String pathOpt = CliArgs.parseStringFlag(args, "--path", null);

        String catalog;
        List<String> parents;
        String leaf;

        if (leafOpt != null || pathOpt != null) {
          int firstDot = token.indexOf('.');
          catalog = Shell.Quotes.unquote((firstDot > 0) ? token.substring(0, firstDot) : token);
          if (catalog.isBlank()) {
            out.println("Error: catalog is required before --path/--display.");
            return;
          }
          parents = (pathOpt == null || pathOpt.isBlank()) ? List.of() : splitPath(pathOpt);
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
                .setCatalogId(
                    CatalogCliSupport.resolveCatalogId(catalog, directory, getCurrentAccountId))
                .addAllPath(parents)
                .setDisplayName(leaf)
                .setDescription(nvl(desc, ""))
                .putAllProperties(properties)
                .setPolicyRef(nvl(policy, ""))
                .build();

        var resp =
            namespaces.createNamespace(CreateNamespaceRequest.newBuilder().setSpec(spec).build());
        printNamespaces(List.of(resp.getNamespace()), out);
      }
      case "get" -> {
        if (args.size() < 2) {
          out.println("usage: namespace get <id|fq>");
          return;
        }
        ResourceId nsId =
            looksLikeQuotedOrRawUuid(args.get(1))
                ? namespaceRid(Shell.Quotes.unquote(args.get(1)), getCurrentAccountId)
                : resolveNamespaceIdFlexible(args.get(1), directory, getCurrentAccountId);
        var resp =
            namespaces.getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(nsId).build());
        printNamespaces(List.of(resp.getNamespace()), out);
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
                ? namespaceRid(Shell.Quotes.unquote(args.get(1)), getCurrentAccountId)
                : directory
                    .resolveNamespace(
                        ResolveNamespaceRequest.newBuilder()
                            .setRef(nameRefForNamespace(args.get(1), false))
                            .build())
                    .getResourceId();

        String display = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--display", null));
        String desc = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--desc", null));
        String policyRef = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--policy", null));
        String pathStr = CliArgs.parseStringFlag(args, "--path", null);
        String catalogStr = Shell.Quotes.unquote(CliArgs.parseStringFlag(args, "--catalog", null));
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
          var pathList = pathStr.isBlank() ? List.<String>of() : splitPath(pathStr);
          sb.clearPath().addAllPath(pathList);
          mask.add("path");
        }
        if (catalogStr != null) {
          ResourceId cid =
              looksLikeUuid(catalogStr)
                  ? catalogRid(catalogStr, getCurrentAccountId)
                  : CatalogCliSupport.resolveCatalogId(catalogStr, directory, getCurrentAccountId);
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

        var updateBuilder =
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setSpec(sb.build())
                .setUpdateMask(FieldMask.newBuilder().addAllPaths(mask).build());
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          updateBuilder.setPrecondition(precondition);
        }
        var resp = namespaces.updateNamespace(updateBuilder.build());
        printNamespaces(List.of(resp.getNamespace()), out);
      }
      case "delete" -> {
        if (args.size() < 2) {
          out.println("usage: namespace delete <id|fq> [--require-empty] [--etag <etag>]");
          return;
        }
        boolean requireEmpty = args.contains("--require-empty");
        ResourceId nsId = resolveNamespaceIdFlexible(args.get(1), directory, getCurrentAccountId);
        var deleteBuilder =
            DeleteNamespaceRequest.newBuilder().setNamespaceId(nsId).setRequireEmpty(requireEmpty);
        Precondition precondition = preconditionFromEtag(args);
        if (precondition != null) {
          deleteBuilder.setPrecondition(precondition);
        }
        namespaces.deleteNamespace(deleteBuilder.build());
        out.println("ok");
      }
      default -> out.println("unknown subcommand");
    }
  }

  // --- resolution helpers ---

  static ResourceId resolveNamespaceIdFlexible(
      String tok,
      DirectoryServiceGrpc.DirectoryServiceBlockingStub directory,
      Supplier<String> getCurrentAccountId) {
    String u = Shell.Quotes.unquote(tok == null ? "" : tok);
    if (looksLikeUuid(u)) {
      return namespaceRid(u, getCurrentAccountId);
    }
    NameRef ref = nameRefForNamespace(tok, false);
    return directory
        .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  private static ResourceId namespaceRid(String id, Supplier<String> getCurrentAccountId) {
    String accountId = getCurrentAccountId.get();
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalStateException("No account set. Use: account <accountId>");
    }
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_NAMESPACE)
        .setId(id)
        .build();
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

  static NameRef nameRefForNamespace(String fqNs, boolean includeLeafInPath) {
    if (fqNs == null) throw new IllegalArgumentException("Fully qualified name is required");
    fqNs = fqNs.trim();
    if (fqNs.isEmpty()) throw new IllegalArgumentException("Namespace path is empty");

    List<String> segs = splitPath(fqNs);
    if (segs.size() < 2) {
      throw new IllegalArgumentException(
          "Invalid namespace path: at least a catalog and one namespace are required "
              + "(e.g. catalog.namespace)");
    }

    String catalog = Shell.Quotes.unquote(segs.get(0));
    List<String> path = segs.subList(1, segs.size()).stream().map(Shell.Quotes::unquote).toList();

    NameRef.Builder b = NameRef.newBuilder().setCatalog(catalog);
    if (includeLeafInPath) {
      b.addAllPath(path);
    } else {
      b.addAllPath(path.subList(0, path.size() - 1)).setName(path.get(path.size() - 1));
    }
    return b.build();
  }

  private static List<String> splitPath(String input) {
    return FQNameParserUtil.segments(input);
  }

  private static boolean looksLikeQuotedOrRawUuid(String s) {
    return looksLikeUuid(Shell.Quotes.unquote(s == null ? "" : s));
  }

  private static boolean looksLikeUuid(String s) {
    if (s == null) return false;
    return s.trim()
        .matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
  }

  // --- output helpers ---

  private static void printNamespaces(List<Namespace> rows, PrintStream out) {
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
          Shell.Quotes.quoteIfNeeded(leaf),
          ns.hasDescription() ? ns.getDescription() : "");
    }
  }

  private static String parentsAsList(List<String> parents) {
    return "["
        + parents.stream().map(Shell.Quotes::quoteIfNeeded).collect(Collectors.joining(", "))
        + "]";
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
