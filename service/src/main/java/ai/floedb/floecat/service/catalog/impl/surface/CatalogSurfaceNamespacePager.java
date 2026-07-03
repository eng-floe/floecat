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
package ai.floedb.floecat.service.catalog.impl.surface;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

final class CatalogSurfaceNamespacePager {

  // System-phase resume token: the carried relative name is a *system* namespace name.
  private static final String NS_TOKEN_PREFIX = "ns:";

  private CatalogSurfaceNamespacePager() {}

  static Page list(int want, String pageToken, Source source, String corr) {

    final int batch = Math.max(want * 4, 64);
    final boolean isSystemToken = pageToken != null && pageToken.startsWith(NS_TOKEN_PREFIX);
    final String resumeSystemRel =
        isSystemToken ? CatalogSurfaceSupport.decodeToken(NS_TOKEN_PREFIX, pageToken, corr) : "";
    // Anything else (blank or a raw store cursor) resumes the repo scan directly.
    String cursor = isSystemToken ? "" : (pageToken == null ? "" : pageToken);

    var out = new ArrayList<Namespace>(want);
    Namespace lastEmitted = null;

    // Repo (user) phase. The scan over-fetches and post-filters, so when the page fills the
    // continuation must resume after the last *emitted* row, not after the scanned batch:
    // source.cursorAfter(lastEmitted) is that precise position. Rows the batch read past (and even
    // rows read after the store reported exhaustion) are still in the store and are re-served by
    // the next page's scan.
    if (!isSystemToken) {
      while (out.size() < want) {
        var next = new StringBuilder();
        final List<Namespace> scanned;
        try {
          scanned = source.listRepo(batch, cursor, next);
        } catch (IllegalArgumentException badToken) {
          throw GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", cursor));
        }

        for (var ns : scanned) {
          if (!matches(ns, source)) {
            continue;
          }

          out.add(ns);
          lastEmitted = ns;
          if (out.size() >= want) {
            break;
          }
        }

        cursor = next.toString();
        if (cursor.isBlank() || out.size() >= want) {
          break;
        }
      }

      if (out.size() >= want) {
        return new Page(out, source.cursorAfter(lastEmitted), countNamespaces(source, corr));
      }
      // Repo exhausted with room left: fall through to the system phase from its start.
    }

    // System phase: entered once the repo is exhausted, or directly via a system-phase token. When
    // transitioning from the repo phase resumeSystemRel is blank, so system namespaces are emitted
    // from the beginning regardless of where the user phase left off.
    String lastSystemRel = "";
    record SysItem(NamespaceNode namespace, String rel) {}

    var sysItems =
        source.systemNamespaces().stream()
            .filter(ns -> matches(ns, source))
            .map(ns -> new SysItem(ns, relativeQualifiedName(ns, source.parentPath())))
            .sorted(Comparator.comparing(SysItem::rel))
            .toList();

    for (var it : sysItems) {
      if (!resumeSystemRel.isBlank() && it.rel().compareTo(resumeSystemRel) <= 0) {
        continue;
      }
      if (out.size() >= want) {
        break;
      }
      out.add(source.mapSystemNode(it.namespace()));
      lastSystemRel = it.rel();
    }

    String nextToken =
        out.size() == want && !lastSystemRel.isBlank()
            ? CatalogSurfaceSupport.encodeToken(NS_TOKEN_PREFIX, lastSystemRel)
            : "";

    int total = countNamespaces(source, corr);

    return new Page(out, nextToken, total);
  }

  private static int countNamespaces(Source source, String corr) {

    int count = 0;
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      final List<Namespace> page;
      try {
        page = source.listRepo(1000, cursor, next);
      } catch (IllegalArgumentException bad) {
        throw GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", cursor));
      }

      for (var ns : page) {
        if (matches(ns, source)) {
          count++;
        }
      }

      cursor = next.toString();
      if (cursor.isBlank()) {
        break;
      }
    }

    for (var ns : source.systemNamespaces()) {
      if (matches(ns, source)) {
        count++;
      }
    }

    return count;
  }

  private static boolean matches(Namespace namespace, Source source) {
    boolean matchesScope =
        source.recursive()
            ? isDescendantOf(namespace.getParentsList(), source.parentPath())
            : isImmediateChildOf(namespace.getParentsList(), source.parentPath());
    if (!matchesScope) {
      return false;
    }

    return source.namePrefix().isBlank()
        || relativeQualifiedName(namespace, source.parentPath()).startsWith(source.namePrefix());
  }

  private static boolean matches(NamespaceNode namespace, Source source) {
    boolean matchesScope =
        source.recursive()
            ? isDescendantOf(namespace.pathSegments(), source.parentPath())
            : isImmediateChildOf(namespace.pathSegments(), source.parentPath());
    if (!matchesScope) {
      return false;
    }

    return source.namePrefix().isBlank()
        || relativeQualifiedName(namespace, source.parentPath()).startsWith(source.namePrefix());
  }

  private static boolean isDescendantOf(List<String> namespaceParentPath, List<String> parentPath) {
    if (namespaceParentPath.size() < parentPath.size()) {
      return false;
    }
    for (int i = 0; i < parentPath.size(); i++) {
      if (!namespaceParentPath.get(i).equals(parentPath.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isImmediateChildOf(List<String> nsParentPath, List<String> parentPath) {
    if (nsParentPath.size() != parentPath.size()) {
      return false;
    }

    for (int i = 0; i < parentPath.size(); i++) {
      if (!nsParentPath.get(i).equals(parentPath.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static String relativeQualifiedName(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    int n = parentPath.size();
    var segs = new ArrayList<String>(p.size() - n + 1);

    for (int i = n; i < p.size(); i++) {
      segs.add(p.get(i));
    }

    if (!ns.getDisplayName().isBlank()) {
      segs.add(ns.getDisplayName());
    }

    return String.join(".", segs);
  }

  private static String relativeQualifiedName(NamespaceNode ns, List<String> parentPath) {
    var p = ns.pathSegments();
    int n = parentPath.size();
    var segs = new ArrayList<String>(Math.max(0, p.size() - n) + 1);

    for (int i = n; i < p.size(); i++) {
      segs.add(p.get(i));
    }

    if (ns.displayName() != null && !ns.displayName().isBlank()) {
      segs.add(ns.displayName());
    }

    return String.join(".", segs);
  }

  interface Source {
    List<String> parentPath();

    String namePrefix();

    boolean recursive();

    List<Namespace> listRepo(int limit, String cursor, StringBuilder next);

    /** Repo cursor that resumes the {@link #listRepo} scan immediately after {@code namespace}. */
    String cursorAfter(Namespace namespace);

    List<NamespaceNode> systemNamespaces();

    Namespace mapSystemNode(NamespaceNode namespace);
  }

  record Page(List<Namespace> items, String nextToken, int totalSize) {}
}
