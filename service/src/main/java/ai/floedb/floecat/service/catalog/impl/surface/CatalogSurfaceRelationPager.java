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

import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

final class CatalogSurfaceRelationPager {

  private CatalogSurfaceRelationPager() {}

  static <P, N> Page<P> list(int want, String pageToken, Source<P, N> source, String corr) {

    final boolean isServiceToken = pageToken != null && pageToken.startsWith(source.tokenPrefix());
    final String resumeAfterRel =
        isServiceToken
            ? CatalogSurfaceSupport.decodeToken(source.tokenPrefix(), pageToken, corr)
            : "";
    String repoCursor = isServiceToken ? "" : pageToken;

    var out = new ArrayList<P>(want);
    String lastEmittedRel = "";

    String repoNext = "";
    if (source.namespace().origin() != GraphNodeOrigin.SYSTEM && !isServiceToken) {
      var next = new StringBuilder();
      final List<P> scanned;
      try {
        scanned = source.listRepo(want, repoCursor, next);
      } catch (IllegalArgumentException badToken) {
        throw GrpcErrors.invalidArgument(
            corr, PAGE_TOKEN_INVALID, Map.of("page_token", repoCursor));
      }

      out.addAll(scanned);
      repoNext = next.toString();
    }

    int sysCount;
    var repoExhausted = repoNext.isBlank();
    var sysNodes = source.systemNodes();
    sysCount = sysNodes.size();

    if (repoExhausted && out.size() < want && sysCount > 0) {
      record SysItem<N>(N node, String rel) {}

      var sysItems =
          sysNodes.stream()
              .map(node -> new SysItem<>(node, source.systemRelativeKey(node)))
              .filter(it -> it.rel() != null && !it.rel().isBlank())
              .sorted(Comparator.comparing(SysItem::rel))
              .toList();

      for (var it : sysItems) {
        if (!resumeAfterRel.isBlank() && it.rel().compareTo(resumeAfterRel) <= 0) {
          continue;
        }
        if (out.size() >= want) {
          break;
        }
        out.add(source.mapSystemNode(it.node()));
        lastEmittedRel = it.rel();
      }
    }

    String nextToken = repoNext;
    if (nextToken.isBlank() && out.size() == want && sysCount > 0) {
      nextToken = CatalogSurfaceSupport.encodeToken(source.tokenPrefix(), lastEmittedRel);
    }

    int repoCount = source.namespace().origin() == GraphNodeOrigin.SYSTEM ? 0 : source.countRepo();

    return new Page<>(out, nextToken, repoCount + sysCount);
  }

  interface Source<P, N> {
    NamespaceNode namespace();

    String tokenPrefix();

    List<P> listRepo(int limit, String cursor, StringBuilder next);

    int countRepo();

    List<N> systemNodes();

    String systemRelativeKey(N node);

    P mapSystemNode(N node);
  }

  record Page<P>(List<P> items, String nextToken, int totalSize) {}
}
