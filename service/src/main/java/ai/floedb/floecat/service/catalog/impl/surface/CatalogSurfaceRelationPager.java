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

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID;

import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** Shared page traversal for typed and kind-neutral relation listings. */
final class CatalogSurfaceRelationPager {

  private CatalogSurfaceRelationPager() {}

  static Page listRefs(int want, String pageToken, RefSource source, String corr) {
    final boolean isServiceToken = pageToken != null && pageToken.startsWith(source.tokenPrefix());
    final String resumeAfterRel =
        isServiceToken
            ? CatalogSurfaceSupport.decodeToken(source.tokenPrefix(), pageToken, corr)
            : "";
    final String repoCursor = isServiceToken ? "" : pageToken == null ? "" : pageToken;

    var out = new ArrayList<CatalogGraphView.RelationRef>(want);
    String repoNext = "";
    if (source.hasUserRelations() && !isServiceToken) {
      var next = new StringBuilder();
      final List<CatalogGraphView.RelationRef> scanned;
      try {
        scanned = source.listUserRelations(want, repoCursor, next);
      } catch (IllegalArgumentException badToken) {
        throw GrpcErrors.invalidArgument(
            corr, PAGE_TOKEN_INVALID, Map.of("page_token", repoCursor));
      }
      out.addAll(scanned);
      repoNext = next.toString();
    }

    var repoExhausted = repoNext.isBlank();
    var systemRelations = systemRelations(source);

    String lastEmittedRel = "";
    if (repoExhausted && !systemRelations.isEmpty() && out.size() < want) {
      for (var relation : systemRelations) {
        String key = relationKey(relation);
        if (!resumeAfterRel.isBlank() && key.compareTo(resumeAfterRel) <= 0) {
          continue;
        }
        if (out.size() >= want) {
          break;
        }
        out.add(relation);
        lastEmittedRel = key;
      }
    }

    String nextToken = repoNext;
    String resume = lastEmittedRel.isBlank() ? resumeAfterRel : lastEmittedRel;
    boolean hasMoreSystem =
        systemRelations.stream().anyMatch(ref -> relationKey(ref).compareTo(resume) > 0);
    if (nextToken.isBlank() && out.size() == want && hasMoreSystem) {
      nextToken = CatalogSurfaceSupport.encodeToken(source.tokenPrefix(), resume);
    }

    return new Page(out, nextToken);
  }

  static int total(RefSource source) {
    return (source.hasUserRelations() ? source.countUserRelations() : 0)
        + systemRelations(source).size();
  }

  private static List<CatalogGraphView.RelationRef> systemRelations(RefSource source) {
    return source.systemRelations().stream()
        .filter(ref -> ref != null && ref.name() != null && !ref.name().isBlank())
        .sorted(Comparator.comparing(CatalogSurfaceRelationPager::relationKey))
        .toList();
  }

  static String relationKey(CatalogGraphView.RelationRef ref) {
    return CatalogSurfaceSupport.normalizeName(ref.name())
        + "\0"
        + ref.id().getAccountId()
        + "\0"
        + ref.id().getId();
  }

  interface RefSource {
    String tokenPrefix();

    boolean hasUserRelations();

    List<CatalogGraphView.RelationRef> listUserRelations(
        int limit, String cursor, StringBuilder next);

    int countUserRelations();

    List<CatalogGraphView.RelationRef> systemRelations();
  }

  record Page(List<CatalogGraphView.RelationRef> relations, String nextToken) {}
}
