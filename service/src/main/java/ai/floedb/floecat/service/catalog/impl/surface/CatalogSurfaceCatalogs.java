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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemCatalogTranslator;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Catalog Surface policy for catalog RPCs. */
public final class CatalogSurfaceCatalogs {

  private static final String PAGE_TOKEN_PREFIX = "svc:catalogs:v1:";
  private static final String PAGE_TOKEN_SYSTEM_PAYLOAD = "s";
  private static final String PAGE_TOKEN_USER_PAYLOAD_PREFIX = "u:";
  private static final String SYSTEM_CATALOG_DESCRIPTION =
      "System catalog (global; visible from all catalogs)";

  private final CatalogRepository catalogRepo;
  private final CatalogOverlay overlay;
  private final EngineContextProvider engineContext;

  public CatalogSurfaceCatalogs(
      CatalogRepository catalogRepo, CatalogOverlay overlay, EngineContextProvider engineContext) {
    this.catalogRepo = catalogRepo;
    this.overlay = overlay;
    this.engineContext = engineContext;
  }

  public ListCatalogsResponse listCatalogs(
      ListCatalogsRequest request, String accountId, String corr) {
    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    var cursor = parseCatalogPageToken(pageIn.token, corr);
    int want = Math.max(1, pageIn.limit);
    var visibleSystemCatalog = visibleSystemCatalogForCurrentEngine();
    Catalog systemCatalog = visibleSystemCatalog.orElse(null);
    boolean hasSystemCatalog = systemCatalog != null;
    int userCount = catalogRepo.count(accountId);
    int totalCount = userCount + (hasSystemCatalog ? 1 : 0);

    boolean systemPhase = cursor.systemPhase;
    if (systemPhase && !hasSystemCatalog) {
      throw GrpcErrors.invalidArgument(
          corr, PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
    }

    List<Catalog> catalogs = new ArrayList<>(want);
    String nextToken = "";

    if (systemPhase) {
      catalogs.add(systemCatalog);
    } else {
      var next = new StringBuilder();
      List<Catalog> userCatalogs;
      try {
        userCatalogs = catalogRepo.list(accountId, want, cursor.repoToken, next);
      } catch (IllegalArgumentException badToken) {
        throw GrpcErrors.invalidArgument(
            corr, PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
      }

      catalogs.addAll(userCatalogs);
      nextToken = encodeUserRepoPageToken(next.toString());
      if (nextToken.isBlank()) {
        if (hasSystemCatalog && catalogs.size() < want) {
          catalogs.add(systemCatalog);
        } else if (hasSystemCatalog) {
          nextToken = encodeSystemPageToken();
        }
      }
    }

    var page = MutationOps.pageOut(nextToken, totalCount);

    return ListCatalogsResponse.newBuilder().addAllCatalogs(catalogs).setPage(page).build();
  }

  public GetCatalogResponse getCatalog(GetCatalogRequest request, String corr) {
    var catalogId = request.getCatalogId();
    Catalog catalog =
        catalogRepo
            .getById(catalogId)
            .orElseGet(
                () -> {
                  if (isVisibleSystemCatalog(catalogId)) {
                    return systemCatalogForCurrentEngine();
                  }
                  throw GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId()));
                });
    return GetCatalogResponse.newBuilder().setCatalog(catalog).build();
  }

  private Catalog systemCatalogForCurrentEngine() {
    String engineKind = engineContext.effectiveEngineKind();
    return Catalog.newBuilder()
        .setResourceId(SystemNodeRegistry.systemCatalogContainerId(engineKind))
        .setDisplayName(engineKind)
        .setDescription(SYSTEM_CATALOG_DESCRIPTION)
        .build();
  }

  private CatalogPageCursor parseCatalogPageToken(String token, String corr) {
    if (token == null || token.isBlank()) {
      return CatalogPageCursor.user("");
    }
    if (!token.startsWith(PAGE_TOKEN_PREFIX)) {
      return CatalogPageCursor.user(token);
    }
    String encodedPayload = token.substring(PAGE_TOKEN_PREFIX.length());
    final String payload;
    try {
      payload = new String(Base64.getUrlDecoder().decode(encodedPayload), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException bad) {
      throw GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", token));
    }
    if (PAGE_TOKEN_SYSTEM_PAYLOAD.equals(payload)) {
      return CatalogPageCursor.system();
    }
    if (payload.startsWith(PAGE_TOKEN_USER_PAYLOAD_PREFIX)) {
      return CatalogPageCursor.user(payload.substring(PAGE_TOKEN_USER_PAYLOAD_PREFIX.length()));
    }
    throw GrpcErrors.invalidArgument(corr, PAGE_TOKEN_INVALID, Map.of("page_token", token));
  }

  private static String encodeUserRepoPageToken(String repoToken) {
    if (repoToken == null || repoToken.isBlank()) {
      return "";
    }
    return encodeCatalogPageTokenPayload(PAGE_TOKEN_USER_PAYLOAD_PREFIX + repoToken);
  }

  private static String encodeSystemPageToken() {
    return encodeCatalogPageTokenPayload(PAGE_TOKEN_SYSTEM_PAYLOAD);
  }

  private static String encodeCatalogPageTokenPayload(String payload) {
    String encodedPayload =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    return PAGE_TOKEN_PREFIX + encodedPayload;
  }

  private Optional<ResourceId> normalizedCurrentSystemCatalogId(ResourceId catalogId) {
    ResourceId normalized = normalizedSystemCatalogIdOrNull(catalogId);
    if (normalized == null) {
      return Optional.empty();
    }
    String currentId =
        SystemNodeRegistry.systemCatalogContainerId(engineContext.effectiveEngineKind()).getId();
    return normalized.getId().equals(currentId) ? Optional.of(normalized) : Optional.empty();
  }

  private ResourceId normalizedSystemCatalogIdOrNull(ResourceId catalogId) {
    if (catalogId == null || catalogId.getKind() != ResourceKind.RK_CATALOG) {
      return null;
    }
    return SystemCatalogTranslator.normalizeSystemId(catalogId);
  }

  private boolean isCurrentSystemCatalog(ResourceId catalogId) {
    return normalizedCurrentSystemCatalogId(catalogId).isPresent();
  }

  private Optional<Catalog> visibleSystemCatalogForCurrentEngine() {
    Catalog systemCatalog = systemCatalogForCurrentEngine();
    return overlay.catalog(systemCatalog.getResourceId()).map(ignored -> systemCatalog);
  }

  private boolean isVisibleSystemCatalog(ResourceId catalogId) {
    return normalizedCurrentSystemCatalogId(catalogId).flatMap(overlay::catalog).isPresent();
  }

  private static final class CatalogPageCursor {
    final boolean systemPhase;
    final String repoToken;

    private CatalogPageCursor(boolean systemPhase, String repoToken) {
      this.systemPhase = systemPhase;
      this.repoToken = repoToken;
    }

    static CatalogPageCursor system() {
      return new CatalogPageCursor(true, "");
    }

    static CatalogPageCursor user(String repoToken) {
      return new CatalogPageCursor(false, repoToken);
    }
  }
}
