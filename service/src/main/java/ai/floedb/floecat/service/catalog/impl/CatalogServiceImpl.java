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

package ai.floedb.floecat.service.catalog.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CatalogService;
import ai.floedb.floecat.catalog.rpc.CatalogSpec;
import ai.floedb.floecat.catalog.rpc.CreateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.CreateCatalogResponse;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogRequest;
import ai.floedb.floecat.catalog.rpc.DeleteCatalogResponse;
import ai.floedb.floecat.catalog.rpc.GetCatalogRequest;
import ai.floedb.floecat.catalog.rpc.GetCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ListCatalogsRequest;
import ai.floedb.floecat.catalog.rpc.ListCatalogsResponse;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogRequest;
import ai.floedb.floecat.catalog.rpc.UpdateCatalogResponse;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.common.PersistedSecretPropertyValidator;
import ai.floedb.floecat.service.context.EngineContextProvider;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.graph.SystemCatalogTranslator;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class CatalogServiceImpl extends BaseServiceImpl implements CatalogService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject MarkerStore markerStore;
  @Inject EngineContextProvider engineContext;
  @Inject CatalogOverlay overlay;

  private static final Set<String> CATALOG_MUTABLE_PATHS =
      Set.of("display_name", "description", "connector_ref", "properties", "policy_ref");
  private static final String PAGE_TOKEN_PREFIX = "svc:catalogs:v1:";
  private static final String PAGE_TOKEN_SYSTEM_PAYLOAD = "s";
  private static final String PAGE_TOKEN_USER_PAYLOAD_PREFIX = "u:";
  private static final String SYSTEM_CATALOG_DESCRIPTION =
      "System catalog (global; visible from all catalogs)";

  private static final Logger LOG = Logger.getLogger(CatalogService.class);

  @Override
  public Uni<ListCatalogsResponse> listCatalogs(ListCatalogsRequest request) {
    var L = LogHelper.start(LOG, "ListCatalogs");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "catalog.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var cursor = parseCatalogPageToken(pageIn.token, correlationId());
                  int want = Math.max(1, pageIn.limit);
                  var visibleSystemCatalog = visibleSystemCatalogForCurrentEngine();
                  Catalog systemCatalog = visibleSystemCatalog.orElse(null);
                  boolean hasSystemCatalog = systemCatalog != null;
                  int userCount = catalogRepo.count(principalContext.getAccountId());
                  int totalCount = userCount + (hasSystemCatalog ? 1 : 0);

                  boolean systemPhase = cursor.systemPhase;
                  if (systemPhase && !hasSystemCatalog) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
                  }

                  List<Catalog> catalogs = new ArrayList<>(want);
                  String nextToken = "";

                  if (systemPhase) {
                    catalogs.add(systemCatalog);
                  } else {
                    var next = new StringBuilder();
                    List<Catalog> userCatalogs;
                    try {
                      userCatalogs =
                          catalogRepo.list(
                              principalContext.getAccountId(), want, cursor.repoToken, next);
                    } catch (IllegalArgumentException badToken) {
                      throw GrpcErrors.invalidArgument(
                          correlationId(), PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
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

                  return ListCatalogsResponse.newBuilder()
                      .addAllCatalogs(catalogs)
                      .setPage(page)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
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
      // Backward-compatible path: pass raw repository tokens through unchanged.
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
      // Blank means the user-catalog repo has no more pages.
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

  @Override
  public Uni<GetCatalogResponse> getCatalog(GetCatalogRequest request) {
    var L = LogHelper.start(LOG, "GetCatalog");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();

                  authz.require(principalContext, "catalog.read");

                  Catalog catalog =
                      catalogRepo
                          .getById(request.getCatalogId())
                          .orElseGet(
                              () -> {
                                if (isVisibleSystemCatalog(request.getCatalogId())) {
                                  return systemCatalogForCurrentEngine();
                                }
                                throw GrpcErrors.notFound(
                                    correlationId(),
                                    CATALOG,
                                    Map.of("id", request.getCatalogId().getId()));
                              });
                  return GetCatalogResponse.newBuilder().setCatalog(catalog).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateCatalogResponse> createCatalog(CreateCatalogRequest request) {
    var L = LogHelper.start(LOG, "CreateCatalog");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var corr = pc.getCorrelationId();
                  var accountId = pc.getAccountId();

                  authz.require(pc, "catalog.write");

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);
                  PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                      spec.getPropertiesMap(), corr, "spec.properties");

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var normalizedSpec = spec.toBuilder().setDisplayName(normName).build();
                  var fingerprint = canonicalFingerprint(normalizedSpec);
                  var catalogId = randomResourceId(accountId, ResourceKind.RK_CATALOG);

                  var built =
                      Catalog.newBuilder()
                          .setResourceId(catalogId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .putAllProperties(spec.getPropertiesMap())
                          .setCreatedAt(tsNow)
                          .build();

                  if (idempotencyKey == null) {
                    var existing = catalogRepo.getByName(accountId, normName);
                    if (existing.isPresent()) {
                      throw GrpcErrors.alreadyExists(
                          corr, CATALOG_ALREADY_EXISTS, Map.of("display_name", normName));
                    }

                    catalogRepo.create(built);
                    metadataGraph.invalidate(catalogId);
                    var meta = catalogRepo.metaForSafe(catalogId);
                    return CreateCatalogResponse.newBuilder()
                        .setCatalog(built)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateCatalog",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    try {
                                      catalogRepo.create(built);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      var existingOpt = catalogRepo.getByName(accountId, normName);
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromCatalog(existingOpt.get());
                                        if (Arrays.equals(
                                            fingerprint, canonicalFingerprint(existingSpec))) {
                                          metadataGraph.invalidate(
                                              existingOpt.get().getResourceId());
                                          return new IdempotencyGuard.CreateResult<>(
                                              existingOpt.get(), existingOpt.get().getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.alreadyExists(
                                          corr,
                                          CATALOG_ALREADY_EXISTS,
                                          Map.of("display_name", normName));
                                    }
                                    metadataGraph.invalidate(catalogId);
                                    return new IdempotencyGuard.CreateResult<>(built, catalogId);
                                  },
                                  c -> catalogRepo.metaForSafe(c.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Catalog::parseFrom));

                  return CreateCatalogResponse.newBuilder()
                      .setCatalog(result.body)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateCatalogResponse> updateCatalog(UpdateCatalogRequest request) {
    var L = LogHelper.start(LOG, "UpdateCatalog");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pctx = principal.get();
                  var corr = pctx.getCorrelationId();
                  authz.require(pctx, "catalog.write");

                  var catalogId = request.getCatalogId();
                  ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);
                  if (isCurrentSystemCatalog(catalogId)) {
                    throw GrpcErrors.permissionDenied(
                        corr,
                        SYSTEM_OBJECT_IMMUTABLE,
                        Map.of("id", catalogId.getId(), "kind", "catalog"));
                  }

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = normalizeMask(request.getUpdateMask());
                  if (maskTargets(mask, "properties")) {
                    PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                        spec.getPropertiesMap(), corr, "spec.properties");
                  }

                  var meta = catalogRepo.metaFor(catalogId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      catalogRepo
                          .getById(catalogId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, CATALOG, Map.of("id", catalogId.getId())));

                  var desired = applyCatalogSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = catalogRepo.metaFor(catalogId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && metaNoop.getPointerVersion() != meta.getPointerVersion()) {
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(metaNoop.getPointerVersion())));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateCatalogResponse.newBuilder()
                        .setCatalog(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  try {
                    boolean ok = catalogRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = catalogRepo.metaForSafe(catalogId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.alreadyExists(
                        corr,
                        CATALOG_ALREADY_EXISTS,
                        Map.of("display_name", desired.getDisplayName()));
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = catalogRepo.metaForSafe(catalogId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }
                  metadataGraph.invalidate(catalogId);

                  var outMeta = catalogRepo.metaForSafe(catalogId);
                  var latest = catalogRepo.getById(catalogId).orElse(desired);

                  return UpdateCatalogResponse.newBuilder()
                      .setCatalog(latest)
                      .setMeta(outMeta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteCatalogResponse> deleteCatalog(DeleteCatalogRequest request) {
    var L = LogHelper.start(LOG, "DeleteCatalog");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "catalog.write");
                  var id = request.getCatalogId();
                  ensureKind(id, ResourceKind.RK_CATALOG, "catalog_id", correlationId);
                  if (isCurrentSystemCatalog(id)) {
                    throw GrpcErrors.permissionDenied(
                        correlationId,
                        SYSTEM_OBJECT_IMMUTABLE,
                        Map.of("id", id.getId(), "kind", "catalog"));
                  }
                  long markerVersion = markerStore.catalogMarkerVersion(id);

                  MutationMeta meta;
                  try {
                    meta = catalogRepo.metaFor(id);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = catalogRepo.metaForSafe(id);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(correlationId, CATALOG, Map.of("id", id.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    metadataGraph.invalidate(id);
                    return DeleteCatalogResponse.newBuilder().setMeta(safe).build();
                  }

                  if (namespaceRepo.count(id.getAccountId(), id.getId(), List.of()) > 0) {
                    var currentCatalog = catalogRepo.getById(id).orElse(null);
                    var displayName =
                        (currentCatalog != null && !currentCatalog.getDisplayName().isBlank())
                            ? currentCatalog.getDisplayName()
                            : id.getId();
                    throw GrpcErrors.conflict(
                        correlationId, CATALOG_NOT_EMPTY, Map.of("display_name", displayName));
                  }

                  if (!markerStore.advanceCatalogMarker(id, markerVersion)) {
                    throw GrpcErrors.preconditionFailed(
                        correlationId, CATALOG_CHILDREN_CHANGED, Map.of());
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> catalogRepo.deleteWithPrecondition(id, expected),
                          () -> catalogRepo.metaForSafe(id),
                          correlationId,
                          "catalog",
                          Map.of("id", id.getId()));

                  metadataGraph.invalidate(id);
                  return DeleteCatalogResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private Catalog applyCatalogSpecPatch(
      Catalog current, CatalogSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
    }

    for (var p : paths) {
      if (!CATALOG_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      var name = spec.getDisplayName();
      if (name == null || name.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, DISPLAY_NAME_REQUIRED, Map.of());
      }
      b.setDisplayName(name);
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    if (maskTargets(mask, "connector_ref")) {
      if (spec.hasConnectorRef()) {
        b.setConnectorRef(spec.getConnectorRef());
      } else {
        b.clearConnectorRef();
      }
    }

    if (maskTargets(mask, "policy_ref")) {
      if (spec.hasPolicyRef()) {
        b.setPolicyRef(spec.getPolicyRef());
      } else {
        b.clearPolicyRef();
      }
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    return b.build();
  }

  private static FieldMask normalizeMask(FieldMask mask) {
    if (mask == null) {
      return null;
    }
    var out = FieldMask.newBuilder();
    for (var p : mask.getPathsList()) {
      if (p == null) {
        continue;
      }
      var t = p.trim().toLowerCase();
      if (!t.isEmpty()) {
        out.addPaths(t);
      }
    }
    return out.build();
  }

  private static byte[] canonicalFingerprint(CatalogSpec s) {
    return new Canonicalizer()
        .scalar("name", normalizeName(s.getDisplayName()))
        .scalar("description", s.getDescription())
        .scalar("connector_ref", s.getConnectorRef())
        .scalar("policy_ref", s.getPolicyRef())
        .map("properties", s.getPropertiesMap())
        .bytes();
  }

  private static CatalogSpec specFromCatalog(Catalog catalog) {
    return CatalogSpec.newBuilder()
        .setDisplayName(normalizeName(catalog.getDisplayName()))
        .setDescription(catalog.getDescription())
        .setConnectorRef(catalog.getConnectorRef())
        .setPolicyRef(catalog.getPolicyRef())
        .putAllProperties(catalog.getPropertiesMap())
        .build();
  }
}
