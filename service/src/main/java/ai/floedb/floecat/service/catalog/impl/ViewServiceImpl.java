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

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewResponse;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewService;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.catalog.hint.EngineHintSchemaCleaner;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class ViewServiceImpl extends BaseServiceImpl implements ViewService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject ViewRepository viewRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject CatalogOverlay overlay;
  @Inject EngineHintSchemaCleaner hintCleaner;

  private static final Set<String> VIEW_MUTABLE_PATHS =
      Set.of(
          "display_name",
          "description",
          "properties",
          "catalog_id",
          "namespace_id",
          "sql_definitions",
          "output_columns",
          "base_relations",
          "creation_search_path");
  private static final String VIEW_TOKEN_PREFIX = "view:";

  private static final Logger LOG = Logger.getLogger(ViewService.class);

  @Override
  public Uni<ListViewsResponse> listViews(ListViewsRequest request) {
    var L = LogHelper.start(LOG, "ListViews");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "view.read");

                  var namespaceId = request.getNamespaceId();
                  ensureKind(
                      namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId());

                  NamespaceNode nsNode =
                      CatalogOverlayGuards.requireVisibleNamespaceNode(
                          overlay, namespaceId, correlationId());
                  var catalogId = nsNode.catalogId();

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  final int want = Math.max(1, pageIn.limit);
                  final boolean isServiceToken =
                      pageIn.token != null && pageIn.token.startsWith(VIEW_TOKEN_PREFIX);
                  final String resumeAfterRel = isServiceToken ? decodeViewToken(pageIn.token) : "";
                  String repoCursor = isServiceToken ? "" : pageIn.token;

                  var out = new ArrayList<View>(want);
                  String lastEmittedRel = "";

                  String repoNext = "";
                  if (nsNode.origin() != GraphNodeOrigin.SYSTEM && !isServiceToken) {
                    var next = new StringBuilder();
                    final List<View> scanned;
                    try {
                      scanned =
                          viewRepo.list(
                              principalContext.getAccountId(),
                              catalogId.getId(),
                              namespaceId.getId(),
                              want,
                              repoCursor,
                              next);
                    } catch (IllegalArgumentException badToken) {
                      throw GrpcErrors.invalidArgument(
                          correlationId(), PAGE_TOKEN_INVALID, Map.of("page_token", repoCursor));
                    }

                    out.addAll(scanned);
                    repoNext = next.toString();
                  }

                  int sysCount = 0;
                  final boolean repoExhausted = repoNext.isBlank();
                  if (repoExhausted) {
                    var sysNodes =
                        overlay.listRelationsInNamespace(catalogId, namespaceId).stream()
                            .filter(ViewNode.class::isInstance)
                            .map(ViewNode.class::cast)
                            .filter(this::isSystemViewNode)
                            .toList();
                    sysCount = sysNodes.size();

                    if (out.size() < want && sysCount > 0) {
                      record SysItem(ViewNode node, String rel) {}

                      var sysItems =
                          sysNodes.stream()
                              .map(node -> new SysItem(node, relativeViewKey(node)))
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
                        out.add(viewFromSystemNode(it.node()));
                        lastEmittedRel = it.rel();
                      }
                    }
                  } else {
                    sysCount =
                        (int)
                            overlay.listRelationsInNamespace(catalogId, namespaceId).stream()
                                .filter(ViewNode.class::isInstance)
                                .map(ViewNode.class::cast)
                                .filter(this::isSystemViewNode)
                                .count();
                  }

                  String nextToken = repoNext;
                  if (nextToken.isBlank() && out.size() == want && sysCount > 0) {
                    nextToken = encodeViewToken(lastEmittedRel);
                  }

                  int repoCount =
                      (nsNode.origin() == GraphNodeOrigin.SYSTEM)
                          ? 0
                          : viewRepo.count(
                              principalContext.getAccountId(),
                              catalogId.getId(),
                              namespaceId.getId());

                  var page = MutationOps.pageOut(nextToken, repoCount + sysCount);

                  return ListViewsResponse.newBuilder().addAllViews(out).setPage(page).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetViewResponse> getView(GetViewRequest request) {
    var L = LogHelper.start(LOG, "GetView");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "view.read");

                  var viewId = request.getViewId();
                  GraphNode node = requireVisibleViewNode(viewId, correlationId());
                  var view = viewFromOverlayNodeOrRepo(node, viewId, correlationId());

                  return GetViewResponse.newBuilder().setView(view).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateViewResponse> createView(CreateViewRequest request) {
    var L = LogHelper.start(LOG, "CreateView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var corr = pc.getCorrelationId();
                  authz.require(pc, "view.write");

                  if (!request.hasSpec()) {
                    throw GrpcErrors.invalidArgument(corr, VIEW_MISSING_SPEC, Map.of());
                  }
                  var spec = request.getSpec();

                  if (!spec.hasCatalogId()) {
                    throw GrpcErrors.invalidArgument(corr, VIEW_MISSING_CATALOG_ID, Map.of());
                  }
                  if (!spec.hasNamespaceId()) {
                    throw GrpcErrors.invalidArgument(corr, VIEW_MISSING_NAMESPACE_ID, Map.of());
                  }

                  var catalogId = spec.getCatalogId();
                  ensureKind(catalogId, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
                  var catalog =
                      CatalogOverlayGuards.requireVisibleCatalogNode(overlay, catalogId, corr);
                  CatalogOverlayGuards.rejectSystemCatalogMutation(catalog.id(), corr);

                  var namespaceId = spec.getNamespaceId();
                  ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", corr);
                  var namespace =
                      CatalogOverlayGuards.requireWritableNamespaceNode(overlay, namespaceId, corr);
                  var nsCatalogId = namespace.catalogId();
                  if (nsCatalogId == null || !nsCatalogId.getId().equals(catalogId.getId())) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        NAMESPACE_CATALOG_MISMATCH,
                        Map.of(
                            "namespace_id", namespaceId.getId(),
                            "namespace.catalog_id", nsCatalogId == null ? "" : nsCatalogId.getId(),
                            "catalog_id", catalogId.getId()));
                  }

                  var tsNow = nowTs();

                  if (spec.getOutputColumnsList().isEmpty()) {
                    throw GrpcErrors.invalidArgument(corr, VIEW_OUTPUT_COLUMNS_REQUIRED, Map.of());
                  }

                  var rawName = mustNonEmpty(spec.getDisplayName(), "spec.display_name", corr);
                  var normName = normalizeName(rawName);

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var normalizedSpec = spec.toBuilder().setDisplayName(normName).build();
                  var fingerprint = canonicalFingerprint(normalizedSpec);

                  var accountId = pc.getAccountId();
                  var viewResourceId = randomResourceId(accountId, ResourceKind.RK_VIEW);

                  var viewBuilder =
                      View.newBuilder()
                          .setResourceId(viewResourceId)
                          .setCatalogId(spec.getCatalogId())
                          .setNamespaceId(spec.getNamespaceId())
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCreatedAt(tsNow)
                          .putAllProperties(spec.getPropertiesMap())
                          .addAllBaseRelations(spec.getBaseRelationsList())
                          .addAllCreationSearchPath(spec.getCreationSearchPathList())
                          .addAllOutputColumns(spec.getOutputColumnsList());
                  applySqlDefinitions(viewBuilder, spec, corr);
                  var view = viewBuilder.build();

                  if (idempotencyKey == null) {
                    var existing =
                        viewRepo.getByName(
                            accountId,
                            spec.getCatalogId().getId(),
                            spec.getNamespaceId().getId(),
                            normName);
                    if (existing.isPresent()) {
                      throw GrpcErrors.alreadyExists(
                          corr,
                          VIEW_ALREADY_EXISTS,
                          Map.of(
                              "display_name", normName,
                              "catalog_id", spec.getCatalogId().getId(),
                              "namespace_id", spec.getNamespaceId().getId()));
                    }
                    viewRepo.create(view);
                    metadataGraph.invalidate(viewResourceId);
                    var meta = viewRepo.metaForSafe(viewResourceId);
                    return CreateViewResponse.newBuilder().setView(view).setMeta(meta).build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateView",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    try {
                                      viewRepo.create(view);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      var existingOpt =
                                          viewRepo.getByName(
                                              accountId,
                                              spec.getCatalogId().getId(),
                                              spec.getNamespaceId().getId(),
                                              normName);
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromView(existingOpt.get());
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
                                          VIEW_ALREADY_EXISTS,
                                          Map.of(
                                              "display_name", normName,
                                              "catalog_id", spec.getCatalogId().getId(),
                                              "namespace_id", spec.getNamespaceId().getId()));
                                    }
                                    metadataGraph.invalidate(viewResourceId);
                                    return new IdempotencyGuard.CreateResult<>(
                                        view, viewResourceId);
                                  },
                                  v -> viewRepo.metaForSafe(v.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  View::parseFrom));

                  return CreateViewResponse.newBuilder()
                      .setView(result.body)
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
  public Uni<UpdateViewResponse> updateView(UpdateViewRequest request) {
    var L = LogHelper.start(LOG, "UpdateView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pctx = principal.get();
                  var corr = pctx.getCorrelationId();
                  authz.require(pctx, "view.write");

                  var viewId = request.getViewId();
                  ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", corr);
                  ensureViewWritable(viewId, corr);

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = normalizeMask(request.getUpdateMask());

                  var meta = viewRepo.metaFor(viewId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      viewRepo
                          .getById(viewId)
                          .orElseThrow(
                              () -> GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId())));

                  var desired = applyViewSpecPatch(current, spec, mask, corr);
                  if (hintCleaner.shouldClearHints(mask)) {
                    View.Builder builder = desired.toBuilder();
                    hintCleaner.cleanViewHints(builder, mask, current, builder.build());
                    desired = builder.build();
                  }

                  if (desired.equals(current)) {
                    var metaNoop = viewRepo.metaFor(viewId);
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
                    return UpdateViewResponse.newBuilder()
                        .setView(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  var conflictInfo =
                      Map.of(
                          "display_name", desired.getDisplayName(),
                          "catalog_id", desired.getCatalogId().getId(),
                          "namespace_id", desired.getNamespaceId().getId());

                  try {
                    boolean ok = viewRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = viewRepo.metaForSafe(viewId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.alreadyExists(corr, VIEW_ALREADY_EXISTS, conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = viewRepo.metaForSafe(viewId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }
                  metadataGraph.invalidate(viewId);

                  var outMeta = viewRepo.metaForSafe(viewId);
                  var latest = viewRepo.getById(viewId).orElse(desired);
                  return UpdateViewResponse.newBuilder().setView(latest).setMeta(outMeta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteViewResponse> deleteView(DeleteViewRequest request) {
    var L = LogHelper.start(LOG, "DeleteView");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "view.write");

                  var viewId = request.getViewId();
                  ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", correlationId);
                  boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                  ensureViewWritableForDelete(viewId, correlationId, callerCares);

                  MutationMeta meta;
                  try {
                    meta = viewRepo.metaFor(viewId);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = viewRepo.metaForSafe(viewId);
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(correlationId, VIEW, Map.of("id", viewId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    metadataGraph.invalidate(viewId);
                    return DeleteViewResponse.newBuilder().setMeta(safe).build();
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> viewRepo.deleteWithPrecondition(viewId, expected),
                          () -> viewRepo.metaForSafe(viewId),
                          correlationId,
                          "view",
                          Map.of("id", viewId.getId()));

                  metadataGraph.invalidate(viewId);
                  return DeleteViewResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private GraphNode resolveViewNode(ResourceId viewId, String corr, boolean throwOnError) {
    if (viewId == null) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", "<missing_view_id>"));
    }
    ensureKind(viewId, ResourceKind.RK_VIEW, "view_id", corr);

    try {
      return overlay.resolve(viewId).orElse(null);
    } catch (RuntimeException e) {
      if (throwOnError) {
        throw e;
      }
      return null;
    }
  }

  private GraphNode requireVisibleViewNode(ResourceId viewId, String corr) {
    GraphNode node = resolveViewNode(viewId, corr, true);
    if (node == null) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId()));
    }
    return node;
  }

  private void ensureViewWritable(ResourceId viewId, String corr) {
    GraphNode node = requireVisibleViewNode(viewId, corr);
    enforceWritableViewNode(node, viewId, corr);
  }

  private void ensureViewWritableForDelete(ResourceId viewId, String corr, boolean callerCares) {
    GraphNode node = resolveViewNode(viewId, corr, callerCares);
    if (node == null) {
      return;
    }
    enforceWritableViewNode(node, viewId, corr);
  }

  private void enforceWritableViewNode(GraphNode node, ResourceId viewId, String corr) {
    if (node instanceof ViewNode vn) {
      if (isSystemViewNode(vn)) {
        throw GrpcErrors.permissionDenied(
            corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", viewId.getId(), "kind", "view"));
      }
      return;
    }
    throw GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId()));
  }

  private View viewFromOverlayNodeOrRepo(GraphNode node, ResourceId viewId, String corr) {
    if (!(node instanceof ViewNode vn)) {
      throw GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId()));
    }

    if (isSystemViewNode(node)) {
      return viewFromSystemNode(vn);
    }

    return viewRepo
        .getById(viewId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, VIEW, Map.of("id", viewId.getId())));
  }

  private static View viewFromSystemNode(ViewNode node) {
    View.Builder builder =
        View.newBuilder()
            .setResourceId(node.id())
            .setCatalogId(node.catalogId())
            .setNamespaceId(node.namespaceId())
            .setDisplayName(node.displayName())
            .putAllProperties(node.properties());
    builder.addAllSqlDefinitions(node.sqlDefinitions());
    return builder.build();
  }

  private boolean isSystemViewNode(GraphNode node) {
    if (node == null || node.id() == null) {
      return false;
    }
    return node.origin() == GraphNodeOrigin.SYSTEM;
  }

  private String relativeViewKey(ViewNode node) {
    if (node == null) {
      return "";
    }
    var name = node.displayName();
    if (name == null) {
      name = "";
    }
    return normalizeName(name);
  }

  private static String encodeViewToken(String resumeAfterRel) {
    if (resumeAfterRel == null) resumeAfterRel = "";
    if (resumeAfterRel.isBlank()) {
      return VIEW_TOKEN_PREFIX;
    }
    return VIEW_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(resumeAfterRel.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeViewToken(String token) {
    if (token == null || token.isBlank() || !token.startsWith(VIEW_TOKEN_PREFIX)) return "";
    if (token.length() == VIEW_TOKEN_PREFIX.length()) {
      return "";
    }
    var s = token.substring(VIEW_TOKEN_PREFIX.length());
    var bytes = Base64.getUrlDecoder().decode(s);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static void validateViewMaskOrThrow(FieldMask mask, String corr) {
    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
    }
    for (var p : paths) {
      if (!VIEW_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }
  }

  private View applyViewSpecPatch(View current, ViewSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);
    validateViewMaskOrThrow(mask, corr);

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(corr, DISPLAY_NAME_CANNOT_CLEAR, Map.of());
      }
      b.setDisplayName(mustNonEmpty(spec.getDisplayName(), "spec.display_name", corr));
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    if (maskTargets(mask, "sql_definitions")) {
      List<ViewSqlDefinition> mergedDefinitions = mergeSqlDefinitions(current, spec, mask, corr);
      b.clearSqlDefinitions();
      mergedDefinitions.forEach(b::addSqlDefinitions);
    }

    if (maskTargets(mask, "output_columns")) {
      if (spec.getOutputColumnsList().isEmpty()) {
        throw GrpcErrors.invalidArgument(corr, VIEW_OUTPUT_COLUMNS_REQUIRED, Map.of());
      }
      b.clearOutputColumns().addAllOutputColumns(spec.getOutputColumnsList());
    }

    if (maskTargets(mask, "base_relations")) {
      b.clearBaseRelations().addAllBaseRelations(spec.getBaseRelationsList());
    }

    if (maskTargets(mask, "creation_search_path")) {
      b.clearCreationSearchPath().addAllCreationSearchPath(spec.getCreationSearchPathList());
    }

    boolean catalogChanged = false;
    boolean namespaceChanged = false;

    if (maskTargets(mask, "catalog_id")) {
      if (!spec.hasCatalogId()) {
        throw GrpcErrors.invalidArgument(corr, CATALOG_ID_CANNOT_CLEAR, Map.of());
      }
      var catId = spec.getCatalogId();
      ensureKind(catId, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
      var catalog = CatalogOverlayGuards.requireVisibleCatalogNode(overlay, catId, corr);
      CatalogOverlayGuards.rejectSystemCatalogMutation(catalog.id(), corr);
      b.setCatalogId(catId);
      catalogChanged = true;
    }

    if (maskTargets(mask, "namespace_id")) {
      if (!spec.hasNamespaceId()) {
        throw GrpcErrors.invalidArgument(corr, NAMESPACE_ID_CANNOT_CLEAR, Map.of());
      }
      var nsId = spec.getNamespaceId();
      ensureKind(nsId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", corr);
      var ns = CatalogOverlayGuards.requireWritableNamespaceNode(overlay, nsId, corr);

      var effectiveCatalogId = catalogChanged ? b.getCatalogId() : current.getCatalogId();
      var nsCatalogId = ns.catalogId();
      if (nsCatalogId == null || !nsCatalogId.getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            NAMESPACE_CATALOG_MISMATCH,
            Map.of(
                "namespace_id", nsId.getId(),
                "namespace.catalog_id", nsCatalogId == null ? "" : nsCatalogId.getId(),
                "catalog_id", effectiveCatalogId.getId()));
      }
      b.setNamespaceId(nsId);
      namespaceChanged = true;
    }

    if (catalogChanged && !namespaceChanged) {
      var effectiveCatalogId = b.getCatalogId();
      var ns = CatalogOverlayGuards.requireWritableNamespaceNode(overlay, b.getNamespaceId(), corr);
      var nsCatalogId = ns.catalogId();
      if (nsCatalogId == null || !nsCatalogId.getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            NAMESPACE_CATALOG_MISMATCH,
            Map.of(
                "namespace_id", b.getNamespaceId().getId(),
                "namespace.catalog_id", nsCatalogId == null ? "" : nsCatalogId.getId(),
                "catalog_id", effectiveCatalogId.getId()));
      }
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

  private static byte[] canonicalFingerprint(ViewSpec s) {
    List<ViewSqlDefinition> definitions = normalizeSqlDefinitions(s, "");
    return new Canonicalizer()
        .scalar("cat", nullSafeId(s.getCatalogId()))
        .scalar("ns", nullSafeId(s.getNamespaceId()))
        .scalar("name", normalizeName(s.getDisplayName()))
        .scalar("description", s.getDescription())
        .list(
            "sql_definitions",
            definitions.stream().map(def -> def.getDialect() + ":" + def.getSql()).toList())
        .map("properties", s.getPropertiesMap())
        .bytes();
  }

  private static ViewSpec specFromView(View view) {
    ViewSpec.Builder builder =
        ViewSpec.newBuilder()
            .setCatalogId(view.getCatalogId())
            .setNamespaceId(view.getNamespaceId())
            .setDisplayName(normalizeName(view.getDisplayName()))
            .setDescription(view.getDescription())
            .putAllProperties(view.getPropertiesMap())
            .addAllBaseRelations(view.getBaseRelationsList())
            .addAllCreationSearchPath(view.getCreationSearchPathList())
            .addAllOutputColumns(view.getOutputColumnsList());
    normalizeSqlDefinitions(view).forEach(builder::addSqlDefinitions);
    return builder.build();
  }

  private static List<ViewSqlDefinition> mergeSqlDefinitions(
      View current, ViewSpec spec, FieldMask mask, String corr) {
    if (!maskTargets(mask, "sql_definitions")) {
      return normalizeSqlDefinitions(current);
    }
    return normalizeSqlDefinitions(spec, corr);
  }

  private static void applySqlDefinitions(View.Builder view, ViewSpec spec, String corr) {
    List<ViewSqlDefinition> definitions = normalizeSqlDefinitions(spec, corr);
    if (definitions.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, SQL_CANNOT_CLEAR, Map.of());
    }
    view.clearSqlDefinitions();
    definitions.forEach(view::addSqlDefinitions);
  }

  private static List<ViewSqlDefinition> normalizeSqlDefinitions(ViewSpec spec, String corr) {
    List<ViewSqlDefinition> definitions =
        spec.getSqlDefinitionsList().stream()
            .filter(def -> def != null && !def.getSql().isBlank())
            .map(
                def ->
                    ViewSqlDefinition.newBuilder()
                        .setSql(requireNonEmptySql(def.getSql(), "spec.sql_definitions.sql", corr))
                        .setDialect(def.getDialect())
                        .build())
            .toList();
    if (!definitions.isEmpty()) {
      return definitions;
    }
    return List.of();
  }

  private static List<ViewSqlDefinition> normalizeSqlDefinitions(View view) {
    return view.getSqlDefinitionsList();
  }

  private static Optional<ViewSqlDefinition> preferredSqlDefinition(
      List<ViewSqlDefinition> definitions) {
    return definitions.stream()
        .filter(def -> def != null && !def.getSql().isBlank())
        .sorted(
            (left, right) ->
                Integer.compare(
                    definitionPriority(left.getDialect()), definitionPriority(right.getDialect())))
        .findFirst();
  }

  private static int definitionPriority(String dialect) {
    if (dialect == null) {
      return 3;
    }
    return switch (dialect.trim().toLowerCase()) {
      case "floe" -> 0;
      case "ansi" -> 1;
      case "spark" -> 2;
      default -> 3;
    };
  }

  private static String requireNonEmptySql(String sql, String fieldName, String corr) {
    if (sql == null || sql.isBlank()) {
      throw GrpcErrors.invalidArgument(corr, SQL_CANNOT_CLEAR, Map.of("field", fieldName));
    }
    return sql;
  }
}
