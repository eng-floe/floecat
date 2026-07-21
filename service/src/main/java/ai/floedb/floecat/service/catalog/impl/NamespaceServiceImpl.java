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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceService;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceNamespaces;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.common.PersistedSecretPropertyValidator;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class NamespaceServiceImpl extends BaseServiceImpl implements NamespaceService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;
  @Inject RecursiveResourceDropper recursiveDropper;

  // Overlay gives access to system namespaces (and other system objects)
  @Inject CatalogOverlay overlay;

  private static final Set<String> NAMESPACE_MUTABLE_PATHS =
      Set.of("display_name", "description", "path", "policy_ref", "properties", "catalog_id");

  private static final Logger LOG = Logger.getLogger(NamespaceService.class);

  // ---------- RPCs ----------

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
    var L = LogHelper.start(LOG, "ListNamespaces");

    return mapFailures(
            run(
                () -> {
                  var princ = principal.get();
                  authz.require(princ, "namespace.read");
                  return namespaceSurface()
                      .listNamespaces(request, princ.getAccountId(), correlationId());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static boolean isImmediateChildOf(Namespace ns, List<String> parentPath) {
    return isImmediateChildOf(ns.getParentsList(), parentPath);
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

  private static ArrayList<String> append(List<String> parents, String last) {
    var pp = new ArrayList<String>(parents.size() + 1);
    pp.addAll(parents);
    pp.add(last);
    return pp;
  }

  @Override
  public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    var L = LogHelper.start(LOG, "GetNamespace");

    return mapFailures(
            run(
                () -> {
                  var princ = principal.get();
                  authz.require(princ, "namespace.read");
                  return namespaceSurface().getNamespace(request, correlationId());
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateNamespaceResponse> createNamespace(CreateNamespaceRequest request) {
    var L = LogHelper.start(LOG, "CreateNamespace");

    return mapFailures(
            runWithRetry(
                () -> {
                  var princ = principal.get();
                  var accountId = princ.getAccountId();
                  var correlationId = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  var spec = request.getSpec();
                  PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                      spec.getPropertiesMap(), correlationId, "spec.properties");
                  var writePolicy = catalogSurfaceWritePolicy();
                  var catalog =
                      writePolicy.requireWritableCatalog(
                          spec.getCatalogId(), "spec.catalog_id", correlationId);
                  String catalogName = catalog.displayName();

                  var tsNow = nowTs();

                  String displayWork =
                      mustNonEmpty(spec.getDisplayName(), "display_name", correlationId);
                  List<String> parentsWork = new ArrayList<>(spec.getPathList());

                  final String display = normalizeName(displayWork);
                  if (display.isBlank()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.DISPLAY_NAME_CANNOT_CLEAR,
                        Map.of());
                  }
                  var normalizedParents = new ArrayList<String>(parentsWork.size());
                  for (String seg : parentsWork) {
                    var s = normalizeName(seg);
                    if (s.isBlank()) {
                      throw GrpcErrors.invalidArgument(
                          correlationId,
                          GeneratedErrorMessages.MessageKey.PATH_SEGMENT_BLANK,
                          Map.of());
                    }
                    normalizedParents.add(s);
                  }
                  final List<String> parents = List.copyOf(normalizedParents);
                  final List<String> fullPath = new ArrayList<>(parents);
                  fullPath.add(display);

                  writePolicy.requireNamespacePathWriteEligible(
                      spec.getCatalogId(), fullPath, correlationId);

                  final byte[] fingerprint =
                      canonicalFingerprint(spec.getCatalogId(), parents, display, spec);

                  if (!request.hasIdempotency() || request.getIdempotency().getKey().isBlank()) {
                    var existing =
                        namespaceRepo.getByPath(accountId, spec.getCatalogId().getId(), fullPath);
                    if (existing.isPresent()) {
                      throw GrpcErrors.alreadyExists(
                          correlationId,
                          GeneratedErrorMessages.MessageKey.NAMESPACE_ALREADY_EXISTS,
                          Map.of("catalog", catalogName, "path", String.join(".", fullPath)));
                    }
                  }

                  final String idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : null;

                  var namespaceProto =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateNamespace",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    if (!parents.isEmpty()) {
                                      ensurePathChainExists(
                                          accountId,
                                          spec.getCatalogId(),
                                          parents,
                                          tsNow,
                                          correlationId);
                                    }

                                    var namespaceId =
                                        randomResourceId(accountId, ResourceKind.RK_NAMESPACE);

                                    var built =
                                        Namespace.newBuilder()
                                            .setResourceId(namespaceId)
                                            .setDisplayName(display)
                                            .clearParents()
                                            .addAllParents(parents)
                                            .setDescription(spec.getDescription())
                                            .putAllProperties(spec.getPropertiesMap())
                                            .setCatalogId(spec.getCatalogId())
                                            .setCreatedAt(tsNow)
                                            .build();

                                    try {
                                      namespaceRepo.create(built);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      var existingOpt =
                                          namespaceRepo.getByPath(
                                              accountId, spec.getCatalogId().getId(), fullPath);
                                      if (existingOpt.isPresent()) {
                                        var existing = existingOpt.get();
                                        var existingSpec = specFromNamespace(existing);
                                        var existingFingerprint =
                                            canonicalFingerprint(
                                                existing.getCatalogId(),
                                                existing.getParentsList(),
                                                existing.getDisplayName(),
                                                existingSpec);
                                        if (Arrays.equals(fingerprint, existingFingerprint)) {
                                          markerStore.bumpCatalogMarker(existing.getCatalogId());
                                          bumpParentNamespaceMarker(
                                              accountId,
                                              existing.getCatalogId(),
                                              existing.getParentsList());
                                          metadataGraph.invalidate(existing.getResourceId());
                                          topology.evictNamespaceRefs(existing.getCatalogId());
                                          return new IdempotencyGuard.CreateResult<>(
                                              existing, existing.getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.alreadyExists(
                                          correlationId,
                                          GeneratedErrorMessages.MessageKey
                                              .NAMESPACE_ALREADY_EXISTS,
                                          Map.of(
                                              "catalog",
                                              catalogName,
                                              "path",
                                              String.join(".", fullPath)));
                                    }
                                    markerStore.bumpCatalogMarker(spec.getCatalogId());
                                    bumpParentNamespaceMarker(
                                        accountId, spec.getCatalogId(), parents);
                                    metadataGraph.invalidate(namespaceId);
                                    topology.evictNamespaceRefs(spec.getCatalogId());
                                    return new IdempotencyGuard.CreateResult<>(built, namespaceId);
                                  },
                                  (ns) -> namespaceRepo.metaForSafe(ns.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Namespace::parseFrom));

                  return CreateNamespaceResponse.newBuilder()
                      .setNamespace(namespaceProto.body)
                      .setMeta(namespaceProto.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private void ensurePathChainExists(
      String accountId,
      ResourceId catalogId,
      List<String> parents,
      com.google.protobuf.Timestamp tsNow,
      String corr) {

    var chain = new ArrayList<String>(parents.size());
    for (String segRaw : parents) {
      var seg = normalizeName(segRaw);
      if (seg.isBlank()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.PATH_SEGMENT_BLANK, Map.of());
      }
      chain.add(seg);

      var existing = namespaceRepo.getByPath(accountId, catalogId.getId(), chain);
      if (existing.isPresent()) {
        continue;
      }

      var rid = randomResourceId(accountId, ResourceKind.RK_NAMESPACE);

      var parentList = chain.size() > 1 ? chain.subList(0, chain.size() - 1) : List.<String>of();
      var display = chain.get(chain.size() - 1);

      var ns =
          Namespace.newBuilder()
              .setResourceId(rid)
              .setCatalogId(catalogId)
              .clearParents()
              .addAllParents(parentList)
              .setDisplayName(display)
              .setCreatedAt(tsNow)
              .build();
      try {
        namespaceRepo.create(ns);
        markerStore.bumpCatalogMarker(catalogId);
        bumpParentNamespaceMarker(accountId, catalogId, parentList);
        metadataGraph.invalidate(rid);
        topology.evictNamespaceRefs(catalogId);
      } catch (BaseResourceRepository.NameConflictException nce) {
        if (namespaceRepo.getByPath(accountId, catalogId.getId(), chain).isPresent()) {
          continue;
        }
        throw nce;
      }
    }
  }

  @Override
  public Uni<UpdateNamespaceResponse> updateNamespace(UpdateNamespaceRequest request) {
    var L = LogHelper.start(LOG, "UpdateNamespace");

    return mapFailures(
            runWithRetry(
                () -> {
                  var princ = principal.get();
                  var corr = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  var nsId = request.getNamespaceId();
                  catalogSurfaceWritePolicy().requireWritableNamespace(nsId, corr);

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(
                        corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var meta = namespaceRepo.metaFor(nsId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      namespaceRepo
                          .getById(nsId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr,
                                      GeneratedErrorMessages.MessageKey.NAMESPACE,
                                      Map.of("id", nsId.getId())));

                  var mask = normalizeMask(request.getUpdateMask());
                  var desired = applyNamespaceSpecPatch(current, request.getSpec(), mask, corr);
                  var desiredPath = new ArrayList<>(desired.getParentsList());
                  desiredPath.add(desired.getDisplayName());
                  catalogSurfaceWritePolicy()
                      .requireNamespacePathWriteEligible(desired.getCatalogId(), desiredPath, corr);
                  if (maskTargets(mask, "properties")) {
                    PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                        request.getSpec().getPropertiesMap(), corr, "spec.properties");
                  }

                  if (desired.equals(current)) {
                    var metaNoop = namespaceRepo.metaFor(nsId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && metaNoop.getPointerVersion() != meta.getPointerVersion()) {
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(metaNoop.getPointerVersion())));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateNamespaceResponse.newBuilder()
                        .setNamespace(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  var conflictPath = new ArrayList<>(desired.getParentsList());
                  conflictPath.add(desired.getDisplayName());
                  String conflictCatalog = resolveCatalogName(desired.getCatalogId());
                  var conflictInfo =
                      Map.of("catalog", conflictCatalog, "path", String.join(".", conflictPath));

                  try {
                    boolean ok = namespaceRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = namespaceRepo.metaForSafe(nsId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.alreadyExists(
                        corr,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_ALREADY_EXISTS,
                        conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = namespaceRepo.metaForSafe(nsId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }
                  topology.evictNamespaceRefs(current.getCatalogId());
                  topology.evictNamespaceRefs(desired.getCatalogId());
                  metadataGraph.invalidate(nsId);

                  var outMeta = namespaceRepo.metaForSafe(nsId);
                  var latest = namespaceRepo.getById(nsId).orElse(desired);

                  bumpParentMoveMarkers(current, desired);
                  return UpdateNamespaceResponse.newBuilder()
                      .setNamespace(latest)
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
  public Uni<DeleteNamespaceResponse> deleteNamespace(DeleteNamespaceRequest request) {
    var L = LogHelper.start(LOG, "DeleteNamespace");

    return mapFailures(
            runWithRetry(
                () -> {
                  var princ = principal.get();
                  var correlationId = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  if (request.getRecursive() && request.getRequireEmpty()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId,
                        null,
                        Map.of("reason", "recursive and require_empty cannot be combined"));
                  }
                  if (request.getRecursive()) {
                    authz.require(princ, "table.write");
                    authz.require(princ, "view.write");
                  }

                  var namespaceId = request.getNamespaceId();
                  catalogSurfaceWritePolicy().requireDeletableNamespace(namespaceId, correlationId);

                  var namespace = namespaceRepo.getById(namespaceId).orElse(null);
                  var catalogId =
                      (namespace != null && namespace.hasCatalogId())
                          ? namespace.getCatalogId()
                          : null;

                  if (catalogId == null) {
                    var safe = namespaceRepo.metaForSafe(namespaceId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(
                          correlationId,
                          GeneratedErrorMessages.MessageKey.NAMESPACE,
                          Map.of("id", namespaceId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    topology.evictRelationRefs(namespaceId);
                    metadataGraph.invalidate(namespaceId);
                    return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
                  }

                  long markerVersion = markerStore.namespaceMarkerVersion(namespaceId);

                  if (request.getRecursive()) {
                    // Check the supplied condition before deleting descendants. The final delete
                    // below still uses the same condition to catch a concurrent root mutation.
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId,
                        namespaceRepo.metaFor(namespaceId),
                        request.getPrecondition());
                    if (!markerStore.advanceNamespaceMarker(namespaceId, markerVersion)) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace children changed before recursive delete: "
                              + namespaceId.getId());
                    }
                    recursiveDropper.dropNamespaceContents(namespace);
                    if (markerStore.namespaceMarkerVersion(namespaceId) != markerVersion + 1) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace children changed during recursive delete: "
                              + namespaceId.getId());
                    }
                    markerVersion++;
                  }

                  if (tableRepo.count(
                          catalogId.getAccountId(), catalogId.getId(), namespaceId.getId())
                      > 0) {
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
                        Map.of("display_name", pretty));
                  }

                  var parentPath = append(namespace.getParentsList(), namespace.getDisplayName());
                  if (hasImmediateChildren(
                      catalogId.getAccountId(), catalogId.getId(), parentPath)) {
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
                        Map.of("display_name", pretty));
                  }

                  if (!markerStore.advanceNamespaceMarker(namespaceId, markerVersion)) {
                    if (request.getRecursive()) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace children changed during recursive delete: "
                              + namespaceId.getId());
                    }
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_CHILDREN_CHANGED,
                        Map.of());
                  }
                  var markerAfterAdvance = markerStore.namespaceMarkerVersion(namespaceId);
                  if (markerAfterAdvance != markerVersion + 1) {
                    if (request.getRecursive()) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace children changed during recursive delete: "
                              + namespaceId.getId());
                    }
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_CHILDREN_CHANGED,
                        Map.of());
                  }
                  if (tableRepo.count(
                          catalogId.getAccountId(), catalogId.getId(), namespaceId.getId())
                      > 0) {
                    if (request.getRecursive()) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace tables changed during recursive delete: "
                              + namespaceId.getId());
                    }
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
                        Map.of("display_name", pretty));
                  }
                  if (hasImmediateChildren(
                      catalogId.getAccountId(), catalogId.getId(), parentPath)) {
                    if (request.getRecursive()) {
                      throw new BaseResourceRepository.AbortRetryableException(
                          "namespace children changed during recursive delete: "
                              + namespaceId.getId());
                    }
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
                        Map.of("display_name", pretty));
                  }

                  var meta =
                      MutationOps.deleteWithPreconditions(
                          () -> namespaceRepo.metaFor(namespaceId),
                          request.getPrecondition(),
                          expected -> namespaceRepo.deleteWithPrecondition(namespaceId, expected),
                          () -> namespaceRepo.metaForSafe(namespaceId),
                          correlationId,
                          "namespace",
                          Map.of("id", namespaceId.getId()));

                  topology.evictRelationRefs(namespaceId);
                  topology.evictNamespaceRefs(catalogId);
                  metadataGraph.invalidate(namespaceId);
                  markerStore.bumpCatalogMarker(catalogId);
                  bumpParentNamespaceMarker(
                      catalogId.getAccountId(), catalogId, namespace.getParentsList());
                  return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private boolean hasImmediateChildren(
      String accountId, String catalogId, List<String> parentPath) {
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      var page = namespaceRepo.list(accountId, catalogId, parentPath, 200, cursor, next);
      for (var ns : page) {
        if (isImmediateChildOf(ns, parentPath)) {
          return true;
        }
      }
      cursor = next.toString();
      if (cursor.isBlank()) {
        break;
      }
    }
    return false;
  }

  private CatalogSurfaceNamespaces namespaceSurface() {
    return new CatalogSurfaceNamespaces(namespaceRepo, overlay);
  }

  private CatalogSurfaceWritePolicy catalogSurfaceWritePolicy() {
    return new CatalogSurfaceWritePolicy(overlay);
  }

  private static byte[] canonicalFingerprint(
      ResourceId catalogId, List<String> parents, String display, NamespaceSpec spec) {
    return new Canonicalizer()
        .scalar("cat", nullSafeId(catalogId))
        .list("parents", parents)
        .scalar("name", display)
        .scalar("description", spec.getDescription())
        .scalar("policy_ref", spec.getPolicyRef())
        .map("properties", spec.getPropertiesMap())
        .bytes();
  }

  private static NamespaceSpec specFromNamespace(Namespace namespace) {
    return NamespaceSpec.newBuilder()
        .setDisplayName(normalizeName(namespace.getDisplayName()))
        .setDescription(namespace.getDescription())
        .setPolicyRef(namespace.getPolicyRef())
        .putAllProperties(namespace.getPropertiesMap())
        .build();
  }

  private Namespace applyNamespaceSpecPatch(
      Namespace current, NamespaceSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(
          corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED, Map.of());
    }
    for (var p : paths) {
      if (!NAMESPACE_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }
    if (paths.contains("path") && paths.contains("display_name")) {
      throw GrpcErrors.invalidArgument(
          corr,
          GeneratedErrorMessages.MessageKey.UPDATE_MASK_PATH_INVALID,
          Map.of("path", "Cannot combine 'path' with 'display_name'"));
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "catalog_id")) {
      if (!spec.hasCatalogId()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.CATALOG_ID_CANNOT_CLEAR, Map.of());
      }
      var cat = spec.getCatalogId();
      catalogSurfaceWritePolicy().requireWritableCatalog(cat, "spec.catalog_id", corr);
      b.setCatalogId(cat);
    }

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.DISPLAY_NAME_CANNOT_CLEAR, Map.of());
      }
      var name = normalizeName(spec.getDisplayName());
      if (name.isBlank()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.DISPLAY_NAME_CANNOT_CLEAR, Map.of());
      }
      b.setDisplayName(name);
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) b.setDescription(spec.getDescription());
      else b.clearDescription();
    }

    if (maskTargets(mask, "policy_ref")) {
      if (spec.hasPolicyRef()) b.setPolicyRef(spec.getPolicyRef());
      else b.clearPolicyRef();
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    if (maskTargets(mask, "path")) {
      var path = spec.getPathList();
      var normalizedPath = new ArrayList<String>(path.size());
      for (var seg : path) {
        var s = normalizeName(seg);
        if (s.isBlank()) {
          throw GrpcErrors.invalidArgument(
              corr, GeneratedErrorMessages.MessageKey.PATH_SEGMENT_BLANK, Map.of());
        }
        normalizedPath.add(s);
      }
      if (normalizedPath.isEmpty()) {
        b.clearParents();
      } else {
        var leaf = normalizedPath.get(normalizedPath.size() - 1);
        var parentsOnly = normalizedPath.subList(0, normalizedPath.size() - 1);
        b.setDisplayName(leaf);
        b.clearParents().addAllParents(parentsOnly);
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

  private String resolveCatalogName(ResourceId catalogId) {
    return catalogRepo
        .getById(catalogId)
        .map(Catalog::getDisplayName)
        .filter(name -> !name.isBlank())
        .orElse(catalogId.getId());
  }

  private void bumpParentNamespaceMarker(
      String accountId, ResourceId catalogId, List<String> parentPath) {
    if (parentPath == null || parentPath.isEmpty()) {
      return;
    }
    namespaceRepo
        .getByPath(accountId, catalogId.getId(), parentPath)
        .ifPresent(ns -> markerStore.bumpNamespaceMarker(ns.getResourceId()));
  }

  private void bumpParentMoveMarkers(Namespace before, Namespace after) {
    if (before == null || after == null) {
      return;
    }

    var beforeCat = before.getCatalogId();
    var afterCat = after.getCatalogId();

    if (!beforeCat.getId().equals(afterCat.getId())) {
      markerStore.bumpCatalogMarker(beforeCat);
      markerStore.bumpCatalogMarker(afterCat);
    }

    var beforeParent = before.getParentsList();
    var afterParent = after.getParentsList();

    if (!beforeParent.equals(afterParent) || !beforeCat.getId().equals(afterCat.getId())) {
      bumpParentNamespaceMarker(beforeCat.getAccountId(), beforeCat, beforeParent);
      bumpParentNamespaceMarker(afterCat.getAccountId(), afterCat, afterParent);
    }
  }
}
