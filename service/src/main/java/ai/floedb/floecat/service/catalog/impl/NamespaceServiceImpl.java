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
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
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
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.grpc.StatusRuntimeException;
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
  @Inject ViewRepository viewRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;

  // The graph view gives access to system namespaces (and other system objects).
  @Inject CatalogGraphView graphView;

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

                  if (!parents.isEmpty()) {
                    ensurePathChainExists(
                        accountId, spec.getCatalogId(), parents, tsNow, correlationId);
                  }

                  var existingForIdempotency =
                      namespaceRepo.getByPath(accountId, spec.getCatalogId().getId(), fullPath);

                  var namespaceProto =
                      MutationOps.createProtoRecoverable(
                          accountId,
                          "CreateNamespace",
                          idempotencyKey,
                          () -> fingerprint,
                          () ->
                              existingForIdempotency
                                  .filter(
                                      existing -> {
                                        var existingSpec = specFromNamespace(existing);
                                        return Arrays.equals(
                                            fingerprint,
                                            canonicalFingerprint(
                                                existing.getCatalogId(),
                                                existing.getParentsList(),
                                                existing.getDisplayName(),
                                                existingSpec));
                                      })
                                  .map(Namespace::getResourceId)
                                  .orElseGet(
                                      () -> randomResourceId(accountId, ResourceKind.RK_NAMESPACE)),
                          (reservedId, committer) -> {
                            var built =
                                Namespace.newBuilder()
                                    .setResourceId(reservedId)
                                    .setDisplayName(display)
                                    .clearParents()
                                    .addAllParents(parents)
                                    .setDescription(spec.getDescription())
                                    .putAllProperties(spec.getPropertiesMap())
                                    .setCatalogId(spec.getCatalogId())
                                    .setCreatedAt(tsNow)
                                    .build();
                            var existingOpt =
                                namespaceRepo.getByPath(
                                    accountId, spec.getCatalogId().getId(), fullPath);
                            if (existingOpt.isPresent()) {
                              var existing = existingOpt.get();
                              var existingFingerprint =
                                  canonicalFingerprint(
                                      existing.getCatalogId(),
                                      existing.getParentsList(),
                                      existing.getDisplayName(),
                                      specFromNamespace(existing));
                              if (!existing.getResourceId().equals(reservedId)
                                  || !Arrays.equals(fingerprint, existingFingerprint)) {
                                throw GrpcErrors.alreadyExists(
                                    correlationId,
                                    GeneratedErrorMessages.MessageKey.NAMESPACE_ALREADY_EXISTS,
                                    Map.of(
                                        "catalog",
                                        catalogName,
                                        "path",
                                        String.join(".", fullPath)));
                              }
                              var currentMeta = namespaceRepo.metaForSafe(reservedId);
                              var completed =
                                  namespaceRepo.completeWithMetaIfUnchanged(
                                      existing,
                                      currentMeta.getPointerVersion(),
                                      resource ->
                                          committer.prepareSuccessOps(
                                              new IdempotencyGuard.CommittedCreate<>(
                                                  resource.value(), reservedId, resource.meta())));
                              if (completed.isEmpty()) {
                                throw new BaseResourceRepository.AbortRetryableException(
                                    "namespace changed while committing idempotency receipt");
                              }
                              return new IdempotencyGuard.CommittedCreate<>(
                                  existing, reservedId, completed.get());
                            }
                            try {
                              var committed =
                                  retryWhileFenceLostForResult(
                                      "create namespace",
                                      () ->
                                          namespaceRepo.createWithCompletionWhilePointersMatch(
                                              built,
                                              namespaceCreateFence(spec.getCatalogId(), parents),
                                              resource ->
                                                  committer.prepareSuccessOps(
                                                      new IdempotencyGuard.CommittedCreate<>(
                                                          resource.value(),
                                                          reservedId,
                                                          resource.meta()))));
                              return new IdempotencyGuard.CommittedCreate<>(
                                  committed.value(), reservedId, committed.meta());
                            } catch (BaseResourceRepository.NameConflictException nce) {
                              throw GrpcErrors.alreadyExists(
                                  correlationId,
                                  GeneratedErrorMessages.MessageKey.NAMESPACE_ALREADY_EXISTS,
                                  Map.of(
                                      "catalog", catalogName, "path", String.join(".", fullPath)));
                            }
                          },
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Namespace::parseFrom);

                  metadataGraph.invalidate(namespaceProto.body.getResourceId());
                  topology.evictNamespaceRefs(namespaceProto.body.getCatalogId());

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
        retryWhileFenceLost(
            "create namespace chain",
            () ->
                namespaceRepo.createWhilePointersMatch(
                    ns, namespaceCreateFence(catalogId, parentList)));
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

                  // A change to catalog, parents or display name re-keys this namespace, and
                  // every descendant derives its own key from that same identity. Nothing
                  // re-derives theirs, so the move would leave them addressable only under a path
                  // that no longer exists -- silently, with the call returning OK. Relations are
                  // unaffected by a rename: their key carries the namespace id, not its path. They
                  // are affected by a catalog move, because the catalog IS in their key.
                  PointerConditions restructureFence =
                      restructureFenceOrRefuse(corr, current, desired, nsId);

                  try {
                    boolean ok =
                        namespaceRepo
                            .updateWhilePointersMatch(
                                desired, meta.getPointerVersion(), restructureFence)
                            .isPresent();
                    if (!ok) {
                      throw MutationOps.lostFenceOrVersionMismatch(
                          corr,
                          "namespace restructure",
                          meta.getPointerVersion(),
                          namespaceRepo.metaForSafe(nsId).getPointerVersion());
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

                  // Sampled BEFORE the checks below. A version read after them is the version a
                  // concurrent write already moved, so the CAS would confirm that write instead of
                  // losing to it -- the window this is here to close.
                  var shapeMarkers = markerStore.namespaceShapeMarkers(namespaceId);

                  if (relationCount(catalogId, namespaceId) > 0) {
                    throw namespaceNotEmpty(correlationId, namespace);
                  }

                  // Checked once each, then fenced. Both markers ride the delete batch below, and
                  // every writer that adds a child namespace or a relation asserts the matching one
                  // in its own batch -- the services, the reconciler, and the transaction applier.
                  // So anything appearing after these checks loses that CAS, or costs this delete
                  // its own; a second check before the commit would narrow nothing.
                  var parentPath = append(namespace.getParentsList(), namespace.getDisplayName());
                  if (namespaceRepo.hasDescendants(
                      catalogId.getAccountId(), catalogId.getId(), parentPath)) {
                    throw namespaceNotEmpty(correlationId, namespace);
                  }

                  var meta =
                      MutationOps.deleteWithPreconditions(
                          () -> namespaceRepo.metaFor(namespaceId),
                          request.getPrecondition(),
                          expected ->
                              namespaceRepo.deleteWhileShapeUnchanged(
                                  namespaceId, expected, shapeMarkers),
                          () -> namespaceRepo.metaForSafe(namespaceId),
                          correlationId,
                          "namespace",
                          Map.of("id", namespaceId.getId()));

                  topology.evictRelationRefs(namespaceId);
                  topology.evictNamespaceRefs(catalogId);
                  metadataGraph.invalidate(namespaceId);
                  return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private CatalogSurfaceNamespaces namespaceSurface() {
    return new CatalogSurfaceNamespaces(namespaceRepo, graphView);
  }

  private CatalogSurfaceWritePolicy catalogSurfaceWritePolicy() {
    return new CatalogSurfaceWritePolicy(graphView);
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

  /**
   * The create fence, with an absent catalog and an absent parent classified apart.
   *
   * <p>They are not alike. A parent namespace's path can be recreated, so a retry can succeed and a
   * lost fence is the honest answer. A catalog id never recurs, so once its row is gone every
   * attempt is guaranteed to fail -- reporting that as retryable spends the whole budget and ends
   * as ABORTED on a request no retry can fix, so it is terminal.
   *
   * <p>Terminal is also what releases the idempotency reservation: {@code IdempotencyGuard} deletes
   * a PENDING record for a non-retryable failure and keeps it for a retryable one. Classifying a
   * dead catalog as retryable would be what wedges the caller's key until TTL.
   */
  private PointerConditions namespaceCreateFence(ResourceId catalogId, List<String> parentPath) {
    try {
      return namespaceRepo.createFence(markerStore, catalogId, parentPath);
    } catch (BaseResourceRepository.NotFoundException absent) {
      if (absent.kind() == ResourceKind.RK_CATALOG) {
        throw absent;
      }
      throw BaseResourceRepository.AbortRetryableException.lostFence(
          "the parent was deleted while joining its child set");
    }
  }

  /**
   * A namespace still holds something that would be orphaned by removing or re-keying it.
   *
   * <p>Rendered from the full path, so a nested namespace names itself the same way whichever
   * operation refused it.
   */
  private StatusRuntimeException namespaceNotEmpty(String corr, Namespace ns) {
    return GrpcErrors.conflict(
        corr,
        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
        Map.of("display_name", prettyNamespacePath(ns.getParentsList(), ns.getDisplayName())));
  }

  private int relationCount(ResourceId catalogId, ResourceId namespaceId) {
    return NamespaceRepository.relationCount(tableRepo, viewRepo, catalogId, namespaceId);
  }

  /**
   * The fence for a restructuring update, after refusing the ones that cannot be made safe.
   *
   * <p>Order matters and is the whole point. A malformed request is classified first, because
   * resolving the fence reports a missing parent as a lost fence and a path that never existed
   * would otherwise burn the retry budget and end as ABORTED. The fence is then sampled before the
   * emptiness checks, because a version read after them is the version a concurrent write already
   * moved -- the CAS would confirm that write instead of losing to it.
   */
  private PointerConditions restructureFenceOrRefuse(
      String corr, Namespace current, Namespace desired, ResourceId nsId) {
    if (!movesDerivedIdentity(current, desired)) {
      return PointerConditions.none();
    }
    var ownPath = append(current.getParentsList(), current.getDisplayName());
    boolean changesCatalog = changesCatalog(current, desired);

    // A namespace cannot be moved beneath itself: the destination parent resolves to this very
    // namespace, so parent-exists and no-children both pass, and the write then vacates the path it
    // just claimed to live under -- stranding itself, with no concurrency involved. Only within one
    // catalog: the same segments under a different catalog name a different namespace entirely.
    if (!changesCatalog
        && desired.getParentsList().size() >= ownPath.size()
        && desired.getParentsList().subList(0, ownPath.size()).equals(ownPath)) {
      throw GrpcErrors.invalidArgument(
          corr,
          GeneratedErrorMessages.MessageKey.NAMESPACE_PATH_BENEATH_ITSELF,
          Map.of("path", String.join(".", desired.getParentsList())));
    }

    // Sampled before the emptiness checks below: a version read after those is the version a
    // concurrent write already moved, so the CAS would confirm that write instead of losing to it.
    //
    // A destination parent that does not resolve is the caller naming a path that is not there --
    // NOT_FOUND, not contention. This request is not retried by the fence loop, so reporting it as
    // retryable would spend the whole budget and end as ABORTED on a request no retry can fix.
    PointerConditions fence;
    try {
      fence = restructureFence(current, desired);
    } catch (BaseResourceRepository.NotFoundException absent) {
      // Naming the wrong one sends the caller looking for a namespace when their catalog is what is
      // gone -- and for a move to a catalog's root the parent path is empty, so it would name
      // nothing at all. The refusal says which it was, so nothing has to be read back to find out.
      throw absent.kind() == ResourceKind.RK_CATALOG
          ? GrpcErrors.notFound(
              corr,
              GeneratedErrorMessages.MessageKey.CATALOG,
              Map.of("id", desired.getCatalogId().getId()))
          : GrpcErrors.notFound(
              corr,
              GeneratedErrorMessages.MessageKey.NAMESPACE,
              Map.of("id", String.join(".", desired.getParentsList())));
    }

    if (namespaceRepo.hasDescendants(
        current.getCatalogId().getAccountId(), current.getCatalogId().getId(), ownPath)) {
      throw namespaceNotEmpty(corr, current);
    }
    if (changesCatalog && relationCount(current.getCatalogId(), nsId) > 0) {
      throw namespaceNotEmpty(corr, current);
    }
    return fence;
  }

  /**
   * The fence for restructuring a namespace: its own child set, and the one it is joining.
   *
   * <p>Its own, because its children derive their keys from the identity being changed. The
   * destination parent's, because the move joins that parent's child set exactly as a create does
   * -- and without it, renaming a parent and re-parenting a child into it fence different markers
   * and both commit, leaving the child under a name that no longer exists.
   */
  private PointerConditions restructureFence(Namespace current, Namespace desired) {
    // A rename re-keys only what derives its key from the path, which is the child namespaces. A
    // catalog move also re-keys every relation, because the catalog is in a relation's key too.
    boolean changesCatalog = changesCatalog(current, desired);

    // The destination's child set is the same pair a create joins, so it is composed by the same
    // method rather than rebuilt here. That matters twice over. The catalog half is
    // UNCONDITIONAL: a same-catalog rename whose destination is the catalog root has an empty
    // parent path, so the parent half is empty and the catalog's own child set is the only thing
    // covering that destination -- exactly what the docs claim for a top-level namespace. And
    // createFence resolves the catalog before the parent, which this path needs: both can be gone,
    // whichever resolves first is the refusal that surfaces, and reporting a missing NAMESPACE to a
    // caller whose CATALOG disappeared is the misdirection the classification exists to avoid.
    // Composed there, that ordering is structural instead of an accident of statement order here.
    //
    // createFence uses the repository entry point rather than the create-path translator beside it:
    // that one reports an absent parent as contention, which is right for a create and wrong here.
    // A caller who named a path that is not there gets NOT_FOUND, classified in
    // restructureFenceOrRefuse where the choice is visible.
    var destination =
        changesCatalog
            // Gains the destination catalog a child, exactly as a create does, so it takes the
            // create's fence -- and a catalog move re-keys the relations too, because the catalog
            // is in their key.
            ? namespaceRepo
                .createFence(markerStore, desired.getCatalogId(), desired.getParentsList())
                .and(markerStore.relationsFence(current.getResourceId()))
            // Within one catalog, only the parent's child set changes. NOT the catalog's: its set
            // of
            // namespaces is the same set before and after, so a concurrent DeleteCatalog counts
            // this
            // namespace and refuses either way -- its emptiness check reads the whole catalog
            // prefix, which this row never leaves. Asserting the catalog marker anyway would make
            // every rename contend with sibling creates and cost a concurrent DeleteCatalog its CAS
            // for a decision it was going to make identically. Reviewed twice in opposite
            // directions: it is left out because it excludes nothing, not because it is cheaper.
            : namespaceRepo.childSetFenceForParent(
                markerStore,
                desired.getCatalogId().getAccountId(),
                desired.getCatalogId().getId(),
                desired.getParentsList());

    return destination.and(markerStore.childNamespacesFence(current.getResourceId()));
  }

  /** Whether the update moves the namespace to a different catalog. */
  private static boolean changesCatalog(Namespace current, Namespace desired) {
    return !current.getCatalogId().getId().equals(desired.getCatalogId().getId());
  }

  /**
   * Whether this update moves the key that everything below the namespace derives from.
   *
   * <p>A namespace's by-path pointer is built from {@code catalogId + parents + displayName}, so a
   * change to any of the three re-keys it -- while its children keep pointers under the old path,
   * because a repository recomputes secondaries only for the row it writes.
   */
  private static boolean movesDerivedIdentity(Namespace current, Namespace desired) {
    return changesCatalog(current, desired)
        || !current.getParentsList().equals(desired.getParentsList())
        || !current.getDisplayName().equals(desired.getDisplayName());
  }
}
