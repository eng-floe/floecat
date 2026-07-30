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
import ai.floedb.floecat.common.rpc.MutationMeta;
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
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
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
  @Inject ViewRepository viewRepo;
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
                                      // Publishing a child namespace is fenced exactly like a
                                      // relation create: the guard advances the parent's children
                                      // marker inside this batch, so a concurrent DeleteNamespace
                                      // on the parent cannot also commit (see BatchGuard).
                                      namespaceRepo.create(
                                          built,
                                          parentNamespaceGuard(
                                              accountId, spec.getCatalogId(), parents));
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
                                    // The parent's children marker was advanced inside the create
                                    // batch by the guard above; only the catalog marker is left.
                                    markerStore.bumpCatalogMarker(spec.getCatalogId());
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
        // Each implicitly created level is a child publish like any other and is fenced on the
        // level above it, so a concurrent delete partway down the chain cannot leave the rest of
        // the chain orphaned. The guard advances the parent's marker inside the create batch.
        namespaceRepo.create(ns, parentNamespaceGuard(accountId, catalogId, parentList));
        markerStore.bumpCatalogMarker(catalogId);
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

                  // Moving a namespace under a different parent republishes it as that parent's
                  // child, so the destination gets the same fence a create would; an in-place
                  // rename or property edit keeps its parent and stays unguarded.
                  boolean reparented =
                      !current.getParentsList().equals(desired.getParentsList())
                          || !current.getCatalogId().getId().equals(desired.getCatalogId().getId());
                  var destinationGuard =
                      reparented
                          ? reparentDestinationGuard(
                              desired.getResourceId().getAccountId(),
                              desired.getCatalogId(),
                              desired.getParentsList(),
                              corr)
                          : BatchGuard.NONE;

                  try {
                    boolean ok =
                        namespaceRepo.update(desired, meta.getPointerVersion(), destinationGuard);
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

                  // Bumps the source and destination parent markers plus the catalog markers. On a
                  // reparent the destination was already advanced inside the update batch by the
                  // guard; advancing it a second time is harmless (markers are opaque monotonic
                  // counters that every reader re-reads) and keeps this one call responsible for
                  // the whole move.
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

    // Destruction accumulated across every attempt of the retried body below. An attempt that drops
    // most of the subtree and then aborts is still destruction the caller has to be told about, so
    // this cannot live inside the lambda: a later attempt starting from a near-empty subtree would
    // report near-zero counts for a subtree that is actually gone. Confined to this one call.
    var destroyed = new RecursiveResourceDropper.DropSummary();

    // Resolved here, on the request thread, not inside the failure transform below. That transform
    // runs after the retries, off the worker hop and outside the context run() re-establishes per
    // subscription, so reading the principal there can itself throw — destroying the
    // partial-teardown
    // report this is all for.
    final String failureCorrelationId = correlationId();

    // Recursive delete performs a large amount of blocking storage I/O and can raise
    // AbortRetryableException; runWithRetryOnWorker keeps the retry re-subscription off the Vert.x
    // event loop, where that blocking work would fail with "current thread cannot be blocked".
    return mapFailures(
            runWithRetryOnWorker(
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
                      // The emptiness gate reconciles stranded relation index rows before trusting
                      // its own count, which deletes pointers belonging to tables and views. Only a
                      // caller who could have deleted those relations outright may do so: recursive
                      // delete has just proven it, and a plain delete has to be asked. Without the
                      // grant the reconcile is skipped and the leftover rows make the namespace
                      // report NOT_EMPTY, which is the honest answer for a caller who cannot clear
                      // them.
                      boolean mayReclaimRelationRows =
                          authz.allows(princ, "table.write") && authz.allows(princ, "view.write");

                      var namespaceId = request.getNamespaceId();

                      // Both of these resolve the namespace through its blob — the write policy via
                      // the overlay, then the read here — so an unreadable blob fails before any
                      // gate
                      // below can run.
                      final Namespace namespace;
                      try {
                        catalogSurfaceWritePolicy()
                            .requireDeletableNamespace(namespaceId, correlationId);
                        namespace = namespaceRepo.getById(namespaceId).orElse(null);
                      } catch (BaseResourceRepository.CorruptionException unreadable) {
                        throw namespaceBlobUnreadable(namespaceId, correlationId, unreadable);
                      }
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
                        // Check the supplied condition before deleting descendants. The final
                        // delete
                        // below still uses the same condition to catch a concurrent root mutation.
                        // A
                        // failure here after an earlier attempt already destroyed part of the
                        // subtree is
                        // relabelled as partial teardown on the way out, along with every other way
                        // this
                        // operation can fail.
                        MutationOps.BaseServiceChecks.enforcePreconditions(
                            correlationId,
                            namespaceRepo.metaFor(namespaceId),
                            request.getPrecondition());
                        if (!markerStore.advanceNamespaceMarker(namespaceId, markerVersion)) {
                          throw new BaseResourceRepository.AbortRetryableException(
                              "namespace children changed before recursive delete: "
                                  + namespaceId.getId());
                        }
                        recursiveDropper.dropNamespaceContents(namespace, destroyed);
                        if (markerStore.namespaceMarkerVersion(namespaceId) != markerVersion + 1) {
                          throw new BaseResourceRepository.AbortRetryableException(
                              "namespace children changed during recursive delete: "
                                  + namespaceId.getId());
                        }
                        markerVersion++;
                      }

                      var parentPath =
                          append(namespace.getParentsList(), namespace.getDisplayName());
                      requireNamespaceEmpty(
                          request,
                          namespace,
                          catalogId,
                          parentPath,
                          mayReclaimRelationRows,
                          correlationId);

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
                      // Same check again, after the marker advance: the scans are reads and cannot
                      // join the delete's CAS batch, so emptiness has to be established on both
                      // sides
                      // of
                      // the fence.
                      requireNamespaceEmpty(
                          request,
                          namespace,
                          catalogId,
                          parentPath,
                          mayReclaimRelationRows,
                          correlationId);

                      if (request.getRecursive()) {
                        // Descendants may already be irreversibly gone, so check the caller's
                        // condition
                        // on the root before the delete rather than letting the delete's own check
                        // decide
                        // — both end up relabelled with the counts, but failing here keeps the
                        // final
                        // delete from being attempted against a root the caller no longer
                        // recognises.
                        MutationOps.BaseServiceChecks.enforcePreconditions(
                            correlationId,
                            namespaceRepo.metaForSafe(namespaceId),
                            request.getPrecondition());
                      }

                      // The emptiness scans above are reads, and no read can be part of a CAS
                      // batch, so
                      // on their own they always leave a window in which a child is published after
                      // the
                      // last scan but before the pointer goes away. Carrying the marker into the
                      // delete
                      // batch closes it: every child-publishing write advances this marker inside
                      // its
                      // own batch (see BatchGuard), so a child that slipped past the scan makes
                      // this
                      // delete fail rather than orphaning itself under a namespace that no longer
                      // exists.
                      final long fencedMarkerVersion = markerVersion + 1;
                      final var childrenGuard =
                          markerStore.namespaceDeleteGuard(namespaceId, fencedMarkerVersion);

                      final MutationMeta meta;
                      try {
                        meta =
                            MutationOps.deleteWithPreconditions(
                                () -> namespaceRepo.metaFor(namespaceId),
                                request.getPrecondition(),
                                expected ->
                                    namespaceRepo.deleteWithPrecondition(
                                        namespaceId, expected, childrenGuard),
                                () -> namespaceRepo.metaForSafe(namespaceId),
                                correlationId,
                                "namespace",
                                Map.of("id", namespaceId.getId()));
                      } catch (BaseResourceRepository.BatchGuardFailedException childAppeared) {
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

                      // This namespace's own marker goes with it. The dropper does the same for
                      // every
                      // descendant it removes, but the root is deleted here, so without this the
                      // row
                      // survives every delete — see MarkerStore#deleteNamespaceMarker.
                      markerStore.deleteNamespaceMarker(namespaceId);
                      topology.evictRelationRefs(namespaceId);
                      topology.evictNamespaceRefs(catalogId);
                      metadataGraph.invalidate(namespaceId);
                      markerStore.bumpCatalogMarker(catalogId);
                      bumpParentNamespaceMarker(
                          catalogId.getAccountId(), catalogId, namespace.getParentsList());
                      return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
                    })
                // Outside the retries on purpose. The likeliest way a contended recursive delete
                // fails is a repeated AbortRetryableException exhausting the budget, and that
                // never re-enters the body — nor do CorruptionException, an immutability refusal,
                // or the page-token guard. Relabelling here catches every one of them, so a
                // caller whose subtree has just been destroyed is never handed a bare "retryable
                // conflict, nothing committed".
                .onFailure()
                .transform(
                    failed -> partialTeardownIfDestroyed(failed, destroyed, failureCorrelationId)),
            failureCorrelationId)
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /**
   * Reports a namespace whose blob cannot be read, for a delete that therefore cannot proceed.
   *
   * <p>Deliberately not the corrupt-blob tolerance {@code DeleteTable} has. A table's owned state —
   * snapshots, stats, root — is keyed by table id, so it can be purged without reading the table. A
   * namespace's children are keyed by (catalog, namespace), and the catalog is only in the blob
   * that cannot be read, so removing the pointer here would orphan tables and views this call has
   * no way to enumerate. Better to refuse than to silently strand them.
   *
   * <p>What it does fix is the shape of the refusal: {@code CorruptionException} escapes as
   * INTERNAL, which tells the caller nothing and looks like a service defect. Recovery exists — a
   * recursive delete of an ancestor, or account teardown, reaches this namespace through its
   * by-path row and drops it and its contents — so the error names it.
   */
  private RuntimeException namespaceBlobUnreadable(
      ResourceId namespaceId, String correlationId, RuntimeException cause) {
    LOG.warnf(
        cause,
        "namespace_delete_blob_unreadable namespace_id=%s: recoverable by recursive delete of an"
            + " ancestor or by account teardown",
        namespaceId.getId());
    return GrpcErrors.conflict(
        correlationId,
        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
        Map.of("display_name", namespaceId.getId()));
  }

  /**
   * Refuses the delete unless the namespace owns nothing.
   *
   * <p>Called on both sides of the children-marker advance, because the scans are reads and a read
   * cannot join the delete's CAS batch. Both calls must behave identically, which is why this is
   * one method: inlined twice, the copies drifted — the pre-marker pair was missing the retryable
   * conversion below for three review rounds, so a recursive delete that had already destroyed the
   * subtree could report a plain NAMESPACE_NOT_EMPTY that reads like a no-op.
   *
   * <p>The relation count is over by-name index rows, but only a live relation can be deleted. A
   * row whose relation is already gone — what a corrupt-blob delete leaves behind — would otherwise
   * make a namespace holding nothing report NOT_EMPTY forever, with no delete path able to clear
   * it, so those rows are reconciled once before the answer is trusted — but only when {@code
   * mayReclaimRelationRows} says the caller holds the relation write grants that reconcile spends.
   */
  private void requireNamespaceEmpty(
      DeleteNamespaceRequest request,
      Namespace namespace,
      ResourceId catalogId,
      List<String> parentPath,
      boolean mayReclaimRelationRows,
      String correlationId) {
    String accountId = catalogId.getAccountId();
    String namespaceId = namespace.getResourceId().getId();

    if (hasRelations(accountId, catalogId.getId(), namespaceId)) {
      if (!mayReclaimRelationRows) {
        throw notEmpty(request, namespace, correlationId, "relations");
      }
      recursiveDropper.reclaimStrandedRelationNames(namespace);
      if (hasRelations(accountId, catalogId.getId(), namespaceId)) {
        throw notEmpty(request, namespace, correlationId, "relations");
      }
    }
    if (hasImmediateChildren(accountId, catalogId.getId(), parentPath)) {
      throw notEmpty(request, namespace, correlationId, "children");
    }
  }

  /**
   * Under {@code --recursive}, a namespace that is not empty here means a child was published while
   * the drop was running: retryable, and the retry re-drops and converges. Without it, non-empty is
   * simply the caller's answer.
   */
  private RuntimeException notEmpty(
      DeleteNamespaceRequest request, Namespace namespace, String correlationId, String what) {
    if (request.getRecursive()) {
      return new BaseResourceRepository.AbortRetryableException(
          "namespace "
              + what
              + " changed during recursive delete: "
              + namespace.getResourceId().getId());
    }
    return GrpcErrors.conflict(
        correlationId,
        GeneratedErrorMessages.MessageKey.NAMESPACE_NOT_EMPTY,
        Map.of(
            "display_name",
            prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName())));
  }

  /**
   * Re-labels any failure as {@code NAMESPACE_RECURSIVE_PARTIAL}, with the accumulated counts, once
   * this operation has destroyed something — and returns it untouched otherwise. The original
   * failure is kept as the cause; only the counts are added.
   *
   * <p>Applied to the retried body as a whole rather than at each throw site. A recursive delete is
   * irreversible but retryable, and the failure a caller is most likely to see is not a
   * precondition miss inside the body but a repeated {@code AbortRetryableException} that exhausts
   * the retry budget — which no in-body handler can observe. {@code CorruptionException}, an
   * immutability refusal and the pagination guard escape the same way. Whatever the cause, telling
   * a caller "nothing committed" about a subtree that is gone is the one outcome this error code
   * exists to prevent.
   *
   * <p>{@code correlationId} is passed in rather than read here: this runs off the request thread,
   * after the worker hop, where the request scope may no longer be active.
   */
  private Throwable partialTeardownIfDestroyed(
      Throwable failed, RecursiveResourceDropper.DropSummary destroyed, String correlationId) {
    if (destroyed.total() == 0) {
      return failed;
    }
    return GrpcErrors.preconditionFailed(
        correlationId,
        GeneratedErrorMessages.MessageKey.NAMESPACE_RECURSIVE_PARTIAL,
        Map.of(
            "deleted_namespaces",
            Integer.toString(destroyed.namespacesDeleted),
            "deleted_tables",
            Integer.toString(destroyed.tablesDeleted),
            "deleted_views",
            Integer.toString(destroyed.viewsDeleted)),
        failed);
  }

  /**
   * A namespace is non-empty if it owns any table or view. Views are namespace-owned relations just
   * like tables, so both must be counted — otherwise a namespace containing only views passes the
   * emptiness check and is deleted without {@code --recursive}, orphaning its view pointers.
   */
  private boolean hasRelations(String accountId, String catalogId, String namespaceId) {
    return tableRepo.count(accountId, catalogId, namespaceId) > 0
        || viewRepo.count(accountId, catalogId, namespaceId) > 0;
  }

  /**
   * Whether {@code parentPath} has a direct child namespace, decided from by-path pointer rows
   * rather than content. This gates a delete, so an unparseable child namespace must be able to
   * block it — but by being counted, not by failing the probe with a corruption error. Streams and
   * stops at the first hit: the prefix spans the whole subtree, and this runs twice per delete
   * request.
   */
  private boolean hasImmediateChildren(
      String accountId, String catalogId, List<String> parentPath) {
    return namespaceRepo.hasChildUnder(accountId, catalogId, parentPath);
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

  /**
   * Fence for publishing a namespace under {@code parentPath}, so the new child and a concurrent
   * delete of its parent cannot both commit. Root-level namespaces have no parent namespace to
   * fence against — their containment is the catalog, whose own children marker is not part of this
   * protocol — so they are published unguarded.
   */
  private BatchGuard parentNamespaceGuard(
      String accountId, ResourceId catalogId, List<String> parentPath) {
    if (parentPath == null || parentPath.isEmpty()) {
      return BatchGuard.NONE;
    }
    return namespaceRepo
        .getByPath(accountId, catalogId.getId(), parentPath)
        .map(parent -> markerStore.namespaceChildGuard(parent.getResourceId()))
        .orElseThrow(
            () ->
                new BaseResourceRepository.BatchGuardFailedException(
                    "parent namespace no longer exists: " + String.join(".", parentPath)));
  }

  /**
   * The destination fence for a reparent, refusing a path that does not exist.
   *
   * <p>{@link #parentNamespaceGuard} treats a missing parent as retryable, which is right where it
   * is used on the create paths: the parent chain was just ensured, so its absence means a
   * concurrent delete raced. A reparent takes the destination straight from the caller, and "that
   * path does not exist" is not a race — retrying cannot make it appear, so the guard's retryable
   * failure would spend the whole budget and answer ABORTED for a request that can never succeed.
   * Report it as the missing parent it is.
   */
  private BatchGuard reparentDestinationGuard(
      String accountId, ResourceId catalogId, List<String> parentPath, String correlationId) {
    if (parentPath == null || parentPath.isEmpty()) {
      return BatchGuard.NONE;
    }
    var parent =
        namespaceRepo
            .getByPath(accountId, catalogId.getId(), parentPath)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE,
                        Map.of("id", String.join(".", parentPath))));
    return markerStore.namespaceChildGuard(parent.getResourceId());
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
