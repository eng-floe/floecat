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
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
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
  @Inject TableCleanupRepository tableCleanupRepo;
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

    // A child namespace is a published child like any other and carries the parent's fence — and an
    // implicitly created chain carries one per level, so a retryable guard break is ordinary here.
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
                    // Existence, not content: a namespace already at this path is a conflict
                    // whether or not its blob can be read, and reading it would answer INTERNAL
                    // instead of ALREADY_EXISTS.
                    var existing =
                        namespaceRepo.refByPath(accountId, spec.getCatalogId().getId(), fullPath);
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
                                          // Neither the parent's nor the catalog's marker is
                                          // bumped:
                                          // this replay publishes nothing, and both are delete
                                          // fences. The read above is already stale — the namespace
                                          // it found can have been deleted since — so a bump could
                                          // fail a legal delete of the parent or the catalog,
                                          // unretryably, for a create that created nothing.
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
                                    // Both the catalog marker and, when nested, the parent
                                    // namespace marker were advanced inside the create batch.
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

      // Existence, not content — see the duplicate check in createNamespace. A level whose blob
      // cannot be read still exists, and parsing it here would fail the whole create with INTERNAL
      // for an ancestor the request is merely passing through.
      if (namespaceRepo.refByPath(accountId, catalogId.getId(), chain).isPresent()) {
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
        metadataGraph.invalidate(rid);
        topology.evictNamespaceRefs(catalogId);
      } catch (BaseResourceRepository.NameConflictException nce) {
        if (namespaceRepo.refByPath(accountId, catalogId.getId(), chain).isPresent()) {
          continue;
        }
        throw nce;
      }
    }
  }

  @Override
  public Uni<UpdateNamespaceResponse> updateNamespace(UpdateNamespaceRequest request) {
    var L = LogHelper.start(LOG, "UpdateNamespace");

    // A reparent publishes into the destination parent and carries its fence; see createNamespace.
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

                  // Read before the scan that decides whether this namespace has children to
                  // strand, so the guard built from it below covers the whole window between the
                  // two. Free for the updates that never relocate: nothing is guarded on it.
                  long childMarkerVersion = markerStore.namespaceMarkerVersion(nsId);

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

                  requireDestinationOutsideOwnSubtree(current, desired, corr);
                  // Same grants the delete path spends on the same reconcile, and for the same
                  // reason: releasing a stranded row deletes a pointer that belongs to a table or a
                  // view, so only a caller who could have deleted those relations may do it.
                  requireRelocationStrandsNothing(
                      current,
                      desired,
                      authz.allows(princ, "table.write") && authz.allows(princ, "view.write"),
                      corr);

                  var conflictPath = new ArrayList<>(desired.getParentsList());
                  conflictPath.add(desired.getDisplayName());
                  String conflictCatalog = resolveCatalogName(desired.getCatalogId());
                  var conflictInfo =
                      Map.of("catalog", conflictCatalog, "path", String.join(".", conflictPath));

                  // Moving a namespace under a different parent republishes it as that parent's
                  // child, so the destination gets the same fence a create would; an in-place
                  // rename or property edit keeps its parent and needs no destination fence.
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

                  // The relocation refusal above is a scan, and a scan cannot join a CAS batch: a
                  // child published after it commits first and this update still moves the parent's
                  // by-path row, leaving the new child at a path whose first segment resolves to
                  // nothing. So the decision is bound to commit time by the same marker the delete
                  // side checks. The destination fence does not cover this — it constrains the
                  // parent being moved *into*, and an in-place rename has no such parent at all,
                  // which is the case that was completely unguarded.
                  //
                  // Only a child *namespace* can invalidate the refusal; a table or view is keyed
                  // by namespace id and stays addressable across a rename. But one marker fences
                  // all three, because the delete side needs exactly that — any child at all blocks
                  // a delete — and splitting the family would mean every publisher advancing the
                  // right one and the delete checking both. So a table create in the namespace
                  // being renamed trips a fence it could not have invalidated. That costs a retry,
                  // which re-reads the marker and commits; it takes eight such losses in a row to
                  // fail the rename, and the alternative is a second marker family whose two halves
                  // must never disagree.
                  var sourceGuard =
                      relocates(current, desired)
                          ? markerStore.namespaceChildrenUnchangedGuard(nsId, childMarkerVersion)
                          : BatchGuard.NONE;

                  try {
                    boolean ok =
                        namespaceRepo.update(
                            desired,
                            meta.getPointerVersion(),
                            BatchGuard.all(destinationGuard, sourceGuard));
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
    // AbortRetryableException. Both are fine on the ordinary retry: run() subscribes on the Mutiny
    // default executor, and a retry re-subscribes through that same operator, so no attempt — first
    // or delayed — ever runs the body on the Vert.x event loop.
    return mapFailures(
            runWithRetry(
                    () -> {
                      var princ = principal.get();
                      var correlationId = princ.getCorrelationId();
                      authz.require(princ, "namespace.write");
                      var namespaceId = request.getNamespaceId();
                      if (!princ.getAccountId().equals(namespaceId.getAccountId())) {
                        throw GrpcErrors.permissionDenied(
                            correlationId, null, Map.of("account_id", namespaceId.getAccountId()));
                      }

                      if (request.getRecursive() && request.getRequireEmpty()) {
                        throw GrpcErrors.invalidArgument(
                            correlationId,
                            GeneratedErrorMessages.MessageKey
                                .NAMESPACE_RECURSIVE_REQUIRE_EMPTY_EXCLUSIVE,
                            Map.of("field", "recursive,require_empty"));
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

                      // Both of these resolve the namespace through its blob — the write policy via
                      // the overlay, then the read here — so an unreadable blob fails before any
                      // gate
                      // below can run.
                      //
                      // Read as a (version, value) pair from one pointer read rather than through
                      // getById, which throws the version away. Everything below is decided from
                      // this value: the subtree prefix the drop walks, and the path the emptiness
                      // gate probes for children. Both are the root's path, and a path is not
                      // identity — rename the root and create a new namespace at the old name, and
                      // that prefix is somebody else's subtree. So the version this value came from
                      // is what the whole operation has to be pinned to. pinDescendantToSubtree
                      // does exactly this for every descendant, for the same reason; the root had
                      // no equivalent.
                      final MutationMeta rootMeta;
                      final Namespace namespace;
                      try {
                        catalogSurfaceWritePolicy()
                            .requireDeletableNamespace(namespaceId, correlationId);
                        rootMeta = namespaceRepo.metaForSafe(namespaceId);
                        namespace =
                            rootMeta.getPointerVersion() == 0L
                                ? null
                                : namespaceRepo
                                    .getByBlobUri(rootMeta.getBlobUri())
                                    .orElseThrow(
                                        () -> rootBlobAbsent(namespaceId, rootMeta, correlationId));
                      } catch (BaseResourceRepository.CorruptionException unreadable) {
                        throw namespaceBlobUnreadable(namespaceId, correlationId, unreadable);
                      }
                      var catalogId =
                          (namespace != null && namespace.hasCatalogId())
                              ? namespace.getCatalogId()
                              : null;

                      // Reached when the pointer is already absent — the idempotent success below —
                      // or when a namespace that does resolve carries no catalog id, which no
                      // create produces and which this cannot delete, because every key it would
                      // contend on is catalog-scoped.
                      if (catalogId == null) {
                        var safe = rootMeta;
                        if (safe.getPointerVersion() != 0L) {
                          // It resolves, but names no catalog, so this call can remove nothing: the
                          // canonical pointer, the by-path row and the whole subtree all stay. The
                          // idempotent success below would report that as a completed delete, which
                          // is the exact answer rootBlobAbsent and namespaceBlobUnreadable were
                          // added to stop giving — this was the third way into that state and the
                          // only one still falling through. Reported as those two are because the
                          // consequence and the remedy are identical: the catalog this delete needs
                          // to reach its children is missing either way.
                          throw namespaceBlobUnreadable(namespaceId, correlationId, null);
                        }
                        boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                        boolean completedOnEarlierAttempt =
                            request.getRecursive() && destroyed.total() > 0;
                        if (callerCares
                            && safe.getPointerVersion() == 0L
                            && !completedOnEarlierAttempt) {
                          throw GrpcErrors.notFound(
                              correlationId,
                              GeneratedErrorMessages.MessageKey.NAMESPACE,
                              Map.of("id", namespaceId.getId()));
                        }
                        if (!completedOnEarlierAttempt) {
                          MutationOps.BaseServiceChecks.enforcePreconditions(
                              correlationId, safe, request.getPrecondition());
                        }
                        if (safe.getPointerVersion() == 0L) {
                          // Already gone, and this idempotent success is the last operation that
                          // will ever name it: whoever removed the pointer without its children
                          // marker left a row that no GC or teardown prefix reaches. Deleting it is
                          // safe only because the pointer is provably absent — a live namespace may
                          // still have a publish fencing on that key, which is why the catalog-less
                          // case above is refused before reaching here rather than swept.
                          markerStore.deleteNamespaceMarker(namespaceId);
                        }
                        topology.evictRelationRefs(namespaceId);
                        metadataGraph.invalidate(namespaceId);
                        var goneResponse = DeleteNamespaceResponse.newBuilder().setMeta(safe);
                        if (request.getRecursive() && destroyed.total() > 0) {
                          // A retry lands here once an earlier attempt removed the root: already
                          // gone, so nothing left to do, but this operation did destroy a subtree
                          // and
                          // the counts are the record of it. Reporting zeros because the last
                          // attempt
                          // found nothing would describe the attempt rather than the operation.
                          goneResponse
                              .setDeletedNamespaces(destroyed.namespacesDeleted)
                              .setDeletedTables(destroyed.tablesDeleted)
                              .setDeletedViews(destroyed.viewsDeleted);
                        }
                        return goneResponse.build();
                      }

                      long markerVersion = markerStore.namespaceMarkerVersion(namespaceId);

                      // The caller's condition, checked before anything below writes, on both
                      // paths. Everything from here on is durable: the emptiness gate reclaims
                      // stranded relation-name rows, and the marker advance is a pointer write. A
                      // stale-etag delete used to reach both and then fail at the final delete,
                      // reporting a precondition miss — "nothing committed" — for a request that
                      // had in fact written, and leaving behind a marker advance no publish made,
                      // which is enough to make a concurrent legitimate delete report
                      // NAMESPACE_CHILDREN_CHANGED with no child anywhere in sight.
                      //
                      // Not a substitute for the check inside the final delete's CAS batch, which
                      // is still the authoritative one and the only one that can catch a root
                      // mutated after this point; this only stops a doomed request from writing on
                      // its way to failing. Under --recursive a failure here after an earlier
                      // attempt already destroyed part of the subtree is relabelled as partial
                      // teardown on the way out, along with every other way this operation fails.
                      final MutationMeta observed;
                      try {
                        observed = namespaceRepo.metaFor(namespaceId);
                      } catch (BaseResourceRepository.NotFoundException vanished) {
                        // Re-enter through the already-absent branch. It drains the namespace
                        // marker before applying the conditional NOT_FOUND or unconditional
                        // idempotent-success contract; reporting NOT_FOUND here skipped that crash
                        // recovery path and gave unconditional deletes the wrong answer.
                        throw new BaseResourceRepository.AbortRetryableException(
                            "namespace vanished while delete was planning: " + namespaceId.getId());
                      }
                      MutationOps.BaseServiceChecks.enforcePreconditions(
                          correlationId, observed, request.getPrecondition());

                      // And the root is still the thing that was read. The precondition above is
                      // the
                      // caller's assertion and is often absent; this one is the service's, and it
                      // holds whether or not the caller supplied anything. Without it a rename
                      // between that read and here leaves every path below pointing at whatever now
                      // occupies the old name — for a recursive delete, at a subtree belonging to a
                      // different namespace. Retryable, and a retry re-reads and converges.
                      requireRootUnchanged(namespaceId, rootMeta, observed);

                      if (request.getRecursive()) {
                        recursiveDropper.dropNamespaceContents(
                            namespace, rootMeta.getPointerVersion(), destroyed);
                        // Re-checked after the drop as well as before it. The window in between is
                        // the long one, and the emptiness gate below probes the root's path: a root
                        // renamed mid-drop would have it counting children under a name that is no
                        // longer its own.
                        requireRootUnchanged(
                            namespaceId, rootMeta, namespaceRepo.metaForSafe(namespaceId));
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

                      // The emptiness scan above is a read, and no read can be part of a CAS batch,
                      // so on its own it always leaves a window in which a child is published after
                      // the scan but before the pointer goes away. Carrying the marker into the
                      // delete batch closes it: every child-publishing write advances this marker
                      // inside its own batch (see BatchGuard), so a child that slipped past the
                      // scan
                      // makes this delete fail rather than orphaning itself under a namespace that
                      // no
                      // longer exists.
                      //
                      // The version read before the scan, unmodified. This delete does not advance
                      // the marker: only a publish may move it, and an advance here is a durable
                      // write that no child made — one that survives a delete which then fails, and
                      // makes the next legitimate delete report NAMESPACE_CHILDREN_CHANGED against
                      // a
                      // namespace no child ever touched. Reading is also all that is needed: a
                      // child
                      // published after the scan advances the marker and fails this check, and one
                      // published before it was seen by the scan. That also removes the second
                      // emptiness scan, which existed only to bracket an advance that no longer
                      // happens.
                      final var childrenGuard =
                          markerStore.namespaceChildrenUnchangedGuard(namespaceId, markerVersion);

                      final MutationMeta meta;
                      try {
                        meta =
                            MutationOps.deleteWithPreconditions(
                                // The version everything above was decided against, not a fresh
                                // read. A fresh read would pin the delete to whatever the root has
                                // become, so a root renamed after the scans would be deleted anyway
                                // — reporting success for a subtree walked under a name that is no
                                // longer its own. Pinned here, the delete's CAS is the last line of
                                // that defence: it fails, and the retry re-reads.
                                () -> rootMeta,
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
                      // For recursive deletion, count the root here at its removal, for the same
                      // reason the dropper counts its own deletes there rather than after cleanup:
                      // everything below can throw. A committed delete returns the live metadata
                      // it deleted. An unconditional CAS loss to another delete returns the fresh
                      // absent metadata instead; that is idempotent success, but this operation did
                      // not remove the root and must not count it. Plain deletion deliberately does
                      // not populate the recursive accumulator.
                      if (request.getRecursive() && meta.getPointerVersion() != 0L) {
                        destroyed.namespacesDeleted++;
                      }

                      // This namespace's own marker goes with it. The dropper does the same for
                      // every descendant it removes, but the root is deleted here, so without this
                      // the row survives every delete — see MarkerStore#deleteNamespaceMarker.
                      markerStore.deleteNamespaceMarker(namespaceId);
                      bestEffortPostDelete(
                          "relation_ref_evict",
                          namespaceId,
                          () -> topology.evictRelationRefs(namespaceId));
                      bestEffortPostDelete(
                          "namespace_ref_evict",
                          namespaceId,
                          () -> topology.evictNamespaceRefs(catalogId));
                      bestEffortPostDelete(
                          "metadata_graph_invalidate",
                          namespaceId,
                          () -> metadataGraph.invalidate(namespaceId));
                      // No bump of the parent's or the catalog's marker: a child that leaves is not
                      // a child publish, and both markers are delete fences that only a publish may
                      // move — see MarkerStore#namespaceChildGuard.
                      //
                      var response = DeleteNamespaceResponse.newBuilder().setMeta(meta);
                      if (request.getRecursive()) {
                        // Only under recursive, which is what the proto promises. A plain delete
                        // removes exactly the namespace the request named, so a count there would
                        // be
                        // 1 by construction and tell nobody anything — and worse, it would read as
                        // though the request had teardown semantics. Zero means no teardown
                        // happened;
                        // what became of the namespace itself is what meta is for.
                        //
                        // Straight from `destroyed`, root included, because the root counted itself
                        // above. `destroyed` accumulates across retries, so these are the totals
                        // for
                        // the operation rather than for its last attempt.
                        response
                            .setDeletedNamespaces(destroyed.namespacesDeleted)
                            .setDeletedTables(destroyed.tablesDeleted)
                            .setDeletedViews(destroyed.viewsDeleted);
                      }
                      return response.build();
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
   * The pointer named a blob that is not there. Distinct from a blob that is there and will not
   * parse, and distinct again from a namespace that is already gone — the pointer is live, so
   * reporting the idempotent success this used to fall through to told the caller a namespace had
   * been deleted while its canonical pointer, its by-path row and its whole subtree remained.
   *
   * <p>The pointer tells the two live cases apart. If it has moved, the blob this read named was
   * superseded and swept between the two reads: ordinary, and a retry reads the new one. If it has
   * not moved, the pointer names a blob that is genuinely absent, which is the unreadable-root case
   * — nothing can resolve what the namespace holds, so only a recursive delete of an ancestor or
   * account teardown can remove it.
   */
  private RuntimeException rootBlobAbsent(
      ResourceId namespaceId, MutationMeta planned, String correlationId) {
    requireRootUnchanged(namespaceId, planned, namespaceRepo.metaForSafe(namespaceId));
    return namespaceBlobUnreadable(namespaceId, correlationId, null);
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
   *
   * <p>FAILED_PRECONDITION, and its own message. Reporting it as a conflict made it ABORTED, the
   * code this service uses for genuine contention, so a client with an ordinary retry policy would
   * loop on a state that no retry can change — and it borrowed NAMESPACE_NOT_EMPTY, which says the
   * namespace holds tables or children when the truth is that nobody can tell what it holds. The
   * message also names the id as an id, rather than passing one off as a display path.
   */
  private RuntimeException namespaceBlobUnreadable(
      ResourceId namespaceId, String correlationId, RuntimeException cause) {
    LOG.warnf(
        cause,
        "namespace_delete_blob_unreadable namespace_id=%s: recoverable by recursive delete of an"
            + " ancestor or by account teardown",
        namespaceId.getId());
    return GrpcErrors.preconditionFailed(
        correlationId,
        GeneratedErrorMessages.MessageKey.NAMESPACE_BLOB_UNREADABLE,
        Map.of("id", namespaceId.getId()));
  }

  /**
   * Refuses the delete unless the root is still at the version the operation was planned against.
   *
   * <p>Every path below the initial read is the root's own — the prefix the recursive drop walks,
   * the path the emptiness gate probes for children — and a path is not an identity. An in-place
   * rename moves no descendant, so the stale prefix keeps resolving; what makes it dangerous is
   * that the name it vacated can be taken. Rename {@code db.n} to {@code db.n2}, create a new
   * {@code db.n}, and a recursive delete of {@code db.n2} planned before the rename walks {@code
   * db/n/} and destroys a subtree belonging to a namespace it was never asked about.
   *
   * <p>Retryable rather than a precondition failure: nothing is wrong with the request, only with
   * the snapshot it was planned from, and a retry re-reads and converges. Under {@code --recursive}
   * a failure here after an earlier attempt destroyed part of the subtree is relabelled as partial
   * teardown on the way out.
   */
  private void requireRootUnchanged(
      ResourceId namespaceId, MutationMeta planned, MutationMeta current) {
    if (current.getPointerVersion() != planned.getPointerVersion()) {
      throw new BaseResourceRepository.AbortRetryableException(
          "namespace changed during delete: "
              + namespaceId.getId()
              + " planned_version="
              + planned.getPointerVersion()
              + " current_version="
              + current.getPointerVersion());
    }
  }

  /**
   * Refuses the delete unless the namespace owns nothing.
   *
   * <p>Called once, after the children marker has been read and before the delete that checks it.
   * One scan is enough: a child published after it advances the marker and fails the delete's own
   * check, and one published before it is simply seen. It used to run on both sides of an advance
   * the deleter performed itself, which is a durable write no child made — see the guard
   * construction in {@code deleteNamespace}. Extracting it was still the right move while there
   * were two: inlined twice, the copies drifted, and the pre-marker one was missing the retryable
   * conversion below for three review rounds, so a recursive delete that had already destroyed the
   * subtree could report a plain NAMESPACE_NOT_EMPTY that reads like a no-op.
   *
   * <p>The relation count is over by-name index rows, but only a live relation can be deleted. A
   * row whose relation is already gone — what a corrupt-blob delete leaves behind — would otherwise
   * make a namespace holding nothing report NOT_EMPTY forever, with no delete path able to clear
   * it, so those rows are reconciled once before the answer is trusted — but only when nothing in
   * the namespace resolves, and only when {@code mayReclaimRelationRows} says the caller holds the
   * relation write grants that reconcile spends.
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
      // Reconcile only when nothing here resolves. The sweep releases rows whose relation is gone,
      // so a namespace holding anything live gains nothing from it, and running it anyway made
      // every
      // ordinary NOT_EMPTY rejection pay a full row scan plus a point read per relation.
      if (!mayReclaimRelationRows || recursiveDropper.hasResolvableRelation(namespace)) {
        throw notEmpty(request, namespace, correlationId, "relations");
      }
      recursiveDropper.reclaimStrandedRelationNames(namespace);
      if (hasRelations(accountId, catalogId.getId(), namespaceId)) {
        throw notEmpty(request, namespace, correlationId, "relations");
      }
    }
    if (tableCleanupRepo.hasAny(namespace.getResourceId())) {
      recursiveDropper.cleanupDeletedTablesInNamespace(namespace.getResourceId());
      if (tableCleanupRepo.hasAny(namespace.getResourceId())) {
        throw notEmpty(request, namespace, correlationId, "table cleanup");
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
   * Refuses a move that would put a namespace inside its own subtree.
   *
   * <p>Reparenting {@code a} to sit under {@code a} makes the namespace its own parent. The update
   * writes {@code by-path/a/b} and deletes {@code by-path/a} in the same batch, so it commits with
   * {@code parents = ["a"]} and nothing at {@code a}: the path chain that every FQ-name lookup and
   * every subtree walk depends on is broken, and the namespace is reachable by id alone. The
   * destination fence does not object, because the parent it resolves and pins is the namespace
   * being moved.
   *
   * <p>Only within one catalog. The same path in another catalog names a different namespace, and
   * moving there is an ordinary reparent.
   *
   * <p>A destination further inside the subtree is already refused before this, by the childless
   * requirement — a namespace with a descendant to be moved underneath cannot relocate at all. The
   * test is written as a prefix anyway: "not inside itself" is the invariant, and resting on
   * another rule to enforce most of it is how a rule change turns into a defect.
   */
  private void requireDestinationOutsideOwnSubtree(
      Namespace current, Namespace desired, String corr) {
    if (!current.getCatalogId().getId().equals(desired.getCatalogId().getId())) {
      return;
    }
    var ownPath = append(current.getParentsList(), current.getDisplayName());
    var destination = desired.getParentsList();
    if (destination.size() < ownPath.size()
        || !destination.subList(0, ownPath.size()).equals(ownPath)) {
      return;
    }
    throw GrpcErrors.invalidArgument(
        corr,
        GeneratedErrorMessages.MessageKey.NAMESPACE_PARENT_INSIDE_ITSELF,
        Map.of("path", prettyNamespacePath(current.getParentsList(), current.getDisplayName())));
  }

  /**
   * Refuses a rename, reparent or catalog move that would leave something behind.
   *
   * <p>A namespace's own by-path row moves with it, but a child's row is built from the child's own
   * blob and does not follow. Rename {@code db} to {@code db2} and {@code db.orders} keeps the key
   * {@code .../by-path/db/orders}, whose first segment no longer resolves to anything. From that
   * point nothing indexes the child beneath a live path, so no walk, no emptiness gate and no
   * recursive delete can reach it again — the subtree is unreachable rather than merely misplaced,
   * and a later delete of the renamed namespace finds no children and succeeds. Rewriting every
   * descendant row here would be an unbounded fenced write over a subtree of unknown size; refusing
   * the relocation is the invariant this can actually hold. A leaf relocates freely.
   *
   * <p>Tables and views are keyed by namespace id, so a rename within one catalog leaves them
   * addressable and is allowed. Their by-name rows do name the catalog, though, so a move across
   * catalogs would strand them in the catalog being left.
   *
   * <p>This is a scan, so it decides nothing on its own: the caller pairs it with the source
   * children guard, which holds the answer true through to the commit.
   *
   * <p>The relation count is over by-name index rows, exactly as in {@link #requireNamespaceEmpty},
   * and gets the same treatment for the same reason: a row whose relation is already gone strands
   * nothing, because there is nothing left to strand. Trusting the raw count refuses a move that is
   * perfectly safe, and refuses it permanently — no relocation clears those rows, so the namespace
   * would be pinned to its catalog for good by residue from a delete that had already happened.
   * Reconciled only when nothing here resolves, and only when the caller holds the relation write
   * grants the reconcile spends.
   */
  void requireRelocationStrandsNothing(
      Namespace current, Namespace desired, boolean mayReclaimRelationRows, String corr) {
    if (!relocates(current, desired)) {
      return;
    }
    boolean movedCatalog = !current.getCatalogId().getId().equals(desired.getCatalogId().getId());

    var catalogId = current.getCatalogId();
    if (namespaceRepo.hasAnyDescendantUnder(
        catalogId.getAccountId(),
        catalogId.getId(),
        append(current.getParentsList(), current.getDisplayName()))) {
      throw wouldStrand(
          GeneratedErrorMessages.MessageKey.NAMESPACE_CHILDREN_WOULD_STRAND, current, corr);
    }
    if (movedCatalog
        && hasRelations(
            catalogId.getAccountId(), catalogId.getId(), current.getResourceId().getId())) {
      if (!mayReclaimRelationRows || recursiveDropper.hasResolvableRelation(current)) {
        throw wouldStrand(
            GeneratedErrorMessages.MessageKey.NAMESPACE_RELATIONS_WOULD_STRAND, current, corr);
      }
      recursiveDropper.reclaimStrandedRelationNames(current);
      if (hasRelations(
          catalogId.getAccountId(), catalogId.getId(), current.getResourceId().getId())) {
        throw wouldStrand(
            GeneratedErrorMessages.MessageKey.NAMESPACE_RELATIONS_WOULD_STRAND, current, corr);
      }
    }
  }

  /**
   * Whether the update moves this namespace's own by-path row: a rename, a reparent, or a move to
   * another catalog.
   *
   * <p>One predicate for both halves of the relocation rule — what {@link
   * #requireRelocationStrandsNothing} refuses on, and what the source children guard fences. Two
   * copies would be free to drift, and either direction of drift is a bug that hides: a relocation
   * the scan refuses but commits unfenced, or one it allows while paying for a guard.
   */
  private static boolean relocates(Namespace current, Namespace desired) {
    return !current.getParentsList().equals(desired.getParentsList())
        || !current.getDisplayName().equals(desired.getDisplayName())
        || !current.getCatalogId().getId().equals(desired.getCatalogId().getId());
  }

  private RuntimeException wouldStrand(
      GeneratedErrorMessages.MessageKey key, Namespace current, String corr) {
    return GrpcErrors.conflict(
        corr,
        key,
        Map.of(
            "display_name",
            prettyNamespacePath(current.getParentsList(), current.getDisplayName())));
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

  private void bestEffortPostDelete(String operation, ResourceId namespaceId, Runnable action) {
    try {
      action.run();
    } catch (RuntimeException failed) {
      // The namespace pointer and subtree are already durably gone. Cache invalidation is an
      // optimization and converges from later reads; labelling its local failure as recursive
      // partial would incorrectly say durable deletion was incomplete. Marker removal remains
      // outside this helper because it is durable cleanup with no other enumerator.
      LOG.warnf(
          failed,
          "namespace_delete_post_commit_failed operation=%s namespace_id=%s",
          operation,
          namespaceId.getId());
    }
  }

  /**
   * A namespace is non-empty if it owns any table or view. Views are namespace-owned relations just
   * like tables, so both must be counted — otherwise a namespace containing only views passes the
   * emptiness check and is deleted without {@code --recursive}, orphaning its view pointers.
   */
  private boolean hasRelations(String accountId, String catalogId, String namespaceId) {
    return tableRepo.anyNamePointer(accountId, catalogId, namespaceId, ignored -> true)
        || viewRepo.anyNamePointer(accountId, catalogId, namespaceId, ignored -> true);
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
   * delete of its parent cannot both commit.
   *
   * <p>Every namespace publish carries the catalog child guard. A nested publish additionally
   * carries the parent namespace guard, so DeleteCatalog and DeleteNamespace each contend on their
   * own children marker in the same batch that makes the child visible.
   *
   * <p>Resolved from the parent's by-path row without loading its blob. The publish pins that exact
   * placement row as well as the canonical parent and children marker; otherwise the parent could
   * move between path resolution and canonical-pointer capture while the child still publishes
   * beneath the stale path.
   */
  BatchGuard parentNamespaceGuard(String accountId, ResourceId catalogId, List<String> parentPath) {
    var catalogGuard =
        markerStore.catalogChildGuard(catalogId.toBuilder().setAccountId(accountId).build());
    if (parentPath == null || parentPath.isEmpty()) {
      return catalogGuard;
    }
    var namespaceGuard =
        namespaceRepo
            .placementRefByPath(accountId, catalogId.getId(), parentPath)
            .map(parent -> parentPlacementGuard(parent, parentPath))
            .orElseThrow(
                () ->
                    new BaseResourceRepository.BatchGuardFailedException(
                        "parent namespace no longer exists: " + String.join(".", parentPath)));
    return BatchGuard.all(catalogGuard, namespaceGuard);
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
   *
   * <p>An empty destination path means catalog root, so only the catalog child guard is required. A
   * nested destination combines it with the namespace child guard.
   */
  private BatchGuard reparentDestinationGuard(
      String accountId, ResourceId catalogId, List<String> parentPath, String correlationId) {
    var catalogGuard =
        markerStore.catalogChildGuard(catalogId.toBuilder().setAccountId(accountId).build());
    if (parentPath == null || parentPath.isEmpty()) {
      return catalogGuard;
    }
    // From the by-path row without loading the blob. The row itself remains part of the guard, so
    // a corrupt or legacy placement with no usable blob URI is still pinned to the destination the
    // caller resolved. See parentNamespaceGuard.
    var parent =
        namespaceRepo
            .placementRefByPath(accountId, catalogId.getId(), parentPath)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE,
                        Map.of("id", String.join(".", parentPath))));
    return BatchGuard.all(catalogGuard, parentPlacementGuard(parent, parentPath));
  }

  private BatchGuard parentPlacementGuard(
      NamespaceRepository.NamespacePlacementRef parent, List<String> parentPath) {
    var placement = parent.placement();
    String expectedBlobUri = placement.getBlobUri().isBlank() ? null : placement.getBlobUri();
    var placementGuard =
        markerStore.pointerPinnedGuard(
            "parent namespace path " + String.join(".", parentPath),
            placement.getKey(),
            placement.getVersion());
    var namespaceGuard = markerStore.namespaceChildGuard(parent.namespace().id(), expectedBlobUri);
    return BatchGuard.all(placementGuard, namespaceGuard);
  }
}
