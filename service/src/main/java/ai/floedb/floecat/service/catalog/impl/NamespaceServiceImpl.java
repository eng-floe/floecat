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
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
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
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
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
  @Inject MarkerStore markerStore;

  // Overlay gives access to system namespaces (and other system objects)
  @Inject CatalogOverlay overlay;

  private static final Set<String> NAMESPACE_MUTABLE_PATHS =
      Set.of("display_name", "description", "path", "policy_ref", "properties", "catalog_id");
  private static final String NS_TOKEN_PREFIX = "ns:";

  private static final Logger LOG = Logger.getLogger(NamespaceService.class);

  // ---------- SYSTEM immutability + strict collision helpers ----------

  private void rejectIfSystemNamespace(ResourceId namespaceId, String corr) {
    if (namespaceId == null) {
      return;
    }

    boolean isSystem =
        overlay
            .resolve(namespaceId)
            .filter(NamespaceNode.class::isInstance)
            .map(NamespaceNode.class::cast)
            .filter(n -> n.origin() == GraphNodeOrigin.SYSTEM)
            .isPresent();

    if (isSystem) {
      throw GrpcErrors.permissionDenied(
          corr,
          GeneratedErrorMessages.MessageKey.SYSTEM_OBJECT_IMMUTABLE,
          Map.of("id", namespaceId.getId()));
    }
  }

  private List<NamespaceNode> listSystemNamespaces(ResourceId catalogId) {
    if (catalogId == null) {
      return List.of();
    }
    return overlay.listNamespaces(catalogId).stream()
        .filter(ns -> ns != null && ns.origin() == GraphNodeOrigin.SYSTEM)
        .toList();
  }

  private static final String PATH_DELIM = "\u001F";

  private enum SystemPathMatch {
    NONE,
    /** Exact match to an existing SYSTEM namespace path. */
    EXACT,
    /** A prefix of the requested path is an existing SYSTEM namespace path (i.e. under SYSTEM). */
    UNDER_SYSTEM
  }

  /**
   * Returns whether {@code fullPath} collides with (EXACT) or is under (UNDER_SYSTEM) a SYSTEM
   * namespace.
   */
  private SystemPathMatch systemNamespacePathMatch(
      ResourceId catalogId, List<String> fullPath, List<NamespaceNode> sysNamespaces) {
    if (catalogId == null || fullPath == null || fullPath.isEmpty()) {
      return SystemPathMatch.NONE;
    }

    // Normalize the candidate path once (defensive: callers should already normalize).
    var fullNorm = new ArrayList<String>(fullPath.size());
    for (var seg : fullPath) {
      fullNorm.add(normalizeName(seg));
    }

    // Build a set of SYSTEM namespace full paths (normalized), encoded as a single string.
    var sysFullPaths = new java.util.HashSet<String>();
    for (var n : sysNamespaces) {
      if (n == null) {
        continue;
      }

      var leaf = n.displayName();
      if (leaf == null || leaf.isBlank()) {
        continue;
      }

      // Encode the SYSTEM full path with a StringBuilder to reduce allocations.
      var sb = new StringBuilder();
      boolean first = true;
      for (var seg : n.pathSegments()) {
        var s = normalizeName(seg);
        if (!first) {
          sb.append(PATH_DELIM);
        }
        sb.append(s);
        first = false;
      }
      var leafNorm = normalizeName(leaf);
      if (!first) {
        sb.append(PATH_DELIM);
      }
      sb.append(leafNorm);

      sysFullPaths.add(sb.toString());
    }

    // Check prefixes efficiently without allocating a new joined string each time.
    var sb = new StringBuilder();
    for (int i = 0; i < fullNorm.size(); i++) {
      if (i > 0) {
        sb.append(PATH_DELIM);
      }
      sb.append(fullNorm.get(i));

      if (sysFullPaths.contains(sb.toString())) {
        return (i == fullNorm.size() - 1) ? SystemPathMatch.EXACT : SystemPathMatch.UNDER_SYSTEM;
      }
    }

    return SystemPathMatch.NONE;
  }

  // ---------- RPCs ----------

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
    var L = LogHelper.start(LOG, "ListNamespaces");

    return mapFailures(
            run(
                () -> {
                  var princ = principal.get();
                  authz.require(princ, "namespace.read");
                  final String accountId = princ.getAccountId();

                  final ResourceId catalogId;
                  final List<String> parentPath;

                  if (request.hasNamespaceId()) {
                    // Repo-first for user namespaces, but allow SYSTEM namespace parents via
                    // overlay.
                    var parentId = request.getNamespaceId();
                    var parentOpt = namespaceRepo.getById(parentId);

                    if (parentOpt.isPresent()) {
                      var parent = parentOpt.get();
                      catalogId = parent.getCatalogId();
                      parentPath = append(parent.getParentsList(), parent.getDisplayName());
                    } else {
                      var parentNode =
                          overlay
                              .resolve(parentId)
                              .filter(NamespaceNode.class::isInstance)
                              .map(NamespaceNode.class::cast)
                              .orElseThrow(
                                  () ->
                                      GrpcErrors.notFound(
                                          correlationId(),
                                          GeneratedErrorMessages.MessageKey.NAMESPACE,
                                          Map.of("id", parentId.getId())));
                      catalogId = parentNode.catalogId();
                      parentPath = append(parentNode.pathSegments(), parentNode.displayName());
                    }
                  } else if (request.hasCatalogId()) {
                    // Keep existing behavior: validate catalog existence via repo.
                    catalogRepo
                        .getById(request.getCatalogId())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(),
                                    GeneratedErrorMessages.MessageKey.CATALOG,
                                    Map.of("id", request.getCatalogId().getId())));
                    catalogId = request.getCatalogId();
                    parentPath = new ArrayList<>(request.getPathList());
                  } else {
                    throw GrpcErrors.invalidArgument(
                        correlationId(),
                        GeneratedErrorMessages.MessageKey.SELECTOR_REQUIRED,
                        Map.of());
                  }

                  final boolean recursive = request.getRecursive();
                  if (request.getChildrenOnly() && recursive) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(),
                        null,
                        Map.of("children_only", "true", "recursive", "true"));
                  }

                  final String namePrefix = request.getNamePrefix().trim();
                  final List<NamespaceNode> sysNamespaces = listSystemNamespaces(catalogId);

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  final int want = Math.max(1, pageIn.limit);
                  final int batch = Math.max(want * 4, 64);

                  final boolean isServiceToken =
                      pageIn.token != null && pageIn.token.startsWith(NS_TOKEN_PREFIX);
                  final String resumeAfterRel = isServiceToken ? decodeNsToken(pageIn.token) : "";
                  String cursor = isServiceToken ? "" : pageIn.token;

                  var out = new ArrayList<Namespace>(want);
                  String lastEmittedRel = "";

                  // Phase 1: existing repo-backed pagination (unchanged)
                  while (out.size() < want) {
                    var next = new StringBuilder();
                    final List<Namespace> scanned;
                    try {
                      scanned =
                          namespaceRepo.list(
                              accountId, catalogId.getId(), parentPath, batch, cursor, next);
                    } catch (IllegalArgumentException badToken) {
                      throw GrpcErrors.invalidArgument(
                          correlationId(),
                          GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID,
                          Map.of("page_token", cursor));
                    }

                    if (recursive) {
                      for (var ns : scanned) {
                        if (!isDescendantOf(ns, parentPath)) {
                          continue;
                        }

                        var rel = relativeQualifiedName(ns, parentPath);
                        if (!namePrefix.isBlank() && !rel.startsWith(namePrefix)) {
                          continue;
                        }

                        if (!resumeAfterRel.isBlank() && rel.compareTo(resumeAfterRel) <= 0) {
                          continue;
                        }

                        out.add(ns);
                        lastEmittedRel = rel;
                        if (out.size() >= want) {
                          break;
                        }
                      }
                    } else {
                      for (var ns : scanned) {
                        if (!isImmediateChildOf(ns, parentPath)) {
                          continue;
                        }

                        var rel = relativeQualifiedName(ns, parentPath);
                        if (!namePrefix.isBlank() && !rel.startsWith(namePrefix)) {
                          continue;
                        }

                        if (!resumeAfterRel.isBlank() && rel.compareTo(resumeAfterRel) <= 0) {
                          continue;
                        }

                        out.add(ns);
                        lastEmittedRel = rel;
                        if (out.size() >= want) {
                          break;
                        }
                      }
                    }

                    cursor = next.toString();
                    if (cursor.isBlank()) {
                      break;
                    }

                    if (out.size() >= want) {
                      break;
                    }
                  }

                  // Phase 2: only once repo cursor is exhausted, append SYSTEM namespaces.
                  if (cursor.isBlank() && out.size() < want) {

                    record SysItem(NamespaceNode node, String rel) {}

                    var sysItems =
                        sysNamespaces.stream()
                            .filter(
                                ns ->
                                    recursive
                                        ? isDescendantOf(ns, parentPath)
                                        : isImmediateChildOf(ns, parentPath))
                            .map(ns -> new SysItem(ns, relativeQualifiedName(ns, parentPath)))
                            .filter(it -> namePrefix.isBlank() || it.rel().startsWith(namePrefix))
                            .sorted(Comparator.comparing(SysItem::rel))
                            .toList();

                    for (var it : sysItems) {
                      if (!resumeAfterRel.isBlank() && it.rel().compareTo(resumeAfterRel) <= 0) {
                        continue;
                      }
                      if (out.size() >= want) {
                        break;
                      }
                      out.add(toProto(it.node()));
                      lastEmittedRel = it.rel();
                    }
                  }

                  String nextToken = cursor;
                  if (nextToken.isBlank() && out.size() == want) {
                    nextToken = encodeNsToken(lastEmittedRel);
                  }

                  final int total =
                      countNamespaces(
                          accountId,
                          catalogId.getId(),
                          parentPath,
                          namePrefix,
                          recursive,
                          sysNamespaces);

                  var page = MutationOps.pageOut(nextToken, total);
                  return ListNamespacesResponse.newBuilder()
                      .addAllNamespaces(out)
                      .setPage(page)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  // Includes SYSTEM namespaces from overlay in totalSize.
  private int countNamespaces(
      String accountId,
      String catalogId,
      List<String> parentPath,
      String namePrefix,
      boolean recursive,
      List<NamespaceNode> sysNamespaces) {

    int count = 0;
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      final List<Namespace> page;
      try {
        page = namespaceRepo.list(accountId, catalogId, parentPath, 1000, cursor, next);
      } catch (IllegalArgumentException bad) {
        throw GrpcErrors.invalidArgument(
            correlationId(),
            GeneratedErrorMessages.MessageKey.PAGE_TOKEN_INVALID,
            Map.of("page_token", cursor));
      }

      for (var ns : page) {
        boolean matchesScope =
            recursive ? isDescendantOf(ns, parentPath) : isImmediateChildOf(ns, parentPath);
        if (!matchesScope) {
          continue;
        }

        if (!namePrefix.isBlank()) {
          var rel = relativeQualifiedName(ns, parentPath);
          if (!rel.startsWith(namePrefix)) {
            continue;
          }
        }

        count++;
      }

      cursor = next.toString();
      if (cursor.isBlank()) {
        break;
      }
    }

    int sysCount = 0;

    for (var ns : sysNamespaces) {
      boolean matchesScope =
          recursive ? isDescendantOf(ns, parentPath) : isImmediateChildOf(ns, parentPath);
      if (!matchesScope) {
        continue;
      }

      if (!namePrefix.isBlank()) {
        var rel = relativeQualifiedName(ns, parentPath);
        if (!rel.startsWith(namePrefix)) {
          continue;
        }
      }

      sysCount++;
    }

    return count + sysCount;
  }

  private static boolean isDescendantOf(Namespace ns, List<String> parentPath) {
    return isDescendantOf(ns.getParentsList(), parentPath);
  }

  private static boolean isDescendantOf(NamespaceNode ns, List<String> parentPath) {
    return isDescendantOf(ns.pathSegments(), parentPath);
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

  private static boolean isImmediateChildOf(Namespace ns, List<String> parentPath) {
    return isImmediateChildOf(ns.getParentsList(), parentPath);
  }

  private static boolean isImmediateChildOf(NamespaceNode ns, List<String> parentPath) {
    return isImmediateChildOf(ns.pathSegments(), parentPath);
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

  private static Namespace toProto(NamespaceNode n) {
    return Namespace.newBuilder()
        .setResourceId(n.id())
        .setCatalogId(n.catalogId())
        .setDisplayName(n.displayName())
        .clearParents()
        .addAllParents(n.pathSegments())
        .putAllProperties(n.properties())
        .build();
  }

  @Override
  public Uni<GetNamespaceResponse> getNamespace(GetNamespaceRequest request) {
    var L = LogHelper.start(LOG, "GetNamespace");

    return mapFailures(
            run(
                () -> {
                  var princ = principal.get();
                  authz.require(princ, "namespace.read");
                  var nsId = request.getNamespaceId();

                  // Repo first (unchanged), then overlay (system).
                  var nsOpt = namespaceRepo.getById(nsId);
                  if (nsOpt.isPresent()) {
                    return GetNamespaceResponse.newBuilder().setNamespace(nsOpt.get()).build();
                  }

                  var node =
                      overlay
                          .resolve(nsId)
                          .filter(NamespaceNode.class::isInstance)
                          .map(NamespaceNode.class::cast)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      GeneratedErrorMessages.MessageKey.NAMESPACE,
                                      Map.of("id", nsId.getId())));

                  return GetNamespaceResponse.newBuilder().setNamespace(toProto(node)).build();
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
                  ensureKind(
                      spec.getCatalogId(),
                      ResourceKind.RK_CATALOG,
                      "spec.catalog_id",
                      correlationId);
                  var catalog =
                      catalogRepo
                          .getById(spec.getCatalogId())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId,
                                      GeneratedErrorMessages.MessageKey.CATALOG,
                                      Map.of("id", spec.getCatalogId().getId())));
                  String catalogName = catalog.getDisplayName();

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
                  final List<NamespaceNode> sysNamespaces =
                      listSystemNamespaces(spec.getCatalogId());
                  fullPath.add(display);

                  var sysMatch =
                      systemNamespacePathMatch(spec.getCatalogId(), fullPath, sysNamespaces);
                  if (sysMatch == SystemPathMatch.EXACT) {
                    // Exact collision with an existing SYSTEM namespace.
                    throw GrpcErrors.conflict(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_ALREADY_EXISTS,
                        Map.of(
                            "display_name", display,
                            "catalog_id", spec.getCatalogId().getId(),
                            "path", String.join(".", fullPath)));
                  } else if (sysMatch == SystemPathMatch.UNDER_SYSTEM) {
                    // Reject creating at/under a SYSTEM namespace subtree.
                    throw GrpcErrors.permissionDenied(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.SYSTEM_OBJECT_IMMUTABLE,
                        Map.of(
                            "catalog_id", spec.getCatalogId().getId(),
                            "path", String.join(".", fullPath)));
                  }

                  final byte[] fingerprint =
                      canonicalFingerprint(spec.getCatalogId(), parents, display, spec);

                  if (!request.hasIdempotency() || request.getIdempotency().getKey().isBlank()) {
                    var existing =
                        namespaceRepo.getByPath(accountId, spec.getCatalogId().getId(), fullPath);
                    if (existing.isPresent()) {
                      throw GrpcErrors.conflict(
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
                                          return new IdempotencyGuard.CreateResult<>(
                                              existing, existing.getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.conflict(
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
                  ensureKind(nsId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);

                  // SYSTEM namespaces are immutable
                  rejectIfSystemNamespace(nsId, corr);

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

                  var desired =
                      applyNamespaceSpecPatch(
                          current, request.getSpec(), normalizeMask(request.getUpdateMask()), corr);

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
                    throw GrpcErrors.conflict(
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

                  var namespaceId = request.getNamespaceId();
                  ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

                  // SYSTEM namespaces are immutable
                  rejectIfSystemNamespace(namespaceId, correlationId);

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
                    metadataGraph.invalidate(namespaceId);
                    return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
                  }

                  long markerVersion = markerStore.namespaceMarkerVersion(namespaceId);

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
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_CHILDREN_CHANGED,
                        Map.of());
                  }
                  var markerAfterAdvance = markerStore.namespaceMarkerVersion(namespaceId);
                  if (markerAfterAdvance != markerVersion + 1) {
                    throw GrpcErrors.preconditionFailed(
                        correlationId,
                        GeneratedErrorMessages.MessageKey.NAMESPACE_CHILDREN_CHANGED,
                        Map.of());
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
                  if (hasImmediateChildren(
                      catalogId.getAccountId(), catalogId.getId(), parentPath)) {
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
      ensureKind(cat, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
      catalogRepo
          .getById(cat)
          .orElseThrow(
              () ->
                  GrpcErrors.notFound(
                      corr, GeneratedErrorMessages.MessageKey.CATALOG, Map.of("id", cat.getId())));
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

  private static String encodeNsToken(String resumeAfterRel) {
    if (resumeAfterRel == null || resumeAfterRel.isBlank()) {
      return "";
    }

    return NS_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(resumeAfterRel.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static String decodeNsToken(String token) {
    if (token == null || token.isBlank() || !token.startsWith(NS_TOKEN_PREFIX)) {
      return "";
    }

    var s = token.substring(NS_TOKEN_PREFIX.length());
    var bytes = Base64.getUrlDecoder().decode(s);
    return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
  }
}
