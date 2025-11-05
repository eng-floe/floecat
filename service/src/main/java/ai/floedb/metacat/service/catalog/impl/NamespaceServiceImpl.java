package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.metacat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.metacat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.catalog.rpc.NamespaceService;
import ai.floedb.metacat.catalog.rpc.NamespaceSpec;
import ai.floedb.metacat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.Canonicalizer;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.LogHelper;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.CatalogRepository;
import ai.floedb.metacat.service.repo.impl.NamespaceRepository;
import ai.floedb.metacat.service.repo.impl.TableRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

@GrpcService
public class NamespaceServiceImpl extends BaseServiceImpl implements NamespaceService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(NamespaceService.class);

  private enum Mode {
    CHILDREN_ONLY,
    RECURSIVE
  }

  @Override
  public Uni<ListNamespacesResponse> listNamespaces(ListNamespacesRequest request) {
    var L = LogHelper.start(LOG, "ListNamespaces");

    return mapFailures(
            run(
                () -> {
                  var princ = principal.get();
                  authz.require(princ, "namespace.read");
                  final String tenantId = princ.getTenantId();

                  final ResourceId catalogId;
                  final List<String> parentPath;
                  if (request.hasNamespaceId()) {
                    var parent =
                        namespaceRepo
                            .getById(request.getNamespaceId())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId(),
                                        "namespace",
                                        Map.of("id", request.getNamespaceId().getId())));
                    catalogId = parent.getCatalogId();
                    parentPath = append(parent.getParentsList(), parent.getDisplayName());
                  } else if (request.hasCatalogId()) {
                    catalogRepo
                        .getById(request.getCatalogId())
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId(),
                                    "catalog",
                                    Map.of("id", request.getCatalogId().getId())));
                    catalogId = request.getCatalogId();
                    parentPath = new ArrayList<>(request.getPathList());
                  } else {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), null, Map.of("field", "parent"));
                  }

                  boolean childrenOnly = request.getChildrenOnly();
                  boolean recursive = request.getRecursive();
                  if (childrenOnly && recursive) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(),
                        null,
                        Map.of("children_only", "true", "recursive", "true"));
                  }
                  final Mode mode =
                      (childrenOnly || !recursive) ? Mode.CHILDREN_ONLY : Mode.RECURSIVE;
                  final String namePrefix = request.getNamePrefix().trim();

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  final int want = pageIn.limit;
                  final int batch = Math.max(1, want * 4);
                  String cursor = pageIn.token;

                  final ArrayList<Namespace> out = new ArrayList<>(want);
                  final HashSet<String> seenVirtual = new HashSet<>();
                  final HashSet<String> realImmediateNames = new HashSet<>();

                  while (out.size() < want) {
                    var next = new StringBuilder();
                    var scanned =
                        namespaceRepo.list(
                            tenantId, catalogId.getId(), parentPath, batch, cursor, next);

                    if (mode == Mode.RECURSIVE) {
                      collectRecursive(scanned, parentPath, namePrefix, out, want);
                    } else {
                      collectImmediateAndRecord(
                          scanned, parentPath, namePrefix, out, want, realImmediateNames);
                      if (out.size() < want) {
                        collectVirtualSegmentsSkippingReal(
                            scanned,
                            catalogId,
                            parentPath,
                            namePrefix,
                            realImmediateNames,
                            seenVirtual,
                            out,
                            want);
                      }
                    }

                    cursor = next.toString();
                    if (cursor.isBlank()) {
                      break;
                    }
                  }

                  final int total =
                      countNamespaces(tenantId, catalogId.getId(), parentPath, namePrefix, mode);

                  var page = MutationOps.pageOut(cursor, total);
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

  private static void collectImmediateAndRecord(
      List<Namespace> batch,
      List<String> parentPath,
      String namePrefix,
      List<Namespace> out,
      int cap,
      Set<String> realImmediateNames) {

    for (var ns : batch) {
      if (!isImmediateChildOf(ns, parentPath)) {
        continue;
      }
      var name = ns.getDisplayName();
      if (!namePrefix.isBlank() && !name.startsWith(namePrefix)) {
        continue;
      }
      realImmediateNames.add(name);
      out.add(ns);

      if (out.size() >= cap) {
        break;
      }
    }
  }

  private void collectVirtualSegmentsSkippingReal(
      List<Namespace> batch,
      ResourceId catalogId,
      List<String> parentPath,
      String namePrefix,
      Set<String> realImmediateNames,
      Set<String> seenVirtual,
      List<Namespace> out,
      int cap) {

    for (var ns : batch) {
      var seg = nextSegmentAfterParent(ns, parentPath);
      if (seg == null) {
        continue;
      }
      if (realImmediateNames.contains(seg)) {
        continue;
      }
      if (!namePrefix.isBlank() && !seg.startsWith(namePrefix)) {
        continue;
      }
      if (seenVirtual.add(seg)) {
        out.add(synthesizeVirtual(catalogId, parentPath, seg));

        if (out.size() >= cap) {
          break;
        }
      }
    }
  }

  private static void collectRecursive(
      List<Namespace> batch,
      List<String> parentPath,
      String namePrefix,
      List<Namespace> out,
      int cap) {
    for (var ns : batch) {
      if (!isDescendantOf(ns, parentPath)) {
        continue;
      }
      if (!namePrefix.isBlank() && !ns.getDisplayName().startsWith(namePrefix)) {
        continue;
      }
      out.add(ns);

      if (out.size() >= cap) {
        break;
      }
    }
  }

  private int countNamespaces(
      String tenantId, String catalogId, List<String> parentPath, String namePrefix, Mode mode) {

    String cursor = "";

    if (mode == Mode.RECURSIVE) {
      int count = 0;
      while (true) {
        var next = new StringBuilder();
        var page = namespaceRepo.list(tenantId, catalogId, parentPath, 1000, cursor, next);
        for (var ns : page) {
          if (isDescendantOf(ns, parentPath)
              && (namePrefix.isBlank() || ns.getDisplayName().startsWith(namePrefix))) {
            count++;
          }
        }
        cursor = next.toString();

        if (cursor.isBlank()) {
          break;
        }
      }
      return count;
    }

    int countReal = 0;
    final Set<String> realImmediateNames = new HashSet<>();
    final Set<String> segsVirtual = new HashSet<>();

    while (true) {
      var next = new StringBuilder();
      var page = namespaceRepo.list(tenantId, catalogId, parentPath, 1000, cursor, next);

      for (var ns : page) {
        if (isImmediateChildOf(ns, parentPath)) {
          var name = ns.getDisplayName();
          if (namePrefix.isBlank() || name.startsWith(namePrefix)) {
            countReal++;
            realImmediateNames.add(name);
          }
        } else {
          var seg = nextSegmentAfterParent(ns, parentPath);
          if (seg != null && (namePrefix.isBlank() || seg.startsWith(namePrefix))) {
            if (!realImmediateNames.contains(seg)) {
              segsVirtual.add(seg);
            }
          }
        }
      }

      cursor = next.toString();

      if (cursor.isBlank()) {
        break;
      }
    }

    return countReal + segsVirtual.size();
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
                  var ns =
                      namespaceRepo
                          .getById(nsId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(), "namespace", Map.of("id", nsId.getId())));
                  return GetNamespaceResponse.newBuilder().setNamespace(ns).build();
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
                  var tenantId = princ.getTenantId();
                  var correlationId = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  catalogRepo
                      .getById(request.getSpec().getCatalogId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId,
                                  "catalog",
                                  Map.of("id", request.getSpec().getCatalogId().getId())));

                  var tsNow = nowTs();

                  var fingerprint = canonicalFingerprint(request.getSpec());
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var namespaceProto =
                      MutationOps.createProto(
                          tenantId,
                          "CreateNamespace",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            String namespaceUuid =
                                deterministicUuid(tenantId, "namespace", idempotencyKey);
                            var namespaceId =
                                ResourceId.newBuilder()
                                    .setTenantId(tenantId)
                                    .setId(namespaceUuid)
                                    .setKind(ResourceKind.RK_NAMESPACE)
                                    .build();

                            String display =
                                mustNonEmpty(
                                    request.getSpec().getDisplayName(),
                                    "display_name",
                                    correlationId);
                            List<String> parents = new ArrayList<>(request.getSpec().getPathList());

                            if (parents.isEmpty()) {
                              var segs =
                                  Arrays.stream(display.split("[./]"))
                                      .map(String::trim)
                                      .filter(s -> !s.isEmpty())
                                      .collect(Collectors.toList());
                              if (segs.size() > 1) {
                                parents = new ArrayList<>(segs.subList(0, segs.size() - 1));
                                display = segs.get(segs.size() - 1);
                              }
                            }

                            var built =
                                Namespace.newBuilder()
                                    .setResourceId(namespaceId)
                                    .setDisplayName(display)
                                    .clearParents()
                                    .addAllParents(parents)
                                    .setDescription(request.getSpec().getDescription())
                                    .setCatalogId(request.getSpec().getCatalogId())
                                    .setCreatedAt(tsNow)
                                    .build();
                            namespaceRepo.create(built);
                            return new IdempotencyGuard.CreateResult<>(built, namespaceId);
                          },
                          (namespace) -> namespaceRepo.metaForSafe(namespace.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Namespace::parseFrom,
                          rec -> namespaceRepo.getById(rec.getResourceId()).isPresent());

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

  @Override
  public Uni<UpdateNamespaceResponse> updateNamespace(UpdateNamespaceRequest request) {
    var L = LogHelper.start(LOG, "UpdateNamespace");

    return mapFailures(
            runWithRetry(
                () -> {
                  var princ = principal.get();
                  var correlationId = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  var namespaceId = request.getNamespaceId();
                  ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

                  var current =
                      namespaceRepo
                          .getById(namespaceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId,
                                      "namespace",
                                      Map.of("id", namespaceId.getId())));

                  ResourceId desiredCatalogId =
                      request.hasCatalogId() && !request.getCatalogId().getId().isBlank()
                          ? request.getCatalogId()
                          : current.getCatalogId();
                  ensureKind(
                      desiredCatalogId, ResourceKind.RK_CATALOG, "catalog_id", correlationId);

                  catalogRepo
                      .getById(desiredCatalogId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId,
                                  "catalog",
                                  Map.of("id", desiredCatalogId.getId())));

                  String desiredDisplay;
                  List<String> desiredParents;

                  if (!request.getDisplayName().isBlank()) {
                    desiredDisplay =
                        mustNonEmpty(request.getDisplayName(), "display_name", correlationId);
                    desiredParents =
                        !request.getPathList().isEmpty()
                            ? request.getPathList()
                            : current.getParentsList();
                  } else if (!request.getPathList().isEmpty()) {
                    var path = request.getPathList();
                    if (path.size() == 1) {
                      desiredDisplay = mustNonEmpty(path.get(0), "path[0]", correlationId);
                      desiredParents = List.of();
                    } else {
                      desiredDisplay =
                          mustNonEmpty(path.get(path.size() - 1), "path[last]", correlationId);
                      desiredParents = path.subList(0, path.size() - 1);
                    }
                  } else {
                    desiredDisplay = current.getDisplayName();
                    desiredParents = current.getParentsList();
                  }

                  var desired =
                      current.toBuilder()
                          .setDisplayName(desiredDisplay)
                          .clearParents()
                          .addAllParents(desiredParents)
                          .setCatalogId(desiredCatalogId)
                          .build();

                  if (desired.equals(current)) {
                    var metaNoop = namespaceRepo.metaForSafe(namespaceId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, metaNoop, request.getPrecondition());
                    return UpdateNamespaceResponse.newBuilder()
                        .setNamespace(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  MutationOps.updateWithPreconditions(
                      () -> namespaceRepo.metaFor(namespaceId),
                      request.getPrecondition(),
                      expected -> namespaceRepo.update(desired, expected),
                      () -> namespaceRepo.metaForSafe(namespaceId),
                      correlationId,
                      "namespace",
                      Map.of(
                          "display_name", desiredDisplay, "catalog_id", desiredCatalogId.getId()));

                  var outMeta = namespaceRepo.metaForSafe(namespaceId);
                  var latest = namespaceRepo.getById(namespaceId).orElse(desired);
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

                  var namespace = namespaceRepo.getById(namespaceId).orElse(null);
                  var catalogId =
                      (namespace != null && namespace.hasCatalogId())
                          ? namespace.getCatalogId()
                          : null;

                  if (catalogId == null) {
                    var safe = namespaceRepo.metaForSafe(namespaceId);
                    namespaceRepo.delete(namespaceId);
                    return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
                  }

                  if (tableRepo.count(
                          catalogId.getTenantId(), catalogId.getId(), namespaceId.getId())
                      > 0) {
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId, "namespace.not_empty", Map.of("display_name", pretty));
                  }

                  var parentPath = append(namespace.getParentsList(), namespace.getDisplayName());
                  if (hasImmediateChildren(
                      catalogId.getTenantId(), catalogId.getId(), parentPath)) {
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId, "namespace.not_empty", Map.of("display_name", pretty));
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

                  return DeleteNamespaceResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private boolean hasImmediateChildren(String tenantId, String catalogId, List<String> parentPath) {
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      var page = namespaceRepo.list(tenantId, catalogId, parentPath, 200, cursor, next);
      for (var ns : page) {
        if (isImmediateChildOf(ns, parentPath)) return true;
      }
      cursor = next.toString();
      if (cursor.isBlank()) break;
    }
    return false;
  }

  private static boolean isDescendantOf(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    if (p.size() < parentPath.size()) return false;
    for (int i = 0; i < parentPath.size(); i++) {
      if (!p.get(i).equals(parentPath.get(i))) return false;
    }
    return true;
  }

  private static boolean isImmediateChildOf(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    if (p.size() != parentPath.size()) return false;
    for (int i = 0; i < parentPath.size(); i++) {
      if (!p.get(i).equals(parentPath.get(i))) return false;
    }
    return true;
  }

  private static String nextSegmentAfterParent(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    if (p.size() < parentPath.size()) return null;
    for (int i = 0; i < parentPath.size(); i++) {
      if (!p.get(i).equals(parentPath.get(i))) return null;
    }
    if (p.size() == parentPath.size()) return null;
    return p.get(parentPath.size());
  }

  private Namespace synthesizeVirtual(ResourceId catalogId, List<String> parentPath, String child) {
    return Namespace.newBuilder()
        .setDisplayName(child)
        .clearParents()
        .addAllParents(parentPath)
        .setCatalogId(catalogId)
        .build();
  }

  private static ArrayList<String> append(List<String> parents, String last) {
    var pp = new ArrayList<String>(parents.size() + 1);
    pp.addAll(parents);
    pp.add(last);
    return pp;
  }

  private static byte[] canonicalFingerprint(NamespaceSpec s) {
    return new Canonicalizer()
        .scalar("cat", nullSafeId(s.getCatalogId()))
        .list("parents", s.getPathList())
        .scalar("name", normalizeName(s.getDisplayName()))
        .bytes();
  }
}
