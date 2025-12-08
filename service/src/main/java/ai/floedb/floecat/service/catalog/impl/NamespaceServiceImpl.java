package ai.floedb.floecat.service.catalog.impl;

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
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.MetadataGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Base64;
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
  @Inject MetadataGraph metadataGraph;

  private static final Set<String> NAMESPACE_MUTABLE_PATHS =
      Set.of("display_name", "description", "path", "policy_ref", "properties", "catalog_id");
  private static final String PATH_DELIM = "\u001F";
  private static final String NS_TOKEN_PREFIX = "ns:";

  private static final Logger LOG = Logger.getLogger(NamespaceService.class);

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
                        correlationId(), "selector.required", Map.of());
                  }

                  final boolean recursive = request.getRecursive();
                  if (request.getChildrenOnly() && recursive) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(),
                        null,
                        Map.of("children_only", "true", "recursive", "true"));
                  }

                  final String namePrefix = request.getNamePrefix().trim();

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  final int want = Math.max(1, pageIn.limit);
                  final int batch = Math.max(want * 4, 64);

                  final boolean isServiceToken =
                      pageIn.token != null && pageIn.token.startsWith(NS_TOKEN_PREFIX);
                  final String resumeAfterRel = isServiceToken ? decodeNsToken(pageIn.token) : "";
                  String cursor = isServiceToken ? "" : pageIn.token;

                  var out = new ArrayList<Namespace>(want);
                  String lastEmittedRel = "";

                  while (out.size() < want) {
                    var next = new StringBuilder();
                    final List<Namespace> scanned;
                    try {
                      scanned =
                          namespaceRepo.list(
                              accountId, catalogId.getId(), parentPath, batch, cursor, next);
                    } catch (IllegalArgumentException badToken) {
                      throw GrpcErrors.invalidArgument(
                          correlationId(), "page_token.invalid", Map.of("page_token", cursor));
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

                        if (!resumeAfterRel.isBlank() && rel.compareTo(resumeAfterRel) <= 0)
                          continue;

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

                  String nextToken = cursor;
                  if (nextToken.isBlank() && out.size() == want) {
                    nextToken = encodeNsToken(lastEmittedRel);
                  }

                  final int total =
                      countNamespaces(
                          accountId, catalogId.getId(), parentPath, namePrefix, recursive);

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

  private int countNamespaces(
      String accountId,
      String catalogId,
      List<String> parentPath,
      String namePrefix,
      boolean recursive) {

    int count = 0;
    String cursor = "";
    while (true) {
      var next = new StringBuilder();
      final List<Namespace> page;
      try {
        page = namespaceRepo.list(accountId, catalogId, parentPath, 1000, cursor, next);
      } catch (IllegalArgumentException bad) {
        throw GrpcErrors.invalidArgument(
            correlationId(), "page_token.invalid", Map.of("page_token", cursor));
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
    return count;
  }

  private static boolean isDescendantOf(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    if (p.size() < parentPath.size()) {
      return false;
    }
    for (int i = 0; i < parentPath.size(); i++) {
      if (!p.get(i).equals(parentPath.get(i))) {
        return false;
      }
    }
    return true;
  }

  private static boolean isImmediateChildOf(Namespace ns, List<String> parentPath) {
    var p = ns.getParentsList();
    if (p.size() != parentPath.size()) {
      return false;
    }

    for (int i = 0; i < parentPath.size(); i++) {
      if (!p.get(i).equals(parentPath.get(i))) {
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
                  var accountId = princ.getAccountId();
                  var correlationId = princ.getCorrelationId();
                  authz.require(princ, "namespace.write");

                  var spec = request.getSpec();
                  catalogRepo
                      .getById(spec.getCatalogId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId,
                                  "catalog",
                                  Map.of("id", spec.getCatalogId().getId())));

                  var tsNow = nowTs();

                  String displayWork =
                      mustNonEmpty(spec.getDisplayName(), "display_name", correlationId);
                  List<String> parentsWork = new ArrayList<>(spec.getPathList());

                  final String display = normalizeName(displayWork);
                  if (display.isBlank()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId, "display_name.cannot_clear", Map.of());
                  }
                  for (String seg : parentsWork) {
                    var s = normalizeName(seg);
                    if (s.isBlank()) {
                      throw GrpcErrors.invalidArgument(
                          correlationId, "path.segment.blank", Map.of());
                    }
                  }
                  final List<String> parents = List.copyOf(parentsWork);
                  final List<String> fullPath = new ArrayList<>(parents);
                  fullPath.add(display);

                  final byte[] fingerprint =
                      new Canonicalizer()
                          .scalar("cat", nullSafeId(spec.getCatalogId()))
                          .list("parents", parents)
                          .scalar("name", display)
                          .bytes();

                  final String idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var namespaceProto =
                      MutationOps.createProto(
                          accountId,
                          "CreateNamespace",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            if (!parents.isEmpty()) {
                              ensurePathChainExists(
                                  accountId, spec.getCatalogId(), parents, tsNow, correlationId);
                            }

                            String namespaceUuid =
                                deterministicUuid(
                                    accountId,
                                    "namespace",
                                    spec.getCatalogId().getId()
                                        + "/"
                                        + String.join(PATH_DELIM, fullPath));

                            var namespaceId =
                                ResourceId.newBuilder()
                                    .setAccountId(accountId)
                                    .setId(namespaceUuid)
                                    .setKind(ResourceKind.RK_NAMESPACE)
                                    .build();

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

                            namespaceRepo.create(built);
                            metadataGraph.invalidate(namespaceId);
                            return new IdempotencyGuard.CreateResult<>(built, namespaceId);
                          },
                          (ns) -> namespaceRepo.metaForSafe(ns.getResourceId()),
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
        throw GrpcErrors.invalidArgument(corr, "path.segment.blank", Map.of());
      }
      chain.add(seg);
      var existing = namespaceRepo.getByPath(accountId, catalogId.getId(), chain);
      if (existing.isPresent()) {
        continue;
      }

      String uuid =
          deterministicUuid(
              accountId, "namespace", catalogId.getId() + "/" + String.join(PATH_DELIM, chain));
      var rid =
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(uuid)
              .setKind(ResourceKind.RK_NAMESPACE)
              .build();

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
      namespaceRepo.create(ns);
      metadataGraph.invalidate(rid);
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

                  var current =
                      namespaceRepo
                          .getById(nsId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "namespace", Map.of("id", nsId.getId())));

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var desired =
                      applyNamespaceSpecPatch(
                          current, request.getSpec(), request.getUpdateMask(), corr);

                  if (desired.equals(current)) {
                    var metaNoop = namespaceRepo.metaForSafe(nsId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateNamespaceResponse.newBuilder()
                        .setNamespace(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  var conflictInfo =
                      Map.of(
                          "display_name", desired.getDisplayName(),
                          "catalog_id", desired.getCatalogId().getId());

                  MutationOps.updateWithPreconditions(
                      () -> namespaceRepo.metaFor(nsId),
                      request.getPrecondition(),
                      expected -> namespaceRepo.update(desired, expected),
                      () -> namespaceRepo.metaForSafe(nsId),
                      corr,
                      "namespace",
                      conflictInfo);
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
                  ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId);

                  var namespace = namespaceRepo.getById(namespaceId).orElse(null);
                  var catalogId =
                      (namespace != null && namespace.hasCatalogId())
                          ? namespace.getCatalogId()
                          : null;

                  if (catalogId == null) {
                    var safe = namespaceRepo.metaForSafe(namespaceId);
                    namespaceRepo.delete(namespaceId);
                    metadataGraph.invalidate(namespaceId);
                    return DeleteNamespaceResponse.newBuilder().setMeta(safe).build();
                  }

                  if (tableRepo.count(
                          catalogId.getAccountId(), catalogId.getId(), namespaceId.getId())
                      > 0) {
                    var pretty =
                        prettyNamespacePath(namespace.getParentsList(), namespace.getDisplayName());
                    throw GrpcErrors.conflict(
                        correlationId, "namespace.not_empty", Map.of("display_name", pretty));
                  }

                  var parentPath = append(namespace.getParentsList(), namespace.getDisplayName());
                  if (hasImmediateChildren(
                      catalogId.getAccountId(), catalogId.getId(), parentPath)) {
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

                  metadataGraph.invalidate(namespaceId);
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

  private Namespace applyNamespaceSpecPatch(
      Namespace current, NamespaceSpec spec, FieldMask mask, String corr) {

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }
    for (var p : paths) {
      if (!NAMESPACE_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", p));
      }
    }
    if (paths.contains("path") && paths.contains("display_name")) {
      throw GrpcErrors.invalidArgument(
          corr,
          "update_mask.path.invalid",
          Map.of("path", "Cannot combine 'path' with 'display_name'"));
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "catalog_id")) {
      if (!spec.hasCatalogId()) {
        throw GrpcErrors.invalidArgument(corr, "catalog_id.cannot_clear", Map.of());
      }
      var cat = spec.getCatalogId();
      ensureKind(cat, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
      catalogRepo
          .getById(cat)
          .orElseThrow(() -> GrpcErrors.notFound(corr, "catalog", Map.of("id", cat.getId())));
      b.setCatalogId(cat);
    }

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.cannot_clear", Map.of());
      }
      var name = normalizeName(spec.getDisplayName());
      if (name.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.cannot_clear", Map.of());
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
      for (var seg : path) {
        var s = normalizeName(seg);
        if (s.isBlank()) {
          throw GrpcErrors.invalidArgument(corr, "path.segment.blank", Map.of());
        }
      }
      if (path.isEmpty()) {
        b.clearParents();
      } else {
        var leaf = normalizeName(path.get(path.size() - 1));
        var parentsOnly = path.subList(0, path.size() - 1);
        b.setDisplayName(leaf);
        b.clearParents().addAllParents(parentsOnly);
      }
    }

    return b.build();
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
