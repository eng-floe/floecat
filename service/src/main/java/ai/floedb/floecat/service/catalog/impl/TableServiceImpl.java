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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.CreateTableResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableResponse;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableService;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableResponse;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
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
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class TableServiceImpl extends BaseServiceImpl implements TableService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject StatsRepository statsRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject MarkerStore markerStore;
  @Inject PointerStore pointerStore;
  @Inject CatalogOverlay overlay;

  private static final Set<String> TABLE_MUTABLE_PATHS =
      Set.of(
          "display_name",
          "description",
          "schema_json",
          "properties",
          "catalog_id",
          "namespace_id",
          "upstream",
          "upstream.connector_id",
          "upstream.uri",
          "upstream.namespace_path",
          "upstream.table_display_name",
          "upstream.format",
          "upstream.partition_keys",
          "upstream.column_id_algorithm");

  private static final String TBL_TOKEN_PREFIX = "tbl:";
  private static final Logger LOG = Logger.getLogger(TableService.class);

  private void ensureTableWritable(ResourceId tableId, String corr) {
    // Only user tables may be mutated.
    GraphNode node = requireVisibleTableNode(tableId, corr);
    enforceWritableTableNode(node, tableId, corr);
  }

  private void enforceWritableTableNode(GraphNode node, ResourceId tableId, String corr) {
    if (node instanceof UserTableNode) {
      return;
    }

    // System tables are immutable; return PERMISSION_DENIED when we can prove it's SYSTEM.
    if (node != null && node.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", tableId.getId(), "kind", "table"));
    }

    // Preserve semantics for other non-user nodes.
    throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
  }

  private void ensureTableWritableForDelete(ResourceId tableId, String corr, boolean callerCares) {
    GraphNode node = resolveTableNode(tableId, corr, callerCares);

    // if bestEffort and overlay failed / unresolved → allow repo-based best-effort delete
    if (node == null) return;

    enforceWritableTableNode(node, tableId, corr);
  }

  private GraphNode resolveTableNode(ResourceId tableId, String corr, boolean throwOnError) {
    if (tableId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_table_id>"));
    }
    ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

    try {
      return overlay.resolve(tableId).orElse(null);
    } catch (RuntimeException e) {
      if (throwOnError) {
        throw e;
      }
      return null;
    }
  }

  private GraphNode requireVisibleTableNode(ResourceId tableId, String corr) {
    GraphNode node = resolveTableNode(tableId, corr, true);
    if (node == null) throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
    return node;
  }

  private Table tableFromOverlayNodeOrRepo(GraphNode node, ResourceId tableId, String corr) {
    if (!(node instanceof TableNode tn)) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId()));
    }

    if (tn.origin() == GraphNodeOrigin.SYSTEM) {
      return tn.toTableProtoTable();
    }

    // USER (and any other future non-system): keep repo behavior
    return tableRepo
        .getById(tableId)
        .orElseThrow(() -> GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));
  }

  // Relative key for SYSTEM paging. Since ListTables is scoped to a namespace, leaf display name is
  // enough.
  // Normalize to match user table naming behavior.
  private String relativeTableKey(TableNode tn) {
    if (tn == null) return "";
    String name = tn.displayName();
    if (name == null) name = "";
    return normalizeName(name);
  }

  private static String encodeTblToken(String resumeAfterRel) {
    // allow an "empty" service token to mean "start SYSTEM phase"
    if (resumeAfterRel == null) resumeAfterRel = "";
    if (resumeAfterRel.isBlank()) {
      return TBL_TOKEN_PREFIX; // "tbl:" only
    }
    return TBL_TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(resumeAfterRel.getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeTblToken(String token) {
    if (token == null || token.isBlank() || !token.startsWith(TBL_TOKEN_PREFIX)) return "";
    if (token.length() == TBL_TOKEN_PREFIX.length()) {
      return ""; // "tbl:" means "start SYSTEM phase"
    }
    var s = token.substring(TBL_TOKEN_PREFIX.length());
    var bytes = Base64.getUrlDecoder().decode(s);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest request) {
    var L = LogHelper.start(LOG, "ListTables");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  final int want = Math.max(1, pageIn.limit);

                  var namespaceId = request.getNamespaceId();
                  ensureKind(
                      namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId());

                  // Overlay is source of truth for visibility (USER + SYSTEM)
                  NamespaceNode nsNode = requireVisibleNamespaceNode(namespaceId, correlationId());
                  ResourceId catalogId = nsNode.catalogId();

                  final boolean isServiceToken =
                      pageIn.token != null && pageIn.token.startsWith(TBL_TOKEN_PREFIX);
                  final String resumeAfterRel = isServiceToken ? decodeTblToken(pageIn.token) : "";

                  // Repo cursor only if we're not already in SYSTEM phase
                  String repoCursor = isServiceToken ? "" : pageIn.token;

                  var out = new java.util.ArrayList<Table>(want);
                  String lastEmittedRel = "";

                  //  Phase 1: repo-backed paging (user tables only)
                  String repoNext = "";
                  if (nsNode.origin() != GraphNodeOrigin.SYSTEM && !isServiceToken) {
                    var next = new StringBuilder();
                    final List<Table> scanned;
                    try {
                      scanned =
                          tableRepo.list(
                              pc.getAccountId(),
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
                    repoNext = next.toString(); // blank => repo exhausted
                  } else {
                    // SYSTEM namespace OR we're already in SYSTEM phase:
                    // treat repo as exhausted for listTables()
                    repoNext = "";
                  }

                  //  Phase 2: append SYSTEM tables once repo is exhausted
                  // Overlay has no paging: we implement an in-memory "resume after rel" like
                  // namespaces.
                  int sysCount = 0;
                  List<TableNode> sysNodes = List.of(); // only materialize when needed

                  final boolean repoExhausted = repoNext.isBlank();
                  if (repoExhausted) {
                    // Always need sysCount for totalSize.
                    // Only need to materialize + sort if we're going to emit.
                    var rels =
                        overlay.listRelationsInNamespace(catalogId, namespaceId).stream()
                            .filter(TableNode.class::isInstance)
                            .map(TableNode.class::cast)
                            .filter(tn -> tn.origin() == GraphNodeOrigin.SYSTEM)
                            .toList();

                    sysCount = rels.size();

                    if (out.size() < want && sysCount > 0) {
                      sysNodes = rels;

                      record SysItem(TableNode node, String rel) {}

                      var sysItems =
                          sysNodes.stream()
                              .map(tn -> new SysItem(tn, relativeTableKey(tn)))
                              .filter(it -> it.rel() != null && !it.rel().isBlank())
                              .sorted(java.util.Comparator.comparing(SysItem::rel))
                              .toList();

                      for (var it : sysItems) {
                        if (!resumeAfterRel.isBlank() && it.rel().compareTo(resumeAfterRel) <= 0) {
                          continue;
                        }
                        if (out.size() >= want) {
                          break;
                        }
                        out.add(it.node().toTableProtoTable());
                        lastEmittedRel = it.rel();
                      }
                    }
                  } else {
                    // Repo not exhausted: still need sysCount for totalSize, but avoid sorting.
                    sysCount =
                        (int)
                            overlay.listRelationsInNamespace(catalogId, namespaceId).stream()
                                .filter(TableNode.class::isInstance)
                                .map(TableNode.class::cast)
                                .filter(tn -> tn.origin() == GraphNodeOrigin.SYSTEM)
                                .count();
                  }

                  //  nextToken
                  // If repo still has more, return repo token.
                  // Else if we filled the page and there may be SYSTEM continuation, return service
                  // token.
                  String nextToken = repoNext;

                  if (nextToken.isBlank() && out.size() == want && sysCount > 0) {
                    // Continue in SYSTEM phase.
                    // lastEmittedRel may be blank if we haven't emitted any SYSTEM yet (bridge
                    // case):
                    // encodeTblToken("") => "tbl:" which means "start SYSTEM phase"
                    nextToken = encodeTblToken(lastEmittedRel);
                  }

                  // totalSize
                  int repoCount =
                      (nsNode.origin() == GraphNodeOrigin.SYSTEM)
                          ? 0
                          : tableRepo.count(
                              pc.getAccountId(), catalogId.getId(), namespaceId.getId());

                  var page = MutationOps.pageOut(nextToken, repoCount + sysCount);

                  return ListTablesResponse.newBuilder().addAllTables(out).setPage(page).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetTableResponse> getTable(GetTableRequest request) {
    var L = LogHelper.start(LOG, "GetTable");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "table.read");

                  GraphNode node = requireVisibleTableNode(request.getTableId(), correlationId());
                  Table table =
                      tableFromOverlayNodeOrRepo(node, request.getTableId(), correlationId());

                  return GetTableResponse.newBuilder().setTable(table).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateTableResponse> createTable(CreateTableRequest request) {
    var L = LogHelper.start(LOG, "CreateTable");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var accountId = pc.getAccountId();
                  var corr = pc.getCorrelationId();
                  authz.require(pc, "table.write");

                  ensureKind(
                      request.getSpec().getCatalogId(),
                      ResourceKind.RK_CATALOG,
                      "spec.catalog_id",
                      corr);
                  ensureKind(
                      request.getSpec().getNamespaceId(),
                      ResourceKind.RK_NAMESPACE,
                      "spec.namespace_id",
                      corr);

                  // Overlay is the source of truth for visibility (SYSTEM + user).
                  var catId = request.getSpec().getCatalogId();
                  requireVisibleCatalogNode(catId, corr);

                  // Namespace must be visible and writable (SYSTEM namespaces are immutable).
                  var nsNode =
                      requireWritableNamespaceNode(request.getSpec().getNamespaceId(), corr);

                  if (nsNode.catalogId() == null
                      || !nsNode.catalogId().getId().equals(catId.getId())) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        NAMESPACE_CATALOG_MISMATCH,
                        Map.of(
                            "namespace_id", nsNode.id().getId(),
                            "namespace.catalog_id",
                                nsNode.catalogId() == null ? "" : nsNode.catalogId().getId(),
                            "catalog_id", catId.getId()));
                  }

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var normalizedSpec = spec.toBuilder().setDisplayName(normName).build();
                  var fingerprint = canonicalFingerprint(normalizedSpec);
                  var tableResourceId = randomResourceId(accountId, ResourceKind.RK_TABLE);

                  var tableBuilder =
                      Table.newBuilder()
                          .setResourceId(tableResourceId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCatalogId(spec.getCatalogId())
                          .setNamespaceId(spec.getNamespaceId())
                          .setCreatedAt(tsNow)
                          .setSchemaJson(mustNonEmpty(spec.getSchemaJson(), "schema_json", corr))
                          .putAllProperties(spec.getPropertiesMap());
                  if (spec.hasUpstream()) {
                    validateUpstreamRef(spec.getUpstream(), corr);
                    tableBuilder.setUpstream(spec.getUpstream());
                  }
                  var table = tableBuilder.build();

                  if (idempotencyKey == null) {
                    var existing =
                        tableRepo.getByName(
                            accountId,
                            spec.getCatalogId().getId(),
                            spec.getNamespaceId().getId(),
                            normName);
                    if (existing.isPresent()) {
                      throw GrpcErrors.conflict(
                          corr,
                          TABLE_ALREADY_EXISTS,
                          Map.of(
                              "display_name", normName,
                              "catalog_id", spec.getCatalogId().getId(),
                              "namespace_id", spec.getNamespaceId().getId()));
                    }
                    try {
                      tableRepo.create(table);
                    } catch (BaseResourceRepository.NameConflictException nce) {
                      throw GrpcErrors.conflict(
                          corr,
                          TABLE_ALREADY_EXISTS,
                          Map.of(
                              "display_name", normName,
                              "catalog_id", spec.getCatalogId().getId(),
                              "namespace_id", spec.getNamespaceId().getId()));
                    }
                    markerStore.bumpNamespaceMarker(table.getNamespaceId());
                    metadataGraph.invalidate(tableResourceId);
                    var meta = tableRepo.metaForSafe(tableResourceId);
                    return CreateTableResponse.newBuilder().setTable(table).setMeta(meta).build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateTable",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    try {
                                      tableRepo.create(table);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      var existingOpt =
                                          tableRepo.getByName(
                                              accountId,
                                              spec.getCatalogId().getId(),
                                              spec.getNamespaceId().getId(),
                                              normName);
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromTable(existingOpt.get());
                                        if (Arrays.equals(
                                            fingerprint, canonicalFingerprint(existingSpec))) {
                                          markerStore.bumpNamespaceMarker(
                                              existingOpt.get().getNamespaceId());
                                          metadataGraph.invalidate(
                                              existingOpt.get().getResourceId());
                                          return new IdempotencyGuard.CreateResult<>(
                                              existingOpt.get(), existingOpt.get().getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.conflict(
                                          corr,
                                          TABLE_ALREADY_EXISTS,
                                          Map.of(
                                              "display_name", normName,
                                              "catalog_id", spec.getCatalogId().getId(),
                                              "namespace_id", spec.getNamespaceId().getId()));
                                    }
                                    markerStore.bumpNamespaceMarker(table.getNamespaceId());
                                    metadataGraph.invalidate(tableResourceId);
                                    return new IdempotencyGuard.CreateResult<>(
                                        table, tableResourceId);
                                  },
                                  t -> tableRepo.metaForSafe(t.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Table::parseFrom));

                  return CreateTableResponse.newBuilder()
                      .setTable(result.body)
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
  public Uni<UpdateTableResponse> updateTable(UpdateTableRequest request) {
    var L = LogHelper.start(LOG, "UpdateTable");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pctx = principal.get();
                  var corr = pctx.getCorrelationId();
                  authz.require(pctx, "table.write");

                  var tableId = request.getTableId();
                  ensureTableWritable(tableId, corr);

                  var current =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var spec = request.getSpec();
                  var mask = normalizeMask(request.getUpdateMask());

                  var meta = tableRepo.metaFor(tableId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  current =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(corr, TABLE, Map.of("id", tableId.getId())));

                  var desired = applyTableSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = tableRepo.metaFor(tableId);
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
                    return UpdateTableResponse.newBuilder()
                        .setTable(current)
                        .setMeta(metaNoop)
                        .build();
                  }

                  var conflictInfo =
                      Map.of(
                          "display_name", desired.getDisplayName(),
                          "catalog_id", desired.getCatalogId().getId(),
                          "namespace_id", desired.getNamespaceId().getId());

                  try {
                    boolean ok = tableRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = tableRepo.metaForSafe(tableId);
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.conflict(corr, TABLE_ALREADY_EXISTS, conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = tableRepo.metaForSafe(tableId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }
                  metadataGraph.invalidate(tableId);

                  if (!current.getNamespaceId().getId().equals(desired.getNamespaceId().getId())) {
                    markerStore.bumpNamespaceMarker(current.getNamespaceId());
                    markerStore.bumpNamespaceMarker(desired.getNamespaceId());
                  }

                  var outMeta = tableRepo.metaForSafe(tableId);
                  var latest = tableRepo.getById(tableId).orElse(desired);

                  return UpdateTableResponse.newBuilder().setTable(latest).setMeta(outMeta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteTableResponse> deleteTable(DeleteTableRequest request) {
    var L = LogHelper.start(LOG, "DeleteTable");

    return mapFailures(
            runWithRetry(
                () -> {
                  var principalContext = principal.get();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "table.write");

                  var tableId = request.getTableId();
                  boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                  ensureTableWritableForDelete(tableId, correlationId, callerCares);

                  Table existing = null;
                  try {
                    existing = tableRepo.getById(tableId).orElse(null);
                  } catch (BaseResourceRepository.CorruptionException ignore) {
                    // marker bump is best-effort; allow delete to proceed even if blob is missing
                  }

                  MutationMeta meta;
                  try {
                    meta = tableRepo.metaFor(tableId);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = tableRepo.metaForSafe(tableId);
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(
                          correlationId, TABLE, Map.of("id", tableId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    metadataGraph.invalidate(tableId);
                    if (existing != null) {
                      markerStore.bumpNamespaceMarker(existing.getNamespaceId());
                    }
                    purgeSnapshotsAndStats(tableId);
                    return DeleteTableResponse.newBuilder().setMeta(safe).build();
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> tableRepo.deleteWithPrecondition(tableId, expected),
                          () -> tableRepo.metaForSafe(tableId),
                          correlationId,
                          "table",
                          Map.of("id", tableId.getId()));

                  metadataGraph.invalidate(tableId);
                  if (existing != null) {
                    markerStore.bumpNamespaceMarker(existing.getNamespaceId());
                  }
                  purgeSnapshotsAndStats(tableId);
                  return DeleteTableResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static void validateTableMaskOrThrow(FieldMask mask, String corr) {
    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_REQUIRED, Map.of());
    }
    for (var p : paths) {
      if (!TABLE_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }
    boolean hasUpstreamWhole = paths.contains("upstream");
    boolean hasUpstreamParts =
        paths.stream().anyMatch(p -> p.startsWith("upstream.") && !p.equals("upstream"));
    if (hasUpstreamWhole && hasUpstreamParts) {
      throw GrpcErrors.invalidArgument(
          corr,
          UPDATE_MASK_UPSTREAM_MIX_FORBIDDEN,
          Map.of("hint", "use either 'upstream' or 'upstream.*' but not both"));
    }
  }

  private Table applyTableSpecPatch(Table current, TableSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);
    validateTableMaskOrThrow(mask, corr);

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(corr, DISPLAY_NAME_CANNOT_CLEAR, Map.of());
      }
      b.setDisplayName(
          normalizeName(mustNonEmpty(spec.getDisplayName(), "spec.display_name", corr)));
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    if (maskTargets(mask, "schema_json")) {
      if (!spec.hasSchemaJson()) {
        throw GrpcErrors.invalidArgument(corr, SCHEMA_JSON_CANNOT_CLEAR, Map.of());
      }
      b.setSchemaJson(mustNonEmpty(spec.getSchemaJson(), "spec.schema_json", corr));
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    boolean catalogChanged = false;
    boolean namespaceChanged = false;

    if (maskTargets(mask, "catalog_id")) {
      if (!spec.hasCatalogId()) {
        throw GrpcErrors.invalidArgument(corr, CATALOG_ID_CANNOT_CLEAR, Map.of());
      }
      var catId = spec.getCatalogId();
      ensureKind(catId, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
      catalogRepo
          .getById(catId)
          .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catId.getId())));
      b.setCatalogId(catId);
      catalogChanged = true;
    }

    if (maskTargets(mask, "namespace_id")) {
      if (!spec.hasNamespaceId()) {
        throw GrpcErrors.invalidArgument(corr, NAMESPACE_ID_CANNOT_CLEAR, Map.of());
      }
      var nsId = spec.getNamespaceId();
      ensureKind(nsId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", corr);
      var ns =
          namespaceRepo
              .getById(nsId)
              .orElseThrow(() -> GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", nsId.getId())));

      var effectiveCatalogId = catalogChanged ? b.getCatalogId() : current.getCatalogId();
      if (!ns.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            NAMESPACE_CATALOG_MISMATCH,
            Map.of(
                "namespace_id", nsId.getId(),
                "namespace.catalog_id", ns.getCatalogId().getId(),
                "catalog_id", effectiveCatalogId.getId()));
      }
      b.setNamespaceId(nsId);
      namespaceChanged = true;
    }

    if (catalogChanged && !namespaceChanged) {
      var effectiveCatalogId = b.getCatalogId();
      var ns =
          namespaceRepo
              .getById(b.getNamespaceId())
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          corr, NAMESPACE, Map.of("id", b.getNamespaceId().getId())));
      if (!ns.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            NAMESPACE_CATALOG_MISMATCH,
            Map.of(
                "namespace_id", b.getNamespaceId().getId(),
                "namespace.catalog_id", ns.getCatalogId().getId(),
                "catalog_id", effectiveCatalogId.getId()));
      }
    }

    var currentUp = current.getUpstream();
    var inUp = spec.getUpstream();

    UpstreamRef mergedUp;

    if (maskTargets(mask, "upstream")) {
      if (!spec.hasUpstream()) {
        b.clearUpstream();
        return b.build();
      }
      mergedUp = inUp;
    } else if (maskTargetsUnder(mask, "upstream")) {
      if (!spec.hasUpstream()) {
        throw GrpcErrors.invalidArgument(corr, UPSTREAM_MISSING_FOR_REPLACEMENT, Map.of());
      }
      if (!current.hasUpstream()) {
        throw GrpcErrors.invalidArgument(
            corr,
            UPSTREAM_MISSING_FOR_REPLACEMENT,
            Map.of("hint", "use update_mask ['upstream'] to set"));
      }
      var ub = currentUp.toBuilder();

      if (maskTargets(mask, "upstream.connector_id")) {
        if (inUp.hasConnectorId()) {
          ensureKind(
              inUp.getConnectorId(), ResourceKind.RK_CONNECTOR, "spec.upstream.connector_id", corr);
          ub.setConnectorId(inUp.getConnectorId());
        } else {
          ub.clearConnectorId();
        }
      }

      if (maskTargets(mask, "upstream.uri")) {
        ub.setUri(inUp.getUri());
      }

      if (maskTargets(mask, "upstream.namespace_path")) {
        ub.clearNamespacePath().addAllNamespacePath(inUp.getNamespacePathList());
      }

      if (maskTargets(mask, "upstream.table_display_name")) {
        ub.setTableDisplayName(inUp.getTableDisplayName());
      }

      if (maskTargets(mask, "upstream.format")) {
        ub.setFormat(inUp.getFormat());
      }

      if (maskTargets(mask, "upstream.partition_keys")) {
        ub.clearPartitionKeys().addAllPartitionKeys(inUp.getPartitionKeysList());
      }

      if (maskTargets(mask, "upstream.column_id_algorithm")) {
        ub.setColumnIdAlgorithm(inUp.getColumnIdAlgorithm());
      }

      mergedUp = ub.build();
    } else {
      mergedUp = currentUp;
    }

    boolean touched = upstreamTouched(mask);
    if (touched) {
      validateUpstreamRef(mergedUp, corr);
      b.setUpstream(mergedUp);
    }

    return b.build();
  }

  private static boolean upstreamTouched(FieldMask mask) {
    if (mask == null) {
      return false;
    }

    if (mask.getPathsList().contains("upstream")) {
      return true;
    }

    for (var p : mask.getPathsList()) {
      if (p.startsWith("upstream.")) {
        return true;
      }
    }
    return false;
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

  private void validateUpstreamRef(UpstreamRef up, String corr) {
    if (up.hasConnectorId()) {
      if (up.getConnectorId().getId().isBlank()) {
        throw GrpcErrors.invalidArgument(corr, UPSTREAM_CONNECTOR_ID_REQUIRED, Map.of());
      }
      ensureKind(
          up.getConnectorId(), ResourceKind.RK_CONNECTOR, "spec.upstream.connector_id", corr);
    }

    if (up.getNamespacePathCount() > 0) {
      for (var seg : up.getNamespacePathList()) {
        if (seg == null || seg.isBlank()) {
          throw GrpcErrors.invalidArgument(corr, UPSTREAM_NAMESPACE_PATH_SEGMENT_BLANK, Map.of());
        }
      }
    }

    if (up.getColumnIdAlgorithm() == ColumnIdAlgorithm.CID_UNKNOWN) {
      throw GrpcErrors.invalidArgument(
          corr,
          UPSTREAM_COLUMN_ID_ALGORITHM_INVALID,
          Map.of("upstream.column_id_algorithm", up.getColumnIdAlgorithm().name()));
    }
  }

  private static TableSpec specFromTable(Table table) {
    var b =
        TableSpec.newBuilder()
            .setCatalogId(table.getCatalogId())
            .setNamespaceId(table.getNamespaceId())
            .setDisplayName(normalizeName(table.getDisplayName()))
            .setDescription(table.getDescription())
            .setSchemaJson(table.getSchemaJson())
            .putAllProperties(table.getPropertiesMap());
    if (table.hasUpstream()) {
      b.setUpstream(table.getUpstream());
    }
    return b.build();
  }

  private static byte[] canonicalFingerprint(TableSpec s) {
    var c =
        new Canonicalizer()
            .scalar("cat", nullSafeId(s.getCatalogId()))
            .scalar("ns", nullSafeId(s.getNamespaceId()))
            .scalar("name", normalizeName(s.getDisplayName()))
            .scalar("description", s.getDescription())
            .scalar("schema_json", s.getSchemaJson())
            .map("properties", s.getPropertiesMap());
    if (s.hasUpstream()) {
      UpstreamRef up = s.getUpstream();
      c.group(
          "upstream",
          g ->
              g.scalar("connector_id", nullSafeId(up.getConnectorId()))
                  .scalar("uri", up.getUri())
                  .list("namespace_path", up.getNamespacePathList())
                  .scalar("table_display_name", up.getTableDisplayName())
                  .scalar("format", up.getFormat())
                  .list("partition_keys", up.getPartitionKeysList())
                  .scalar("column_id_algorithm", up.getColumnIdAlgorithm()));
    }
    return c.bytes();
  }

  private void purgeSnapshotsAndStats(ResourceId tableId) {
    String prefix = Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId());
    pointerStore.deleteByPrefix(prefix);
  }

  private CatalogNode requireVisibleCatalogNode(ResourceId catalogId, String corr) {
    if (catalogId == null) {
      throw GrpcErrors.notFound(corr, CATALOG, Map.of("id", "<missing_catalog_id>"));
    }

    ensureKind(catalogId, ResourceKind.RK_CATALOG, "catalog_id", corr);

    return overlay
        .resolve(catalogId)
        .filter(CatalogNode.class::isInstance)
        .map(CatalogNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, CATALOG, Map.of("id", catalogId.getId())));
  }

  private NamespaceNode requireVisibleNamespaceNode(ResourceId namespaceId, String corr) {
    if (namespaceId == null) {
      throw GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", "<missing_namespace_id>"));
    }

    ensureKind(namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", corr);

    return overlay
        .resolve(namespaceId)
        .filter(NamespaceNode.class::isInstance)
        .map(NamespaceNode.class::cast)
        .orElseThrow(() -> GrpcErrors.notFound(corr, NAMESPACE, Map.of("id", namespaceId.getId())));
  }

  private NamespaceNode requireWritableNamespaceNode(ResourceId namespaceId, String corr) {
    NamespaceNode ns = requireVisibleNamespaceNode(namespaceId, corr);
    if (ns.origin() == GraphNodeOrigin.SYSTEM) {
      throw GrpcErrors.permissionDenied(
          corr, SYSTEM_OBJECT_IMMUTABLE, Map.of("id", namespaceId.getId(), "kind", "namespace"));
    }
    return ns;
  }
}
