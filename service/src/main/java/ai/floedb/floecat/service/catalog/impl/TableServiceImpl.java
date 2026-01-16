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
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Arrays;
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
          "upstream.field_id_by_path");

  private static final Logger LOG = Logger.getLogger(TableService.class);

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest request) {
    var L = LogHelper.start(LOG, "ListTables");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "table.read");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  var namespaceId = request.getNamespaceId();
                  ensureKind(
                      namespaceId, ResourceKind.RK_NAMESPACE, "namespace_id", correlationId());

                  var namespace =
                      namespaceRepo
                          .getById(namespaceId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      "namespace",
                                      Map.of("id", namespaceId.getId())));

                  var catalogId = namespace.getCatalogId();

                  List<Table> tables;
                  try {

                    tables =
                        tableRepo.list(
                            principalContext.getAccountId(),
                            catalogId.getId(),
                            namespaceId.getId(),
                            Math.max(1, pageIn.limit),
                            pageIn.token,
                            next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), "page_token.invalid", Map.of("page_token", pageIn.token));
                  }

                  var page =
                      MutationOps.pageOut(
                          next.toString(),
                          tableRepo.count(
                              principalContext.getAccountId(),
                              catalogId.getId(),
                              namespaceId.getId()));

                  return ListTablesResponse.newBuilder().addAllTables(tables).setPage(page).build();
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
                  ensureKind(
                      request.getTableId(), ResourceKind.RK_TABLE, "table_id", correlationId());

                  var table =
                      tableRepo
                          .getById(request.getTableId())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId(),
                                      "table",
                                      Map.of("id", request.getTableId().getId())));
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

                  catalogRepo
                      .getById(request.getSpec().getCatalogId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  corr,
                                  "catalog",
                                  Map.of("id", request.getSpec().getCatalogId().getId())));

                  var ns =
                      namespaceRepo
                          .getById(request.getSpec().getNamespaceId())
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr,
                                      "namespace",
                                      Map.of("id", request.getSpec().getNamespaceId().getId())));
                  var catId = request.getSpec().getCatalogId();
                  if (!ns.getCatalogId().getId().equals(catId.getId())) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        "namespace.catalog_mismatch",
                        Map.of(
                            "namespace_id", ns.getResourceId().getId(),
                            "namespace.catalog_id", ns.getCatalogId().getId(),
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
                          "table.already_exists",
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
                          "table.already_exists",
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
                                          "table.already_exists",
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
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", corr);

                  var current =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr, "table", Map.of("id", tableId.getId())));

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
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
                                  GrpcErrors.notFound(
                                      corr, "table", Map.of("id", tableId.getId())));

                  var desired = applyTableSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = tableRepo.metaFor(tableId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && metaNoop.getPointerVersion() != meta.getPointerVersion()) {
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          "version_mismatch",
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
                          "version_mismatch",
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.conflict(corr, "table.already_exists", conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = tableRepo.metaForSafe(tableId);
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        "version_mismatch",
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
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

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
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(
                          correlationId, "table", Map.of("id", tableId.getId()));
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
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }
    for (var p : paths) {
      if (!TABLE_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", p));
      }
    }
    boolean hasUpstreamWhole = paths.contains("upstream");
    boolean hasUpstreamParts =
        paths.stream().anyMatch(p -> p.startsWith("upstream.") && !p.equals("upstream"));
    if (hasUpstreamWhole && hasUpstreamParts) {
      throw GrpcErrors.invalidArgument(
          corr,
          "update_mask.upstream.mix_forbidden",
          Map.of("hint", "use either 'upstream' or 'upstream.*' but not both"));
    }
  }

  private Table applyTableSpecPatch(Table current, TableSpec spec, FieldMask mask, String corr) {
    mask = normalizeMask(mask);
    validateTableMaskOrThrow(mask, corr);

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.cannot_clear", Map.of());
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
        throw GrpcErrors.invalidArgument(corr, "schema_json.cannot_clear", Map.of());
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
        throw GrpcErrors.invalidArgument(corr, "catalog_id.cannot_clear", Map.of());
      }
      var catId = spec.getCatalogId();
      ensureKind(catId, ResourceKind.RK_CATALOG, "spec.catalog_id", corr);
      catalogRepo
          .getById(catId)
          .orElseThrow(() -> GrpcErrors.notFound(corr, "catalog", Map.of("id", catId.getId())));
      b.setCatalogId(catId);
      catalogChanged = true;
    }

    if (maskTargets(mask, "namespace_id")) {
      if (!spec.hasNamespaceId()) {
        throw GrpcErrors.invalidArgument(corr, "namespace_id.cannot_clear", Map.of());
      }
      var nsId = spec.getNamespaceId();
      ensureKind(nsId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", corr);
      var ns =
          namespaceRepo
              .getById(nsId)
              .orElseThrow(
                  () -> GrpcErrors.notFound(corr, "namespace", Map.of("id", nsId.getId())));

      var effectiveCatalogId = catalogChanged ? b.getCatalogId() : current.getCatalogId();
      if (!ns.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            "namespace.catalog_mismatch",
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
                          corr, "namespace", Map.of("id", b.getNamespaceId().getId())));
      if (!ns.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
        throw GrpcErrors.invalidArgument(
            corr,
            "namespace.catalog_mismatch",
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
        throw GrpcErrors.invalidArgument(corr, "upstream.missing_for_replacement", Map.of());
      }
      if (!current.hasUpstream()) {
        throw GrpcErrors.invalidArgument(
            corr,
            "upstream.missing_for_replacement",
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

      if (maskTargets(mask, "upstream.field_id_by_path")) {
        ub.clearFieldIdByPath().putAllFieldIdByPath(inUp.getFieldIdByPathMap());
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
        throw GrpcErrors.invalidArgument(corr, "upstream.connector_id.required", Map.of());
      }
      ensureKind(
          up.getConnectorId(), ResourceKind.RK_CONNECTOR, "spec.upstream.connector_id", corr);
    }

    if (up.getNamespacePathCount() > 0) {
      for (var seg : up.getNamespacePathList()) {
        if (seg == null || seg.isBlank()) {
          throw GrpcErrors.invalidArgument(corr, "upstream.namespace_path.segment.blank", Map.of());
        }
      }
    }

    for (var e : up.getFieldIdByPathMap().entrySet()) {
      var path = e.getKey();
      var id = e.getValue();
      if (path == null || path.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "upstream.field_id_by_path.key.blank", Map.of());
      }

      if (id < 0) {
        throw GrpcErrors.invalidArgument(
            corr, "upstream.field_id_by_path.id.negative", Map.of("path", path));
      }
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
                  .map("field_id_by_path", up.getFieldIdByPathMap()));
    }
    return c.bytes();
  }

  private void purgeSnapshotsAndStats(ResourceId tableId) {
    String prefix = Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId());
    pointerStore.deleteByPrefix(prefix);
  }
}
