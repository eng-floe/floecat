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
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
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
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject MetadataGraph metadataGraph;

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

                  catalogRepo
                      .getById(request.getSpec().getCatalogId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  corr,
                                  "catalog",
                                  Map.of("id", request.getSpec().getCatalogId().getId())));

                  namespaceRepo
                      .getById(request.getSpec().getNamespaceId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  corr,
                                  "namespace",
                                  Map.of("id", request.getSpec().getNamespaceId().getId())));

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var fingerprint =
                      canonicalFingerprint(spec.toBuilder().setDisplayName(normName).build());
                  var tableUuid =
                      deterministicUuid(
                          accountId,
                          "table",
                          Base64.getUrlEncoder().withoutPadding().encodeToString(fingerprint));

                  var tableResourceId =
                      ResourceId.newBuilder()
                          .setAccountId(accountId)
                          .setId(tableUuid)
                          .setKind(ResourceKind.RK_TABLE)
                          .build();

                  var table =
                      Table.newBuilder()
                          .setResourceId(tableResourceId)
                          .setDisplayName(normName)
                          .setDescription(spec.getDescription())
                          .setCatalogId(spec.getCatalogId())
                          .setNamespaceId(spec.getNamespaceId())
                          .setCreatedAt(tsNow)
                          .setSchemaJson(mustNonEmpty(spec.getSchemaJson(), "schema_json", corr))
                          .setUpstream(spec.getUpstream())
                          .putAllProperties(spec.getPropertiesMap())
                          .build();

                  if (idempotencyKey == null) {
                    var existing =
                        tableRepo.getByName(
                            accountId,
                            spec.getCatalogId().getId(),
                            spec.getNamespaceId().getId(),
                            normName);
                    if (existing.isPresent()) {
                      var meta = tableRepo.metaForSafe(existing.get().getResourceId());
                      return CreateTableResponse.newBuilder()
                          .setTable(existing.get())
                          .setMeta(meta)
                          .build();
                    }
                    tableRepo.create(table);
                    metadataGraph.invalidate(tableResourceId);
                    var meta = tableRepo.metaForSafe(tableResourceId);
                    return CreateTableResponse.newBuilder().setTable(table).setMeta(meta).build();
                  }

                  var result =
                      MutationOps.createProto(
                          accountId,
                          "CreateTable",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            tableRepo.create(table);
                            metadataGraph.invalidate(tableResourceId);
                            return new IdempotencyGuard.CreateResult<>(table, tableResourceId);
                          },
                          t -> tableRepo.metaForSafe(t.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Table::parseFrom,
                          rec -> tableRepo.getById(rec.getResourceId()).isPresent());

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
                  var mask = request.getUpdateMask();

                  var desired = applyTableSpecPatch(current, spec, mask, corr);

                  if (desired.equals(current)) {
                    var metaNoop = tableRepo.metaForSafe(tableId);
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

                  MutationOps.updateWithPreconditions(
                      () -> tableRepo.metaFor(tableId),
                      request.getPrecondition(),
                      expectedVersion -> tableRepo.update(desired, expectedVersion),
                      () -> tableRepo.metaForSafe(tableId),
                      corr,
                      "table",
                      conflictInfo);
                  metadataGraph.invalidate(tableId);

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

                  try {
                    var meta =
                        MutationOps.deleteWithPreconditions(
                            () -> tableRepo.metaFor(tableId),
                            request.getPrecondition(),
                            expected -> tableRepo.deleteWithPrecondition(tableId, expected),
                            () -> tableRepo.metaForSafe(tableId),
                            correlationId,
                            "table",
                            Map.of("id", tableId.getId()));

                    metadataGraph.invalidate(tableId);
                    return DeleteTableResponse.newBuilder().setMeta(meta).build();

                  } catch (BaseResourceRepository.NotFoundException pointerMissing) {
                    tableRepo.delete(tableId);
                    metadataGraph.invalidate(tableId);
                    return DeleteTableResponse.newBuilder()
                        .setMeta(tableRepo.metaForSafe(tableId))
                        .build();
                  }
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
    validateTableMaskOrThrow(mask, corr);

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.cannot_clear", Map.of());
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
        throw GrpcErrors.invalidArgument(corr, "upstream.missing_for_replacement", Map.of());
      }
      mergedUp = inUp;
    } else if (maskTargetsUnder(mask, "upstream")) {
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
    }
    b.setUpstream(mergedUp);

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

  private void validateUpstreamRef(UpstreamRef up, String corr) {
    if (!up.hasConnectorId() || up.getConnectorId().getId().isBlank()) {
      throw GrpcErrors.invalidArgument(corr, "upstream.connector_id.required", Map.of());
    }

    if (up.getNamespacePathCount() == 0) {
      throw GrpcErrors.invalidArgument(corr, "upstream.namespace_path.required", Map.of());
    }
    for (var seg : up.getNamespacePathList()) {
      if (seg == null || seg.isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "upstream.namespace_path.segment.blank", Map.of());
      }
    }

    if (up.getTableDisplayName().isBlank()) {
      throw GrpcErrors.invalidArgument(corr, "upstream.table_display_name.required", Map.of());
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

  private static byte[] canonicalFingerprint(TableSpec s) {
    return new Canonicalizer()
        .scalar("cat", nullSafeId(s.getCatalogId()))
        .scalar("ns", nullSafeId(s.getNamespaceId()))
        .scalar("name", normalizeName(s.getDisplayName()))
        .bytes();
  }
}
