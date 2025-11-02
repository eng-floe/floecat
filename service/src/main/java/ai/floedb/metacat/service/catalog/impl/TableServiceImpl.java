package ai.floedb.metacat.service.catalog.impl;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableResponse;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableResponse;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableResponse;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesResponse;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableService;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableResponse;
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
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class TableServiceImpl extends BaseServiceImpl implements TableService {

  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

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
                  var tables =
                      tableRepo.list(
                          principalContext.getTenantId(),
                          catalogId.getId(),
                          namespaceId.getId(),
                          Math.max(1, pageIn.limit),
                          pageIn.token,
                          next);

                  var page =
                      MutationOps.pageOut(
                          next.toString(),
                          tableRepo.count(
                              principalContext.getTenantId(),
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
                  var principalContext = principal.get();
                  var tenantId = principalContext.getTenantId();
                  var correlationId = principalContext.getCorrelationId();
                  authz.require(principalContext, "table.write");

                  catalogRepo
                      .getById(request.getSpec().getCatalogId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId,
                                  "catalog",
                                  Map.of("id", request.getSpec().getCatalogId().getId())));

                  namespaceRepo
                      .getById(request.getSpec().getNamespaceId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId,
                                  "namespace",
                                  Map.of("id", request.getSpec().getNamespaceId().getId())));

                  var tsNow = nowTs();

                  var fingerprint = canonicalFingerprint(request.getSpec());
                  var idempotencyKey =
                      request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                          ? request.getIdempotency().getKey()
                          : hashFingerprint(fingerprint);

                  var tableProto =
                      MutationOps.createProto(
                          tenantId,
                          "CreateTable",
                          idempotencyKey,
                          () -> fingerprint,
                          () -> {
                            String tableUuid = deterministicUuid(tenantId, "table", idempotencyKey);

                            var tableResourceId =
                                ResourceId.newBuilder()
                                    .setTenantId(tenantId)
                                    .setId(tableUuid)
                                    .setKind(ResourceKind.RK_TABLE)
                                    .build();

                            var table =
                                Table.newBuilder()
                                    .setResourceId(tableResourceId)
                                    .setDisplayName(
                                        mustNonEmpty(
                                            request.getSpec().getDisplayName(),
                                            "display_name",
                                            correlationId))
                                    .setDescription(request.getSpec().getDescription())
                                    .setFormat(request.getSpec().getFormat())
                                    .setCatalogId(request.getSpec().getCatalogId())
                                    .setNamespaceId(request.getSpec().getNamespaceId())
                                    .setRootUri(
                                        mustNonEmpty(
                                            request.getSpec().getRootUri(),
                                            "root_uri",
                                            correlationId))
                                    .setSchemaJson(
                                        mustNonEmpty(
                                            request.getSpec().getSchemaJson(),
                                            "schema_json",
                                            correlationId))
                                    .setCreatedAt(tsNow)
                                    .build();

                            try {
                              tableRepo.create(table);
                            } catch (BaseResourceRepository.NameConflictException e) {
                              var existing =
                                  tableRepo.getByName(
                                      tenantId,
                                      request.getSpec().getCatalogId().getId(),
                                      request.getSpec().getNamespaceId().getId(),
                                      table.getDisplayName());

                              if (existing.isPresent()) {
                                throw GrpcErrors.conflict(
                                    correlationId,
                                    "table.already_exists",
                                    Map.of("display_name", table.getDisplayName()));
                              }

                              throw new BaseResourceRepository.AbortRetryableException(
                                  "name conflict visibility window");
                            }

                            return new IdempotencyGuard.CreateResult<>(table, tableResourceId);
                          },
                          (table) -> tableRepo.metaForSafe(table.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Table::parseFrom);

                  return CreateTableResponse.newBuilder()
                      .setTable(tableProto.body)
                      .setMeta(tableProto.meta)
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
                  var pincipalContext = principal.get();
                  var correlationId = pincipalContext.getCorrelationId();
                  authz.require(pincipalContext, "table.write");

                  var tableId = request.getTableId();
                  ensureKind(tableId, ResourceKind.RK_TABLE, "table_id", correlationId);

                  var current =
                      tableRepo
                          .getById(tableId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      correlationId, "table", Map.of("id", tableId.getId())));

                  var spec = request.getSpec();
                  var updatedTableBuilder = current.toBuilder();

                  if (spec.getDisplayName() != null && !spec.getDisplayName().isBlank()) {
                    updatedTableBuilder.setDisplayName(
                        mustNonEmpty(spec.getDisplayName(), "spec.display_name", correlationId));
                  }
                  if (spec.getDescription() != null && !spec.getDescription().isBlank()) {
                    updatedTableBuilder.setDescription(spec.getDescription());
                  }
                  if (spec.getRootUri() != null && !spec.getRootUri().isBlank()) {
                    updatedTableBuilder.setRootUri(
                        mustNonEmpty(spec.getRootUri(), "spec.root_uri", correlationId));
                  }
                  if (spec.getSchemaJson() != null && !spec.getSchemaJson().isBlank()) {
                    updatedTableBuilder.setSchemaJson(
                        mustNonEmpty(spec.getSchemaJson(), "spec.schema_json", correlationId));
                  }

                  if (spec.getFormat() != TableFormat.TF_UNSPECIFIED) {
                    updatedTableBuilder.setFormat(spec.getFormat());
                  }

                  if (spec.getPartitionKeysCount() > 0) {
                    updatedTableBuilder
                        .clearPartitionKeys()
                        .addAllPartitionKeys(spec.getPartitionKeysList());
                  }
                  if (!spec.getPropertiesMap().isEmpty()) {
                    updatedTableBuilder.clearProperties().putAllProperties(spec.getPropertiesMap());
                  }

                  boolean catalogChanged = false;
                  boolean namespaceChanged = false;

                  if (spec.hasCatalogId()) {
                    var catId = spec.getCatalogId();
                    ensureKind(catId, ResourceKind.RK_CATALOG, "spec.catalog_id", correlationId);
                    catalogRepo
                        .getById(catId)
                        .orElseThrow(
                            () ->
                                GrpcErrors.notFound(
                                    correlationId, "catalog", Map.of("id", catId.getId())));
                    updatedTableBuilder.setCatalogId(catId);
                    catalogChanged = true;
                  }

                  if (spec.hasNamespaceId()) {
                    var namespaceId = spec.getNamespaceId();
                    ensureKind(
                        namespaceId, ResourceKind.RK_NAMESPACE, "spec.namespace_id", correlationId);
                    var namespace =
                        namespaceRepo
                            .getById(namespaceId)
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId,
                                        "namespace",
                                        Map.of("id", namespaceId.getId())));
                    var effectiveCatalogId =
                        catalogChanged
                            ? updatedTableBuilder.getCatalogId()
                            : current.getCatalogId();
                    if (!namespace.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
                      throw GrpcErrors.invalidArgument(
                          correlationId,
                          "namespace.catalog_mismatch",
                          Map.of(
                              "namespace_id", namespaceId.getId(),
                              "namespace.catalog_id", namespace.getCatalogId().getId(),
                              "catalog_id", effectiveCatalogId.getId()));
                    }
                    updatedTableBuilder.setNamespaceId(namespaceId);
                    namespaceChanged = true;
                  }

                  if (catalogChanged && !namespaceChanged) {
                    var effectiveCatalogId = updatedTableBuilder.getCatalogId();
                    var namespace =
                        namespaceRepo
                            .getById(updatedTableBuilder.getNamespaceId())
                            .orElseThrow(
                                () ->
                                    GrpcErrors.notFound(
                                        correlationId,
                                        "namespace",
                                        Map.of(
                                            "id", updatedTableBuilder.getNamespaceId().getId())));
                    if (!namespace.getCatalogId().getId().equals(effectiveCatalogId.getId())) {
                      throw GrpcErrors.invalidArgument(
                          correlationId,
                          "namespace.catalog_mismatch",
                          Map.of(
                              "namespace_id", updatedTableBuilder.getNamespaceId().getId(),
                              "namespace.catalog_id", namespace.getCatalogId().getId(),
                              "catalog_id", effectiveCatalogId.getId()));
                    }
                  }

                  var desired = updatedTableBuilder.build();

                  if (desired.equals(current)) {
                    var metaNoop = tableRepo.metaForSafe(tableId);
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, metaNoop, request.getPrecondition());
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
                      (expectedVersion) -> tableRepo.update(desired, expectedVersion),
                      () -> tableRepo.metaForSafe(tableId),
                      correlationId,
                      "table",
                      conflictInfo);

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

                    return DeleteTableResponse.newBuilder().setMeta(meta).build();

                  } catch (BaseResourceRepository.NotFoundException pointerMissing) {
                    tableRepo.delete(tableId);
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

  private static byte[] canonicalFingerprint(TableSpec s) {
    return new Canonicalizer()
        .scalar("cat", s.getCatalogId().getId())
        .scalar("ns", s.getNamespaceId().getId())
        .scalar("name", s.getDisplayName())
        .scalar("description", s.getDescription())
        .scalar("format", s.getFormat().getNumber())
        .scalar("root", s.getRootUri())
        .scalar("schema", s.getSchemaJson())
        .list("pk", s.getPartitionKeysList())
        .map("prop", s.getPropertiesMap())
        .bytes();
  }
}
