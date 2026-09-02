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
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.hint.EngineHintSchemaCleaner;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceTables;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.FenceRetry;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.common.PersistedSecretPropertyValidator;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.types.ManagedTableProperties;
import com.google.protobuf.FieldMask;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class TableServiceImpl extends BaseServiceImpl implements TableService {

  @Inject TableRepository tableRepo;
  @Inject SnapshotRepository snapshotRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;
  @Inject PointerStore pointerStore;
  @Inject TableRootWriter rootWriter;
  @Inject TableRootRepository tableRoots;
  @Inject EngineHintSchemaCleaner hintCleaner;
  @Inject CatalogGraphView graphView;

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
          "upstream.catalog_integration_id",
          "upstream.catalog_overlay_id",
          "upstream.uri",
          "upstream.namespace_path",
          "upstream.table_display_name",
          "upstream.format",
          "upstream.partition_keys",
          "upstream.column_id_algorithm");

  private static final Logger LOG = Logger.getLogger(TableService.class);

  private CatalogSurfaceTables catalogSurfaceTables() {
    return new CatalogSurfaceTables(tableRepo, graphView);
  }

  private CatalogSurfaceWritePolicy catalogSurfaceWritePolicy() {
    return new CatalogSurfaceWritePolicy(graphView);
  }

  @Override
  public Uni<ListTablesResponse> listTables(ListTablesRequest request) {
    var L = LogHelper.start(LOG, "ListTables");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.read");

                  return catalogSurfaceTables()
                      .listTables(request, pc.getAccountId(), pc.getCorrelationId());
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

                  return catalogSurfaceTables()
                      .getTable(request, principalContext.getCorrelationId());
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

                  var writePolicy = catalogSurfaceWritePolicy();
                  var catId = request.getSpec().getCatalogId();
                  writePolicy.requireWritableCatalog(catId, "spec.catalog_id", corr);
                  var nsNode =
                      writePolicy.requireWritableNamespace(
                          request.getSpec().getNamespaceId(), "spec.namespace_id", corr);
                  writePolicy.requireNamespaceInCatalog(
                      nsNode, request.getSpec().getNamespaceId(), catId, corr);

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);
                  PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                      spec.getPropertiesMap(), corr, "spec.properties");

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
                      throw GrpcErrors.alreadyExists(
                          corr,
                          TABLE_ALREADY_EXISTS,
                          Map.of(
                              "display_name", normName,
                              "catalog_id", spec.getCatalogId().getId(),
                              "namespace_id", spec.getNamespaceId().getId()));
                    }
                    try {
                      FenceRetry.retryWhileFenceLost(
                          "create table", () -> createTableFenced(spec, table, writePolicy, corr));
                    } catch (BaseResourceRepository.NameConflictException nce) {
                      throw GrpcErrors.alreadyExists(
                          corr,
                          TABLE_ALREADY_EXISTS,
                          Map.of(
                              "display_name", normName,
                              "catalog_id", spec.getCatalogId().getId(),
                              "namespace_id", spec.getNamespaceId().getId()));
                    }
                    // The namespace's relation marker advanced inside the create batch above.
                    metadataGraph.invalidate(tableResourceId);
                    topology.evictRelationRefs(table.getNamespaceId());
                    var meta = tableRepo.metaForSafe(tableResourceId);
                    commitDefinitionToRoot(tableResourceId, meta);
                    return CreateTableResponse.newBuilder().setTable(table).setMeta(meta).build();
                  }

                  var existingForIdempotency =
                      tableRepo.getByName(
                          accountId,
                          spec.getCatalogId().getId(),
                          spec.getNamespaceId().getId(),
                          normName);
                  var result =
                      MutationOps.createProtoRecoverable(
                          accountId,
                          "CreateTable",
                          idempotencyKey,
                          () -> fingerprint,
                          () ->
                              existingForIdempotency
                                  .filter(
                                      existing ->
                                          Arrays.equals(
                                              fingerprint,
                                              canonicalFingerprint(specFromTable(existing))))
                                  .map(Table::getResourceId)
                                  .orElse(tableResourceId),
                          (reservedId, committer) -> {
                            var existingOpt =
                                tableRepo.getByName(
                                    accountId,
                                    spec.getCatalogId().getId(),
                                    spec.getNamespaceId().getId(),
                                    normName);
                            if (existingOpt.isPresent()) {
                              var existing = existingOpt.get();
                              if (!existing.getResourceId().equals(reservedId)
                                  || !Arrays.equals(
                                      fingerprint, canonicalFingerprint(specFromTable(existing)))) {
                                throw GrpcErrors.alreadyExists(
                                    corr,
                                    TABLE_ALREADY_EXISTS,
                                    Map.of(
                                        "display_name",
                                        normName,
                                        "catalog_id",
                                        spec.getCatalogId().getId(),
                                        "namespace_id",
                                        spec.getNamespaceId().getId()));
                              }
                              var currentMeta = tableRepo.metaForSafe(reservedId);
                              var completed =
                                  tableRepo.completeWithMetaIfUnchanged(
                                      existing,
                                      currentMeta.getPointerVersion(),
                                      resource ->
                                          committer.prepareSuccessOps(
                                              new IdempotencyGuard.CommittedCreate<>(
                                                  resource.value(), reservedId, resource.meta())));
                              if (completed.isEmpty()) {
                                throw new BaseResourceRepository.AbortRetryableException(
                                    "table changed while committing idempotency receipt");
                              }
                              return new IdempotencyGuard.CommittedCreate<>(
                                  existing, reservedId, completed.get());
                            }
                            try {
                              var reserved = table.toBuilder().setResourceId(reservedId).build();
                              var committed =
                                  FenceRetry.retryWhileFenceLostForResult(
                                      "create table",
                                      () ->
                                          tableRepo.createWithCompletionWhilePointersMatch(
                                              reserved,
                                              tableCreateFence(spec, reserved, writePolicy, corr),
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
                                  corr,
                                  TABLE_ALREADY_EXISTS,
                                  Map.of(
                                      "display_name", normName,
                                      "catalog_id", spec.getCatalogId().getId(),
                                      "namespace_id", spec.getNamespaceId().getId()));
                            }
                          },
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Table::parseFrom);

                  metadataGraph.invalidate(result.body.getResourceId());
                  topology.evictRelationRefs(result.body.getNamespaceId());

                  // Parity with the non-idempotent path: record the definition on the root at
                  // create time. Idempotent (the committer no-ops when the ref already matches), so
                  // it is safe on both a genuine create and an idempotent replay, and ensures every
                  // supported table has a root before it becomes queryable.
                  commitDefinitionToRoot(result.body.getResourceId(), result.meta);
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
                  catalogSurfaceWritePolicy().requireWritableTable(tableId, corr);

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
                  if (maskTargets(mask, "properties")) {
                    PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                        spec.getPropertiesMap(), corr, "spec.properties");
                  }

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
                  // Sampled before the destination namespace is checked, not after. A namespace
                  // delete committing between the two moves the marker, and a version read after it
                  // is that writer's version -- so the CAS would confirm the delete instead of
                  // losing to it, landing this relation in a namespace that is gone. The read
                  // dependency on the destination's own pointer refuses one that already finished.
                  // Translated for the same reason a create is: a destination deleted a moment ago
                  // is reported by the fence, whose message is internal.
                  final var fromNamespace = current.getNamespaceId();
                  final var toNamespace = desired.getNamespaceId();
                  // A catalog-only change still re-keys the relation, because its by-name key and
                  // the count behind the fence both carry the catalog.
                  final boolean changesCatalog =
                      !current.getCatalogId().getId().equals(desired.getCatalogId().getId());
                  PointerConditions relationsFence =
                      namespaceFenceOrNotFound(
                          corr,
                          toNamespace,
                          () ->
                              markerStore.relationMoveFence(
                                  fromNamespace, toNamespace, changesCatalog));
                  var writePolicy = catalogSurfaceWritePolicy();
                  var desiredNamespace =
                      writePolicy.requireWritableNamespace(
                          desired.getNamespaceId(), "namespace_id", corr);
                  writePolicy.requireNamespaceInCatalog(
                      desiredNamespace, desired.getNamespaceId(), desired.getCatalogId(), corr);
                  if (hintCleaner.shouldClearHints(mask)) {
                    Table.Builder builder = desired.toBuilder();
                    hintCleaner.cleanTableHints(builder, mask, current, builder.build());
                    desired = builder.build();
                  }

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
                    boolean ok =
                        tableRepo
                            .updateWhilePointersMatch(
                                desired, meta.getPointerVersion(), relationsFence)
                            .isPresent();
                    if (!ok) {
                      throw tableUpdateConflict(
                          corr,
                          tableId,
                          meta.getPointerVersion(),
                          hasMeaningfulPrecondition(request.getPrecondition()));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    throw GrpcErrors.alreadyExists(corr, TABLE_ALREADY_EXISTS, conflictInfo);
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    throw tableUpdateConflict(
                        corr,
                        tableId,
                        meta.getPointerVersion(),
                        hasMeaningfulPrecondition(request.getPrecondition()));
                  }
                  topology.evict(tableId);
                  metadataGraph.invalidate(tableId);

                  if (!current.getNamespaceId().getId().equals(desired.getNamespaceId().getId())) {
                    topology.evictRelationRefs(desired.getNamespaceId());
                  }

                  var outMeta = tableRepo.metaForSafe(tableId);
                  var latest = tableRepo.getById(tableId).orElse(desired);

                  // The table blob changed (e.g. schema DDL) without a new snapshot: republish the
                  // coherent current pair last so a CURRENT pin sees the new table blob.
                  commitDefinitionToRoot(tableId, outMeta);

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
                  catalogSurfaceWritePolicy()
                      .requireWritableTableForDelete(tableId, correlationId, callerCares);

                  Table existing = null;
                  try {
                    existing = tableRepo.getById(tableId).orElse(null);
                  } catch (BaseResourceRepository.CorruptionException ignore) {
                    // Read for the delete's relation fence. A missing blob leaves no namespace id
                    // to fence on, and the delete still has to clean the pointer up.
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
                    topology.evict(tableId);
                    metadataGraph.invalidate(tableId);
                    purgeSnapshotsAndStats(tableId);
                    return DeleteTableResponse.newBuilder().setMeta(safe).build();
                  } catch (BaseResourceRepository.CorruptionException corrupt) {
                    var safe = tableRepo.metaForSafe(tableId);
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(
                          correlationId, TABLE, Map.of("id", tableId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        correlationId, safe, request.getPrecondition());
                    if (!tableRepo.deleteWhilePointersMatch(
                        tableId, safe.getPointerVersion(), PointerConditions.none())) {
                      // Nothing was deleted, so reporting success would say a corrupt row is gone
                      // while it is still addressable. Which of the two causes it was -- a stale
                      // expected version, or a lost fence -- is the same question every other
                      // conditional write here asks, and it is answered in one place.
                      throw MutationOps.lostFenceOrVersionMismatch(
                          correlationId,
                          "table delete",
                          safe.getPointerVersion(),
                          tableRepo.metaForSafe(tableId).getPointerVersion());
                    }
                    topology.evict(tableId);
                    metadataGraph.invalidate(tableId);
                    purgeSnapshotsAndStats(tableId);
                    return DeleteTableResponse.newBuilder().setMeta(safe).build();
                  }

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected ->
                              tableRepo.deleteWhilePointersMatch(
                                  tableId, expected, PointerConditions.none()),
                          () -> tableRepo.metaForSafe(tableId),
                          correlationId,
                          "table",
                          Map.of("id", tableId.getId()));

                  topology.evict(tableId);
                  metadataGraph.invalidate(tableId);
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
      b.clearProperties().putAllProperties(mergeTableProperties(current, spec.getPropertiesMap()));
    }

    boolean catalogChanged = false;
    boolean namespaceChanged = false;
    var writePolicy = catalogSurfaceWritePolicy();

    if (maskTargets(mask, "catalog_id")) {
      if (!spec.hasCatalogId()) {
        throw GrpcErrors.invalidArgument(corr, CATALOG_ID_CANNOT_CLEAR, Map.of());
      }
      var catId = spec.getCatalogId();
      writePolicy.requireWritableCatalog(catId, "spec.catalog_id", corr);
      b.setCatalogId(catId);
      catalogChanged = true;
    }

    if (maskTargets(mask, "namespace_id")) {
      if (!spec.hasNamespaceId()) {
        throw GrpcErrors.invalidArgument(corr, NAMESPACE_ID_CANNOT_CLEAR, Map.of());
      }
      var nsId = spec.getNamespaceId();
      var ns = writePolicy.requireWritableNamespace(nsId, "spec.namespace_id", corr);

      var effectiveCatalogId = catalogChanged ? b.getCatalogId() : current.getCatalogId();
      writePolicy.requireNamespaceInCatalog(ns, nsId, effectiveCatalogId, corr);
      b.setNamespaceId(nsId);
      namespaceChanged = true;
    }

    if (catalogChanged && !namespaceChanged) {
      var effectiveCatalogId = b.getCatalogId();
      var ns = writePolicy.requireWritableNamespace(b.getNamespaceId(), "namespace_id", corr);
      writePolicy.requireNamespaceInCatalog(ns, b.getNamespaceId(), effectiveCatalogId, corr);
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

      if (maskTargets(mask, "upstream.catalog_integration_id")) {
        if (inUp.hasCatalogIntegrationId()) {
          ensureKind(
              inUp.getCatalogIntegrationId(),
              ResourceKind.RK_CATALOG_INTEGRATION,
              "spec.upstream.catalog_integration_id",
              corr);
          ub.setCatalogIntegrationId(inUp.getCatalogIntegrationId());
        } else {
          ub.clearCatalogIntegrationId();
        }
      }

      if (maskTargets(mask, "upstream.catalog_overlay_id")) {
        if (inUp.hasCatalogOverlayId()) {
          ensureKind(
              inUp.getCatalogOverlayId(),
              ResourceKind.RK_CATALOG_OVERLAY,
              "spec.upstream.catalog_overlay_id",
              corr);
          ub.setCatalogOverlayId(inUp.getCatalogOverlayId());
        } else {
          ub.clearCatalogOverlayId();
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

  private Map<String, String> mergeTableProperties(Table current, Map<String, String> requested) {
    var merged = new LinkedHashMap<String, String>();
    if (requested != null) {
      merged.putAll(requested);
    }
    for (String key : ManagedTableProperties.engineManagedKeys()) {
      if (current.containsProperties(key)) {
        merged.put(key, current.getPropertiesOrThrow(key));
      }
    }
    return merged;
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
    if (up.hasCatalogIntegrationId()) {
      ensureKind(
          up.getCatalogIntegrationId(),
          ResourceKind.RK_CATALOG_INTEGRATION,
          "spec.upstream.catalog_integration_id",
          corr);
    }
    if (up.hasCatalogOverlayId()) {
      ensureKind(
          up.getCatalogOverlayId(),
          ResourceKind.RK_CATALOG_OVERLAY,
          "spec.upstream.catalog_overlay_id",
          corr);
      if (!up.hasCatalogIntegrationId()) {
        throw GrpcErrors.invalidArgument(
            corr, null, Map.of("field", "spec.upstream.catalog_integration_id"));
      }
    }
    if (up.hasConnectorId() && up.hasCatalogIntegrationId()) {
      throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "spec.upstream"));
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
                  .scalar("catalog_integration_id", nullSafeId(up.getCatalogIntegrationId()))
                  .scalar("catalog_overlay_id", nullSafeId(up.getCatalogOverlayId()))
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
    // The table-root pointer lives outside the snapshot prefix; left behind it would shadow the
    // initial state of a table later recreated with the same id. Its blobs are reclaimed by
    // CasBlobGc once the table drops out of the live set. Routed through the repository so the
    // root-pointer cache drops its entry with the pointer (same-process read-your-writes).
    tableRoots.purgeRoot(tableId);
  }

  /**
   * Creates the table under its namespace's relation fence, one attempt.
   *
   * <p>The namespace is re-validated here rather than once before the loop: a namespace delete is
   * one of the writers that takes this fence, so losing it can mean the namespace is gone. A retry
   * that skipped the check would commit a table into a deleted namespace, turning a correct
   * exclusion into the orphan the fence exists to prevent.
   */
  private boolean createTableFenced(
      TableSpec spec, Table table, CatalogSurfaceWritePolicy writePolicy, String corr) {
    return tableRepo
        .createWhilePointersMatch(table, tableCreateFence(spec, table, writePolicy, corr))
        .isPresent();
  }

  /** Samples and validates the namespace fence for one table-create attempt. */
  private PointerConditions tableCreateFence(
      TableSpec spec, Table table, CatalogSurfaceWritePolicy writePolicy, String corr) {
    // Sampled before the namespace is checked, not after. A delete that commits between the two
    // moves the marker, and a version read after it is that writer's version -- so the CAS would
    // confirm the delete instead of losing to it. Sampling first means the CAS requires the
    // pre-delete version and fails.
    //
    // The marker excludes a delete racing this write. The namespace's own pointer version excludes
    // one that already finished: the check below resolves through a per-process cache that a delete
    // on another instance does not invalidate, so it can pass for a namespace that is already gone.
    // The fence is sampled before the namespace is checked, so a namespace already gone is reported
    // by the fence rather than by the check -- and its message is internal. Translated here so the
    // caller gets the same NOT_FOUND they would have got had the check run first.
    PointerConditions fence =
        namespaceFenceOrNotFound(
            corr,
            table.getNamespaceId(),
            () -> markerStore.relationCreateFence(table.getNamespaceId()));
    var ns = writePolicy.requireWritableNamespace(spec.getNamespaceId(), "spec.namespace_id", corr);
    writePolicy.requireNamespaceInCatalog(ns, spec.getNamespaceId(), spec.getCatalogId(), corr);
    return fence;
  }

  private RuntimeException tableUpdateConflict(
      String corr, ResourceId tableId, long expectedVersion, boolean callerCares) {
    if (!callerCares) {
      return new BaseResourceRepository.AbortRetryableException(
          "unconditional table update conflicted with a concurrent mutation: " + tableId.getId());
    }
    return MutationOps.lostFenceOrVersionMismatch(
        corr, "table update", expectedVersion, tableRepo.metaForSafe(tableId).getPointerVersion());
  }

  /** Record the table's (possibly new) immutable definition blob on its root. */
  private void commitDefinitionToRoot(
      ResourceId tableId, ai.floedb.floecat.common.rpc.MutationMeta meta) {
    if (rootWriter != null) {
      rootWriter.commitDefinition(tableId, meta);
    }
  }
}
