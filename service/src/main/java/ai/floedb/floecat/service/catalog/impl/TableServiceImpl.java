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
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.hint.EngineHintSchemaCleaner;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceTables;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.common.PersistedSecretPropertyValidator;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.types.ManagedTableProperties;
import com.google.protobuf.FieldMask;
import io.grpc.StatusRuntimeException;
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
  @Inject TableCleanupRepository tableCleanupRepo;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;
  @Inject TableRootWriter rootWriter;
  @Inject RecursiveResourceDropper recursiveDropper;
  @Inject EngineHintSchemaCleaner hintCleaner;
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

  private static final Logger LOG = Logger.getLogger(TableService.class);

  private CatalogSurfaceTables catalogSurfaceTables() {
    return new CatalogSurfaceTables(tableRepo, overlay);
  }

  private CatalogSurfaceWritePolicy catalogSurfaceWritePolicy() {
    return new CatalogSurfaceWritePolicy(overlay);
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

    // Publishing a child carries the namespace fence, so BatchGuardFailedException — retryable by
    // design — is now an ordinary outcome here: any concurrent write to the parent namespace, a
    // rename included, breaks the guard. The retry is the plain one; run() already subscribes on
    // the
    // Mutiny default executor, so a re-subscribed attempt blocks on a worker like the first.
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
                  var namespaceId =
                      request.getSpec().getNamespaceId().toBuilder()
                          .setAccountId(accountId)
                          .build();
                  var nsNode =
                      writePolicy.requireWritableNamespace(namespaceId, "spec.namespace_id", corr);
                  writePolicy.requireNamespaceInCatalog(nsNode, namespaceId, catId, corr);
                  var namespaceGuard =
                      markerStore.namespaceChildGuard(namespaceId, nsNode.blobUri());

                  var tsNow = nowTs();

                  var spec = request.getSpec();
                  var rawName = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var normName = normalizeName(rawName);
                  PersistedSecretPropertyValidator.validateNoGeneralMetadataSecretKeys(
                      spec.getPropertiesMap(), corr, "spec.properties");

                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var normalizedSpec = normalizedForPersistence(spec, normName, namespaceId);
                  var fingerprint = canonicalFingerprint(normalizedSpec);
                  var tableResourceId = randomResourceId(accountId, ResourceKind.RK_TABLE);

                  var tableBuilder =
                      Table.newBuilder()
                          .setResourceId(tableResourceId)
                          .setDisplayName(normName)
                          .setDescription(normalizedSpec.getDescription())
                          .setCatalogId(normalizedSpec.getCatalogId())
                          .setNamespaceId(normalizedSpec.getNamespaceId())
                          .setCreatedAt(tsNow)
                          .setSchemaJson(
                              mustNonEmpty(normalizedSpec.getSchemaJson(), "schema_json", corr))
                          .putAllProperties(normalizedSpec.getPropertiesMap());
                  if (normalizedSpec.hasUpstream()) {
                    validateUpstreamRef(normalizedSpec.getUpstream(), corr);
                    tableBuilder.setUpstream(normalizedSpec.getUpstream());
                  }
                  var table = tableBuilder.build();

                  if (idempotencyKey == null) {
                    java.util.Optional<Table> existing;
                    try {
                      existing =
                          tableRepo.getByName(
                              accountId,
                              spec.getCatalogId().getId(),
                              spec.getNamespaceId().getId(),
                              normName);
                    } catch (BaseResourceRepository.CorruptionException dangling) {
                      // The name resolves to a row whose blob is unreadable. A delete that could
                      // not
                      // parse its target removes the canonical pointer alone — the secondary keys
                      // are
                      // derived from the value — so this row can outlive the table and keep its
                      // name
                      // reserved. Release provably orphaned rows and re-ask; if the name is still
                      // held, the row belongs to something live and the read genuinely failed.
                      reclaimStrandedNames(spec, normName);
                      existing =
                          tableRepo.getByName(
                              accountId,
                              spec.getCatalogId().getId(),
                              spec.getNamespaceId().getId(),
                              normName);
                    }
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
                      // The namespace guard advances the children marker inside the create batch
                      // and pins the namespace pointer, so this table and a concurrent
                      // DeleteNamespace cannot both commit (see BatchGuard).
                      tableRepo.create(table, namespaceGuard);
                    } catch (BaseResourceRepository.NameConflictException nce) {
                      // The name may be held by a row whose relation no longer exists. A delete
                      // that
                      // could not parse its target removes the canonical pointer but cannot derive
                      // the
                      // secondary keys to remove with it, so the name stays reserved by a table
                      // that
                      // is gone — and this create is where that first becomes visible and where the
                      // namespace needed to clean it up is actually known. Reconcile provably
                      // orphaned rows and try once more before reporting the name as taken.
                      if (!retryCreateAfterReclaimingStrandedNames(
                          table, spec, normName, namespaceGuard)) {
                        throw relationNameConflict(
                            corr, accountId, spec.getCatalogId(), spec.getNamespaceId(), normName);
                      }
                    }
                    metadataGraph.invalidate(tableResourceId);
                    topology.evictRelationRefs(table.getNamespaceId());
                    var meta = tableRepo.metaForSafe(tableResourceId);
                    commitDefinitionToRoot(tableResourceId, meta);
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
                                      tableRepo.create(table, namespaceGuard);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      // Same two hazards as the unkeyed path above, and the same
                                      // handling: the name may be held by a row whose relation is
                                      // already gone, and the read that would tell us may itself
                                      // hit
                                      // that unreadable blob. Left out here, a keyed create was the
                                      // one caller with no way back from a name a corrupt-blob
                                      // delete
                                      // had reserved — ALREADY_EXISTS on every attempt, for a table
                                      // that does not exist, while the identical unkeyed request
                                      // recovered.
                                      java.util.Optional<Table> existingOpt;
                                      boolean releasedHere = false;
                                      try {
                                        existingOpt =
                                            tableRepo.getByName(
                                                accountId,
                                                spec.getCatalogId().getId(),
                                                spec.getNamespaceId().getId(),
                                                normName);
                                      } catch (
                                          BaseResourceRepository.CorruptionException dangling) {
                                        // The read itself hit the unreadable blob the stranded row
                                        // names. Release the provably orphaned rows and ask again.
                                        reclaimStrandedNames(spec, normName);
                                        releasedHere = true;
                                        existingOpt =
                                            tableRepo.getByName(
                                                accountId,
                                                spec.getCatalogId().getId(),
                                                spec.getNamespaceId().getId(),
                                                normName);
                                      }
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromTable(existingOpt.get());
                                        if (Arrays.equals(
                                            fingerprint, canonicalFingerprint(existingSpec))) {
                                          // No children-marker bump: this replay publishes nothing,
                                          // and the marker is a delete fence. The read above is
                                          // already stale by now — the table it found can have been
                                          // deleted since — so bumping could fail a legal
                                          // DeleteNamespace that scanned the emptied namespace,
                                          // unretryably, on behalf of a create that created
                                          // nothing.
                                          metadataGraph.invalidate(
                                              existingOpt.get().getResourceId());
                                          topology.evictRelationRefs(
                                              existingOpt.get().getNamespaceId());
                                          return new IdempotencyGuard.CreateResult<>(
                                              existingOpt.get(), existingOpt.get().getResourceId());
                                        }
                                      }
                                      // Nothing resolves the name, so it is held by index rows
                                      // alone.
                                      // Try once more before calling the name taken — releasing
                                      // those
                                      // rows first, unless the read above has already done it.
                                      if (!(releasedHere
                                          ? createOnceMore(table, namespaceGuard)
                                          : retryCreateAfterReclaimingStrandedNames(
                                              table, spec, normName, namespaceGuard))) {
                                        throw relationNameConflict(
                                            corr,
                                            accountId,
                                            spec.getCatalogId(),
                                            spec.getNamespaceId(),
                                            normName);
                                      }
                                    }
                                    metadataGraph.invalidate(tableResourceId);
                                    topology.evictRelationRefs(table.getNamespaceId());
                                    return new IdempotencyGuard.CreateResult<>(
                                        table, tableResourceId);
                                  },
                                  t -> tableRepo.metaForSafe(t.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Table::parseFrom));

                  // Parity with the non-idempotent path: record the definition on the root at
                  // create time. Idempotent (the committer no-ops when the ref already matches), so
                  // it is safe on both a genuine create and an idempotent replay, and it saves the
                  // first reader a lazy root synthesis.
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

    // A reparent publishes into the destination namespace and carries its fence; see createTable.
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
                  var writePolicy = catalogSurfaceWritePolicy();
                  var desiredNamespaceId =
                      desired.getNamespaceId().toBuilder()
                          .setAccountId(pctx.getAccountId())
                          .build();
                  var desiredNamespace =
                      writePolicy.requireWritableNamespace(
                          desiredNamespaceId, "namespace_id", corr);
                  writePolicy.requireNamespaceInCatalog(
                      desiredNamespace, desiredNamespaceId, desired.getCatalogId(), corr);
                  desired = normalizedForPersistence(desired, desiredNamespaceId);
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

                  // A reparent republishes the table under a different namespace, so it is a
                  // child-publishing write into the destination and needs the same fence a create
                  // does; an in-place update touches no namespace and stays unguarded.
                  boolean reparented =
                      !current.getNamespaceId().getId().equals(desired.getNamespaceId().getId())
                          || !current.getCatalogId().getId().equals(desired.getCatalogId().getId());
                  var destinationGuard =
                      reparented
                          ? markerStore.namespaceChildGuard(
                              desiredNamespaceId, desiredNamespace.blobUri())
                          : BatchGuard.NONE;

                  try {
                    boolean ok =
                        tableRepo.update(desired, meta.getPointerVersion(), destinationGuard);
                    if (!ok) {
                      throw tableUpdateConflict(
                          corr,
                          tableId,
                          meta.getPointerVersion(),
                          hasMeaningfulPrecondition(request.getPrecondition()));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    // A rename collides on the shared relation claim for the same reasons a create
                    // does, so it gets the same two answers. The name may be held by rows whose
                    // relation a corrupt-blob delete already took, and this rename is as able to
                    // release them as a create is — without it, renaming onto such a name failed
                    // forever while creating a new table with it succeeded. And a collision that
                    // survives the release is classified rather than always naming a table.
                    switch (retryUpdateAfterReclaimingStrandedNames(
                        desired, meta.getPointerVersion(), destinationGuard)) {
                      case COMMITTED -> {}
                      case NAME_HELD ->
                          throw relationNameConflict(
                              corr,
                              pctx.getAccountId(),
                              desired.getCatalogId(),
                              desired.getNamespaceId(),
                              desired.getDisplayName());
                      // The name was released and then the version moved under us. That is the same
                      // conflict the unreclaimed path reports for the same storage state, and it is
                      // retryable — reporting a name collision here made a benign race terminal.
                      case LOST_UPDATE ->
                          throw tableUpdateConflict(
                              corr,
                              tableId,
                              meta.getPointerVersion(),
                              hasMeaningfulPrecondition(request.getPrecondition()));
                    }
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    throw tableUpdateConflict(
                        corr,
                        tableId,
                        meta.getPointerVersion(),
                        hasMeaningfulPrecondition(request.getPrecondition()));
                  }
                  topology.evict(tableId);
                  metadataGraph.invalidate(tableId);

                  if (reparented) {
                    topology.evictRelationRefs(desired.getNamespaceId());
                    // The destination marker was advanced inside the update batch by the guard.
                    // The source marker is deliberately left alone: losing a child is not a
                    // publish,
                    // so bumping it would only break a concurrent delete of the source namespace.
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

    // A lost pointer CAS on a table that still exists is retryable now, so this body — the pinned
    // delete and the purge of the table's snapshots, stats and root, all blocking — can run more
    // than once. run() subscribes on the Mutiny default executor and the retry re-subscribes
    // through
    // it, so every attempt is on a worker.
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
                    recursiveDropper.cleanupDeletedTable(tableId);
                    return DeleteTableResponse.newBuilder().setMeta(safe).build();
                  }

                  // metaFor does not parse the blob, but staging the namespace-scoped durable
                  // cleanup handle does. If that parse fails, retain the historic corrupt-table
                  // behavior: the repository can still remove the canonical pointer, and the
                  // table-id-scoped fallback can purge snapshots, stats and root state without the
                  // blob. Unlike DeleteNamespace, no owned-state key needs the unreadable value.
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      correlationId, meta, request.getPrecondition());
                  TableCleanupRepository.DeletePlan cleanupPlan = null;
                  try {
                    cleanupPlan =
                        tableRepo
                            .getByBlobUri(meta.getBlobUri())
                            .flatMap(
                                table -> {
                                  var namespaceId =
                                      table.getNamespaceId().toBuilder()
                                          .setAccountId(tableId.getAccountId())
                                          .build();
                                  return markerStore
                                      .namespacePinnedGuardIfPresent(namespaceId)
                                      .map(
                                          namespaceGuard ->
                                              tableCleanupRepo.planDelete(
                                                  namespaceId, tableId, namespaceGuard));
                                })
                            .orElse(null);
                  } catch (BaseResourceRepository.CorruptionException corrupt) {
                    LOG.warnf(
                        "delete_table_blob_unreadable account_id=%s table_id=%s blob_uri=%s",
                        tableId.getAccountId(), tableId.getId(), meta.getBlobUri());
                  }
                  var plannedCleanup = cleanupPlan;
                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected ->
                              plannedCleanup == null
                                  ? tableRepo.deleteWithPrecondition(tableId, expected)
                                  : tableRepo.deleteWithPrecondition(
                                      tableId, expected, plannedCleanup.guard()),
                          () -> tableRepo.metaForSafe(tableId),
                          correlationId,
                          "table",
                          Map.of("id", tableId.getId()));

                  if (plannedCleanup != null) {
                    var pending = tableCleanupRepo.pending(plannedCleanup.cleanup());
                    if (pending.isPresent()) {
                      recursiveDropper.cleanupDeletedTable(pending.get());
                      return DeleteTableResponse.newBuilder().setMeta(out).build();
                    }
                  }
                  // A missing namespace uses the table-absence path directly. This is also the
                  // recovery path when another deleter won the canonical CAS and staged a task
                  // under the namespace it observed.
                  recursiveDropper.cleanupDeletedTable(tableId);
                  return DeleteTableResponse.newBuilder().setMeta(out).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /**
   * Re-attempts a create whose name collided, after releasing name rows whose relation is gone.
   *
   * <p>A delete that cannot parse its target removes only the canonical pointer — the repository
   * derives secondary keys from the value, and an unreadable value yields none — so the by-name row
   * and the shared relation-name claim outlive the table and keep its name reserved. Nothing on the
   * delete path can clean that up: the namespace those keys are built from lives in the very blob
   * that could not be read. A create is the first operation that both notices and knows the
   * namespace, so the reconcile belongs here.
   *
   * <p>Only rows whose relation has no canonical pointer left are released, so a live table sharing
   * the namespace is untouched. Retried once and only when something was actually released.
   *
   * <p>Restricted to the kinds this caller may write. The relation-name claim is shared across
   * kinds, so a stranded view row can be what holds this name — but releasing it is a view write,
   * which {@code table.write} alone does not buy. Without {@code view.write} the name is reported
   * as taken, the same answer {@code DeleteNamespace} gives a caller who cannot clear what is in
   * its way.
   *
   * @return how many rows were released
   */
  private int reclaimStrandedNames(TableSpec spec, String normName) {
    var pc = principal.get();
    var kinds =
        authz.allows(pc, "view.write")
            ? RecursiveResourceDropper.ALL_RELATION_KINDS
            : Set.of(ResourceKind.RK_TABLE);
    int reclaimed = recursiveDropper.reclaimStrandedRelationNames(namespaceOf(spec), kinds);
    if (reclaimed > 0) {
      LOG.infof(
          "table_create_reclaimed_stranded_names namespace_id=%s display_name=%s rows=%d",
          spec.getNamespaceId().getId(), normName, reclaimed);
    }
    return reclaimed;
  }

  /** The namespace the reconcile works in, scoped to the caller's account. */
  ai.floedb.floecat.catalog.rpc.Namespace namespaceOf(TableSpec spec) {
    String accountId = principal.get().getAccountId();
    return ai.floedb.floecat.catalog.rpc.Namespace.newBuilder()
        .setResourceId(spec.getNamespaceId().toBuilder().setAccountId(accountId))
        .setCatalogId(spec.getCatalogId().toBuilder().setAccountId(accountId))
        .build();
  }

  static TableSpec normalizedForPersistence(
      TableSpec spec, String displayName, ResourceId namespaceId) {
    return spec.toBuilder().setDisplayName(displayName).setNamespaceId(namespaceId).build();
  }

  static Table normalizedForPersistence(Table table, ResourceId namespaceId) {
    return table.toBuilder().setNamespaceId(namespaceId).build();
  }

  private boolean retryCreateAfterReclaimingStrandedNames(
      Table table, TableSpec spec, String normName, BatchGuard namespaceGuard) {
    // A name held by a live relation — a table, or a view through the shared claim — collides on
    // every attempt, deterministically. Establish that in a bounded number of reads before spending
    // a sweep of every by-name row in the namespace on it: the sweep only ever releases rows whose
    // relation is gone, so it has nothing to offer here.
    if (recursiveDropper.relationNameHeld(namespaceOf(spec), normName)) {
      return false;
    }
    if (reclaimStrandedNames(spec, normName) == 0) {
      return false;
    }
    return createOnceMore(table, namespaceGuard);
  }

  /**
   * The rename counterpart of {@link #retryCreateAfterReclaimingStrandedNames}: releases name rows
   * whose relation is gone and retries the update once.
   *
   * <p>The reclaim was originally justified by a create being "the first operation that both
   * notices a stranded name and knows the namespace its keys are built from". A rename notices and
   * knows the same things, so leaving it out made renaming onto such a name a permanent failure
   * while creating a new relation with it succeeded — the same name, two answers, decided by which
   * verb the caller reached for.
   *
   * @return which of the three outcomes happened: the retry committed, the name is genuinely held
   *     by a live relation, or the canonical CAS lost to a concurrent writer. A boolean conflated
   *     the last two, and the caller answered both with a terminal name collision.
   */
  private ReclaimedRetry retryUpdateAfterReclaimingStrandedNames(
      Table desired, long expectedVersion, BatchGuard destinationGuard) {
    var spec = specFromTable(desired);
    if (recursiveDropper.relationNameHeld(namespaceOf(spec), desired.getDisplayName())) {
      return ReclaimedRetry.NAME_HELD;
    }
    if (reclaimStrandedNames(spec, desired.getDisplayName()) == 0) {
      return ReclaimedRetry.NAME_HELD;
    }
    try {
      return tableRepo.update(desired, expectedVersion, destinationGuard)
          ? ReclaimedRetry.COMMITTED
          : ReclaimedRetry.LOST_UPDATE;
    } catch (BaseResourceRepository.NameConflictException stillTaken) {
      return ReclaimedRetry.NAME_HELD;
    }
  }

  /**
   * What a reclaim-and-retry settled. Three outcomes, because a boolean conflated two of them: the
   * retry's {@code update} reports false for a lost canonical CAS — an ordinary version race with a
   * concurrent writer, which has nothing to do with the name — and reading that as "the name is
   * taken" answered a benign race with a terminal ALREADY_EXISTS. The same false on the path that
   * never reclaims is classified as an update conflict, so the two paths disagreed about identical
   * storage state.
   */
  private enum ReclaimedRetry {
    /** The retry committed. */
    COMMITTED,
    /** A live relation still owns the name, so the collision is real. */
    NAME_HELD,
    /** The retry lost the canonical CAS; the name was released, the version moved. */
    LOST_UPDATE
  }

  /**
   * Builds the conflict error for a relation-name collision reported by the repository.
   *
   * <p>The relation-name claim is shared across kinds, so a collision does not mean a table holds
   * the name — a view can. Re-read the table index to tell them apart: a same-kind table is a
   * genuine {@code TABLE_ALREADY_EXISTS}, anything else is the kind-agnostic {@code
   * RELATION_NAME_ALREADY_CLAIMED}. Reporting every collision as an existing table told callers a
   * table was there when none was, which is the mirror image of what ViewServiceImpl already
   * avoided.
   */
  private StatusRuntimeException relationNameConflict(
      String corr,
      String accountId,
      ResourceId catalogId,
      ResourceId namespaceId,
      String normName) {
    boolean sameKindTable;
    try {
      sameKindTable =
          tableRepo
              .getByName(accountId, catalogId.getId(), namespaceId.getId(), normName)
              .isPresent();
    } catch (BaseResourceRepository.CorruptionException unresolvable) {
      // The name is held by a row whose relation cannot be read, and the reclaim could not release
      // it — the caller may lack the write grant for the kind that owns it. So it is held by
      // something this call cannot name, which is exactly what the kind-agnostic claim says.
      // Swallowed deliberately: this method builds an error, and an error path that throws an
      // INTERNAL of its own replaces a true conflict with a false service defect.
      sameKindTable = false;
    }
    var params =
        Map.of(
            "display_name", normName,
            "catalog_id", catalogId.getId(),
            "namespace_id", namespaceId.getId());
    return sameKindTable
        ? GrpcErrors.alreadyExists(corr, TABLE_ALREADY_EXISTS, params)
        : GrpcErrors.alreadyExists(corr, RELATION_NAME_ALREADY_CLAIMED, params);
  }

  /**
   * One more create attempt, for a caller that has already released the rows holding the name.
   *
   * <p>Separate from {@link #retryCreateAfterReclaimingStrandedNames} because that method's
   * reclaimed-nothing shortcut is wrong for such a caller: it would read the zero as "no stranded
   * rows, so the name is genuinely taken" when the truth is that this request had already released
   * them a moment earlier.
   */
  private boolean createOnceMore(Table table, BatchGuard namespaceGuard) {
    try {
      tableRepo.create(table, namespaceGuard);
      return true;
    } catch (BaseResourceRepository.NameConflictException stillTaken) {
      return false;
    }
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

  private RuntimeException tableUpdateConflict(
      String corr, ResourceId tableId, long expectedVersion, boolean callerCares) {
    if (!callerCares) {
      return new BaseResourceRepository.AbortRetryableException(
          "unconditional table update conflicted with a concurrent mutation: " + tableId.getId());
    }
    var nowMeta = tableRepo.metaForSafe(tableId);
    return GrpcErrors.preconditionFailed(
        corr,
        VERSION_MISMATCH,
        Map.of(
            "expected", Long.toString(expectedVersion),
            "actual", Long.toString(nowMeta.getPointerVersion())));
  }

  /** Record the table's (possibly new) immutable definition blob on its root. */
  private void commitDefinitionToRoot(
      ResourceId tableId, ai.floedb.floecat.common.rpc.MutationMeta meta) {
    if (rootWriter != null) {
      rootWriter.commitDefinition(tableId, meta);
    }
  }
}
