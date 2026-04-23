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

package ai.floedb.floecat.service.constraints.impl;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.*;

import ai.floedb.floecat.catalog.rpc.AddTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.AddTableConstraintResponse;
import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintResponse;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.ListTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ListTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableConstraintsService;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.CatalogOverlayGuards;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.ConstraintNormalizer;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class TableConstraintsServiceImpl extends BaseServiceImpl
    implements TableConstraintsService {

  @Inject SnapshotRepository snapshots;
  @Inject ConstraintRepository constraints;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject CatalogOverlay overlay;

  private static final Logger LOG = Logger.getLogger(TableConstraintsServiceImpl.class);

  /** Returns snapshot-scoped constraints for one table snapshot. */
  @Override
  public Uni<GetTableConstraintsResponse> getTableConstraints(GetTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "GetTableConstraints");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "catalog.read");

                  var tableId = request.getTableId();
                  ensureTableVisible(tableId);

                  var stored =
                      constraints
                          .getSnapshotConstraints(tableId, request.getSnapshotId())
                          .orElseThrow(
                              () -> constraintsBundleNotFound(tableId, request.getSnapshotId()));
                  return GetTableConstraintsResponse.newBuilder()
                      .setConstraints(stored)
                      .setMeta(constraints.metaFor(tableId, request.getSnapshotId()))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Lists snapshot-scoped constraints for a table with cursor pagination. */
  @Override
  public Uni<ListTableConstraintsResponse> listTableConstraints(
      ListTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "ListTableConstraints");

    return mapFailures(
            run(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "catalog.read");

                  var tableId = request.getTableId();
                  ensureTableVisible(tableId);

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();
                  final List<SnapshotConstraints> items;
                  try {
                    items =
                        constraints.listSnapshotConstraints(
                            tableId, Math.max(1, pageIn.limit), pageIn.token, next);
                  } catch (IllegalArgumentException badToken) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), PAGE_TOKEN_INVALID, Map.of("page_token", pageIn.token));
                  }

                  return ListTableConstraintsResponse.newBuilder()
                      .addAllConstraints(items)
                      .setPage(
                          MutationOps.pageOut(
                              next.toString(), constraints.countSnapshotConstraints(tableId)))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Upserts snapshot-scoped constraints for one table snapshot. */
  @Override
  public Uni<PutTableConstraintsResponse> putTableConstraints(PutTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "PutTableConstraints");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  var accountId = pc.getAccountId();
                  authz.require(pc, "table.write");

                  if (!request.hasConstraints()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraints"));
                  }
                  SnapshotConstraints incoming = canonicalizeAndValidate(request.getConstraints());

                  var tsNow = nowTs();
                  String tableCatalog = writableTableCatalogName(request.getTableId());

                  // Constraints are strictly snapshot-scoped: the snapshot must exist.
                  ensureSnapshotExists(request.getTableId(), request.getSnapshotId());

                  var normalized =
                      normalizeConstraintsIdentity(
                          incoming, request.getTableId(), request.getSnapshotId(), tableCatalog);
                  var fingerprint = canonicalFingerprint(normalized);
                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  if (idempotencyKey == null) {
                    boolean changed =
                        constraints.putSnapshotConstraints(
                            request.getTableId(), request.getSnapshotId(), normalized);
                    var meta =
                        constraints.metaFor(request.getTableId(), request.getSnapshotId(), tsNow);
                    return PutTableConstraintsResponse.newBuilder()
                        .setConstraints(normalized)
                        .setMeta(meta)
                        .setChanged(changed)
                        .build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "PutTableConstraints",
                                  idempotencyKey,
                                  () -> fingerprint,
                                  () -> {
                                    boolean changed =
                                        constraints.putSnapshotConstraints(
                                            request.getTableId(),
                                            request.getSnapshotId(),
                                            normalized);
                                    return new IdempotencyGuard.CreateResult<>(
                                        PutTableConstraintsResponse.newBuilder()
                                            .setConstraints(normalized)
                                            .setChanged(changed)
                                            .build(),
                                        request.getTableId());
                                  },
                                  response ->
                                      constraints.metaFor(
                                          request.getTableId(), request.getSnapshotId(), tsNow),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  PutTableConstraintsResponse::parseFrom));

                  return result.body.toBuilder().setMeta(result.meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Merges incoming constraints into one snapshot bundle, keyed by constraint name. */
  @Override
  public Uni<MergeTableConstraintsResponse> mergeTableConstraints(
      MergeTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "MergeTableConstraints");
    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.write");

                  if (!request.hasConstraints()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraints"));
                  }
                  SnapshotConstraints incoming = canonicalizeAndValidate(request.getConstraints());

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  String tableCatalog = writableTableCatalogName(tableId);
                  ensureSnapshotExists(tableId, snapshotId);

                  return MergeTableConstraintsResponse.newBuilder()
                      .setConstraints(
                          mutateSnapshotConstraints(
                              tableId,
                              snapshotId,
                              request.getPrecondition(),
                              current ->
                                  mergeConstraintsByName(
                                      current,
                                      normalizeConstraintsIdentity(
                                          incoming, tableId, snapshotId, tableCatalog))))
                      .setMeta(constraints.metaFor(tableId, snapshotId))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Appends incoming constraints into one snapshot bundle; existing names are rejected. */
  @Override
  public Uni<AppendTableConstraintsResponse> appendTableConstraints(
      AppendTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "AppendTableConstraints");
    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.write");

                  if (!request.hasConstraints()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraints"));
                  }
                  SnapshotConstraints incoming = canonicalizeAndValidate(request.getConstraints());

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  String tableCatalog = writableTableCatalogName(tableId);
                  ensureSnapshotExists(tableId, snapshotId);

                  return AppendTableConstraintsResponse.newBuilder()
                      .setConstraints(
                          mutateSnapshotConstraints(
                              tableId,
                              snapshotId,
                              request.getPrecondition(),
                              current ->
                                  appendConstraints(
                                      current,
                                      normalizeConstraintsIdentity(
                                          incoming, tableId, snapshotId, tableCatalog))))
                      .setMeta(constraints.metaFor(tableId, snapshotId))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Deletes snapshot-scoped constraints for one table snapshot. */
  @Override
  public Uni<DeleteTableConstraintsResponse> deleteTableConstraints(
      DeleteTableConstraintsRequest request) {
    var L = LogHelper.start(LOG, "DeleteTableConstraints");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.write");

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  ensureTableWritable(tableId);

                  var meta = constraints.metaForSafe(tableId, snapshotId);
                  if (!constraints.deleteSnapshotConstraints(tableId, snapshotId)) {
                    throw constraintsBundleNotFound(tableId, snapshotId);
                  }

                  return DeleteTableConstraintsResponse.newBuilder().setMeta(meta).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Upserts one constraint by name in a snapshot bundle while preserving other constraints. */
  @Override
  public Uni<AddTableConstraintResponse> addTableConstraint(AddTableConstraintRequest request) {
    var L = LogHelper.start(LOG, "AddTableConstraint");
    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.write");

                  if (!request.hasConstraint()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraint"));
                  }
                  ConstraintDefinition constraint =
                      canonicalizeConstraintName(request.getConstraint());
                  if (constraint.getName().isEmpty()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraint.name"));
                  }

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  String tableCatalog = writableTableCatalogName(tableId);
                  ensureSnapshotExists(tableId, snapshotId);

                  return AddTableConstraintResponse.newBuilder()
                      .setConstraints(
                          mutateSnapshotConstraints(
                              tableId,
                              snapshotId,
                              request.getPrecondition(),
                              current ->
                                  upsertConstraint(
                                      current, normalizeOneConstraint(constraint, tableCatalog))))
                      .setMeta(constraints.metaFor(tableId, snapshotId))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Deletes one constraint by name from a snapshot bundle while preserving other constraints. */
  @Override
  public Uni<DeleteTableConstraintResponse> deleteTableConstraint(
      DeleteTableConstraintRequest request) {
    var L = LogHelper.start(LOG, "DeleteTableConstraint");
    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principal.get();
                  authz.require(pc, "table.write");

                  var name = request.getConstraintName().trim();
                  if (name.isEmpty()) {
                    throw GrpcErrors.invalidArgument(
                        correlationId(), FIELD, Map.of("field", "constraint_name"));
                  }

                  var tableId = request.getTableId();
                  long snapshotId = request.getSnapshotId();
                  ensureTableWritable(tableId);
                  ensureSnapshotExists(tableId, snapshotId);

                  return DeleteTableConstraintResponse.newBuilder()
                      .setConstraints(
                          mutateSnapshotConstraints(
                              tableId,
                              snapshotId,
                              request.getPrecondition(),
                              current -> removeConstraintByName(current, name)))
                      .setMeta(constraints.metaFor(tableId, snapshotId))
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  /** Returns the canonical bytes used for idempotency fingerprint comparison. */
  private static byte[] canonicalFingerprint(SnapshotConstraints constraints) {
    return ConstraintNormalizer.normalize(constraints).toByteArray();
  }

  /**
   * Normalizes payload identity so persisted constraints always match request table/snapshot IDs.
   */
  private static SnapshotConstraints normalizeConstraintsIdentity(
      SnapshotConstraints source, ResourceId tableId, long snapshotId, String localCatalog) {
    SnapshotConstraints normalizedReferencedTables =
        normalizeReferencedTableCatalogs(source, localCatalog);
    return ConstraintNormalizer.normalize(normalizedReferencedTables.toBuilder().build());
  }

  /** Normalizes a single constraint payload into a canonical standalone entry. */
  private static ConstraintDefinition normalizeOneConstraint(
      ConstraintDefinition source, String localCatalog) {
    SnapshotConstraints normalized =
        ConstraintNormalizer.normalize(
            normalizeReferencedTableCatalogs(
                SnapshotConstraints.newBuilder().addConstraints(source).build(), localCatalog));
    return normalized.getConstraints(0);
  }

  private static SnapshotConstraints normalizeReferencedTableCatalogs(
      SnapshotConstraints source, String localCatalog) {
    if (localCatalog == null || localCatalog.isBlank() || source.getConstraintsCount() == 0) {
      return source;
    }
    boolean changed = false;
    SnapshotConstraints.Builder out = source.toBuilder().clearConstraints();
    for (ConstraintDefinition definition : source.getConstraintsList()) {
      if (!definition.hasReferencedTable()
          || !definition.getReferencedTable().getCatalog().isBlank()) {
        out.addConstraints(definition);
        continue;
      }
      NameRef referenced =
          NameRef.newBuilder(definition.getReferencedTable()).setCatalog(localCatalog).build();
      out.addConstraints(definition.toBuilder().setReferencedTable(referenced).build());
      changed = true;
    }
    return changed ? out.build() : source;
  }

  /**
   * Applies an atomic read-modify-write update on one snapshot constraints bundle using CAS.
   *
   * <p>When the bundle does not exist and the caller did not provide a meaningful precondition, an
   * empty bundle is created and the mutator is applied.
   */
  private SnapshotConstraints mutateSnapshotConstraints(
      ResourceId tableId,
      long snapshotId,
      ai.floedb.floecat.common.rpc.Precondition precondition,
      java.util.function.Function<SnapshotConstraints, SnapshotConstraints> mutator) {
    int attempts = 0;
    while (true) {
      if (++attempts > 32) {
        LOG.warnf(
            "mutateSnapshotConstraints: %d CAS attempts for table=%s snapshot=%d;"
                + " possible storage contention",
            attempts - 1, tableId.getId(), snapshotId);
      }
      var currentOpt = constraints.getSnapshotConstraints(tableId, snapshotId);
      if (currentOpt.isEmpty()) {
        if (hasMeaningfulPrecondition(precondition)) {
          throw constraintsBundleNotFound(tableId, snapshotId);
        }
        SnapshotConstraints created = mutator.apply(SnapshotConstraints.newBuilder().build());
        if (constraints.createSnapshotConstraintsIfAbsent(tableId, snapshotId, created)) {
          return constraints.getSnapshotConstraints(tableId, snapshotId).orElse(created);
        }
        continue;
      }

      SnapshotConstraints current = currentOpt.get();
      var currentMeta = constraints.metaFor(tableId, snapshotId);
      MutationOps.BaseServiceChecks.enforcePreconditions(
          correlationId(), currentMeta, precondition);

      SnapshotConstraints next = mutator.apply(current);
      if (ConstraintNormalizer.normalize(next).equals(ConstraintNormalizer.normalize(current))) {
        return current;
      }
      try {
        boolean updated =
            constraints.updateSnapshotConstraints(
                tableId, snapshotId, next, currentMeta.getPointerVersion());
        if (updated) {
          return constraints.getSnapshotConstraints(tableId, snapshotId).orElse(next);
        }
      } catch (BaseResourceRepository.NotFoundException ignore) {
        // Concurrent delete/create race; retry.
      }
    }
  }

  /** Replaces or appends one constraint, keyed by non-blank constraint name. */
  private static SnapshotConstraints upsertConstraint(
      SnapshotConstraints source, ConstraintDefinition constraint) {
    SnapshotConstraints canonicalSource = canonicalizeConstraintNames(source);
    List<ConstraintDefinition> kept = new ArrayList<>(canonicalSource.getConstraintsCount() + 1);
    for (ConstraintDefinition existing : canonicalSource.getConstraintsList()) {
      if (!constraint.getName().equals(existing.getName().trim())) {
        kept.add(existing);
      }
    }
    kept.add(constraint);
    return ConstraintNormalizer.normalize(
        canonicalSource.toBuilder().clearConstraints().addAllConstraints(kept).build());
  }

  /** Merges incoming constraints by name, replacing existing entries with matching names. */
  private SnapshotConstraints mergeConstraintsByName(
      SnapshotConstraints source, SnapshotConstraints incoming) {
    SnapshotConstraints canonicalSource = canonicalizeConstraintNames(source);
    Map<String, ConstraintDefinition> merged = new HashMap<>();
    for (ConstraintDefinition current : canonicalSource.getConstraintsList()) {
      merged.put(current.getName().trim(), canonicalizeConstraintName(current));
    }
    for (ConstraintDefinition update : incoming.getConstraintsList()) {
      merged.put(update.getName().trim(), canonicalizeConstraintName(update));
    }
    SnapshotConstraints.Builder out = SnapshotConstraints.newBuilder(canonicalSource);
    if (!incoming.getPropertiesMap().isEmpty()) {
      out.putAllProperties(incoming.getPropertiesMap());
    }
    return ConstraintNormalizer.normalize(
        out.clearConstraints().addAllConstraints(merged.values()).build());
  }

  /** Appends incoming constraints; fails when any incoming name already exists. */
  private SnapshotConstraints appendConstraints(
      SnapshotConstraints source, SnapshotConstraints incoming) {
    SnapshotConstraints canonicalSource = canonicalizeConstraintNames(source);

    Set<String> existingNames = new HashSet<>();
    for (ConstraintDefinition current : canonicalSource.getConstraintsList()) {
      existingNames.add(current.getName().trim());
    }
    for (ConstraintDefinition add : incoming.getConstraintsList()) {
      String addName = add.getName().trim();
      if (existingNames.contains(addName)) {
        throw GrpcErrors.alreadyExists(
            correlationId(),
            TABLE_CONSTRAINT_ALREADY_EXISTS,
            Map.of(
                "constraint_name",
                addName,
                "table_id",
                canonicalSource.getTableId().getId(),
                "snapshot_id",
                Long.toString(canonicalSource.getSnapshotId())));
      }
    }
    return ConstraintNormalizer.normalize(
        canonicalSource.toBuilder()
            .addAllConstraints(canonicalizeConstraintNames(incoming).getConstraintsList())
            .build());
  }

  /**
   * Canonicalizes constraint names (trims whitespace), then validates that no name is blank, that
   * no two constraints share the same name within this payload, and that each constraint definition
   * is internally consistent (e.g. FK local/referenced column counts match). Returns the
   * canonicalized payload.
   */
  private SnapshotConstraints canonicalizeAndValidate(SnapshotConstraints payload) {
    SnapshotConstraints canonical = canonicalizeConstraintNames(payload);
    validateConstraintNames(canonical);
    List<String> dups = duplicateNamesInPayload(canonical.getConstraintsList());
    if (!dups.isEmpty()) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          FIELD,
          Map.of("field", "constraints.constraints.name", "duplicate", dups.get(0)));
    }
    for (int i = 0; i < canonical.getConstraintsCount(); i++) {
      validateConstraintDefinition(canonical.getConstraints(i), i);
    }
    return canonical;
  }

  /**
   * Validates internal consistency of a single {@link ConstraintDefinition}. Currently checks:
   *
   * <ul>
   *   <li>FK: {@code columns} and {@code referenced_columns} must both be absent (unbound FK) or
   *       both be present with equal counts. A one-sided definition is always rejected.
   * </ul>
   */
  private void validateConstraintDefinition(ConstraintDefinition c, int index) {
    if (c.getType() != ConstraintType.CT_FOREIGN_KEY) {
      return;
    }
    boolean hasLocal = !c.getColumnsList().isEmpty();
    boolean hasReferenced = !c.getReferencedColumnsList().isEmpty();
    String field = "constraints.constraints[" + index + "].referenced_columns";
    if (hasLocal != hasReferenced) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          FIELD,
          Map.of(
              "field",
              field,
              "reason",
              "foreign key must have both local and referenced columns, or neither"));
    }
    if (hasLocal && c.getColumnsCount() != c.getReferencedColumnsCount()) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          FIELD,
          Map.of(
              "field",
              field,
              "reason",
              "column count must match local columns (" + c.getColumnsCount() + ")"));
    }
  }

  private static List<String> duplicateNamesInPayload(List<ConstraintDefinition> incoming) {
    Set<String> names = new HashSet<>();
    List<String> duplicates = new ArrayList<>();
    for (ConstraintDefinition definition : incoming) {
      String name = definition.getName().trim();
      if (!names.add(name)) {
        duplicates.add(name);
      }
    }
    return duplicates;
  }

  private void validateConstraintNames(SnapshotConstraints constraintsPayload) {
    for (int i = 0; i < constraintsPayload.getConstraintsCount(); i++) {
      String name = constraintsPayload.getConstraints(i).getName().trim();
      if (name.isEmpty()) {
        throw GrpcErrors.invalidArgument(
            correlationId(),
            FIELD,
            Map.of("field", "constraints.constraints.name", "index", Integer.toString(i)));
      }
    }
  }

  /** Removes one named constraint and fails with NOT_FOUND when absent. */
  private SnapshotConstraints removeConstraintByName(SnapshotConstraints source, String name) {
    SnapshotConstraints canonicalSource = canonicalizeConstraintNames(source);
    List<ConstraintDefinition> kept = new ArrayList<>(source.getConstraintsCount());
    boolean removed = false;
    for (ConstraintDefinition c : canonicalSource.getConstraintsList()) {
      if (!removed && name.equals(c.getName().trim())) {
        removed = true;
        continue;
      }
      kept.add(c);
    }
    if (!removed) {
      throw constraintNotFound(canonicalSource.getTableId(), canonicalSource.getSnapshotId(), name);
    }
    return ConstraintNormalizer.normalize(
        canonicalSource.toBuilder().clearConstraints().addAllConstraints(kept).build());
  }

  private static ConstraintDefinition canonicalizeConstraintName(ConstraintDefinition source) {
    String name = source.getName().trim();
    if (name.equals(source.getName())) {
      return source;
    }
    return source.toBuilder().setName(name).build();
  }

  private static SnapshotConstraints canonicalizeConstraintNames(SnapshotConstraints source) {
    SnapshotConstraints.Builder builder = source.toBuilder().clearConstraints();
    for (ConstraintDefinition definition : source.getConstraintsList()) {
      builder.addConstraints(canonicalizeConstraintName(definition));
    }
    return builder.build();
  }

  /** Ensures the request table is visible through overlay resolution (user or system). */
  private void ensureTableVisible(ResourceId tableId) {
    CatalogOverlayGuards.requireVisibleTableNode(overlay, tableId, correlationId());
  }

  /** Ensures the request table is mutable; system tables are immutable and rejected. */
  private void ensureTableWritable(ResourceId tableId) {
    CatalogOverlayGuards.requireWritableTableNode(overlay, tableId, correlationId());
  }

  private String writableTableCatalogName(ResourceId tableId) {
    var table = CatalogOverlayGuards.requireWritableTableNode(overlay, tableId, correlationId());
    if (!(table instanceof UserTableNode userTable)) {
      return "";
    }
    return overlay.catalog(userTable.catalogId()).map(CatalogNode::displayName).orElse("");
  }

  /** Ensures the target snapshot exists before strict snapshot-scoped writes proceed. */
  private void ensureSnapshotExists(ResourceId tableId, long snapshotId) {
    snapshots
        .getById(tableId, snapshotId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId(),
                    SNAPSHOT,
                    Map.of("id", Long.toString(snapshotId), "table_id", tableId.getId())));
  }

  private RuntimeException constraintsBundleNotFound(ResourceId tableId, long snapshotId) {
    return GrpcErrors.notFound(
        correlationId(),
        TABLE_CONSTRAINTS,
        Map.of("table_id", tableId.getId(), "snapshot_id", Long.toString(snapshotId)));
  }

  private RuntimeException constraintNotFound(
      ResourceId tableId, long snapshotId, String constraintName) {
    return GrpcErrors.notFound(
        correlationId(),
        TABLE_CONSTRAINT,
        Map.of(
            "table_id",
            tableId.getId(),
            "snapshot_id",
            Long.toString(snapshotId),
            "constraint_name",
            constraintName));
  }
}
