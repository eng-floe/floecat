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

import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.catalog.rpc.TableConstraintsService;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.util.ConstraintNormalizer;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@GrpcService
public class TableConstraintsServiceImpl extends BaseServiceImpl
    implements TableConstraintsService {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject ConstraintRepository constraints;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(TableConstraintsServiceImpl.class);

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

                  var tsNow = nowTs();

                  tables
                      .getById(request.getTableId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  TABLE,
                                  Map.of("id", request.getTableId().getId())));

                  // Constraints are strictly snapshot-scoped: the snapshot must exist.
                  snapshots
                      .getById(request.getTableId(), request.getSnapshotId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  SNAPSHOT,
                                  Map.of(
                                      "id", Long.toString(request.getSnapshotId()),
                                      "table_id", request.getTableId().getId())));

                  var normalized =
                      normalizeConstraintsIdentity(
                          request.getConstraints(), request.getTableId(), request.getSnapshotId());
                  var fingerprint = canonicalFingerprint(normalized);
                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  if (idempotencyKey == null) {
                    constraints.putSnapshotConstraints(
                        request.getTableId(), request.getSnapshotId(), normalized);
                    var meta =
                        constraints.metaFor(request.getTableId(), request.getSnapshotId(), tsNow);
                    return PutTableConstraintsResponse.newBuilder()
                        .setConstraints(normalized)
                        .setMeta(meta)
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
                                    constraints.putSnapshotConstraints(
                                        request.getTableId(), request.getSnapshotId(), normalized);
                                    return new IdempotencyGuard.CreateResult<>(
                                        normalized, request.getTableId());
                                  },
                                  ignored ->
                                      constraints.metaFor(
                                          request.getTableId(), request.getSnapshotId(), tsNow),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  SnapshotConstraints::parseFrom));

                  return PutTableConstraintsResponse.newBuilder()
                      .setConstraints(normalized)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static byte[] canonicalFingerprint(SnapshotConstraints constraints) {
    return ConstraintNormalizer.normalize(constraints).toByteArray();
  }

  private static SnapshotConstraints normalizeConstraintsIdentity(
      SnapshotConstraints source, ResourceId tableId, long snapshotId) {
    return ConstraintNormalizer.normalize(
        source.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build());
  }
}
