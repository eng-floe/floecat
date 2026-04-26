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

import ai.floedb.floecat.catalog.rpc.GetIndexArtifactRequest;
import ai.floedb.floecat.catalog.rpc.GetIndexArtifactResponse;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.ListIndexArtifactsResponse;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactItem;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactsRequest;
import ai.floedb.floecat.catalog.rpc.PutIndexArtifactsResponse;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableIndexService;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.BlobStore;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jboss.logging.Logger;

@GrpcService
public class TableIndexServiceImpl extends BaseServiceImpl implements TableIndexService {

  @Inject TableRepository tables;
  @Inject SnapshotRepository snapshots;
  @Inject IndexArtifactRepository indexArtifacts;
  @Inject BlobStore blobStore;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;

  private static final Logger LOG = Logger.getLogger(TableIndexService.class);
  private static final String DEFAULT_INDEX_CONTENT_TYPE = "application/x-parquet";

  @Override
  public Uni<GetIndexArtifactResponse> getIndexArtifact(GetIndexArtifactRequest request) {
    var L = LogHelper.start(LOG, "GetIndexArtifact");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "catalog.read");

                  ResourceId tableId = request.getTableId();
                  long snapshotId = resolveSnapshotId(tableId, request.getSnapshot());
                  IndexTarget target = requireTarget(request.getTarget());

                  return indexArtifacts
                      .getIndexArtifact(tableId, snapshotId, target)
                      .map(
                          record -> GetIndexArtifactResponse.newBuilder().setRecord(record).build())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId(),
                                  null,
                                  Map.of(
                                      "table_id",
                                      tableId.getId(),
                                      "snapshot_id",
                                      Long.toString(snapshotId))));
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ListIndexArtifactsResponse> listIndexArtifacts(ListIndexArtifactsRequest request) {
    var L = LogHelper.start(LOG, "ListIndexArtifacts");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principal.get();
                  authz.require(principalContext, "catalog.read");

                  ResourceId tableId = request.getTableId();
                  long snapshotId = resolveSnapshotId(tableId, request.getSnapshot());
                  int limit =
                      request.hasPage() && request.getPage().getPageSize() > 0
                          ? request.getPage().getPageSize()
                          : 200;
                  String token = request.hasPage() ? request.getPage().getPageToken() : "";
                  StringBuilder next = new StringBuilder();
                  var records =
                      indexArtifacts.listIndexArtifacts(
                          tableId, snapshotId, Math.max(1, limit), token, next);
                  int total = indexArtifacts.countIndexArtifacts(tableId, snapshotId);

                  return ListIndexArtifactsResponse.newBuilder()
                      .addAllRecords(records)
                      .setPage(
                          PageResponse.newBuilder()
                              .setNextPageToken(next.toString())
                              .setTotalSize(total)
                              .build())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PutIndexArtifactsResponse> putIndexArtifacts(
      Multi<PutIndexArtifactsRequest> requests) {
    var L = LogHelper.start(LOG, "PutIndexArtifacts");

    var state = new AtomicReference<>(StreamState.initial());
    AtomicInteger upserted = new AtomicInteger();

    return mapFailures(
            requests
                .onItem()
                .transformToUniAndConcatenate(
                    req -> runWithRetry(() -> processIndexArtifacts(state, req, upserted)))
                .collect()
                .last()
                .onItem()
                .ifNull()
                .failWith(
                    () ->
                        GrpcErrors.invalidArgument(
                            correlationId(), null, Map.of("reason", "empty index artifact stream")))
                .replaceWith(
                    () ->
                        PutIndexArtifactsResponse.newBuilder().setUpserted(upserted.get()).build()),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private record StreamState(
      ResourceId tableId, long snapshotId, String idempotencyKey, boolean validated) {
    static StreamState initial() {
      return new StreamState(null, -1L, null, false);
    }

    StreamState with(
        ResourceId tableId, long snapshotId, String idempotencyKey, boolean validated) {
      return new StreamState(tableId, snapshotId, idempotencyKey, validated);
    }
  }

  private StreamState ensureState(StreamState state, ResourceId tableId, long snapshotId) {
    if (state.tableId == null) {
      return state.with(tableId, snapshotId, null, false);
    }
    if (!state.tableId.equals(tableId) || state.snapshotId != snapshotId) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          null,
          Map.of("reason", "index artifact stream must target one table snapshot"));
    }
    return state;
  }

  private StreamState ensureIdempotency(StreamState state, String candidate) {
    if (candidate == null || candidate.isBlank()) {
      return state;
    }
    if (state.idempotencyKey == null) {
      return state.with(state.tableId, state.snapshotId, candidate.trim(), state.validated);
    }
    if (!state.idempotencyKey.equals(candidate.trim())) {
      throw GrpcErrors.invalidArgument(correlationId(), IDEMPOTENCY_INCONSISTENT_KEY, Map.of());
    }
    return state;
  }

  private StreamState validateOnce(StreamState state) {
    if (state.validated) {
      return state;
    }
    var pc = principal.get();
    authz.require(pc, "table.write");

    tables
        .getById(state.tableId)
        .orElseThrow(
            () -> GrpcErrors.notFound(correlationId(), TABLE, Map.of("id", state.tableId.getId())));

    snapshots
        .getById(state.tableId, state.snapshotId)
        .orElseThrow(
            () ->
                GrpcErrors.notFound(
                    correlationId(), SNAPSHOT, Map.of("id", Long.toString(state.snapshotId))));

    return state.with(state.tableId, state.snapshotId, state.idempotencyKey, true);
  }

  private Boolean processIndexArtifacts(
      AtomicReference<StreamState> stateRef, PutIndexArtifactsRequest req, AtomicInteger upserted) {
    StreamState computed = ensureState(stateRef.get(), req.getTableId(), req.getSnapshotId());
    computed =
        ensureIdempotency(computed, req.hasIdempotency() ? req.getIdempotency().getKey() : null);
    computed = validateOnce(computed);
    stateRef.set(computed);
    StreamState next = computed;

    String accountId = principal.get().getAccountId();
    var nowTs = nowTs();
    for (PutIndexArtifactItem raw : req.getItemsList()) {
      var item = validateItem(raw, next.tableId(), next.snapshotId());

      if (next.idempotencyKey() == null) {
        persistItem(item);
        upserted.incrementAndGet();
        continue;
      }

      String itemKey =
          itemIdempotencyKey(
              next.idempotencyKey(),
              "index_artifact",
              hashString(targetStorageId(item.getRecord().getTarget())));

      MutationOps.createProto(
          accountId,
          "PutIndexArtifacts",
          itemKey,
          item::toByteArray,
          () -> {
            persistItem(item);
            return new IdempotencyGuard.CreateResult<>(item, next.tableId());
          },
          staged ->
              indexArtifacts.metaForIndexArtifact(
                  next.tableId(), next.snapshotId(), staged.getRecord().getTarget(), nowTs),
          idempotencyStore,
          nowTs,
          idempotencyTtlSeconds(),
          this::correlationId,
          PutIndexArtifactItem::parseFrom);

      upserted.incrementAndGet();
    }

    return Boolean.TRUE;
  }

  private IndexArtifactRecord validateRecord(
      IndexArtifactRecord record, ResourceId tableId, long snapshotId) {
    if (record == null) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("reason", "record missing"));
    }
    if (!record.hasTableId() || !record.getTableId().equals(tableId)) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("reason", "record.table_id must match request table_id"));
    }
    if (record.getSnapshotId() != snapshotId) {
      throw GrpcErrors.invalidArgument(
          correlationId(),
          null,
          Map.of("reason", "record.snapshot_id must match request snapshot_id"));
    }
    requireTarget(record.getTarget());
    return record;
  }

  private PutIndexArtifactItem validateItem(
      PutIndexArtifactItem item, ResourceId tableId, long snapshotId) {
    if (item == null) {
      throw GrpcErrors.invalidArgument(correlationId(), null, Map.of("reason", "item missing"));
    }
    IndexArtifactRecord record = validateRecord(item.getRecord(), tableId, snapshotId);
    if (item.getContent().isEmpty()) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("reason", "item.content is required"));
    }
    if (item.getContentType() == null || item.getContentType().isBlank()) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("reason", "item.content_type is required"));
    }
    return item.toBuilder().setRecord(record).build();
  }

  private void persistItem(PutIndexArtifactItem item) {
    IndexArtifactRecord record = item.getRecord();
    String contentType =
        item.getContentType() == null || item.getContentType().isBlank()
            ? DEFAULT_INDEX_CONTENT_TYPE
            : item.getContentType();
    blobStore.put(record.getArtifactUri(), item.getContent().toByteArray(), contentType);
    String etag =
        blobStore
            .head(record.getArtifactUri())
            .map(h -> h.getEtag())
            .orElse(record.getContentEtag());
    indexArtifacts.putIndexArtifact(record.toBuilder().setContentEtag(etag).build());
  }

  private IndexTarget requireTarget(IndexTarget target) {
    if (target == null || target.getTargetCase() == IndexTarget.TargetCase.TARGET_NOT_SET) {
      throw GrpcErrors.invalidArgument(
          correlationId(), null, Map.of("reason", "index target must be set"));
    }
    return target;
  }

  private static String itemIdempotencyKey(String baseKey, String kind, Object itemId) {
    return baseKey + ":" + kind + ":" + String.valueOf(itemId);
  }

  private static String hashString(String value) {
    if (value == null || value.isBlank()) {
      return "empty";
    }
    return hashFingerprint(value.getBytes(StandardCharsets.UTF_8));
  }

  private static String targetStorageId(IndexTarget target) {
    return switch (target.getTargetCase()) {
      case FILE -> "file:" + target.getFile().getFilePath();
      case TARGET_NOT_SET -> throw new IllegalArgumentException("target must be set");
    };
  }

  private long resolveSnapshotId(ResourceId tableId, SnapshotRef ref) {
    if (ref == null || ref.getWhichCase() == SnapshotRef.WhichCase.WHICH_NOT_SET) {
      throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_MISSING, Map.of());
    }
    return switch (ref.getWhichCase()) {
      case SNAPSHOT_ID -> ref.getSnapshotId();
      case SPECIAL -> {
        if (ref.getSpecial() != SpecialSnapshot.SS_CURRENT) {
          throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_SPECIAL_MISSING, Map.of());
        }
        yield snapshots
            .getCurrentSnapshot(tableId)
            .map(Snapshot::getSnapshotId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(correlationId(), SNAPSHOT, Map.of("id", tableId.getId())));
      }
      case AS_OF ->
          snapshots
              .getAsOf(tableId, ref.getAsOf())
              .map(Snapshot::getSnapshotId)
              .orElseThrow(
                  () ->
                      GrpcErrors.notFound(
                          correlationId(), SNAPSHOT, Map.of("id", tableId.getId())));
      default -> throw GrpcErrors.invalidArgument(correlationId(), SNAPSHOT_MISSING, Map.of());
    };
  }
}
