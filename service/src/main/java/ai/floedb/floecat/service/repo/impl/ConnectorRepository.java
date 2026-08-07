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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.model.ConnectorKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.CredentialCleanupState;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@ApplicationScoped
public class ConnectorRepository {

  private final GenericResourceRepository<Connector, ConnectorKey> repo;
  private final PointerStore pointerStore;

  @Inject
  public ConnectorRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.pointerStore = pointerStore;
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CONNECTOR,
            Connector::parseFrom,
            Connector::toByteArray,
            "application/x-protobuf");
  }

  /** Durable handle for deleting a connector's external credential after its pointer is gone. */
  public record CredentialCleanup(
      ResourceId connectorId, String credentialId, String pointerKey, long pointerVersion) {}

  /**
   * Stages the connector's durable credential cleanup handle before pointer deletion. The handle
   * must be present before the caller removes the connector: after that row is gone, this is the
   * only durable enumeration path a retry has to the external secret.
   */
  public List<CredentialCleanup> prepareCredentialCleanup(ResourceId connectorId) {
    String key =
        Keys.connectorCredentialCleanupPointer(
            connectorId.getAccountId(), connectorId.getId(), connectorId.getId());
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var existing = pointerStore.get(key).orElse(null);
      if (existing != null) {
        if (CredentialCleanupState.isWriting(existing)) {
          throw new BaseResourceRepository.AbortRetryableException(
              "connector credential write still in flight for: " + connectorId.getId());
        }
        return List.of(
            new CredentialCleanup(connectorId, connectorId.getId(), key, existing.getVersion()));
      }
      var marker = PointerReferences.opaqueMarkerPointer(key, connectorId.getId(), 1L);
      if (pointerStore.compareAndSetBatch(List.of(new PointerStore.CasUpsert(key, 0L, marker)))) {
        return List.of(new CredentialCleanup(connectorId, connectorId.getId(), key, 1L));
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential cleanup staging contended for: " + key);
  }

  public CredentialCleanupState.Write beginCredentialWrite(
      ResourceId connectorId, long expectedPointerVersion, BatchGuard accountLive) {
    String key =
        Keys.connectorCredentialCleanupPointer(
            connectorId.getAccountId(), connectorId.getId(), connectorId.getId());
    String connectorPointer =
        Keys.connectorPointerById(connectorId.getAccountId(), connectorId.getId());
    var resourceState =
        CredentialCleanupState.pointerVersionGuard(
            pointerStore,
            connectorPointer,
            expectedPointerVersion,
            "connector " + connectorId.getId());
    return CredentialCleanupState.begin(
        pointerStore, key, connectorId.getId(), BatchGuard.all(accountLive, resourceState));
  }

  public BatchGuard credentialWriteCommitGuard(CredentialCleanupState.Write write) {
    return CredentialCleanupState.commitGuard(pointerStore, write);
  }

  public void abortCredentialWrite(CredentialCleanupState.Write write) {
    CredentialCleanupState.abort(pointerStore, write);
  }

  public void abortCredentialCreate(CredentialCleanupState.Write write) {
    CredentialCleanupState.abortCreate(pointerStore, write);
  }

  public boolean credentialWriteCommitted(CredentialCleanupState.Write write) {
    return CredentialCleanupState.committed(pointerStore, write);
  }

  public BatchGuard credentialCleanupReadyGuard(ResourceId connectorId) {
    String key =
        Keys.connectorCredentialCleanupPointer(
            connectorId.getAccountId(), connectorId.getId(), connectorId.getId());
    return CredentialCleanupState.readyGuard(
        pointerStore, key, connectorId.getId(), "connector " + connectorId.getId());
  }

  public List<CredentialCleanup> pendingCredentialCleanups(ResourceId connectorId) {
    return credentialCleanups(connectorId);
  }

  /** Streams all durable credential cleanup handles left in an account. */
  public void forEachCredentialCleanup(String accountId, Consumer<CredentialCleanup> action) {
    scanCredentialCleanups(accountId, Keys.connectorCredentialCleanupPrefix(accountId), action);
  }

  /**
   * Claims a cleanup only after the connector pointer is absent in the same batch. The optional
   * account guard joins that batch for account teardown; normal DeleteConnector passes NONE.
   */
  public Optional<CredentialCleanup> claimCredentialCleanup(
      CredentialCleanup cleanup, BatchGuard guard) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return Optional.empty();
      }
      if (CredentialCleanupState.isWriting(current)) {
        throw new BaseResourceRepository.AbortRetryableException(
            "connector credential write still in flight for: " + cleanup.connectorId().getId());
      }
      String connectorPointer =
          Keys.connectorPointerById(
              cleanup.connectorId().getAccountId(), cleanup.connectorId().getId());
      var claimed =
          PointerReferences.opaqueMarkerPointer(
              cleanup.pointerKey(), cleanup.credentialId(), current.getVersion() + 1L);
      var ops = new java.util.ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(cleanup.pointerKey(), current.getVersion(), claimed));
      ops.add(new PointerStore.CasCheckAbsent(connectorPointer));
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return Optional.of(
            new CredentialCleanup(
                cleanup.connectorId(),
                cleanup.credentialId(),
                cleanup.pointerKey(),
                claimed.getVersion()));
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "credential cleanup lost the race against " + guard.describe());
      }
      if (pointerStore.get(connectorPointer).isPresent()) {
        return Optional.empty();
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential cleanup claim contended for: " + cleanup.pointerKey());
  }

  /** Completes a claimed task; another idempotent worker may already have removed it. */
  public void completeCredentialCleanup(CredentialCleanup cleanup) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(new PointerStore.CasDelete(cleanup.pointerKey(), current.getVersion())))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "credential cleanup completion contended for: " + cleanup.pointerKey());
  }

  private List<CredentialCleanup> credentialCleanups(ResourceId connectorId) {
    var out = new java.util.ArrayList<CredentialCleanup>();
    scanCredentialCleanups(
        connectorId.getAccountId(),
        Keys.connectorCredentialCleanupPrefix(connectorId.getAccountId(), connectorId.getId()),
        out::add);
    return out;
  }

  private void scanCredentialCleanups(
      String accountId, String prefix, Consumer<CredentialCleanup> action) {
    var seenTokens = new java.util.HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row : pointerStore.listPointersByPrefix(prefix, 200, token, next, true)) {
        String suffix =
            row.getKey().substring(Keys.connectorCredentialCleanupPrefix(accountId).length());
        int slash = suffix.indexOf('/');
        if (slash <= 0 || slash == suffix.length() - 1) {
          continue;
        }
        String connectorId = Keys.extractLastSegment(suffix.substring(0, slash));
        String credentialId = Keys.extractLastSegment(row.getKey());
        action.accept(
            new CredentialCleanup(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(connectorId)
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build(),
                credentialId,
                row.getKey(),
                row.getVersion()));
      }
      token = next.toString();
      if (token.isBlank()) {
        return;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  public void create(Connector connector) {
    repo.create(connector);
  }

  /**
   * Guarded create: the connector becomes visible only while {@code guard} still holds. Used to
   * publish into an account atomically with respect to that account's deletion — see {@link
   * ai.floedb.floecat.service.repo.util.MarkerStore#accountLiveGuard}.
   */
  public void create(Connector connector, BatchGuard guard) {
    repo.create(connector, guard);
  }

  public boolean update(Connector connector, long expectedPointerVersion) {
    return repo.update(connector, expectedPointerVersion);
  }

  public boolean update(
      Connector connector, long expectedPointerVersion, BatchGuard accountLiveGuard) {
    return repo.update(connector, expectedPointerVersion, accountLiveGuard);
  }

  public boolean delete(ResourceId connectorResourceId) {
    return repo.delete(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }

  /** Guarded delete by id; see {@link TableRepository#delete(ResourceId, BatchGuard)}. */
  public boolean delete(ResourceId connectorResourceId, BatchGuard guard) {
    return repo.delete(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()), guard);
  }

  public boolean deleteWithPrecondition(
      ResourceId connectorResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()),
        expectedPointerVersion);
  }

  public boolean deleteWithPrecondition(
      ResourceId connectorResourceId, long expectedPointerVersion, BatchGuard guard) {
    return repo.deleteWithPrecondition(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()),
        expectedPointerVersion,
        guard);
  }

  public Optional<Connector> getById(ResourceId connectorResourceId) {
    return repo.getByKey(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }

  public boolean existsById(ResourceId connectorResourceId) {
    return repo.existsByKey(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }

  public Optional<Connector> getByName(String accountId, String displayName) {
    return repo.get(Keys.connectorPointerByName(accountId, displayName));
  }

  public List<Connector> list(
      String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.connectorPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.connectorPointerByNamePrefix(accountId));
  }

  /**
   * Deletes the account's leftover connector index rows — its by-id and by-name families — and
   * reports how many.
   *
   * <p>Teardown only, and for the same reason as {@code CatalogRepository#deleteResidualRows}: a
   * connector whose blob cannot be read loses its canonical pointer but keeps the by-name row that
   * blob would have named. Scoped to the two index families rather than the connector root, so the
   * sweep can only remove rows that index a connector.
   *
   * <p>Every removal is guarded by {@code accountGone}, so a reused account id cannot authorize a
   * sweep of the replacement account's rows.
   */
  public int deleteResidualRows(String accountId, BatchGuard accountGone) {
    return deleteResidualRows(
        accountId, accountGone, new BaseResourceRepository.GuardedDeleteProgress());
  }

  public int deleteResidualRows(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    return repo.deleteByPrefix(
            Keys.connectorPointerByIdPrefix(accountId), accountGone, deleteProgress)
        + repo.deleteByPrefix(
            Keys.connectorPointerByNamePrefix(accountId), accountGone, deleteProgress);
  }

  /**
   * The account's connector ids, streamed a page at a time from canonical pointer rows.
   *
   * <p>Identity only, and deliberately so: {@link #list} parses every connector blob, so one
   * unreadable blob fails the whole enumeration. Teardown cannot survive that — it runs after the
   * account pointer is gone, so the exception is not retryable and every attempt fails identically,
   * stranding the rest of the account. The by-id key carries the id, which is all a delete needs.
   */
  public void forEachId(String accountId, java.util.function.Consumer<ResourceId> action) {
    repo.forEachRefByPrefixConsistent(
        Keys.connectorPointerByIdPrefix(accountId),
        pointer ->
            action.accept(
                ResourceId.newBuilder()
                    .setAccountId(accountId)
                    .setId(Keys.extractLastSegment(pointer.getKey()))
                    .setKind(ResourceKind.RK_CONNECTOR)
                    .build()));
  }

  public MutationMeta metaFor(ResourceId connectorResourceId) {
    return repo.metaFor(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId connectorResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId connectorResourceId) {
    return repo.metaForSafe(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }
}
