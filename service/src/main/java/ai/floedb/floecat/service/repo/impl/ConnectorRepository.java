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
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ConnectorRepository {

  private final GenericResourceRepository<Connector, ConnectorKey> repo;

  @Inject
  public ConnectorRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CONNECTOR,
            Connector::parseFrom,
            Connector::toByteArray,
            "application/x-protobuf");
  }

  public void create(Connector connector) {
    repo.create(connector);
  }

  public boolean update(Connector connector, long expectedPointerVersion) {
    return repo.update(connector, expectedPointerVersion);
  }

  public boolean delete(ResourceId connectorResourceId) {
    return repo.delete(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()));
  }

  public boolean deleteWithPrecondition(
      ResourceId connectorResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ConnectorKey(connectorResourceId.getAccountId(), connectorResourceId.getId()),
        expectedPointerVersion);
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
   * Deletes whatever pointer rows are still under the account's connector root, and reports how
   * many.
   *
   * <p>Teardown only, and for the same reason as {@code CatalogRepository#deleteResidualRows}: a
   * connector whose blob cannot be read loses its canonical pointer but keeps the by-name row that
   * blob would have named.
   */
  public int deleteResidualRows(String accountId) {
    return repo.deleteByPrefix(Keys.connectorRootPrefix(accountId));
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
    repo.forEachRefByPrefix(
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
