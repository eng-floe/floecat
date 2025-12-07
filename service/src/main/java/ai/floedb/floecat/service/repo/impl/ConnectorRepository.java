package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.service.repo.model.ConnectorKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
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
