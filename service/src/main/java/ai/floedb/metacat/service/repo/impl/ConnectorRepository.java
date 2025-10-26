package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.service.repo.model.ConnectorKey;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.util.GenericResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
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
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()));
  }

  public boolean deleteWithPrecondition(
      ResourceId connectorResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Connector> getById(ResourceId connectorResourceId) {
    return repo.getByKey(
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()));
  }

  public Optional<Connector> getByName(String tenantId, String displayName) {
    return repo.get(Keys.connectorPointerByName(tenantId, displayName));
  }

  public List<Connector> list(String tenantId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(
        Keys.connectorPointerByNamePrefix(tenantId), limit, pageToken, nextOut);
  }

  public int count(String tenantId) {
    return repo.countByPrefix(Keys.connectorPointerByNamePrefix(tenantId));
  }

  public MutationMeta metaFor(ResourceId connectorResourceId) {
    return repo.metaFor(
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId connectorResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId connectorResourceId) {
    return repo.metaForSafe(
        new ConnectorKey(connectorResourceId.getTenantId(), connectorResourceId.getId()));
  }
}
