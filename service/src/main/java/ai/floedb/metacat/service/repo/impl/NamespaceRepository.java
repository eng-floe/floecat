package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.NamespaceKey;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.util.GenericRepository;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class NamespaceRepository {

  private final GenericRepository<Namespace, NamespaceKey> repo;

  @Inject
  public NamespaceRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericRepository<>(
            pointerStore,
            blobStore,
            Schemas.NAMESPACE,
            Namespace::parseFrom,
            Namespace::toByteArray,
            "application/x-protobuf");
  }

  public void create(Namespace namespace) {
    repo.create(namespace);
  }

  public boolean update(Namespace namespace, long expectedPointerVersion) {
    return repo.update(namespace, expectedPointerVersion);
  }

  public boolean delete(ResourceId namespaceResourceId) {
    return repo.delete(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()));
  }

  public boolean deleteWithPrecondition(
      ResourceId namespaceResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Namespace> getById(ResourceId namespaceResourceId) {
    return repo.getByKey(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()));
  }

  public Optional<Namespace> getByPath(
      String tenantId, String catalogId, List<String> pathSegments) {
    return repo.get(Keys.namespacePointerByPath(tenantId, catalogId, pathSegments));
  }

  public List<Namespace> list(
      String tenantId,
      String catalogId,
      List<String> parentSegmentsOrEmpty,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.namespacePointerByPathPrefix(tenantId, catalogId, parentSegmentsOrEmpty);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String tenantId, String catalogId, List<String> parentSegmentsOrEmpty) {
    String prefix = Keys.namespacePointerByPathPrefix(tenantId, catalogId, parentSegmentsOrEmpty);
    return repo.countByPrefix(prefix);
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId namespaceResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId namespaceResourceId) {
    return repo.metaForSafe(
        new NamespaceKey(namespaceResourceId.getTenantId(), namespaceResourceId.getId()));
  }
}
