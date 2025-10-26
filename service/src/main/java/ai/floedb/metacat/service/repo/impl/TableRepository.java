package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.model.TableKey;
import ai.floedb.metacat.service.repo.util.GenericResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TableRepository {

  private final GenericResourceRepository<Table, TableKey> repo;

  @Inject
  public TableRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE,
            Table::parseFrom,
            Table::toByteArray,
            "application/x-protobuf");
  }

  public void create(Table table) {
    repo.create(table);
  }

  public boolean update(Table table, long expectedPointerVersion) {
    return repo.update(table, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableResourceId) {
    return repo.delete(new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId tableResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Table> getById(ResourceId tableResourceId) {
    return repo.getByKey(new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()));
  }

  public Optional<Table> getByName(
      String tenantId, String catalogId, String namespaceId, String tableName) {
    return repo.get(Keys.tablePointerByName(tenantId, catalogId, namespaceId, tableName));
  }

  public List<Table> list(
      String tenantId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.tablePointerByNamePrefix(tenantId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String tenantId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.tablePointerByNamePrefix(tenantId, catalogId, namespaceId));
  }

  public MutationMeta metaFor(ResourceId tableResourceId) {
    return repo.metaFor(new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId tableResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableResourceId) {
    return repo.metaForSafe(new TableKey(tableResourceId.getTenantId(), tableResourceId.getId()));
  }
}
