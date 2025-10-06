package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.common.rpc.Pointer;

@ApplicationScoped
public class CatalogRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static String key(ResourceId rid) {
    return "/tenants/" + rid.getTenantId() + "/catalogs/" + rid.getId();
  }

  public Optional<Catalog> getCatalog(ResourceId rid) {
    return ptr.get(key(rid)).map(p -> {
      byte[] data = blobs.get(p.getBlobUri());
      try { return Catalog.parseFrom(data); } catch (Exception e) { throw new RuntimeException(e); }
    });
  }

  public List<Catalog> listCatalogs(String tenantId, int limit, String pageToken, StringBuilder nextToken) {
    String prefix = "/tenants/" + tenantId + "/catalogs/";
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextToken);
    List<Catalog> out = new ArrayList<>();
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> {
        byte[] data = blobs.get(p.getBlobUri());
        try { out.add(Catalog.parseFrom(data)); } catch (Exception e) { throw new RuntimeException(e); }
      });
    }
    return out;
  }

  public void putCatalog(Catalog c) {
    ResourceId rid = c.getResourceId();
    String key = key(rid);
    String uri = "mem://" + key + ".pb";
    blobs.put(uri, c.toByteArray(), "application/x-protobuf");
    long nextVersion = ptr.get(key).map(Pointer::getVersion).orElse(0L) + 1;
    Pointer p = Pointer.newBuilder().setKey(key).setBlobUri(uri).setVersion(nextVersion).build();
    ptr.put(key, p);
  }

  public int countCatalogs(String tenantId) {
    String prefix = "/tenants/" + tenantId + "/catalogs/";
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }
}