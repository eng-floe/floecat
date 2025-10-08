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

  private static String catalogBlobUri(String tenantId, String catalogId) {
    return "mem://tenants/" + tenantId + "/catalogs/" + catalogId + "/catalog.pb";
  }

  private static String catalogIndexPtr(String tenantId, String catalogId) {
    return "/tenants/" + tenantId + "/catalogs/" + catalogId;
  }

  public Optional<Catalog> getCatalog(ResourceId rid) {
    return ptr.get(catalogIndexPtr(rid.getTenantId(), rid.getId())).map(p -> {
      byte[] data = blobs.get(p.getBlobUri());
      try { 
        return Catalog.parseFrom(data); 
      } catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public List<Catalog> listCatalogs(String tenantId, int limit, String pageToken, StringBuilder nextToken) {
    String prefix = catalogIndexPtr(tenantId, "");
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextToken);
    List<Catalog> out = new ArrayList<>();
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> {
        byte[] data = blobs.get(p.getBlobUri());
        try { 
          out.add(Catalog.parseFrom(data)); 
        } catch (Exception e) { 
          throw new RuntimeException(e); 
        }
      });
    }
    return out;
  }

  public void putCatalog(Catalog c) {
    ResourceId rid = c.getResourceId();
    String key = catalogIndexPtr(rid.getTenantId(), rid.getId());
    String uri = catalogBlobUri(rid.getTenantId(), rid.getId());

    blobs.put(uri, c.toByteArray(), "application/x-protobuf");
    casUpsert(key, uri);
  }

  public int countCatalogs(String tenantId) {
    String prefix = catalogIndexPtr(tenantId, "");
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }

  private void casUpsert(String key, String blobUri) {
    for (int i = 0; i < 10; i++) {
      long expected = ptr.get(key).map(Pointer::getVersion).orElse(0L);
      Pointer next = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(expected + 1).build();
      if (ptr.compareAndSet(key, expected, next)) return;
    }
    throw new IllegalStateException("CAS failed: " + key);
  }
}