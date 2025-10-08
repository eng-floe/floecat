package ai.floedb.metacat.service.repo.impl;

import java.util.Optional;
import java.util.List;
import java.util.ArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.Namespace;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class NamespaceRepository {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static String nsBlobUri(String tenantId, String catalogId, String nsId) {
    return "mem://tenants/" + tenantId + "/catalogs/" + catalogId + "/namespaces/" + nsId + "/namespace.pb";
  }

  private static String namespaceIndexPtr(String tenantId, String catalogId, String nsId) {
    return "/tenants/" + tenantId + "/catalogs/" + catalogId + "/namespaces/" + nsId;
  }

  public Optional<Namespace> get(ResourceId nsId, ResourceId catalogId) {
    validateCatalogId(catalogId);
    validateSameTenant(nsId, catalogId);
    var k = namespaceIndexPtr(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return ptr.get(k).map(p -> {
      byte[] data = blobs.get(p.getBlobUri());
      try { 
        return Namespace.parseFrom(data); 
      }
      catch (Exception e) { 
        throw new RuntimeException(e); 
      }
    });
  }

  public List<Namespace> list(ResourceId catalogId, int limit, String pageToken, StringBuilder nextToken) {
    validateCatalogId(catalogId);
    String prefix = namespaceIndexPtr(catalogId.getTenantId(), catalogId.getId(), "");
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextToken);
    List<Namespace> out = new ArrayList<>(keys.size());
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> {
        byte[] data = blobs.get(p.getBlobUri());
        try { 
          out.add(Namespace.parseFrom(data)); 
        }
        catch (Exception e) { 
          throw new RuntimeException(e); 
        }
      });
    }
    return out;
  }

  public int count(ResourceId catalogId) {
    validateCatalogId(catalogId);
    String prefix = namespaceIndexPtr(catalogId.getTenantId(), catalogId.getId(), "");
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }

  public void put(Namespace ns, ResourceId catalogId) {
    validateCatalogId(catalogId);
    validateSameTenant(ns.getResourceId(), catalogId);

    var rid = ns.getResourceId();
    String key = namespaceIndexPtr(rid.getTenantId(), catalogId.getId(), rid.getId());
    String uri = nsBlobUri(rid.getTenantId(), catalogId.getId(), rid.getId());

    blobs.put(uri, ns.toByteArray(), "application/x-protobuf");
    casUpsert(key, uri);
  }

  private static void validateCatalogId(ResourceId catalogId) {
    if (catalogId.getKind() != ResourceKind.RK_CATALOG) {
      throw new IllegalArgumentException("catalogId.kind must be RK_CATALOG");
    }
  }

  private static void validateSameTenant(ResourceId a, ResourceId b) {
    if (!a.getTenantId().equals(b.getTenantId())) {
      throw new IllegalArgumentException("tenant_id mismatch between namespace and catalog");
    }
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