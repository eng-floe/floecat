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

  private static String key(String tenantId, String catalogId, String nsId) {
    return "/tenants/" + tenantId + "/catalogs/" + catalogId + "/namespaces/" + nsId;
  }

  public Optional<Namespace> get(ResourceId nsId, ResourceId catalogId) {
    validateCatalogId(catalogId);
    validateSameTenant(nsId, catalogId);
    var k = key(nsId.getTenantId(), catalogId.getId(), nsId.getId());
    return ptr.get(k).map(p -> {
      byte[] data = blobs.get(p.getBlobUri());
      try { return Namespace.parseFrom(data); }
      catch (Exception e) { throw new RuntimeException(e); }
    });
  }

  public List<Namespace> list(ResourceId catalogId, int limit, String pageToken, StringBuilder nextToken) {
    validateCatalogId(catalogId);
    String prefix = "/tenants/" + catalogId.getTenantId() + "/catalogs/" + catalogId.getId() + "/namespaces/";
    List<String> keys = ptr.listByPrefix(prefix, Math.max(1, limit), pageToken, nextToken);
    List<Namespace> out = new ArrayList<>(keys.size());
    for (String k : keys) {
      ptr.get(k).ifPresent(p -> {
        byte[] data = blobs.get(p.getBlobUri());
        try { out.add(Namespace.parseFrom(data)); }
        catch (Exception e) { throw new RuntimeException(e); }
      });
    }
    return out;
  }

  public int count(ResourceId catalogId) {
    validateCatalogId(catalogId);
    String prefix = "/tenants/" + catalogId.getTenantId() + "/catalogs/" + catalogId.getId() + "/namespaces/";
    StringBuilder ignore = new StringBuilder();
    return ptr.listByPrefix(prefix, Integer.MAX_VALUE, "", ignore).size();
  }

  public void put(Namespace ns, ResourceId catalogId) {
    validateCatalogId(catalogId);
    validateSameTenant(ns.getResourceId(), catalogId);

    var rid = ns.getResourceId();
    String k = key(rid.getTenantId(), catalogId.getId(), rid.getId());
    String uri = "mem://" + k + ".pb";

    blobs.put(uri, ns.toByteArray(), "application/x-protobuf");

    int maxRetries = 10;
    for (int i = 0; i < maxRetries; i++) {
      long expected = ptr.get(k).map(Pointer::getVersion).orElse(0L);
      Pointer next = Pointer.newBuilder()
          .setKey(k)
          .setBlobUri(uri)
          .setVersion(expected + 1)
          .build();
      if (ptr.compareAndSet(k, expected, next)) return;
    }
    throw new IllegalStateException("putNamespace CAS failed after retries: " + k);
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
}