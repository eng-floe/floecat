package ai.floedb.metacat.service.util;

import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class TestDataResetter {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static final String GLOBAL_TENANTS_BY_ID_PREFIX = "/tenants/by-id/";

  public void wipeAll() {
    var tenantIds = listTenantIds();

    tenantIds.parallelStream().forEach(tid -> blobs.deletePrefix("/tenants/" + tid + "/"));
    blobs.deletePrefix("/idempotency/");

    tenantIds.parallelStream().forEach(tid -> ptr.deleteByPrefix("/tenants/" + tid + "/"));
    ptr.deleteByPrefix("/idempotency/");

    blobs.deletePrefix("/tenants/");
    ptr.deleteByPrefix("/tenants/");
    blobs.deletePrefix("/idempotency/");
    ptr.deleteByPrefix("/idempotency/");
  }

  private List<String> listTenantIds() {
    ArrayList<String> ids = new ArrayList<>();
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_TENANTS_BY_ID_PREFIX, 200, token, next);
      for (var r : rows) {
        String k = r.getKey();
        int idx = k.lastIndexOf('/');
        if (idx > 0 && idx + 1 < k.length()) ids.add(k.substring(idx + 1));
      }
      token = next.toString();
    } while (!token.isBlank());
    return ids;
  }
}
