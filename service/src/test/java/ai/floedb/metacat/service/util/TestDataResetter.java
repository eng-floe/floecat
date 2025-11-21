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

    for (var tid : tenantIds) {
      ptr.deleteByPrefix("/tenants/" + tid + "/");
    }
    ptr.deleteByPrefix("/tenants/");

    for (var tid : tenantIds) {
      blobs.deletePrefix("/tenants/" + tid + "/");
    }
    blobs.deletePrefix("/tenants/");
  }

  private List<String> listTenantIds() {
    var ids = new ArrayList<String>();
    String token = "";
    do {
      var next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_TENANTS_BY_ID_PREFIX, 200, token, next);
      for (var r : rows) {
        var k = r.getKey();
        int idx = k.lastIndexOf('/');
        if (idx > 0 && idx + 1 < k.length()) {
          ids.add(k.substring(idx + 1));
        }
      }
      token = next.toString();
    } while (!token.isBlank());
    return ids;
  }
}
