package ai.floedb.floecat.service.util;

import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class TestDataResetter {
  @Inject PointerStore ptr;
  @Inject BlobStore blobs;

  private static final String GLOBAL_ACCOUNTS_BY_ID_PREFIX = "/accounts/by-id/";

  public void wipeAll() {
    var accountIds = listAccountIds();

    for (var tid : accountIds) {
      ptr.deleteByPrefix("/accounts/" + tid + "/");
    }
    ptr.deleteByPrefix("/accounts/");

    for (var tid : accountIds) {
      blobs.deletePrefix("/accounts/" + tid + "/");
    }
    blobs.deletePrefix("/accounts/");
  }

  private List<String> listAccountIds() {
    var ids = new ArrayList<String>();
    String token = "";
    do {
      var next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_ACCOUNTS_BY_ID_PREFIX, 200, token, next);
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
