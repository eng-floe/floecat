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

  private static final String GLOBAL_TENANTS_PREFIX = "/tenants/";
  private static final String GLOBAL_TENANTS_BY_ID_PREFIX = "/tenants/by-id/";
  private static final String GLOBAL_IDEMPOTENCY_PREFIX = "/idempotency/";

  public void wipeAll() {
    var tenantIds = listTenantIds();
    for (String tid : tenantIds) {
      wipePrefixUntilEmpty("/tenants/" + tid + "/");
    }

    wipePrefixUntilEmpty("/idempotency/");

    blobs.deletePrefix("/tenants/");
    blobs.deletePrefix("/idempotency/");

    wipePrefixUntilEmpty("/tenants/");

    assertEmpty("/tenants/");
    assertEmpty("/idempotency/");
  }

  private List<String> listTenantIds() {
    ArrayList<String> ids = new ArrayList<>();
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(GLOBAL_TENANTS_BY_ID_PREFIX, 200, token, next);
      for (var r : rows) {
        String k = r.key();
        int idx = k.lastIndexOf('/');
        if (idx > 0 && idx + 1 < k.length()) ids.add(k.substring(idx + 1));
      }
      token = next.toString();
    } while (!token.isBlank());
    return ids;
  }

  private void wipePrefixUntilEmpty(String prefix) {
    int passes = 0;
    while (true) {
      passes++;
      int deleted = wipeOnce(prefix);
      if (deleted == 0 && count(prefix) == 0) break;
      if (passes > 20) {
        dump(prefix);
        throw new IllegalStateException("wipe stuck for prefix=" + prefix);
      }

      try {
        Thread.sleep(25);
      } catch (InterruptedException ignored) {
      }
    }
  }

  private int wipeOnce(String prefix) {
    int deleted = 0;
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(prefix, 200, token, next);

      List<PointerStore.Row> batch = new ArrayList<>(25);
      for (var r : rows) {
        try {
          blobs.delete(r.blobUri());
        } catch (Throwable ignore) {
        }
        batch.add(r);
        if (batch.size() == 25) {
          deleted += batchDelete(batch);
          batch.clear();
        }
      }
      if (!batch.isEmpty()) deleted += batchDelete(batch);

      token = next.toString();
    } while (!token.isBlank());
    return deleted;
  }

  private int batchDelete(List<PointerStore.Row> batch) {
    int n = 0;
    for (var r : batch) {
      try {
        if (ptr.delete(r.key())) n++;
      } catch (Throwable ignore) {
      }
    }
    return n;
  }

  private int count(String prefix) {
    int total = 0;
    String token = "";
    do {
      StringBuilder next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(prefix, 200, token, next);
      total += rows.size();
      token = next.toString();
    } while (!token.isBlank());
    return total;
  }

  private void assertEmpty(String prefix) {
    int leftover = count(prefix);
    if (leftover == 0) return;
    dump(prefix);
    throw new IllegalStateException("leftover keys under " + prefix + ": " + leftover);
  }

  private void dump(String prefix) {
    String token = "";
    System.err.println("=== SURVIVORS under " + prefix + " ===");
    do {
      StringBuilder next = new StringBuilder();
      var rows = ptr.listPointersByPrefix(prefix, 200, token, next);
      for (var r : rows) {
        System.err.println("key=" + r.key() + " blob=" + r.blobUri() + " v=" + r.version());
      }
      token = next.toString();
    } while (!token.isBlank());
    System.err.println("=== END SURVIVORS ===");
  }
}
