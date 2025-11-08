package ai.floedb.metacat.service.gc;

import ai.floedb.metacat.common.rpc.Pointer;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.storage.errors.StorageException;
import ai.floedb.metacat.storage.rpc.IdempotencyRecord;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.ConfigProvider;

@ApplicationScoped
public class IdempotencyGc {

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;

  public record Result(
      int scanned, int expired, int ptrDeleted, int blobDeleted, String nextToken) {}

  public Result runSliceForTenant(String tenantId, String pageTokenIn) {
    final var cfg = ConfigProvider.getConfig();

    final int pageSize =
        cfg.getOptionalValue("metacat.gc.idempotency.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("metacat.gc.idempotency.batch-limit", Integer.class).orElse(1000);
    final long sliceMillis =
        cfg.getOptionalValue("metacat.gc.idempotency.slice-millis", Long.class).orElse(4000L);

    final long deadline = System.currentTimeMillis() + sliceMillis;
    final String prefix = Keys.idempotencyPrefixTenant(tenantId);
    String token = (pageTokenIn == null) ? "" : pageTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;

    while (scanned < batchLimit && System.currentTimeMillis() < deadline) {
      StringBuilder nextToken = new StringBuilder();
      var pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, nextToken);
      token = nextToken.toString();
      if (pointers.isEmpty()) {
        break;
      }

      for (Pointer p : pointers) {
        if (System.currentTimeMillis() >= deadline || scanned >= batchLimit) {
          break;
        }

        scanned++;

        boolean hasPtrExpiry = p.hasExpiresAt();
        boolean isExpiredByPtr =
            hasPtrExpiry && Timestamps.toMillis(p.getExpiresAt()) <= System.currentTimeMillis();

        if (isExpiredByPtr) {
          expired++;
          if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
            ptrDeleted++;
          }
          if (blobStore.delete(p.getBlobUri())) {
            blobDeleted++;
          }
          continue;
        }

        if (!hasPtrExpiry) {
          byte[] bytes = null;
          try {
            bytes = blobStore.get(p.getBlobUri());
          } catch (StorageException ignore) {
          }

          if (bytes == null || bytes.length == 0) {
            if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
              ptrDeleted++;
            }
            continue;
          }

          IdempotencyRecord rec = null;
          try {
            rec = IdempotencyRecord.parseFrom(bytes);
          } catch (Exception ignore) {
          }

          if (rec != null && rec.hasExpiresAt()) {
            long expMs = Timestamps.toMillis(rec.getExpiresAt());
            if (expMs <= System.currentTimeMillis()) {
              expired++;
              if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
                ptrDeleted++;
              }
              if (blobStore.delete(p.getBlobUri())) {
                blobDeleted++;
              }
            }
          }
        }
      }

      if (token.isEmpty()) {
        break;
      }
    }

    return new Result(scanned, expired, ptrDeleted, blobDeleted, token);
  }
}
