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
import java.time.Clock;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class IdempotencyGc {

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;
  @Inject Clock clock;

  @ConfigProperty(name = "metacat.gc.idempotency.page-size", defaultValue = "200")
  int pageSize;

  @ConfigProperty(name = "metacat.gc.idempotency.batch-limit", defaultValue = "1000")
  int batchLimit;

  @ConfigProperty(name = "metacat.gc.idempotency.slice-millis", defaultValue = "4000")
  long sliceMillis;

  public record Result(
      int scanned, int expired, int ptrDeleted, int blobDeleted, String nextToken) {}

  public Result runSliceForTenant(String tenantId, String pageTokenIn) {
    final long deadline = clock.millis() + sliceMillis;
    final String prefix = Keys.idempotencyPrefixTenant(tenantId);
    String token = (pageTokenIn == null) ? "" : pageTokenIn;

    int scanned = 0;
    int expired = 0;
    int ptrDeleted = 0;
    int blobDeleted = 0;

    while (scanned < batchLimit && clock.millis() < deadline) {
      StringBuilder nextTokenBuilder = new StringBuilder();
      var pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, nextTokenBuilder);
      token = nextTokenBuilder.toString();

      if (pointers.isEmpty()) {
        break;
      }

      for (Pointer p : pointers) {
        if (clock.millis() >= deadline || scanned >= batchLimit) break;
        scanned++;

        boolean hasPtrExpiry = p.hasExpiresAt();
        boolean isExpiredByPtr =
            hasPtrExpiry && Timestamps.toMillis(p.getExpiresAt()) <= clock.millis();

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
            if (expMs <= clock.millis()) {
              expired++;
              if (pointerStore.compareAndDelete(p.getKey(), p.getVersion())) {
                ptrDeleted++;
              }
              if (blobStore.delete(p.getBlobUri())) {
                blobDeleted++;
              }

              continue;
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
