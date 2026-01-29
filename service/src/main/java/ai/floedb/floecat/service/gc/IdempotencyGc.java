/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageException;
import ai.floedb.floecat.storage.rpc.IdempotencyRecord;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
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

  public Result runSliceForAccount(String accountId, String pageTokenIn) {
    final var cfg = ConfigProvider.getConfig();

    final int pageSize =
        cfg.getOptionalValue("floecat.gc.idempotency.page-size", Integer.class).orElse(200);
    final int batchLimit =
        cfg.getOptionalValue("floecat.gc.idempotency.batch-limit", Integer.class).orElse(1000);
    final long sliceMillis =
        cfg.getOptionalValue("floecat.gc.idempotency.slice-millis", Long.class).orElse(4000L);

    final long deadline = System.currentTimeMillis() + sliceMillis;
    final String prefix = Keys.idempotencyPrefixAccount(accountId);
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
          blobStore.deletePrefix(Keys.idempotencyBlobPrefixForPointerKey(p.getKey()));
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
              blobStore.deletePrefix(Keys.idempotencyBlobPrefixForPointerKey(p.getKey()));
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
