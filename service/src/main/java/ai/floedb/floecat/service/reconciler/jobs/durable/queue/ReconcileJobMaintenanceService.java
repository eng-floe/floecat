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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileJobMaintenanceService.class);
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";

  @FunctionalInterface
  public interface ReclaimCanonicalJob {
    void accept(Pointer leaseExpiryPointer, long nowMs);
  }

  private PointerStore pointerStore;
  private Function<String, Long> parseLeaseExpiryMillis;
  private ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer;
  private int readyScanLimit;
  private long reclaimIntervalMs;
  private long invalidOrderedPointerMs;

  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

  public void bind(
      PointerStore pointerStore,
      Function<String, Long> parseLeaseExpiryMillis,
      ReclaimCanonicalJob reclaimExpiredLeaseFromCanonicalPointer,
      int readyScanLimit,
      long reclaimIntervalMs,
      long invalidOrderedPointerMs) {
    this.pointerStore = pointerStore;
    this.parseLeaseExpiryMillis = parseLeaseExpiryMillis;
    this.reclaimExpiredLeaseFromCanonicalPointer = reclaimExpiredLeaseFromCanonicalPointer;
    this.readyScanLimit = readyScanLimit;
    this.reclaimIntervalMs = reclaimIntervalMs;
    this.invalidOrderedPointerMs = invalidOrderedPointerMs;
  }

  public void runMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
    LOG.debugf(
        "runMaintenanceOnce total_ms=%d reclaim_ms=%d",
        System.currentTimeMillis() - startedAtMs, System.currentTimeMillis() - startedAtMs);
  }

  private void reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    if (!reclaimLock.tryLock()) {
      return;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return;
      }
      lastReclaimAtMs = nowMs;

      scanLeaseExpiryPointersForReclaim(nowMs);
    } finally {
      reclaimLock.unlock();
    }
  }

  private void scanLeaseExpiryPointersForReclaim(long nowMs) {
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> leaseExpiries =
          pointerStore.listPointersByPrefix(
              LEASE_EXPIRY_POINTER_PREFIX, readyScanLimit, token, next);
      if (leaseExpiries.isEmpty()) {
        return;
      }
      for (Pointer leaseExpiry : leaseExpiries) {
        long expiresAtMs = parseLeaseExpiryMillis.apply(leaseExpiry.getKey());
        if (expiresAtMs == invalidOrderedPointerMs) {
          continue;
        }
        if (expiresAtMs > nowMs) {
          return;
        }
        reclaimExpiredLeaseFromCanonicalPointer.accept(leaseExpiry, nowMs);
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return;
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile lease-expiry reclaim pagination token did not advance; aborting scan to"
                + " avoid livelock");
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease-expiry reclaim pagination hit safety page cap; aborting scan");
        return;
      }
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
