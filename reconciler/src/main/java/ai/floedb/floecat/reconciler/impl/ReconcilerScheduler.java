package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class ReconcilerScheduler {
  @Inject GrpcClients clients;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Scheduled(
      every = "{reconciler.pollEvery:1s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void pollOnce() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    try {
      var lease = jobs.leaseNext().orElse(null);
      if (lease == null) {
        return;
      }

      jobs.markRunning(lease.jobId, System.currentTimeMillis());

      try {
        var connectorId =
            ResourceId.newBuilder()
                .setTenantId(lease.tenantId)
                .setId(lease.connectorId)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build();

        var result = reconcilerService.reconcile(connectorId, lease.fullRescan, lease.scope);

        long finished = System.currentTimeMillis();
        if (result.ok()) {
          jobs.markSucceeded(lease.jobId, finished, result.scanned, result.changed);
        } else {
          jobs.markFailed(
              lease.jobId,
              finished,
              result.message(),
              result.scanned,
              result.changed,
              result.errors);
        }
      } catch (Exception e) {
        var msg = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage());
        jobs.markFailed(lease.jobId, System.currentTimeMillis(), msg, 0, 0, 1);
      }
    } finally {
      running.set(false);
    }
  }
}
