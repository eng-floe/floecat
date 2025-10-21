package ai.floedb.metacat.reconciler.impl;

import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.GetConnectorRequest;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;

@ApplicationScoped
public class ReconcilerScheduler {
  @Inject GrpcClients clients;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;

  private final AtomicBoolean running = new AtomicBoolean(false);

  public void signalScheduler() {
    pollOnce();
  }

  @Scheduled(
      every = "{reconciler.pollEvery:10s}",
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
        var resourceId = ResourceId.newBuilder()
            .setTenantId(lease.tenantId)
            .setId(lease.connectorId)
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();

        Connector connector = clients.connector().getConnector(
            GetConnectorRequest.newBuilder().setConnectorId(resourceId).build()
        ).getConnector();

        ConnectorConfig cfg = toConfig(connector);

        var result = reconcilerService.reconcile(cfg, lease.fullRescan);

        long finished = System.currentTimeMillis();
        if (result.ok()) {
          jobs.markSucceeded(lease.jobId, finished, result.scanned, result.changed);
        } else {
          jobs.markFailed(lease.jobId, finished, result.message(),
              result.scanned, result.changed, result.errors);
        }
      } catch (Exception e) {
        jobs.markFailed(lease.jobId, System.currentTimeMillis(), e.getMessage(), 0, 0, 1);
      }
    } finally {
      running.set(false);
    }
  }

  private static ConnectorConfig toConfig(Connector connector) {
    Kind kind = switch (connector.getKind()) {
      case CK_ICEBERG_REST -> Kind.ICEBERG_REST;
      case CK_DELTA        -> Kind.DELTA;
      case CK_GLUE         -> Kind.GLUE;
      case CK_UNITY        -> Kind.UNITY;
      default -> throw new IllegalArgumentException("Unsupported kind: " + connector.getKind());
    };
    var auth = new ConnectorConfig.Auth(
        connector.getAuth().getScheme(),
        connector.getAuth().getPropsMap(),
        connector.getAuth().getHeaderHintsMap(),
        connector.getAuth().getSecretRef()
    );
    return new ConnectorConfig(
        kind,
        connector.getDisplayName(),
        connector.getTargetCatalogDisplayName(),
        connector.getTargetTenantId(),
        connector.getUri(),
        connector.getOptionsMap(),
        auth
    );
  }
}
