package ai.floedb.metacat.service.connector.impl;

import java.util.Map;
import java.util.UUID;

import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.connector.rpc.*;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.ConnectorRepository;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;

@GrpcService
public class ConnectorsImpl extends BaseServiceImpl implements Connectors {

  @Inject ConnectorRepository connectors;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;
  @Inject ReconcileJobStore jobs;

  @Override
  public Uni<CreateConnectorResponse> createConnector(CreateConnectorRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "connector.manage");

      var tenant = principalContext.getTenantId();
      var idempotencyKey = request.hasIdempotency() ? request.getIdempotency().getKey() : "";
      var tsNow = nowTs();

      var spec = request.getSpec();
      byte[] fingerprint = spec.toBuilder().clearPolicy().build().toByteArray();

      var connectorProto = MutationOps.createProto(
          tenant,
          "CreateConnector",
          idempotencyKey,
          () -> fingerprint,
          () -> {
              String connUuid =
                  !idempotencyKey.isBlank()
                      ? deterministicUuid(tenant, "connector", idempotencyKey)
                      : UUID.randomUUID().toString();

            var connectorId = ResourceId.newBuilder()
                .setTenantId(tenant)
                .setId(connUuid)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build();

            var c = Connector.newBuilder()
                .setResourceId(connectorId)
                .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name", correlationId))
                .setKind(spec.getKind())
                .setTargetCatalogDisplayName(mustNonEmpty(
                    spec.getTargetCatalogDisplayName(),
                        "target_catalog_display_name", correlationId))
                .setTargetTenantId(tenant)
                .setUri(mustNonEmpty(spec.getUri(), "uri", correlationId))
                .putAllOptions(spec.getOptionsMap())
                .setAuth(spec.getAuth())
                .setPolicy(spec.getPolicy())
                .setCreatedAt(tsNow)
                .setUpdatedAt(tsNow)
                .setState(ConnectorState.CS_ACTIVE)
                .build();
            try {
              connectors.create(c);
            } catch (BaseRepository.NameConflictException nce) {
            if (!idempotencyKey.isBlank()) {
              var existing = connectors.getById(connectorId)
                  .or(() -> connectors.getByName(tenant, c.getDisplayName()));
              if (existing.isPresent()) {
                return new IdempotencyGuard.CreateResult<>(
                    existing.get(), existing.get().getResourceId());
              }
            }
            throw GrpcErrors.conflict(correlationId, "connector.already_exists",
                Map.of("display_name", c.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(c, connectorId);
          },
          (conn) -> connectors.metaFor(conn.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::correlationId,
          Connector::parseFrom
      );

      return CreateConnectorResponse.newBuilder()
          .setConnector(connectorProto.body)
          .setMeta(connectorProto.meta)
          .build();
    }), correlationId());
  }

  @Override
  public Uni<GetConnectorResponse> getConnector(GetConnectorRequest request) {
    return mapFailures(run(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "connector.manage");

      var connectorId = request.getConnectorId();
      ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);

      var connector = connectors.getById(connectorId)
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "connector", Map.of("id", connectorId.getId())));

      return GetConnectorResponse.newBuilder().setConnector(connector).build();
    }), correlationId());
  }

  @Override
  public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest request) {
    return mapFailures(run(() -> {
      var principalContext = principal.get();

      authz.require(principalContext, "connector.manage");

      var tenant = principalContext.getTenantId();

      var page = request.getPage();
      int limit = page.getPageSize() > 0 ? page.getPageSize() : 100;
      String token = page.getPageToken();

      var next = new StringBuilder();
      var list = connectors.listByName(tenant, limit, token, next);
      int total = connectors.countAll(tenant);

      return ListConnectorsResponse.newBuilder()
          .addAllConnectors(list)
          .setPage(PageResponse.newBuilder()
              .setNextPageToken(next.toString())
              .setTotalSize(total)
              .build())
          .build();
    }), correlationId());
  }

  @Override
  public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest request) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var correlationId = p.getCorrelationId();

      authz.require(p, "connector.manage");

      var connectorId = request.getConnectorId();
      ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);

      var connector = connectors.getById(connectorId)
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "connector", Map.of("id", connectorId.getId())));

      var tsNow = nowTs();
      var currentMeta = connectors.metaFor(connectorId, tsNow);
      enforcePreconditions(correlationId, currentMeta, request.getPrecondition());

      var spec = request.getSpec();
      var desired = connector.toBuilder()
          .setDisplayName(spec.getDisplayName().isBlank()
              ? connector.getDisplayName() : spec.getDisplayName())
          .setKind(spec.getKind() == ConnectorKind.CK_UNSPECIFIED
              ? connector.getKind() : spec.getKind())
          .setTargetCatalogDisplayName(
              spec.getTargetCatalogDisplayName().isBlank()
                  ? connector.getTargetCatalogDisplayName()
                  : spec.getTargetCatalogDisplayName())
          .setTargetTenantId(p.getTenantId())
          .setUri(spec.getUri().isBlank() ? connector.getUri() : spec.getUri())
          .clearOptions().putAllOptions(spec.getOptionsMap().isEmpty()
              ? connector.getOptionsMap()
              : spec.getOptionsMap())
          .setAuth(spec.hasAuth() ? spec.getAuth() : connector.getAuth())
          .setPolicy(spec.hasPolicy() ? spec.getPolicy() : connector.getPolicy())
          .setUpdatedAt(tsNow)
          .build();

      if (desired.equals(connector)) {
        return UpdateConnectorResponse.newBuilder()
            .setConnector(connector)
            .setMeta(currentMeta)
            .build();
      }

      boolean nameChanged = !desired.getDisplayName().equals(connector.getDisplayName());
      if (nameChanged) {
        try {
          connectors.rename(desired, connector.getDisplayName(), currentMeta.getPointerVersion());
        } catch (BaseRepository.NameConflictException nce) {
          throw GrpcErrors.conflict(correlationId, "connector.already_exists",
              Map.of("display_name", desired.getDisplayName()));
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = connectors.metaFor(connectorId, tsNow);
          throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
              Map.of("expected", Long.toString(currentMeta.getPointerVersion()),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      } else {
        try {
          connectors.update(desired, currentMeta.getPointerVersion());
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = connectors.metaFor(connectorId, tsNow);
          throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
              Map.of("expected", Long.toString(currentMeta.getPointerVersion()),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      }

      var outMeta = connectors.metaFor(connectorId, tsNow);
      var outConn = connectors.getById(connectorId).orElse(desired);

      return UpdateConnectorResponse.newBuilder()
          .setConnector(outConn)
          .setMeta(outMeta)
          .build();
    }), correlationId());
  }

  @Override
  public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest request) {
    return mapFailures(runWithRetry(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "connector.manage");

      var connectorId = request.getConnectorId();
      ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);

      var tsNow = nowTs();

      var currentMeta = connectors.metaForSafe(connectorId, tsNow);
      if (connectors.getById(connectorId).isEmpty()) {
        return DeleteConnectorResponse.newBuilder().setMeta(currentMeta).build();
      }

      currentMeta = connectors.metaFor(connectorId, tsNow);
      enforcePreconditions(correlationId, currentMeta, request.getPrecondition());

      try {
        connectors.deleteWithPrecondition(connectorId, currentMeta.getPointerVersion());
      } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = connectors.metaFor(connectorId, tsNow);
        throw GrpcErrors.preconditionFailed(correlationId, "version_mismatch",
            Map.of("expected", Long.toString(currentMeta.getPointerVersion()),
                "actual", Long.toString(nowMeta.getPointerVersion())));
      } catch (BaseRepository.NotFoundException nfe) {
        throw GrpcErrors.notFound(correlationId, "connector", Map.of("id", connectorId.getId()));
      }

      return DeleteConnectorResponse.newBuilder()
          .setMeta(currentMeta)
          .build();
    }), correlationId());
  }

  @Override
  public Uni<ValidateConnectorResponse> validateConnector(ValidateConnectorRequest request) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var correlationId = p.getCorrelationId();

      authz.require(p, "connector.manage");

      var spec = request.getSpec();

      var kind = switch (spec.getKind()) {
        case CK_ICEBERG_REST -> Kind.ICEBERG_REST;
        case CK_DELTA        -> Kind.DELTA;
        case CK_GLUE         -> Kind.GLUE;
        case CK_UNITY        -> Kind.UNITY;
        default -> throw GrpcErrors.invalidArgument(
            correlationId, null, Map.of("field", "kind"));
      };

      var auth = new ConnectorConfig.Auth(
          spec.getAuth().getScheme(),
          spec.getAuth().getPropsMap(),
          spec.getAuth().getHeaderHintsMap(),
          spec.getAuth().getSecretRef()
      );

      var cfg = new ConnectorConfig(
          kind,
          spec.getDisplayName(),
          spec.getTargetCatalogDisplayName(),
          spec.getTargetTenantId(),
          spec.getUri(),
          spec.getOptionsMap(),
          auth
      );

      try (var connector = ConnectorFactory.create(cfg)) {
        var namespaces = connector.listNamespaces();
        return ValidateConnectorResponse.newBuilder()
            .setOk(true)
            .setSummary(
                "OK: " + (namespaces.isEmpty()
                    ? "no namespaces"
                    : "namespaces=" + namespaces.size()))
            .putCapabilities("supportsStats", Boolean.toString(connector.supportsTableStats()))
            .build();
      } catch (Exception e) {
        return ValidateConnectorResponse.newBuilder()
            .setOk(false)
            .setSummary("Validation failed: " + e.getMessage())
            .build();
      }
    }), correlationId());
  }

  @Override
  public Uni<TriggerReconcileResponse> triggerReconcile(TriggerReconcileRequest request) {
    return mapFailures(run(() -> {
      var princpalContext = principal.get();
      var correlationId = princpalContext.getCorrelationId();

      authz.require(princpalContext, "connector.manage");

      var connectorId = request.getConnectorId();
      ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);
      connectors.getById(connectorId)
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "connector", Map.of("id", connectorId.getId())));

      var jobId = jobs.enqueue(
          connectorId.getTenantId(), connectorId.getId(), request.getFullRescan());

      return TriggerReconcileResponse.newBuilder().setJobId(jobId).build();
    }), correlationId());
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest request) {
    return mapFailures(run(() -> {
      var principalContext = principal.get();
      var correlationId = principalContext.getCorrelationId();

      authz.require(principalContext, "connector.manage");

      var job = jobs.get(request.getJobId())
          .orElseThrow(() -> GrpcErrors.notFound(
              correlationId, "job", Map.of("id", request.getJobId())));

      return GetReconcileJobResponse.newBuilder()
          .setJobId(job.jobId)
          .setConnectorId(job.connectorId)
          .setState(toProtoState(job.state))
          .setMessage(job.message == null ? "" : job.message)
          .setStartedAt(Timestamps.fromMillis(job.startedAtMs))
          .setFinishedAt(job.finishedAtMs == 0
              ? Timestamps.fromMillis(0)
              : Timestamps.fromMillis(job.finishedAtMs))
          .setTablesScanned(job.tablesScanned)
          .setTablesChanged(job.tablesChanged)
          .setErrors(job.errors)
          .build();
    }), correlationId());
  }

  private static JobState toProtoState(String state) {
    if (state == null) {
      return JobState.JS_UNSPECIFIED;
    }

    return switch (state) {
      case "JS_QUEUED"    -> JobState.JS_QUEUED;
      case "JS_RUNNING"   -> JobState.JS_RUNNING;
      case "JS_SUCCEEDED" -> JobState.JS_SUCCEEDED;
      case "JS_FAILED"    -> JobState.JS_FAILED;
      case "JS_CANCELLED" -> JobState.JS_CANCELLED;
      default -> JobState.JS_UNSPECIFIED;
    };
  }
}
