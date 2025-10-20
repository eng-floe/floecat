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
  public Uni<CreateConnectorResponse> createConnector(CreateConnectorRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      final var tenant = p.getTenantId();
      var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";
      var tsNow = nowTs();

      var spec = req.getSpec();
      byte[] fp = spec.toBuilder().clearPolicy().build().toByteArray();

      var out = MutationOps.createProto(
          tenant,
          "CreateConnector",
          idemKey,
          () -> fp,
          () -> {
            final String connUuid =
            !idemKey.isBlank()
                ? deterministicUuid(tenant, "connector", idemKey)
                : UUID.randomUUID().toString();

            var rid = ResourceId.newBuilder()
                .setTenantId(tenant)
                .setId(connUuid)
                .setKind(ResourceKind.RK_CONNECTOR)
                .build();

            var c = Connector.newBuilder()
                .setResourceId(rid)
                .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name", corr))
                .setKind(spec.getKind())
                .setTargetCatalogDisplayName(mustNonEmpty(
                    spec.getTargetCatalogDisplayName(), "target_catalog_display_name", corr))
                .setTargetTenantId(tenant)
                .setUri(mustNonEmpty(spec.getUri(), "uri", corr))
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
            if (!idemKey.isBlank()) {
              var existing = connectors.getById(rid)
                  .or(() -> connectors.getByName(tenant, c.getDisplayName()));
              if (existing.isPresent()) {
                return new IdempotencyGuard.CreateResult<>(existing.get(), existing.get().getResourceId());
              }
            }
            throw GrpcErrors.conflict(corr, "connector.already_exists",
                Map.of("display_name", c.getDisplayName()));
            }

            return new IdempotencyGuard.CreateResult<>(c, rid);
          },
          (conn) -> connectors.metaFor(conn.getResourceId(), tsNow),
          idempotencyStore,
          tsNow,
          IDEMPOTENCY_TTL_SECONDS,
          this::corrId,
          Connector::parseFrom
      );

      return CreateConnectorResponse.newBuilder()
          .setConnector(out.body)
          .setMeta(out.meta)
          .build();
    }), corrId());
  }

  @Override
  public Uni<GetConnectorResponse> getConnector(GetConnectorRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var rid = req.getConnectorId();
      ensureKind(rid, ResourceKind.RK_CONNECTOR, "connector_id", corr);

      var c = connectors.getById(rid)
          .orElseThrow(() -> GrpcErrors.notFound(
              corr, "connector", Map.of("id", rid.getId())));

      return GetConnectorResponse.newBuilder().setConnector(c).build();
    }), corrId());
  }

  @Override
  public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      authz.require(p, "connector.manage");

      var tenant = p.getTenantId();

      var page = req.getPage();
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
    }), corrId());
  }

  @Override
  public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var rid = req.getConnectorId();
      ensureKind(rid, ResourceKind.RK_CONNECTOR, "connector_id", corr);

      var cur = connectors.getById(rid)
          .orElseThrow(() -> GrpcErrors.notFound(
              corr, "connector", Map.of("id", rid.getId())));

      var tsNow = nowTs();
      var curMeta = connectors.metaFor(rid, tsNow);
      enforcePreconditions(corr, curMeta, req.getPrecondition());

      var spec = req.getSpec();
      var desired = cur.toBuilder()
          .setDisplayName(spec.getDisplayName().isBlank()
              ? cur.getDisplayName() : spec.getDisplayName())
          .setKind(spec.getKind() == ConnectorKind.CK_UNSPECIFIED
              ? cur.getKind() : spec.getKind())
          .setTargetCatalogDisplayName(
              spec.getTargetCatalogDisplayName().isBlank()
                  ? cur.getTargetCatalogDisplayName()
                  : spec.getTargetCatalogDisplayName())
          .setTargetTenantId(p.getTenantId())
          .setUri(spec.getUri().isBlank() ? cur.getUri() : spec.getUri())
          .clearOptions().putAllOptions(spec.getOptionsMap().isEmpty()
              ? cur.getOptionsMap()
              : spec.getOptionsMap())
          .setAuth(spec.hasAuth() ? spec.getAuth() : cur.getAuth())
          .setPolicy(spec.hasPolicy() ? spec.getPolicy() : cur.getPolicy())
          .setUpdatedAt(tsNow)
          .build();

      if (desired.equals(cur)) {
        return UpdateConnectorResponse.newBuilder()
            .setConnector(cur)
            .setMeta(curMeta)
            .build();
      }

      boolean nameChanged = !desired.getDisplayName().equals(cur.getDisplayName());
      if (nameChanged) {
        try {
          connectors.rename(desired, cur.getDisplayName(), curMeta.getPointerVersion());
        } catch (BaseRepository.NameConflictException nce) {
          throw GrpcErrors.conflict(corr, "connector.already_exists",
              Map.of("display_name", desired.getDisplayName()));
        } catch (BaseRepository.PreconditionFailedException pfe) {
        var nowMeta = connectors.metaFor(rid, tsNow);
        throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
            Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      } else {
        try {
          connectors.update(desired, curMeta.getPointerVersion());
        } catch (BaseRepository.PreconditionFailedException pfe) {
          var nowMeta = connectors.metaFor(rid, tsNow);
          throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
              Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                  "actual", Long.toString(nowMeta.getPointerVersion())));
        }
      }

      var outMeta = connectors.metaFor(rid, tsNow);
      var outConn = connectors.getById(rid).orElse(desired);

      return UpdateConnectorResponse.newBuilder()
          .setConnector(outConn)
          .setMeta(outMeta)
          .build();
    }), corrId());
  }

  @Override
  public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest req) {
    return mapFailures(runWithRetry(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var rid = req.getConnectorId();
      ensureKind(rid, ResourceKind.RK_CONNECTOR, "connector_id", corr);

      var tsNow = nowTs();

      var meta = connectors.metaForSafe(rid, tsNow);
      if (connectors.getById(rid).isEmpty()) {
        return DeleteConnectorResponse.newBuilder().setMeta(meta).build();
      }

      var curMeta = connectors.metaFor(rid, tsNow);
      enforcePreconditions(corr, curMeta, req.getPrecondition());

      var cur = connectors.metaForSafe(rid, tsNow);
      try {
        connectors.deleteWithPrecondition(rid, curMeta.getPointerVersion());
    } catch (BaseRepository.PreconditionFailedException pfe) {
      var nowMeta = connectors.metaFor(rid, tsNow);
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
          Map.of("expected", Long.toString(curMeta.getPointerVersion()),
              "actual", Long.toString(nowMeta.getPointerVersion())));
    } catch (BaseRepository.NotFoundException nfe) {
      throw GrpcErrors.notFound(corr, "connector", Map.of("id", rid.getId()));
    }

    return DeleteConnectorResponse.newBuilder()
        .setMeta(curMeta)
        .build();
    }), corrId());
  }

  @Override
  public Uni<ValidateConnectorResponse> validateConnector(ValidateConnectorRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var spec = req.getSpec();

      var kind = switch (spec.getKind()) {
        case CK_ICEBERG_REST -> Kind.ICEBERG_REST;
        case CK_DELTA        -> Kind.DELTA;
        case CK_GLUE         -> Kind.GLUE;
        case CK_UNITY        -> Kind.UNITY;
        default -> throw GrpcErrors.invalidArgument(
            corr, null, Map.of("field", "kind"));
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

      try (var c = ConnectorFactory.create(cfg)) {
        var namespaces = c.listNamespaces();
        return ValidateConnectorResponse.newBuilder()
            .setOk(true)
            .setSummary(
                "OK: " + (namespaces.isEmpty()
                    ? "no namespaces"
                    : "namespaces=" + namespaces.size()))
            .putCapabilities("supportsStats", Boolean.toString(c.supportsTableStats()))
            .build();
      } catch (Exception e) {
        return ValidateConnectorResponse.newBuilder()
            .setOk(false)
            .setSummary("Validation failed: " + e.getMessage())
            .build();
      }
    }), corrId());
  }

  @Override
  public Uni<TriggerReconcileResponse> triggerReconcile(TriggerReconcileRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var rid = req.getConnectorId();
      ensureKind(rid, ResourceKind.RK_CONNECTOR, "connector_id", corr);
      connectors.getById(rid)
          .orElseThrow(() -> GrpcErrors.notFound(
              corr, "connector", Map.of("id", rid.getId())));

      var jobId = jobs.enqueue(rid.getTenantId(), rid.getId(), req.getFullRescan());
      return TriggerReconcileResponse.newBuilder().setJobId(jobId).build();
      }), corrId());
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest req) {
    return mapFailures(run(() -> {
      var p = principal.get();
      var corr = p.getCorrelationId();
      authz.require(p, "connector.manage");

      var j = jobs.get(req.getJobId())
          .orElseThrow(() -> GrpcErrors.notFound(
              corr, "job", Map.of("id", req.getJobId())));

      return GetReconcileJobResponse.newBuilder()
          .setJobId(j.jobId)
          .setConnectorId(j.connectorId)
          .setState(toProtoState(j.state))
          .setMessage(j.message == null ? "" : j.message)
          .setStartedAt(Timestamps.fromMillis(j.startedAtMs))
          .setFinishedAt(j.finishedAtMs == 0
              ? Timestamps.fromMillis(0)
              : Timestamps.fromMillis(j.finishedAtMs))
          .setTablesScanned(j.tablesScanned)
          .setTablesChanged(j.tablesChanged)
          .setErrors(j.errors)
          .build();
    }), corrId());
  }

  private static JobState toProtoState(String s) {
    if (s == null) return JobState.JS_UNSPECIFIED;
    return switch (s) {
      case "JS_QUEUED"    -> JobState.JS_QUEUED;
      case "JS_RUNNING"   -> JobState.JS_RUNNING;
      case "JS_SUCCEEDED" -> JobState.JS_SUCCEEDED;
      case "JS_FAILED"    -> JobState.JS_FAILED;
      case "JS_CANCELLED" -> JobState.JS_CANCELLED;
      default -> JobState.JS_UNSPECIFIED;
    };
  }
}
