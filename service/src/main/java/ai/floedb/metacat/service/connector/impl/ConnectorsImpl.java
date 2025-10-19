package ai.floedb.metacat.service.connector.impl;

import java.time.Clock;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Precondition;
import ai.floedb.metacat.connector.rpc.*;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.catalog.util.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.impl.ConnectorRepository;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import ai.floedb.metacat.service.storage.IdempotencyStore;
import ai.floedb.metacat.service.storage.util.IdempotencyGuard;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;

@GrpcService
public class ConnectorsImpl implements Connectors {

  @Inject ConnectorRepository connectors;
  @Inject PrincipalProvider principal;
  @Inject Authorizer authz;
  @Inject IdempotencyStore idempotencyStore;
  @Inject ReconcileJobStore jobs;

  private final Clock clock = Clock.systemUTC();

  @Override
  public Uni<CreateConnectorResponse> createConnector(CreateConnectorRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    final var tenant = p.getTenantId();
    final var targetTenant = req.getSpec().getTargetTenantId();
    if (targetTenant == null || targetTenant.isBlank()) {
      throw GrpcErrors.invalidArgument(corrId(), null,
          Map.of("field", "tenant_id"));
    }

    if (!targetTenant.equals(tenant)) {
      throw GrpcErrors.permissionDenied(corrId(), "tenant", Map.of());
    }

    final var idemKey = req.hasIdempotency() ? req.getIdempotency().getKey() : "";
    final var nowTs = Timestamps.fromMillis(clock.millis());

    var spec = req.getSpec();
    byte[] fp = spec.toBuilder().clearPolicy().build().toByteArray();

    var out = MutationOps.createProto(
        tenant,
        "CreateConnector",
        idemKey,
        () -> fp,
        () -> {
          var rid = ResourceId.newBuilder()
              .setTenantId(tenant)
              .setId(UUID.randomUUID().toString())
              .setKind(ResourceKind.RK_CONNECTOR)
              .build();

          var c = Connector.newBuilder()
              .setResourceId(rid)
              .setDisplayName(mustNonEmpty(spec.getDisplayName(), "display_name"))
              .setKind(spec.getKind())
              .setTargetCatalogDisplayName(mustNonEmpty(
                    spec.getTargetCatalogDisplayName(), "target_catalog_display_name"))
              .setTargetTenantId(spec.getTargetTenantId().isBlank()
                  ? tenant : spec.getTargetTenantId())
              .setUri(mustNonEmpty(spec.getUri(), "uri"))
              .putAllOptions(spec.getOptionsMap())
              .setAuth(spec.getAuth())
              .setPolicy(spec.getPolicy())
              .setCreatedAt(nowTs)
              .setUpdatedAt(nowTs)
              .setState(ConnectorState.CS_ACTIVE)
              .build();

          try {
            if (connectors.getByName(tenant, c.getDisplayName()).isPresent()) {
              throw GrpcErrors.conflict(corrId(), "connector.already_exists",
                  Map.of("display_name", c.getDisplayName()));
            }
            connectors.create(c);
          } catch (IllegalStateException ise) {
            throw GrpcErrors.conflict(corrId(), "connector.already_exists",
                Map.of("display_name", c.getDisplayName()));
          }
          return new IdempotencyGuard.CreateResult<>(c, rid);
        },
        (conn) -> connectors.metaFor(conn.getResourceId(), nowTs),
        idempotencyStore,
        nowTs,
        86_400L,
        this::corrId,
        Connector::parseFrom
    );

    return Uni.createFrom().item(
        CreateConnectorResponse.newBuilder()
            .setConnector(out.body)
            .setMeta(out.meta)
            .build());
  }

  @Override
  public Uni<GetConnectorResponse> getConnector(GetConnectorRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var rid = req.getConnectorId();
    ensureKind(rid, "GetConnector");

    var c = connectors.getById(rid)
        .orElseThrow(() -> GrpcErrors.notFound(
            corrId(), "connector", Map.of("id", rid.getId())));

    return Uni.createFrom().item(GetConnectorResponse.newBuilder().setConnector(c).build());
  }

  @Override
  public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var tenant = req.getTenantId().isBlank() ? p.getTenantId() : req.getTenantId();

    var page = req.getPage();
    int limit = page.getPageSize() > 0 ? page.getPageSize() : 100;
    String token = page.getPageToken();

    var next = new StringBuilder();
    var list = connectors.listByName(tenant, limit, token, next);
    int total = connectors.countAll(tenant);

    var out = ListConnectorsResponse.newBuilder()
        .addAllConnectors(list)
        .setPage(PageResponse.newBuilder()
            .setNextPageToken(next.toString())
            .setTotalSize(total)
            .build())
        .build();

    return Uni.createFrom().item(out);
  }

  @Override
  public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var rid = req.getConnectorId();
    ensureKind(rid, "UpdateConnector");

    var corr = corrId();
    var cur = connectors.getById(rid)
        .orElseThrow(() -> GrpcErrors.notFound(
          corr, "connector", Map.of("id", rid.getId())));

    var spec = req.getSpec();
    var nowTs = Timestamps.fromMillis(clock.millis());
    var curMeta = connectors.metaFor(rid, nowTs);
    enforcePreconditions(corr, curMeta, req.getPrecondition());

    var desired = cur.toBuilder()
        .setDisplayName(spec.getDisplayName().isBlank()
            ? cur.getDisplayName() : spec.getDisplayName())
        .setKind(spec.getKind() == ConnectorKind.CK_UNSPECIFIED ? cur.getKind() : spec.getKind())
        .setTargetCatalogDisplayName(spec.getTargetCatalogDisplayName().isBlank()
            ? cur.getTargetCatalogDisplayName() : spec.getTargetCatalogDisplayName())
        .setTargetTenantId(spec.getTargetTenantId().isBlank()
            ? cur.getTargetTenantId() : spec.getTargetTenantId())
        .setUri(spec.getUri().isBlank() ? cur.getUri() : spec.getUri())
        .clearOptions().putAllOptions(spec.getOptionsMap().isEmpty()
            ? cur.getOptionsMap() : spec.getOptionsMap())
        .setAuth(spec.hasAuth() ? spec.getAuth() : cur.getAuth())
        .setPolicy(spec.hasPolicy() ? spec.getPolicy() : cur.getPolicy())
        .setUpdatedAt(nowTs)
        .build();

    if (desired.equals(cur)) {
      return Uni.createFrom().item(UpdateConnectorResponse.newBuilder()
          .setConnector(cur).setMeta(curMeta).build());
    }

    boolean nameChanged = !desired.getDisplayName().equals(cur.getDisplayName());
    if (nameChanged) {
      try {
        connectors.rename(desired, cur.getDisplayName(), curMeta.getPointerVersion());
      } catch (IllegalStateException e) {
        throw GrpcErrors.conflict(corr, "connector.already_exists",
            Map.of("display_name", desired.getDisplayName()));
      }
    } else {
      connectors.update(desired, curMeta.getPointerVersion());
    }

    var outMeta = connectors.metaFor(rid, nowTs);
    return Uni.createFrom().item(UpdateConnectorResponse.newBuilder()
        .setConnector(connectors.getById(rid).orElse(desired))
        .setMeta(outMeta)
        .build());
  }

  @Override
  public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var rid = req.getConnectorId();
    ensureKind(rid, "DeleteConnector");

    var corr = corrId();
    var nowTs = Timestamps.fromMillis(clock.millis());

    var meta = connectors.metaForSafe(rid, nowTs);
    if (connectors.getById(rid).isEmpty()) {
      return Uni.createFrom().item(DeleteConnectorResponse.newBuilder().setMeta(meta).build());
    }

    var curMeta = connectors.metaFor(rid, nowTs);
    enforcePreconditions(corr, curMeta, req.getPrecondition());

    boolean ok = connectors.deleteWithPrecondition(rid, curMeta.getPointerVersion());
    if (!ok) {
      var cur = connectors.metaForSafe(rid, nowTs);
      throw GrpcErrors.preconditionFailed(corr, "version_mismatch",
          Map.of("expected", Long.toString(curMeta.getPointerVersion()),
                 "actual", Long.toString(cur.getPointerVersion())));
    }
    return Uni.createFrom().item(DeleteConnectorResponse.newBuilder().setMeta(curMeta).build());
  }

  @Override
  public Uni<ValidateConnectorResponse> validateConnector(ValidateConnectorRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var spec = req.getSpec();

    var kind = switch (spec.getKind()) {
      case CK_ICEBERG_REST -> Kind.ICEBERG_REST;
      case CK_DELTA        -> Kind.DELTA;
      case CK_GLUE         -> Kind.GLUE;
      case CK_UNITY        -> Kind.UNITY;
      default -> throw GrpcErrors.invalidArgument(
          corrId(), null, Map.of("field", "kind"));
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
      return Uni.createFrom().item(
          ValidateConnectorResponse.newBuilder()
              .setOk(true)
              .setSummary(
                  "OK: " + (namespaces.isEmpty() 
                      ? "no namespaces" 
                      : "namespaces=" + namespaces.size()))
              .putCapabilities("supportsStats", Boolean.toString(c.supportsTableStats()))
              .build());
    } catch (Exception e) {
      return Uni.createFrom().item(
          ValidateConnectorResponse.newBuilder()
              .setOk(false)
              .setSummary("Validation failed: " + e.getMessage())
              .build());
    }
  }

  @Override
  public Uni<TriggerReconcileResponse> triggerReconcile(TriggerReconcileRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var rid = req.getConnectorId();
    ensureKind(rid, "TriggerReconcile");
    connectors.getById(rid)
        .orElseThrow(() -> GrpcErrors.notFound(corrId(), "connector", Map.of("id", rid.getId())));

    var jobId = jobs.enqueue(rid.getTenantId(), rid.getId(), req.getFullRescan());
    return Uni.createFrom().item(TriggerReconcileResponse.newBuilder().setJobId(jobId).build());
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest req) {
    var p = principal.get();
    authz.require(p, "connector.manage");

    var j = jobs.get(req.getJobId())
        .orElseThrow(() -> GrpcErrors.notFound(
            corrId(), "job", Map.of("id", req.getJobId())));

    return Uni.createFrom().item(GetReconcileJobResponse.newBuilder()
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
        .build());
  }

  private void ensureKind(ResourceId rid, String op) {
    if (rid == null || rid.getKind() == null)
      throw GrpcErrors.invalidArgument(corrId(), null, Map.of("field", op));
    if (rid.getKind() != ResourceKind.RK_CONNECTOR)
      throw GrpcErrors.invalidArgument(corrId(), null, Map.of("field", op));
  }

  private static String mustNonEmpty(String v, String name) {
    if (v == null || v.isBlank())
      throw GrpcErrors.invalidArgument("", null, Map.of("field", name));
    return v;
  }

  private String corrId() {
    var pctx = principal != null ? principal.get() : null;
    return pctx != null ? pctx.getCorrelationId() : "";
  }

  private void enforcePreconditions(String corrId, MutationMeta cur, Precondition pc) {
    if (pc == null) {
      return;
    }
    boolean checkVer = pc.getExpectedVersion() > 0;
    boolean checkTag = pc.getExpectedEtag() != null && !pc.getExpectedEtag().isBlank();
    if (!checkVer && !checkTag) {
      return;
    }
    if (checkVer && cur.getPointerVersion() != pc.getExpectedVersion()) {
      throw GrpcErrors.preconditionFailed(corrId, "version_mismatch",
          Map.of("expected", Long.toString(pc.getExpectedVersion()),
                 "actual", Long.toString(cur.getPointerVersion())));
    }
    if (checkTag && !Objects.equals(cur.getEtag(), pc.getExpectedEtag())) {
      throw GrpcErrors.preconditionFailed(corrId, "etag_mismatch",
          Map.of("expected", pc.getExpectedEtag(), "actual", cur.getEtag()));
    }
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
