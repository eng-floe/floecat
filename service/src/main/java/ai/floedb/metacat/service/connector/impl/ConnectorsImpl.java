package ai.floedb.metacat.service.connector.impl;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.connector.rpc.ConnectorKind;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorState;
import ai.floedb.metacat.connector.rpc.Connectors;
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.connector.rpc.CreateConnectorResponse;
import ai.floedb.metacat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.metacat.connector.rpc.DeleteConnectorResponse;
import ai.floedb.metacat.connector.rpc.GetConnectorRequest;
import ai.floedb.metacat.connector.rpc.GetConnectorResponse;
import ai.floedb.metacat.connector.rpc.GetReconcileJobRequest;
import ai.floedb.metacat.connector.rpc.GetReconcileJobResponse;
import ai.floedb.metacat.connector.rpc.JobState;
import ai.floedb.metacat.connector.rpc.ListConnectorsRequest;
import ai.floedb.metacat.connector.rpc.ListConnectorsResponse;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.metacat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.metacat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.metacat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.metacat.connector.rpc.ValidateConnectorResponse;
import ai.floedb.metacat.connector.spi.ConnectorConfig;
import ai.floedb.metacat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.metacat.connector.spi.ConnectorFactory;
import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.metacat.service.common.BaseServiceImpl;
import ai.floedb.metacat.service.common.Canonicalizer;
import ai.floedb.metacat.service.common.IdempotencyGuard;
import ai.floedb.metacat.service.common.MutationOps;
import ai.floedb.metacat.service.error.impl.GrpcErrors;
import ai.floedb.metacat.service.repo.IdempotencyRepository;
import ai.floedb.metacat.service.repo.impl.ConnectorRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository;
import ai.floedb.metacat.service.repo.util.BaseResourceRepository.AbortRetryableException;
import ai.floedb.metacat.service.security.impl.Authorizer;
import ai.floedb.metacat.service.security.impl.PrincipalProvider;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Map;

@GrpcService
public class ConnectorsImpl extends BaseServiceImpl implements Connectors {
  @Inject ConnectorRepository connectorRepo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject ReconcileJobStore jobs;

  @Override
  public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              authz.require(principalContext, "connector.manage");

              var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
              var next = new StringBuilder();

              var connectors =
                  connectorRepo.list(
                      principalContext.getTenantId(),
                      Math.max(1, pageIn.limit),
                      pageIn.token,
                      next);

              var page =
                  MutationOps.pageOut(
                      next.toString(), connectorRepo.count(principalContext.getTenantId()));

              return ListConnectorsResponse.newBuilder()
                  .addAllConnectors(connectors)
                  .setPage(page)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetConnectorResponse> getConnector(GetConnectorRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "connector.manage");

              var connectorId = request.getConnectorId();
              ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);

              var connector =
                  connectorRepo
                      .getById(connectorId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "connector", Map.of("id", connectorId.getId())));

              return GetConnectorResponse.newBuilder().setConnector(connector).build();
            }),
        correlationId());
  }

  @Override
  public Uni<CreateConnectorResponse> createConnector(CreateConnectorRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var principalContext = principalProvider.get();
              var tenantId = principalContext.getTenantId();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "connector.manage");

              var tsNow = nowTs();

              var fingerprint = canonicalFingerprint(request.getSpec());
              var idempotencyKey =
                  request.hasIdempotency() && !request.getIdempotency().getKey().isBlank()
                      ? request.getIdempotency().getKey()
                      : hashFingerprint(fingerprint);

              var connectorProto =
                  MutationOps.createProto(
                      tenantId,
                      "CreateConnector",
                      idempotencyKey,
                      () -> fingerprint,
                      () -> {
                        String connUuid = deterministicUuid(tenantId, "connector", idempotencyKey);

                        var connectorId =
                            ResourceId.newBuilder()
                                .setTenantId(tenantId)
                                .setId(connUuid)
                                .setKind(ResourceKind.RK_CONNECTOR)
                                .build();

                        var c =
                            Connector.newBuilder()
                                .setResourceId(connectorId)
                                .setDisplayName(
                                    mustNonEmpty(
                                        request.getSpec().getDisplayName(),
                                        "display_name",
                                        correlationId))
                                .setKind(request.getSpec().getKind())
                                .setTargetCatalogDisplayName(
                                    mustNonEmpty(
                                        request.getSpec().getTargetCatalogDisplayName(),
                                        "target_catalog_display_name",
                                        correlationId))
                                .setTargetTenantId(tenantId)
                                .setUri(
                                    mustNonEmpty(request.getSpec().getUri(), "uri", correlationId))
                                .putAllOptions(request.getSpec().getOptionsMap())
                                .setAuth(request.getSpec().getAuth())
                                .setPolicy(request.getSpec().getPolicy())
                                .setCreatedAt(tsNow)
                                .setUpdatedAt(tsNow)
                                .setState(ConnectorState.CS_ACTIVE)
                                .build();
                        try {
                          connectorRepo.create(c);
                        } catch (BaseResourceRepository.NameConflictException nce) {
                          var existing =
                              connectorRepo
                                  .getById(connectorId)
                                  .or(() -> connectorRepo.getByName(tenantId, c.getDisplayName()));
                          if (existing.isPresent()) {
                            throw GrpcErrors.conflict(
                                correlationId,
                                "connector.already_exists",
                                Map.of("display_name", c.getDisplayName()));
                          }

                          throw new AbortRetryableException("name conflict visibility window");
                        }

                        return new IdempotencyGuard.CreateResult<>(c, connectorId);
                      },
                      (conn) -> connectorRepo.metaFor(conn.getResourceId()),
                      idempotencyStore,
                      tsNow,
                      IDEMPOTENCY_TTL_SECONDS,
                      this::correlationId,
                      Connector::parseFrom);

              return CreateConnectorResponse.newBuilder()
                  .setConnector(connectorProto.body)
                  .setMeta(connectorProto.meta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var pc = principalProvider.get();
              var corr = pc.getCorrelationId();

              authz.require(pc, "connector.manage");

              var connectorId = request.getConnectorId();
              ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", corr);

              var current =
                  connectorRepo
                      .getById(connectorId)
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  corr, "connector", Map.of("id", connectorId.getId())));

              var tsNow = nowTs();
              var spec = request.getSpec();

              var desired =
                  current.toBuilder()
                      .setDisplayName(
                          spec.getDisplayName().isBlank()
                              ? current.getDisplayName()
                              : spec.getDisplayName())
                      .setKind(
                          spec.getKind() == ConnectorKind.CK_UNSPECIFIED
                              ? current.getKind()
                              : spec.getKind())
                      .setTargetCatalogDisplayName(
                          spec.getTargetCatalogDisplayName().isBlank()
                              ? current.getTargetCatalogDisplayName()
                              : spec.getTargetCatalogDisplayName())
                      .setTargetTenantId(pc.getTenantId())
                      .setUri(spec.getUri().isBlank() ? current.getUri() : spec.getUri())
                      .clearOptions()
                      .putAllOptions(
                          spec.getOptionsMap().isEmpty()
                              ? current.getOptionsMap()
                              : spec.getOptionsMap())
                      .setAuth(spec.hasAuth() ? spec.getAuth() : current.getAuth())
                      .setPolicy(spec.hasPolicy() ? spec.getPolicy() : current.getPolicy())
                      .setUpdatedAt(tsNow)
                      .build();

              if (desired.equals(current)) {
                var metaNoop = connectorRepo.metaForSafe(connectorId);
                MutationOps.BaseServiceChecks.enforcePreconditions(
                    corr, metaNoop, request.getPrecondition());
                return UpdateConnectorResponse.newBuilder()
                    .setConnector(current)
                    .setMeta(metaNoop)
                    .build();
              }

              MutationOps.updateWithPreconditions(
                  () -> connectorRepo.metaFor(connectorId),
                  request.getPrecondition(),
                  expected -> connectorRepo.update(desired, expected),
                  () -> connectorRepo.metaForSafe(connectorId),
                  corr,
                  "connector",
                  Map.of("display_name", desired.getDisplayName()));

              var outMeta = connectorRepo.metaForSafe(connectorId);
              var outConnector = connectorRepo.getById(connectorId).orElse(desired);

              return UpdateConnectorResponse.newBuilder()
                  .setConnector(outConnector)
                  .setMeta(outMeta)
                  .build();
            }),
        correlationId());
  }

  @Override
  public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest request) {
    return mapFailures(
        runWithRetry(
            () -> {
              var pc = principalProvider.get();
              var corr = pc.getCorrelationId();

              authz.require(pc, "connector.manage");

              var connectorId = request.getConnectorId();
              ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", corr);

              var meta =
                  MutationOps.deleteWithPreconditions(
                      () -> connectorRepo.metaFor(connectorId),
                      request.getPrecondition(),
                      expected -> connectorRepo.deleteWithPrecondition(connectorId, expected),
                      () -> connectorRepo.metaForSafe(connectorId),
                      corr,
                      "connector",
                      Map.of("id", connectorId.getId()));

              return DeleteConnectorResponse.newBuilder().setMeta(meta).build();
            }),
        correlationId());
  }

  @Override
  public Uni<ValidateConnectorResponse> validateConnector(ValidateConnectorRequest request) {
    return mapFailures(
        run(
            () -> {
              var p = principalProvider.get();
              var correlationId = p.getCorrelationId();

              authz.require(p, "connector.manage");

              var spec = request.getSpec();

              var kind =
                  switch (spec.getKind()) {
                    case CK_ICEBERG_REST -> Kind.ICEBERG_REST;
                    case CK_DELTA -> Kind.DELTA;
                    case CK_GLUE -> Kind.GLUE;
                    case CK_UNITY -> Kind.UNITY;
                    default ->
                        throw GrpcErrors.invalidArgument(
                            correlationId, null, Map.of("field", "kind"));
                  };

              var auth =
                  new ConnectorConfig.Auth(
                      spec.getAuth().getScheme(),
                      spec.getAuth().getPropsMap(),
                      spec.getAuth().getHeaderHintsMap(),
                      spec.getAuth().getSecretRef());

              var cfg =
                  new ConnectorConfig(
                      kind,
                      spec.getDisplayName(),
                      spec.getTargetCatalogDisplayName(),
                      spec.getTargetTenantId(),
                      spec.getUri(),
                      spec.getOptionsMap(),
                      auth);

              try (var connector = ConnectorFactory.create(cfg)) {
                var namespaces = connector.listNamespaces();
                return ValidateConnectorResponse.newBuilder()
                    .setOk(true)
                    .setSummary(
                        "OK: "
                            + (namespaces.isEmpty()
                                ? "no namespaces"
                                : "namespaces=" + namespaces.size()))
                    .putCapabilities(
                        "supportsStats", Boolean.toString(connector.supportsTableStats()))
                    .build();
              } catch (Exception e) {
                return ValidateConnectorResponse.newBuilder()
                    .setOk(false)
                    .setSummary("Validation failed: " + e.getMessage())
                    .build();
              }
            }),
        correlationId());
  }

  @Override
  public Uni<TriggerReconcileResponse> triggerReconcile(TriggerReconcileRequest request) {
    return mapFailures(
        run(
            () -> {
              var princpalContext = principalProvider.get();
              var correlationId = princpalContext.getCorrelationId();

              authz.require(princpalContext, "connector.manage");

              var connectorId = request.getConnectorId();
              ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", correlationId);
              connectorRepo
                  .getById(connectorId)
                  .orElseThrow(
                      () ->
                          GrpcErrors.notFound(
                              correlationId, "connector", Map.of("id", connectorId.getId())));

              var jobId =
                  jobs.enqueue(
                      connectorId.getTenantId(), connectorId.getId(), request.getFullRescan());

              return TriggerReconcileResponse.newBuilder().setJobId(jobId).build();
            }),
        correlationId());
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest request) {
    return mapFailures(
        run(
            () -> {
              var principalContext = principalProvider.get();
              var correlationId = principalContext.getCorrelationId();

              authz.require(principalContext, "connector.manage");

              var job =
                  jobs.get(request.getJobId())
                      .orElseThrow(
                          () ->
                              GrpcErrors.notFound(
                                  correlationId, "job", Map.of("id", request.getJobId())));

              return GetReconcileJobResponse.newBuilder()
                  .setJobId(job.jobId)
                  .setConnectorId(job.connectorId)
                  .setState(toProtoState(job.state))
                  .setMessage(job.message == null ? "" : job.message)
                  .setStartedAt(Timestamps.fromMillis(job.startedAtMs))
                  .setFinishedAt(
                      job.finishedAtMs == 0
                          ? Timestamps.fromMillis(0)
                          : Timestamps.fromMillis(job.finishedAtMs))
                  .setTablesScanned(job.tablesScanned)
                  .setTablesChanged(job.tablesChanged)
                  .setErrors(job.errors)
                  .build();
            }),
        correlationId());
  }

  private static JobState toProtoState(String state) {
    if (state == null) {
      return JobState.JS_UNSPECIFIED;
    }

    return switch (state) {
      case "JS_QUEUED" -> JobState.JS_QUEUED;
      case "JS_RUNNING" -> JobState.JS_RUNNING;
      case "JS_SUCCEEDED" -> JobState.JS_SUCCEEDED;
      case "JS_FAILED" -> JobState.JS_FAILED;
      case "JS_CANCELLED" -> JobState.JS_CANCELLED;
      default -> JobState.JS_UNSPECIFIED;
    };
  }

  private static byte[] canonicalFingerprint(ConnectorSpec s) {
    return new Canonicalizer()
        .scalar("name", s.getDisplayName())
        .scalar("policy", s.getPolicy())
        .map("opt", s.getOptionsMap())
        .scalar("targetCat", s.getTargetCatalogDisplayName())
        .bytes();
  }
}
