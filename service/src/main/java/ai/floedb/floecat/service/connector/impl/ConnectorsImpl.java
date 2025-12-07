package ai.floedb.floecat.service.connector.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorSpec;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.Connectors;
import ai.floedb.floecat.connector.rpc.CreateConnectorRequest;
import ai.floedb.floecat.connector.rpc.CreateConnectorResponse;
import ai.floedb.floecat.connector.rpc.DeleteConnectorRequest;
import ai.floedb.floecat.connector.rpc.DeleteConnectorResponse;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.GetConnectorRequest;
import ai.floedb.floecat.connector.rpc.GetConnectorResponse;
import ai.floedb.floecat.connector.rpc.GetReconcileJobRequest;
import ai.floedb.floecat.connector.rpc.GetReconcileJobResponse;
import ai.floedb.floecat.connector.rpc.JobState;
import ai.floedb.floecat.connector.rpc.ListConnectorsRequest;
import ai.floedb.floecat.connector.rpc.ListConnectorsResponse;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.ReconcilePolicy;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.rpc.SyncCaptureRequest;
import ai.floedb.floecat.connector.rpc.SyncCaptureResponse;
import ai.floedb.floecat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.floecat.connector.rpc.TriggerReconcileResponse;
import ai.floedb.floecat.connector.rpc.UpdateConnectorRequest;
import ai.floedb.floecat.connector.rpc.UpdateConnectorResponse;
import ai.floedb.floecat.connector.rpc.ValidateConnectorRequest;
import ai.floedb.floecat.connector.rpc.ValidateConnectorResponse;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@GrpcService
public class ConnectorsImpl extends BaseServiceImpl implements Connectors {
  @Inject ConnectorRepository connectorRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;

  private static final Set<String> CONNECTOR_MUTABLE_PATHS =
      Set.of(
          "display_name",
          "description",
          "kind",
          "uri",
          "properties",
          "source",
          "source.namespace",
          "source.table",
          "source.columns",
          "destination",
          "destination.catalog_id",
          "destination.catalog_display_name",
          "destination.namespace",
          "destination.namespace_id",
          "destination.table_display_name",
          "destination.table_id",
          "auth",
          "auth.scheme",
          "auth.secret_ref",
          "auth.header_hints",
          "auth.properties",
          "policy",
          "policy.interval",
          "policy.enabled",
          "policy.max_parallel",
          "policy.not_before");

  private static final Logger LOG = Logger.getLogger(Connectors.class);

  @Override
  public Uni<ListConnectorsResponse> listConnectors(ListConnectorsRequest request) {
    var L = LogHelper.start(LOG, "ListConnectors");

    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "connector.manage");

                  var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
                  var next = new StringBuilder();

                  var connectors =
                      connectorRepo.list(
                          principalContext.getAccountId(),
                          Math.max(1, pageIn.limit),
                          pageIn.token,
                          next);

                  var page =
                      MutationOps.pageOut(
                          next.toString(), connectorRepo.count(principalContext.getAccountId()));

                  return ListConnectorsResponse.newBuilder()
                      .addAllConnectors(connectors)
                      .setPage(page)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetConnectorResponse> getConnector(GetConnectorRequest request) {
    var L = LogHelper.start(LOG, "GetConnector");

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
                                      correlationId,
                                      "connector",
                                      Map.of("id", connectorId.getId())));

                  return GetConnectorResponse.newBuilder().setConnector(connector).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CreateConnectorResponse> createConnector(CreateConnectorRequest request) {
    var L = LogHelper.start(LOG, "CreateConnector");

    return mapFailures(
            runWithRetry(
                () -> {
                  var pc = principalProvider.get();
                  var accountId = pc.getAccountId();
                  var corr = pc.getCorrelationId();

                  authz.require(pc, "connector.manage");

                  var tsNow = nowTs();
                  var fp = canonicalFingerprint(request.getSpec());
                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var connUuid =
                      deterministicUuid(
                          accountId,
                          "connector",
                          Base64.getUrlEncoder().withoutPadding().encodeToString(fp));

                  var connectorId =
                      ResourceId.newBuilder()
                          .setAccountId(accountId)
                          .setId(connUuid)
                          .setKind(ResourceKind.RK_CONNECTOR)
                          .build();

                  var spec = request.getSpec();
                  var display = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var uri = mustNonEmpty(spec.getUri(), "uri", corr);

                  if (!spec.hasDestination()
                      || (!spec.getDestination().hasCatalogId()
                          && spec.getDestination().getCatalogDisplayName().isBlank())) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        "connector.missing_destination_catalog",
                        Map.of("id", "destination.catalog_id|catalog_display_name"));
                  }

                  var dest = spec.getDestination();
                  var destB = dest.toBuilder();

                  if (dest.hasCatalogDisplayName() && !dest.hasCatalogId()) {
                    final String dName = dest.getCatalogDisplayName().trim();
                    catalogRepo
                        .getByName(accountId, dName)
                        .ifPresentOrElse(
                            cat -> {
                              destB.setCatalogId(cat.getResourceId());
                              destB.clearCatalogDisplayName();
                            },
                            () -> {
                              throw GrpcErrors.notFound(
                                  corr,
                                  "connector.destination_catalog_not_found",
                                  Map.of("display_name", dName));
                            });
                  }

                  if (dest.hasCatalogId() && dest.hasCatalogDisplayName()) {
                    var byName =
                        catalogRepo.getByName(accountId, dest.getCatalogDisplayName().trim());
                    if (byName.isEmpty()
                        || !byName
                            .get()
                            .getResourceId()
                            .getId()
                            .equals(dest.getCatalogId().getId())) {
                      throw GrpcErrors.invalidArgument(
                          corr,
                          "connector.destination_catalog_mismatch",
                          Map.of(
                              "catalog_id",
                              dest.getCatalogId().getId(),
                              "catalog_display_name",
                              dest.getCatalogDisplayName()));
                    }
                    destB.clearCatalogDisplayName();
                  }

                  if (!spec.hasSource()
                      || !spec.getSource().hasNamespace()
                      || spec.getSource().getNamespace().getSegmentsCount() == 0) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        "connector.missing_source_namespace",
                        Map.of("field", "source.namespace"));
                  }

                  if (destB.hasCatalogId() && dest.hasNamespace() && !dest.hasNamespaceId()) {
                    NamespacePath dNs = dest.getNamespace();
                    namespaceRepo
                        .getByPath(accountId, destB.getCatalogId().getId(), dNs.getSegmentsList())
                        .ifPresent(
                            ns -> {
                              destB.setNamespaceId(ns.getResourceId());
                              destB.clearNamespace();
                            });
                  }

                  if (destB.hasCatalogId()
                      && destB.hasNamespaceId()
                      && dest.hasTableDisplayName()
                      && !dest.hasTableId()) {
                    String dTbl = dest.getTableDisplayName().trim();
                    var tblOpt =
                        tableRepo.getByName(
                            accountId,
                            destB.getCatalogId().getId(),
                            destB.getNamespaceId().getId(),
                            dTbl);

                    if (tblOpt.isPresent()) {
                      destB.setTableId(tblOpt.get().getResourceId());
                      destB.clearTableDisplayName();
                    } else {
                      destB.setTableDisplayName(dTbl);
                    }
                  }

                  var builder =
                      Connector.newBuilder()
                          .setResourceId(connectorId)
                          .setDisplayName(display)
                          .setKind(spec.getKind())
                          .setUri(uri)
                          .putAllProperties(spec.getPropertiesMap())
                          .setAuth(spec.getAuth())
                          .setPolicy(spec.getPolicy())
                          .setCreatedAt(tsNow)
                          .setUpdatedAt(tsNow)
                          .setState(ConnectorState.CS_ACTIVE);

                  if (spec.hasDescription()) builder.setDescription(spec.getDescription());
                  if (spec.hasSource()) builder.setSource(spec.getSource());
                  builder.setDestination(destB.build());

                  var connector = builder.build();

                  if (idempotencyKey == null) {
                    var existing = connectorRepo.getById(connectorId);
                    if (existing.isPresent()) {
                      var meta = connectorRepo.metaFor(existing.get().getResourceId());
                      return CreateConnectorResponse.newBuilder()
                          .setConnector(existing.get())
                          .setMeta(meta)
                          .build();
                    }
                    connectorRepo.create(connector);
                    var meta = connectorRepo.metaFor(connectorId);
                    return CreateConnectorResponse.newBuilder()
                        .setConnector(connector)
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      MutationOps.createProto(
                          accountId,
                          "CreateConnector",
                          idempotencyKey,
                          () -> fp,
                          () -> {
                            connectorRepo.create(connector);
                            return new IdempotencyGuard.CreateResult<>(connector, connectorId);
                          },
                          c -> connectorRepo.metaFor(c.getResourceId()),
                          idempotencyStore,
                          tsNow,
                          idempotencyTtlSeconds(),
                          this::correlationId,
                          Connector::parseFrom,
                          rec -> connectorRepo.getById(rec.getResourceId()).isPresent());

                  return CreateConnectorResponse.newBuilder()
                      .setConnector(result.body)
                      .setMeta(result.meta)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<UpdateConnectorResponse> updateConnector(UpdateConnectorRequest request) {
    var L = LogHelper.start(LOG, "UpdateConnector");

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

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
                  }

                  var desired =
                      applyConnectorSpecPatch(
                              current, request.getSpec(), request.getUpdateMask(), corr)
                          .toBuilder()
                          .setUpdatedAt(nowTs())
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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<DeleteConnectorResponse> deleteConnector(DeleteConnectorRequest request) {
    var L = LogHelper.start(LOG, "DeleteConnector");

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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<ValidateConnectorResponse> validateConnector(ValidateConnectorRequest request) {
    var L = LogHelper.start(LOG, "ValidateConnector");

    return mapFailures(
            run(
                () -> {
                  var p = principalProvider.get();
                  var corr = p.getCorrelationId();

                  authz.require(p, "connector.manage");

                  var spec = request.getSpec();

                  var kind =
                      switch (spec.getKind()) {
                        case CK_ICEBERG -> Kind.ICEBERG;
                        case CK_DELTA -> Kind.DELTA;
                        case CK_GLUE -> Kind.GLUE;
                        case CK_UNITY -> Kind.UNITY;
                        default ->
                            throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "kind"));
                      };

                  var auth =
                      new ConnectorConfig.Auth(
                          spec.getAuth().getScheme(),
                          spec.getAuth().getPropertiesMap(),
                          spec.getAuth().getHeaderHintsMap(),
                          spec.getAuth().getSecretRef());

                  var cfg =
                      new ConnectorConfig(
                          kind,
                          spec.getDisplayName() != null ? spec.getDisplayName() : "",
                          mustNonEmpty(spec.getUri(), "uri", corr),
                          spec.getPropertiesMap(),
                          auth);

                  try (var connector = ConnectorFactory.create(cfg)) {
                    var namespaces = connector.listNamespaces();

                    if (spec.hasSource()) {
                      var src = spec.getSource();
                      var ns =
                          (src.hasNamespace()
                              ? String.join(".", src.getNamespace().getSegmentsList())
                              : null);
                      var tbl = (src.getTable().isBlank() ? null : src.getTable());
                      if (ns != null && tbl != null) {
                        try {
                          connector.describe(ns, tbl);
                        } catch (Exception ignored) {
                        }
                      }
                    }

                    return ValidateConnectorResponse.newBuilder()
                        .setOk(true)
                        .setSummary(
                            namespaces.isEmpty()
                                ? "OK: no namespaces"
                                : "OK: namespaces=" + namespaces.size())
                        .build();
                  } catch (Exception e) {
                    return ValidateConnectorResponse.newBuilder()
                        .setOk(false)
                        .setSummary("Validation failed: " + e.getMessage())
                        .build();
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<SyncCaptureResponse> syncCapture(SyncCaptureRequest request) {
    var L = LogHelper.start(LOG, "SyncCapture");

    return mapFailures(
            run(
                () -> {
                  var pc = principalProvider.get();
                  authz.require(pc, "connector.manage");
                  var corr = pc.getCorrelationId();

                  var connectorId = request.getConnectorId();
                  ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", corr);

                  List<List<String>> nsPaths =
                      request.getDestinationNamespacePathsList().stream()
                          .map(NamespacePath::getSegmentsList)
                          .map(List::copyOf)
                          .toList();
                  var scope =
                      ReconcileScope.of(
                          nsPaths,
                          request.getDestinationTableDisplayName(),
                          request.getDestinationTableColumnsList());

                  CaptureMode mode =
                      request.getIncludeStatistics()
                          ? CaptureMode.METADATA_AND_STATS
                          : CaptureMode.METADATA_ONLY;

                  var result = reconcilerService.reconcile(connectorId, false, scope, mode);
                  if (!result.ok()) {
                    if (result.error != null) {
                      throw new RuntimeException("sync capture failed", result.error);
                    }
                    throw new IllegalStateException("sync capture failed");
                  }
                  return SyncCaptureResponse.newBuilder()
                      .setTablesScanned(result.scanned)
                      .setTablesChanged(result.changed)
                      .setErrors(result.errors)
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<TriggerReconcileResponse> triggerReconcile(TriggerReconcileRequest request) {
    var L = LogHelper.start(LOG, "TriggerReconcile");

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
                          connectorId.getAccountId(),
                          connectorId.getId(),
                          request.getFullRescan(),
                          scopeFromRequest(request));

                  return TriggerReconcileResponse.newBuilder().setJobId(jobId).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetReconcileJobResponse> getReconcileJob(GetReconcileJobRequest request) {
    var L = LogHelper.start(LOG, "GetReconcileJob");

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
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private static ReconcileScope scopeFromRequest(TriggerReconcileRequest request) {
    if (request == null) {
      return ReconcileScope.empty();
    }
    var namespaces =
        request.getDestinationNamespacePathsList().stream()
            .map(NamespacePath::getSegmentsList)
            .map(List::copyOf)
            .toList();
    return ReconcileScope.of(
        namespaces,
        request.getDestinationTableDisplayName(),
        request.getDestinationTableColumnsList());
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

  static List<List<String>> toPaths(List<NamespacePath> in) {
    if (in == null || in.isEmpty()) {
      return List.of();
    }

    return in.stream()
        .map(
            np -> {
              var segs = np.getSegmentsList();
              var cleaned =
                  segs.stream()
                      .map(s -> s == null ? "" : s.trim())
                      .filter(s -> !s.isEmpty())
                      .toList();
              return List.copyOf(cleaned);
            })
        .toList();
  }

  private Connector applyConnectorSpecPatch(
      Connector current, ConnectorSpec spec, FieldMask mask, String corr) {

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(corr, "update_mask.required", Map.of());
    }
    for (var p : paths) {
      if (!CONNECTOR_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(corr, "update_mask.path.invalid", Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName() || normalizeName(spec.getDisplayName()).isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "display_name.cannot_clear", Map.of());
      }
      b.setDisplayName(normalizeName(spec.getDisplayName()));
    }

    if (maskTargets(mask, "description")) {
      if (spec.hasDescription()) {
        b.setDescription(spec.getDescription());
      } else {
        b.clearDescription();
      }
    }

    if (maskTargets(mask, "kind")) {
      if (spec.getKind() == ConnectorKind.CK_UNSPECIFIED) {
        throw GrpcErrors.invalidArgument(corr, "field", Map.of("field", "kind"));
      }
      b.setKind(spec.getKind());
    }

    if (maskTargets(mask, "uri")) {
      if (!spec.hasUri() || spec.getUri().isBlank()) {
        throw GrpcErrors.invalidArgument(corr, "uri.cannot_clear", Map.of());
      }
      b.setUri(spec.getUri());
    }

    if (maskTargets(mask, "properties")) {
      b.clearProperties().putAllProperties(spec.getPropertiesMap());
    }

    var curSrc = current.hasSource() ? current.getSource() : SourceSelector.getDefaultInstance();
    var inSrc = spec.hasSource() ? spec.getSource() : SourceSelector.getDefaultInstance();

    if (maskTargets(mask, "source")) {
      if (!(inSrc.hasNamespace() && inSrc.getNamespace().getSegmentsCount() > 0)) {
        throw GrpcErrors.invalidArgument(
            corr, "connector.missing_source_namespace", Map.of("field", "source.namespace"));
      }
      b.setSource(inSrc);
    } else if (maskTargetsUnder(mask, "source")) {
      var sb = curSrc.toBuilder();

      if (maskTargets(mask, "source.namespace")) {
        if (!(inSrc.hasNamespace() && inSrc.getNamespace().getSegmentsCount() > 0)) {
          throw GrpcErrors.invalidArgument(
              corr, "connector.missing_source_namespace", Map.of("field", "source.namespace"));
        }
        sb.setNamespace(inSrc.getNamespace());
      }

      if (maskTargets(mask, "source.table")) {
        if (inSrc.hasTable()) {
          sb.setTable(inSrc.getTable());
        } else {
          sb.clearTable();
        }
      }

      if (maskTargets(mask, "source.columns")) {
        sb.clearColumns().addAllColumns(inSrc.getColumnsList());
      }

      b.setSource(sb.build());
    }

    var curDst =
        current.hasDestination()
            ? current.getDestination()
            : DestinationTarget.getDefaultInstance();
    var inDst =
        spec.hasDestination() ? spec.getDestination() : DestinationTarget.getDefaultInstance();

    if (maskTargets(mask, "destination")) {
      boolean hasCatalogRef =
          inDst.hasCatalogId()
              || (inDst.hasCatalogDisplayName() && !inDst.getCatalogDisplayName().isBlank());
      boolean hasNamespaceRef =
          inDst.hasNamespaceId()
              || (inDst.hasNamespace() && inDst.getNamespace().getSegmentsCount() > 0);

      if (!hasCatalogRef) {
        throw GrpcErrors.invalidArgument(
            corr, "connector.missing_destination_catalog", Map.of("field", "destination.catalog"));
      }
      if (!hasNamespaceRef) {
        throw GrpcErrors.invalidArgument(corr, "field", Map.of("field", "destination.namespace"));
      }
      if (inDst.hasCatalogId()) {
        ensureKind(
            inDst.getCatalogId(), ResourceKind.RK_CATALOG, "spec.destination.catalog_id", corr);
      }
      if (inDst.hasNamespaceId()) {
        ensureKind(
            inDst.getNamespaceId(),
            ResourceKind.RK_NAMESPACE,
            "spec.destination.namespace_id",
            corr);
      }
      if (inDst.hasTableId()) {
        ensureKind(inDst.getTableId(), ResourceKind.RK_TABLE, "spec.destination.table_id", corr);
      }
      b.setDestination(inDst);

    } else if (maskTargetsUnder(mask, "destination")) {
      var db = curDst.toBuilder();

      boolean touchingCatalogRef =
          maskTargets(mask, "destination.catalog_id")
              || maskTargets(mask, "destination.catalog_display_name");
      if (touchingCatalogRef) {
        boolean incomingHasCatalog =
            inDst.hasCatalogId()
                || (inDst.hasCatalogDisplayName() && !inDst.getCatalogDisplayName().isBlank());
        if (!incomingHasCatalog) {
          throw GrpcErrors.invalidArgument(
              corr,
              "connector.missing_destination_catalog",
              Map.of("field", "destination.catalog"));
        }
        if (inDst.hasCatalogId()) {
          ensureKind(
              inDst.getCatalogId(), ResourceKind.RK_CATALOG, "spec.destination.catalog_id", corr);
          db.setCatalogId(inDst.getCatalogId());
        } else {
          db.setCatalogDisplayName(inDst.getCatalogDisplayName());
        }
      }

      boolean touchingNamespaceRef =
          maskTargets(mask, "destination.namespace_id")
              || maskTargets(mask, "destination.namespace");
      if (touchingNamespaceRef) {
        boolean incomingHasNs =
            inDst.hasNamespaceId()
                || (inDst.hasNamespace() && inDst.getNamespace().getSegmentsCount() > 0);
        if (!incomingHasNs) {
          throw GrpcErrors.invalidArgument(corr, "field", Map.of("field", "destination.namespace"));
        }
        if (inDst.hasNamespaceId()) {
          ensureKind(
              inDst.getNamespaceId(),
              ResourceKind.RK_NAMESPACE,
              "spec.destination.namespace_id",
              corr);
          db.setNamespaceId(inDst.getNamespaceId());
        } else {
          db.setNamespace(inDst.getNamespace());
        }
      }

      if (maskTargets(mask, "destination.table_id")) {
        if (inDst.hasTableId()) {
          ensureKind(inDst.getTableId(), ResourceKind.RK_TABLE, "spec.destination.table_id", corr);
          db.setTableId(inDst.getTableId());
        } else {
          db.clearTableId();
        }
      }
      if (maskTargets(mask, "destination.table_display_name")) {
        if (inDst.hasTableDisplayName()) {
          db.setTableDisplayName(inDst.getTableDisplayName());
        } else {
          db.clearTableDisplayName();
        }
      }

      b.setDestination(db.build());
    }

    var curAuth = current.hasAuth() ? current.getAuth() : AuthConfig.getDefaultInstance();
    var inAuth = spec.hasAuth() ? spec.getAuth() : AuthConfig.getDefaultInstance();

    if (maskTargets(mask, "auth")) {
      b.setAuth(inAuth);
    } else if (maskTargetsUnder(mask, "auth")) {
      var ab = curAuth.toBuilder();

      if (maskTargets(mask, "auth.scheme")) {
        if (inAuth.getScheme() != null && !inAuth.getScheme().isBlank()) {
          ab.setScheme(inAuth.getScheme());
        } else {
          ab.clearScheme();
        }
      }
      if (maskTargets(mask, "auth.secret_ref")) {
        if (inAuth.getSecretRef() != null && !inAuth.getSecretRef().isBlank()) {
          ab.setSecretRef(inAuth.getSecretRef());
        } else {
          ab.clearSecretRef();
        }
      }
      if (maskTargets(mask, "auth.header_hints")) {
        ab.clearHeaderHints().putAllHeaderHints(inAuth.getHeaderHintsMap());
      }
      if (maskTargets(mask, "auth.properties")) {
        ab.clearProperties().putAllProperties(inAuth.getPropertiesMap());
      }

      b.setAuth(ab.build());
    }

    var curPol = current.hasPolicy() ? current.getPolicy() : ReconcilePolicy.getDefaultInstance();
    var inPol = spec.hasPolicy() ? spec.getPolicy() : ReconcilePolicy.getDefaultInstance();

    if (maskTargets(mask, "policy")) {
      b.setPolicy(inPol);
    } else if (maskTargetsUnder(mask, "policy")) {
      var pb = curPol.toBuilder();
      if (maskTargets(mask, "policy.interval")) {
        if (inPol.hasInterval()) {
          pb.setInterval(inPol.getInterval());
        } else {
          pb.clearInterval();
        }
      }
      if (maskTargets(mask, "policy.enabled")) {
        pb.setEnabled(inPol.getEnabled());
      }
      if (maskTargets(mask, "policy.max_parallel")) {
        pb.setMaxParallel(inPol.getMaxParallel());
      }
      if (maskTargets(mask, "policy.not_before")) {
        if (inPol.hasNotBefore()) {
          pb.setNotBefore(inPol.getNotBefore());
        } else {
          pb.clearNotBefore();
        }
      }
      b.setPolicy(pb.build());
    }

    var out = b.build();

    boolean touchedDest = maskTargets(mask, "destination") || maskTargetsUnder(mask, "destination");
    boolean touchedSrc = maskTargets(mask, "source") || maskTargetsUnder(mask, "source");

    if (touchedDest) {
      var d = out.getDestination();
      boolean hasCatalogRef =
          d.hasCatalogId() || (d.hasCatalogDisplayName() && !d.getCatalogDisplayName().isBlank());
      if (!hasCatalogRef) {
        throw GrpcErrors.invalidArgument(
            corr, "connector.missing_destination_catalog", Map.of("field", "destination.catalog"));
      }
      boolean hasNamespaceRef =
          d.hasNamespaceId() || (d.hasNamespace() && d.getNamespace().getSegmentsCount() > 0);
      if (!hasNamespaceRef) {
        throw GrpcErrors.invalidArgument(corr, "field", Map.of("field", "destination.namespace"));
      }
    }

    if (touchedSrc && out.hasSource()) {
      var s = out.getSource();
      boolean hasNs = s.hasNamespace() && s.getNamespace().getSegmentsCount() > 0;
      if (!hasNs) {
        throw GrpcErrors.invalidArgument(
            corr, "connector.missing_source_namespace", Map.of("field", "source.namespace"));
      }
    }

    return out;
  }

  private static byte[] canonicalFingerprint(ConnectorSpec s) {
    return new Canonicalizer().scalar("name", normalizeName(s.getDisplayName())).bytes();
  }
}
