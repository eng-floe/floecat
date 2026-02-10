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

package ai.floedb.floecat.service.connector.impl;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.AuthCredentials;
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
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.Canonicalizer;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  @Inject CredentialResolver credentialResolver;

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
          "auth.credentials",
          "auth.header_hints",
          "auth.properties",
          "policy",
          "policy.interval",
          "policy.enabled",
          "policy.max_parallel",
          "policy.not_before");

  private static final Logger LOG = Logger.getLogger(Connectors.class);
  private static final String REDACTED = "****";
  private static final List<String> SENSITIVE_TOKENS =
      List.of(
          "secret",
          "token",
          "password",
          "key",
          "pem",
          "private",
          "client_id",
          "clientid",
          "access_key",
          "session",
          "refresh",
          "authorization",
          "bearer",
          "jwt",
          "assertion",
          "external_id");

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

                  var masked = connectors.stream().map(ConnectorsImpl::maskConnector).toList();
                  return ListConnectorsResponse.newBuilder()
                      .addAllConnectors(masked)
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
                                      GeneratedErrorMessages.MessageKey.CONNECTOR,
                                      Map.of("id", connectorId.getId())));

                  return GetConnectorResponse.newBuilder()
                      .setConnector(maskConnector(connector))
                      .build();
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
                  var spec = request.getSpec();
                  var fp = canonicalFingerprint(spec);
                  var explicitKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  var idempotencyKey = explicitKey.isEmpty() ? null : explicitKey;

                  var connectorId = randomResourceId(accountId, ResourceKind.RK_CONNECTOR);

                  var display = mustNonEmpty(spec.getDisplayName(), "display_name", corr);
                  var uri = mustNonEmpty(spec.getUri(), "uri", corr);

                  if (!spec.hasDestination()
                      || (!spec.getDestination().hasCatalogId()
                          && spec.getDestination().getCatalogDisplayName().isBlank())) {
                    throw GrpcErrors.invalidArgument(
                        corr,
                        GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_DESTINATION_CATALOG,
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
                                  GeneratedErrorMessages.MessageKey
                                      .CONNECTOR_DESTINATION_CATALOG_NOT_FOUND,
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
                          GeneratedErrorMessages.MessageKey.CONNECTOR_DESTINATION_CATALOG_MISMATCH,
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
                        GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_SOURCE_NAMESPACE,
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

                  if (destB.hasCatalogId()) {
                    ensureKind(
                        destB.getCatalogId(),
                        ResourceKind.RK_CATALOG,
                        "spec.destination.catalog_id",
                        corr);
                  }
                  if (destB.hasNamespaceId()) {
                    ensureKind(
                        destB.getNamespaceId(),
                        ResourceKind.RK_NAMESPACE,
                        "spec.destination.namespace_id",
                        corr);
                  }
                  if (destB.hasTableId()) {
                    ensureKind(
                        destB.getTableId(),
                        ResourceKind.RK_TABLE,
                        "spec.destination.table_id",
                        corr);
                  }

                  var builder =
                      Connector.newBuilder()
                          .setResourceId(connectorId)
                          .setDisplayName(display)
                          .setKind(spec.getKind())
                          .setUri(uri)
                          .putAllProperties(spec.getPropertiesMap())
                          .setPolicy(spec.getPolicy())
                          .setCreatedAt(tsNow)
                          .setUpdatedAt(tsNow)
                          .setState(ConnectorState.CS_ACTIVE);

                  if (spec.hasDescription()) builder.setDescription(spec.getDescription());
                  if (spec.hasSource()) builder.setSource(spec.getSource());
                  builder.setDestination(destB.build());

                  if (idempotencyKey == null) {
                    var existing = connectorRepo.getByName(accountId, display);
                    if (existing.isPresent()) {
                      throw GrpcErrors.conflict(
                          corr,
                          GeneratedErrorMessages.MessageKey.CONNECTOR_ALREADY_EXISTS,
                          Map.of("display_name", display));
                    }
                    boolean storedCredentials = hasAuthCredentials(spec.getAuth());
                    String secretId = connectorId.getId();
                    AuthConfig storedAuth =
                        storeAuthCredentials(spec.getAuth(), accountId, connectorId.getId());
                    ensureNoStoredCredentials(storedAuth);
                    var connector = builder.setAuth(storedAuth).build();
                    try {
                      connectorRepo.create(connector);
                    } catch (BaseResourceRepository.NameConflictException nce) {
                      if (storedCredentials) {
                        credentialResolver.delete(accountId, secretId);
                      }
                      throw GrpcErrors.conflict(
                          corr,
                          GeneratedErrorMessages.MessageKey.CONNECTOR_ALREADY_EXISTS,
                          Map.of("display_name", display));
                    }
                    var meta = connectorRepo.metaFor(connectorId);
                    return CreateConnectorResponse.newBuilder()
                        .setConnector(maskConnector(connector))
                        .setMeta(meta)
                        .build();
                  }

                  var result =
                      runIdempotentCreate(
                          () ->
                              MutationOps.createProto(
                                  accountId,
                                  "CreateConnector",
                                  idempotencyKey,
                                  () -> fp,
                                  () -> {
                                    AuthConfig storedAuth =
                                        storeAuthCredentials(
                                            spec.getAuth(), accountId, connectorId.getId());
                                    boolean storedCredentials = hasAuthCredentials(spec.getAuth());
                                    String secretId = connectorId.getId();
                                    ensureNoStoredCredentials(storedAuth);
                                    var connector = builder.setAuth(storedAuth).build();
                                    try {
                                      connectorRepo.create(connector);
                                    } catch (BaseResourceRepository.NameConflictException nce) {
                                      if (storedCredentials) {
                                        credentialResolver.delete(accountId, secretId);
                                      }
                                      var existingOpt = connectorRepo.getByName(accountId, display);
                                      if (existingOpt.isPresent()) {
                                        var existingSpec = specFromConnector(existingOpt.get());
                                        if (hasAuthCredentials(spec.getAuth())) {
                                          var existingAuth = existingSpec.getAuth();
                                          String existingSecretId =
                                              existingOpt.get().getResourceId().getId();
                                          var resolved =
                                              credentialResolver.resolve(
                                                  accountId, existingSecretId);
                                          if (resolved.isPresent()) {
                                            existingSpec =
                                                existingSpec.toBuilder()
                                                    .setAuth(
                                                        existingAuth.toBuilder()
                                                            .setCredentials(resolved.get())
                                                            .build())
                                                    .build();
                                          }
                                        }
                                        if (Arrays.equals(fp, canonicalFingerprint(existingSpec))) {
                                          return new IdempotencyGuard.CreateResult<>(
                                              existingOpt.get(), existingOpt.get().getResourceId());
                                        }
                                      }
                                      throw GrpcErrors.conflict(
                                          corr,
                                          GeneratedErrorMessages.MessageKey
                                              .CONNECTOR_ALREADY_EXISTS,
                                          Map.of("display_name", display));
                                    }
                                    return new IdempotencyGuard.CreateResult<>(
                                        connector, connectorId);
                                  },
                                  c -> connectorRepo.metaFor(c.getResourceId()),
                                  idempotencyStore,
                                  tsNow,
                                  idempotencyTtlSeconds(),
                                  this::correlationId,
                                  Connector::parseFrom));

                  return CreateConnectorResponse.newBuilder()
                      .setConnector(maskConnector(result.body))
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

                  if (!request.hasUpdateMask() || request.getUpdateMask().getPathsCount() == 0) {
                    throw GrpcErrors.invalidArgument(
                        corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED, Map.of());
                  }

                  var meta = connectorRepo.metaFor(connectorId);
                  MutationOps.BaseServiceChecks.enforcePreconditions(
                      corr, meta, request.getPrecondition());

                  var current =
                      connectorRepo
                          .getById(connectorId)
                          .orElseThrow(
                              () ->
                                  GrpcErrors.notFound(
                                      corr,
                                      GeneratedErrorMessages.MessageKey.CONNECTOR,
                                      Map.of("id", connectorId.getId())));

                  FieldMask normalizedMask = normalizeMask(request.getUpdateMask());
                  var desired =
                      applyConnectorSpecPatch(current, request.getSpec(), normalizedMask, corr)
                          .toBuilder()
                          .setUpdatedAt(nowTs())
                          .build();
                  String accountId = pc.getAccountId();
                  String secretId = connectorId.getId();
                  boolean authTouched =
                      maskTargets(normalizedMask, "auth")
                          || maskTargets(normalizedMask, "auth.credentials");
                  boolean incomingHasCredentials = hasAuthCredentials(desired.getAuth());
                  Optional<AuthCredentials> priorCredentials = Optional.empty();
                  AuthConfig storedAuth = desired.getAuth();
                  if (incomingHasCredentials) {
                    priorCredentials = credentialResolver.resolve(accountId, secretId);
                    storedAuth =
                        storeAuthCredentials(desired.getAuth(), accountId, connectorId.getId());
                    if (storedAuth != desired.getAuth()) {
                      desired = desired.toBuilder().setAuth(storedAuth).build();
                    }
                  }
                  ensureNoStoredCredentials(desired.getAuth());
                  boolean shouldDeleteSecret = authTouched && !incomingHasCredentials;

                  if (desired.equals(current)) {
                    if (shouldDeleteSecret) {
                      credentialResolver.delete(accountId, secretId);
                    }
                    var metaNoop = connectorRepo.metaFor(connectorId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && metaNoop.getPointerVersion() != meta.getPointerVersion()) {
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(metaNoop.getPointerVersion())));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, metaNoop, request.getPrecondition());
                    return UpdateConnectorResponse.newBuilder()
                        .setConnector(maskConnector(current))
                        .setMeta(metaNoop)
                        .build();
                  }

                  try {
                    boolean ok = connectorRepo.update(desired, meta.getPointerVersion());
                    if (!ok) {
                      var nowMeta = connectorRepo.metaForSafe(connectorId);
                      if (incomingHasCredentials) {
                        restoreCredentials(accountId, secretId, priorCredentials);
                      }
                      throw GrpcErrors.preconditionFailed(
                          corr,
                          GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                          Map.of(
                              "expected", Long.toString(meta.getPointerVersion()),
                              "actual", Long.toString(nowMeta.getPointerVersion())));
                    }
                  } catch (BaseResourceRepository.NameConflictException nce) {
                    if (incomingHasCredentials) {
                      restoreCredentials(accountId, secretId, priorCredentials);
                    }
                    throw GrpcErrors.conflict(
                        corr,
                        GeneratedErrorMessages.MessageKey.CONNECTOR_ALREADY_EXISTS,
                        Map.of("display_name", desired.getDisplayName()));
                  } catch (BaseResourceRepository.PreconditionFailedException pfe) {
                    var nowMeta = connectorRepo.metaForSafe(connectorId);
                    if (incomingHasCredentials) {
                      restoreCredentials(accountId, secretId, priorCredentials);
                    }
                    throw GrpcErrors.preconditionFailed(
                        corr,
                        GeneratedErrorMessages.MessageKey.VERSION_MISMATCH,
                        Map.of(
                            "expected", Long.toString(meta.getPointerVersion()),
                            "actual", Long.toString(nowMeta.getPointerVersion())));
                  }

                  if (shouldDeleteSecret) {
                    credentialResolver.delete(accountId, secretId);
                  }

                  var outMeta = connectorRepo.metaForSafe(connectorId);
                  var outConnector = connectorRepo.getById(connectorId).orElse(desired);

                  return UpdateConnectorResponse.newBuilder()
                      .setConnector(maskConnector(outConnector))
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

                  MutationMeta meta;
                  try {
                    meta = connectorRepo.metaFor(connectorId);
                  } catch (BaseResourceRepository.NotFoundException missing) {
                    var safe = connectorRepo.metaForSafe(connectorId);
                    boolean callerCares = hasMeaningfulPrecondition(request.getPrecondition());
                    if (callerCares && safe.getPointerVersion() == 0L) {
                      throw GrpcErrors.notFound(
                          corr,
                          GeneratedErrorMessages.MessageKey.CONNECTOR,
                          Map.of("id", connectorId.getId()));
                    }
                    MutationOps.BaseServiceChecks.enforcePreconditions(
                        corr, safe, request.getPrecondition());
                    return DeleteConnectorResponse.newBuilder().setMeta(safe).build();
                  }

                  String secretId = connectorId.getId();

                  var out =
                      MutationOps.deleteWithPreconditions(
                          () -> meta,
                          request.getPrecondition(),
                          expected -> connectorRepo.deleteWithPrecondition(connectorId, expected),
                          () -> connectorRepo.metaForSafe(connectorId),
                          corr,
                          "connector",
                          Map.of("id", connectorId.getId()));

                  credentialResolver.delete(pc.getAccountId(), secretId);

                  return DeleteConnectorResponse.newBuilder().setMeta(out).build();
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

                  var auth = toConnectorAuth(spec.getAuth());

                  var cfg =
                      new ConnectorConfig(
                          kind,
                          spec.getDisplayName() != null ? spec.getDisplayName() : "",
                          mustNonEmpty(spec.getUri(), "uri", corr),
                          spec.getPropertiesMap(),
                          auth);

                  var resolved = resolveCredentials(cfg, spec.getAuth());
                  try (var connector = ConnectorFactory.create(resolved)) {
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
                  var connector = connectorRepo.getById(connectorId).orElse(null);

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
                                  correlationId,
                                  GeneratedErrorMessages.MessageKey.CONNECTOR,
                                  Map.of("id", connectorId.getId())));

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
                                      correlationId,
                                      GeneratedErrorMessages.MessageKey.JOB,
                                      Map.of("id", request.getJobId())));

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

  private static boolean hasAuthCredentials(AuthConfig auth) {
    if (auth == null || !auth.hasCredentials()) {
      return false;
    }
    return auth.getCredentials().getCredentialCase()
        != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET;
  }

  private void restoreCredentials(
      String accountId, String secretId, Optional<AuthCredentials> priorCredentials) {
    if (priorCredentials != null && priorCredentials.isPresent()) {
      credentialResolver.store(accountId, secretId, priorCredentials.get());
    } else {
      credentialResolver.delete(accountId, secretId);
    }
  }

  private AuthConfig storeAuthCredentials(AuthConfig auth, String accountId, String connectorId) {
    AuthConfig safeAuth = auth == null ? AuthConfig.getDefaultInstance() : auth;
    if (!hasAuthCredentials(safeAuth)) {
      return safeAuth;
    }
    credentialResolver.store(accountId, connectorId, safeAuth.getCredentials());
    // AuthCredentials are stored in the secrets manager; connector records must not persist them.
    return safeAuth.toBuilder().clearCredentials().build();
  }

  private static void ensureNoStoredCredentials(AuthConfig auth) {
    if (hasAuthCredentials(auth)) {
      throw new IllegalStateException("AuthCredentials must not be stored in connector records");
    }
  }

  private static Connector maskConnector(Connector connector) {
    if (connector == null || !connector.hasAuth()) {
      return connector;
    }
    AuthConfig masked = maskAuthConfig(connector.getAuth());
    return connector.toBuilder().setAuth(masked).build();
  }

  private static AuthConfig maskAuthConfig(AuthConfig auth) {
    if (auth == null) {
      return AuthConfig.getDefaultInstance();
    }
    var builder = auth.toBuilder();
    if (hasAuthCredentials(auth)) {
      builder.setCredentials(maskAuthCredentials(auth.getCredentials()));
    } else {
      builder.clearCredentials();
    }
    builder.clearProperties().putAllProperties(maskSensitiveMap(auth.getPropertiesMap()));
    builder.clearHeaderHints().putAllHeaderHints(maskSensitiveMap(auth.getHeaderHintsMap()));
    return builder.build();
  }

  private static AuthCredentials maskAuthCredentials(AuthCredentials credentials) {
    if (credentials == null) {
      return AuthCredentials.getDefaultInstance();
    }
    AuthCredentials.Builder builder = AuthCredentials.newBuilder();
    switch (credentials.getCredentialCase()) {
      case BEARER -> builder.setBearer(AuthCredentials.BearerToken.newBuilder().setToken(REDACTED));
      case CLIENT ->
          builder.setClient(
              AuthCredentials.ClientCredentials.newBuilder()
                  .setEndpoint(credentials.getClient().getEndpoint())
                  .setClientId(REDACTED)
                  .setClientSecret(REDACTED));
      case CLI ->
          builder.setCli(
              AuthCredentials.CliCredentials.newBuilder()
                  .setProvider(credentials.getCli().getProvider()));
      case RFC8693_TOKEN_EXCHANGE ->
          builder.setRfc8693TokenExchange(
              AuthCredentials.Rfc8693TokenExchange.newBuilder()
                  .setBase(maskTokenExchange(credentials.getRfc8693TokenExchange().getBase())));
      case AZURE_TOKEN_EXCHANGE ->
          builder.setAzureTokenExchange(
              AuthCredentials.AzureTokenExchange.newBuilder()
                  .setBase(maskTokenExchange(credentials.getAzureTokenExchange().getBase())));
      case GCP_TOKEN_EXCHANGE ->
          builder.setGcpTokenExchange(
              AuthCredentials.GcpTokenExchange.newBuilder()
                  .setBase(maskTokenExchange(credentials.getGcpTokenExchange().getBase()))
                  .setServiceAccountEmail(
                      credentials.getGcpTokenExchange().getServiceAccountEmail())
                  .setDelegatedUser(credentials.getGcpTokenExchange().getDelegatedUser())
                  .setServiceAccountPrivateKeyPem(REDACTED)
                  .setServiceAccountPrivateKeyId(REDACTED));
      case AWS ->
          builder.setAws(
              AuthCredentials.AwsCredentials.newBuilder()
                  .setAccessKeyId(REDACTED)
                  .setSecretAccessKey(REDACTED)
                  .setSessionToken(REDACTED));
      case AWS_WEB_IDENTITY ->
          builder.setAwsWebIdentity(
              AuthCredentials.AwsWebIdentity.newBuilder()
                  .setRoleArn(credentials.getAwsWebIdentity().getRoleArn())
                  .setRoleSessionName(credentials.getAwsWebIdentity().getRoleSessionName())
                  .setProviderId(credentials.getAwsWebIdentity().getProviderId())
                  .setDurationSeconds(credentials.getAwsWebIdentity().getDurationSeconds()));
      case AWS_ASSUME_ROLE ->
          builder.setAwsAssumeRole(
              AuthCredentials.AwsAssumeRole.newBuilder()
                  .setRoleArn(credentials.getAwsAssumeRole().getRoleArn())
                  .setRoleSessionName(credentials.getAwsAssumeRole().getRoleSessionName())
                  .setExternalId(REDACTED)
                  .setDurationSeconds(credentials.getAwsAssumeRole().getDurationSeconds()));
      case CREDENTIAL_NOT_SET -> {}
    }

    builder.putAllProperties(maskSensitiveMap(credentials.getPropertiesMap()));
    builder.putAllHeaders(maskSensitiveMap(credentials.getHeadersMap()));
    return builder.build();
  }

  private static AuthCredentials.TokenExchange maskTokenExchange(
      AuthCredentials.TokenExchange base) {
    if (base == null) {
      return AuthCredentials.TokenExchange.getDefaultInstance();
    }
    return AuthCredentials.TokenExchange.newBuilder()
        .setEndpoint(base.getEndpoint())
        .setSubjectTokenType(base.getSubjectTokenType())
        .setRequestedTokenType(base.getRequestedTokenType())
        .setAudience(base.getAudience())
        .setScope(base.getScope())
        .setClientId(REDACTED)
        .setClientSecret(REDACTED)
        .build();
  }

  private static Map<String, String> maskSensitiveMap(Map<String, String> input) {
    if (input == null || input.isEmpty()) {
      return Map.of();
    }
    var out = new java.util.HashMap<String, String>(input.size());
    for (var entry : input.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      out.put(key, isSensitiveKey(key) ? REDACTED : value);
    }
    return out;
  }

  private static boolean isSensitiveKey(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    String lower = key.toLowerCase(java.util.Locale.ROOT);
    for (String token : SENSITIVE_TOKENS) {
      if (lower.contains(token)) {
        return true;
      }
    }
    return false;
  }

  private ConnectorConfig resolveCredentials(ConnectorConfig base, AuthConfig auth) {
    if (hasAuthCredentials(auth)) {
      AuthResolutionContext context = AuthResolutionContexts.fromInboundContext();
      return CredentialResolverSupport.apply(base, auth.getCredentials(), context);
    }
    return base;
  }

  private static ConnectorConfig.Auth toConnectorAuth(AuthConfig auth) {
    return new ConnectorConfig.Auth(
        auth.getScheme(), auth.getPropertiesMap(), auth.getHeaderHintsMap());
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
    mask = normalizeMask(mask);

    var paths = normalizedMaskPaths(mask);
    if (paths.isEmpty()) {
      throw GrpcErrors.invalidArgument(
          corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_REQUIRED, Map.of());
    }
    for (var p : paths) {
      if (!CONNECTOR_MUTABLE_PATHS.contains(p)) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.UPDATE_MASK_PATH_INVALID, Map.of("path", p));
      }
    }

    var b = current.toBuilder();

    if (maskTargets(mask, "display_name")) {
      if (!spec.hasDisplayName() || normalizeName(spec.getDisplayName()).isBlank()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.DISPLAY_NAME_CANNOT_CLEAR, Map.of());
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
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.FIELD, Map.of("field", "kind"));
      }
      b.setKind(spec.getKind());
    }

    if (maskTargets(mask, "uri")) {
      if (!spec.hasUri() || spec.getUri().isBlank()) {
        throw GrpcErrors.invalidArgument(
            corr, GeneratedErrorMessages.MessageKey.URI_CANNOT_CLEAR, Map.of());
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
            corr,
            GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_SOURCE_NAMESPACE,
            Map.of("field", "source.namespace"));
      }
      b.setSource(inSrc);
    } else if (maskTargetsUnder(mask, "source")) {
      var sb = curSrc.toBuilder();

      if (maskTargets(mask, "source.namespace")) {
        if (!(inSrc.hasNamespace() && inSrc.getNamespace().getSegmentsCount() > 0)) {
          throw GrpcErrors.invalidArgument(
              corr,
              GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_SOURCE_NAMESPACE,
              Map.of("field", "source.namespace"));
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
            corr,
            GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_DESTINATION_CATALOG,
            Map.of("field", "destination.catalog"));
      }
      if (!hasNamespaceRef) {
        throw GrpcErrors.invalidArgument(
            corr,
            GeneratedErrorMessages.MessageKey.FIELD,
            Map.of("field", "destination.namespace"));
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
              GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_DESTINATION_CATALOG,
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
          throw GrpcErrors.invalidArgument(
              corr,
              GeneratedErrorMessages.MessageKey.FIELD,
              Map.of("field", "destination.namespace"));
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
      if (maskTargets(mask, "auth.credentials")) {
        if (hasAuthCredentials(inAuth)) {
          ab.setCredentials(inAuth.getCredentials());
        } else {
          ab.clearCredentials();
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
            corr,
            GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_DESTINATION_CATALOG,
            Map.of("field", "destination.catalog"));
      }
      boolean hasNamespaceRef =
          d.hasNamespaceId() || (d.hasNamespace() && d.getNamespace().getSegmentsCount() > 0);
      if (!hasNamespaceRef) {
        throw GrpcErrors.invalidArgument(
            corr,
            GeneratedErrorMessages.MessageKey.FIELD,
            Map.of("field", "destination.namespace"));
      }
    }

    if (touchedSrc && out.hasSource()) {
      var s = out.getSource();
      boolean hasNs = s.hasNamespace() && s.getNamespace().getSegmentsCount() > 0;
      if (!hasNs) {
        throw GrpcErrors.invalidArgument(
            corr,
            GeneratedErrorMessages.MessageKey.CONNECTOR_MISSING_SOURCE_NAMESPACE,
            Map.of("field", "source.namespace"));
      }
    }

    return out;
  }

  private static FieldMask normalizeMask(FieldMask mask) {
    if (mask == null) {
      return null;
    }
    var out = FieldMask.newBuilder();
    for (var p : mask.getPathsList()) {
      if (p == null) {
        continue;
      }
      var t = p.trim().toLowerCase();
      if (!t.isEmpty()) {
        out.addPaths(t);
      }
    }
    return out.build();
  }

  private static byte[] canonicalFingerprint(ConnectorSpec s) {
    var c = new Canonicalizer();
    c.scalar("name", normalizeName(s.getDisplayName()))
        .scalar("description", s.getDescription())
        .scalar("kind", s.getKind())
        .scalar("uri", s.getUri());

    var source = s.getSource();
    c.group(
        "source",
        g ->
            g.list("namespace", source.getNamespace().getSegmentsList())
                .scalar("table", source.getTable())
                .list("columns", source.getColumnsList()));

    var dest = s.getDestination();
    c.group(
        "destination",
        g ->
            g.scalar("catalog_id", nullSafeId(dest.getCatalogId()))
                .scalar("catalog_display_name", dest.getCatalogDisplayName())
                .scalar("namespace_id", nullSafeId(dest.getNamespaceId()))
                .list("namespace", dest.getNamespace().getSegmentsList())
                .scalar("table_id", nullSafeId(dest.getTableId()))
                .scalar("table_display_name", dest.getTableDisplayName()));

    var auth = s.getAuth();
    c.group(
        "auth",
        g ->
            g.scalar("scheme", auth.getScheme())
                .scalar("credentials", auth.getCredentials())
                .map("header_hints", auth.getHeaderHintsMap())
                .map("properties", auth.getPropertiesMap()));

    var policy = s.getPolicy();
    c.group(
        "policy",
        g ->
            g.scalar("enabled", policy.getEnabled())
                .scalar("max_parallel", policy.getMaxParallel())
                .scalar("interval_seconds", policy.getInterval().getSeconds())
                .scalar("interval_nanos", policy.getInterval().getNanos())
                .scalar("not_before_seconds", policy.getNotBefore().getSeconds())
                .scalar("not_before_nanos", policy.getNotBefore().getNanos()));

    c.map("properties", s.getPropertiesMap());
    return c.bytes();
  }

  private static ConnectorSpec specFromConnector(Connector connector) {
    var b =
        ConnectorSpec.newBuilder()
            .setDisplayName(normalizeName(connector.getDisplayName()))
            .setDescription(connector.getDescription())
            .setKind(connector.getKind())
            .setUri(connector.getUri())
            .putAllProperties(connector.getPropertiesMap());
    if (connector.hasSource()) {
      b.setSource(connector.getSource());
    }
    if (connector.hasDestination()) {
      b.setDestination(connector.getDestination());
    }
    if (connector.hasAuth()) {
      b.setAuth(connector.getAuth());
    }
    if (connector.hasPolicy()) {
      b.setPolicy(connector.getPolicy());
    }
    return b.build();
  }
}
