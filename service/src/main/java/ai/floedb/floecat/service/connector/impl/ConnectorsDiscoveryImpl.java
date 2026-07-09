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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.*;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfig.Kind;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.credentials.AuthResolutionContexts;
import ai.floedb.floecat.service.error.impl.GeneratedErrorMessages;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

// Note: the connector SPI (listNamespaces, listTables, listViews) always returns a
// full list with no cursor support, so pagination cannot be pushed to the source. Page parameters
// in these RPCs are ignored and all results are returned in a single response.

@GrpcService
public class ConnectorsDiscoveryImpl extends BaseServiceImpl implements ConnectorDiscovery {
  @Inject ConnectorRepository connectorRepo;
  @Inject CatalogRepository catalogRepo;
  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject PrincipalProvider principalProvider;
  @Inject Authorizer authz;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject CredentialResolver credentialResolver;

  private static final Logger LOG = Logger.getLogger(ConnectorDiscovery.class);

  @Override
  public Uni<DiscoverNamespacesResponse> discoverNamespaces(DiscoverNamespacesRequest request) {
    var L = LogHelper.start(LOG, "DiscoverNamespaces");

    return mapFailures(
            run(
                () -> {
                  var p = principalProvider.get();
                  var corr = p.getCorrelationId();
                  authz.require(p, "connector.manage");

                  var cfg = buildConnectorConfig(p.getAccountId(), request.getTarget(), corr);

                  String query = request.getQuery().trim().toLowerCase(java.util.Locale.ROOT);

                  try (var connector = ConnectorFactory.create(cfg)) {
                    var namespaces = connector.listNamespaces();

                    var refs = namespaces.stream();
                    if (!query.isEmpty()) {
                      refs =
                          refs.filter(ns -> ns.toLowerCase(java.util.Locale.ROOT).contains(query));
                    }

                    var results =
                        refs.map(
                                ns -> {
                                  var segments = List.of(ns.split("\\.", -1));
                                  var b =
                                      DiscoveryObject.newBuilder()
                                          .setObjectKind(DiscoveryObjectKind.DOK_NAMESPACE)
                                          .setDisplayName(ns)
                                          .addAllNamespaceSegments(segments);
                                  return b.build();
                                })
                            .toList();

                    return DiscoverNamespacesResponse.newBuilder()
                        .addAllNamespaces(results)
                        .setStatus(
                            DiscoveryStatus.newBuilder()
                                .setOk(true)
                                .setSummary("ok: " + results.size() + " namespaces"))
                        .build();
                  } catch (Exception e) {
                    LOG.error("DiscoverNamespaces connector error", e);
                    return DiscoverNamespacesResponse.newBuilder()
                        .setStatus(buildErrorStatus(e))
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
  public Uni<DiscoverObjectsResponse> discoverObjects(DiscoverObjectsRequest request) {
    var L = LogHelper.start(LOG, "DiscoverObjects");

    return mapFailures(
            run(
                () -> {
                  var p = principalProvider.get();
                  var corr = p.getCorrelationId();
                  authz.require(p, "connector.manage");

                  var cfg = buildConnectorConfig(p.getAccountId(), request.getTarget(), corr);

                  var nsSegments = request.getNamespaceSegmentsList();
                  if (nsSegments.isEmpty()) {
                    throw GrpcErrors.invalidArgument(
                        corr, null, Map.of("field", "namespace_segments"));
                  }
                  String namespaceFq = String.join(".", nsSegments);

                  Set<DiscoveryObjectKind> requestedKinds =
                      request.getKindsList().isEmpty()
                          ? Set.of(DiscoveryObjectKind.DOK_TABLE, DiscoveryObjectKind.DOK_VIEW)
                          : new HashSet<>(request.getKindsList());

                  String query = request.getQuery().trim().toLowerCase(java.util.Locale.ROOT);

                  try (var connector = ConnectorFactory.create(cfg)) {
                    var objects = new ArrayList<DiscoveryObject>();

                    if (requestedKinds.contains(DiscoveryObjectKind.DOK_TABLE)) {
                      connector.listTables(namespaceFq).stream()
                          .filter(
                              t ->
                                  query.isEmpty()
                                      || t.toLowerCase(java.util.Locale.ROOT).contains(query))
                          .map(
                              t ->
                                  DiscoveryObject.newBuilder()
                                      .setObjectKind(DiscoveryObjectKind.DOK_TABLE)
                                      .addAllNamespaceSegments(nsSegments)
                                      .setObjectName(t)
                                      .setDisplayName(t)
                                      .build())
                          .forEach(objects::add);
                    }

                    if (requestedKinds.contains(DiscoveryObjectKind.DOK_VIEW)) {
                      connector.listViews(namespaceFq).stream()
                          .filter(
                              v ->
                                  query.isEmpty()
                                      || v.toLowerCase(java.util.Locale.ROOT).contains(query))
                          .map(
                              v ->
                                  DiscoveryObject.newBuilder()
                                      .setObjectKind(DiscoveryObjectKind.DOK_VIEW)
                                      .addAllNamespaceSegments(nsSegments)
                                      .setObjectName(v)
                                      .setDisplayName(v)
                                      .build())
                          .forEach(objects::add);
                    }

                    return DiscoverObjectsResponse.newBuilder()
                        .addAllObjects(objects)
                        .setStatus(
                            DiscoveryStatus.newBuilder()
                                .setOk(true)
                                .setSummary("ok: " + objects.size() + " objects"))
                        .build();
                  } catch (Exception e) {
                    LOG.error("DiscoverObjects connector error", e);
                    return DiscoverObjectsResponse.newBuilder()
                        .setStatus(buildErrorStatus(e))
                        .build();
                  }
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private ConnectorConfig buildConnectorConfig(
      String accountId, ConnectorDiscoveryTarget target, String corr) {
    if (target.hasSpec()) {
      return buildConnectorConfigFromSpec(target.getSpec(), corr);
    }
    if (target.hasConnectorId()) {
      return buildConnectorConfigFromId(accountId, target.getConnectorId(), corr);
    }
    throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "target"));
  }

  private ConnectorConfig buildConnectorConfigFromSpec(ConnectorSpec spec, String corr) {
    var kind = resolveKind(spec.getKind(), corr);
    var auth = toConnectorAuth(spec.getAuth());
    var cfg =
        new ConnectorConfig(
            kind,
            spec.getDisplayName() != null ? spec.getDisplayName() : "",
            mustNonEmpty(spec.getUri(), "uri", corr),
            spec.getPropertiesMap(),
            auth);
    return resolveCredentials(cfg, spec.getAuth());
  }

  private ConnectorConfig buildConnectorConfigFromId(
      String accountId, ResourceId connectorId, String corr) {
    var scopedId = scopedConnectorId(accountId, connectorId, corr);
    Connector connector =
        connectorRepo
            .getById(scopedId)
            .orElseThrow(
                () ->
                    GrpcErrors.notFound(
                        corr,
                        GeneratedErrorMessages.MessageKey.CONNECTOR,
                        Map.of("id", scopedId.getId())));

    var resolvedCreds = credentialResolver.resolve(accountId, scopedId.getId());
    AuthConfig authForResolve = connector.getAuth();
    if (resolvedCreds.isPresent()) {
      authForResolve = authForResolve.toBuilder().setCredentials(resolvedCreds.get()).build();
    }

    var kind = resolveKind(connector.getKind(), corr);
    var auth = toConnectorAuth(connector.getAuth());
    var cfg =
        new ConnectorConfig(
            kind,
            connector.getDisplayName(),
            connector.getUri(),
            connector.getPropertiesMap(),
            auth);
    return resolveCredentials(cfg, authForResolve);
  }

  private static Kind resolveKind(ConnectorKind kind, String corr) {
    return switch (kind) {
      case CK_ICEBERG -> Kind.ICEBERG;
      case CK_DELTA -> Kind.DELTA;
      case CK_GLUE -> Kind.GLUE;
      case CK_UNITY -> Kind.UNITY;
      default -> throw GrpcErrors.invalidArgument(corr, null, Map.of("field", "kind"));
    };
  }

  private ResourceId scopedConnectorId(String accountId, ResourceId connectorId, String corr) {
    ensureKind(connectorId, ResourceKind.RK_CONNECTOR, "connector_id", corr);
    return connectorId.toBuilder().setAccountId(accountId).build();
  }

  private static ConnectorConfig.Auth toConnectorAuth(AuthConfig auth) {
    return new ConnectorConfig.Auth(
        auth.getScheme(), auth.getPropertiesMap(), auth.getHeaderHintsMap());
  }

  private static boolean hasAuthCredentials(AuthConfig auth) {
    if (auth == null || !auth.hasCredentials()) {
      return false;
    }
    return auth.getCredentials().getCredentialCase()
        != AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET;
  }

  private ConnectorConfig resolveCredentials(ConnectorConfig base, AuthConfig auth) {
    if (hasAuthCredentials(auth)) {
      AuthResolutionContext context = AuthResolutionContexts.fromInboundContext();
      return CredentialResolverSupport.apply(base, auth.getCredentials(), context);
    }
    return base;
  }

  private static DiscoveryStatus buildErrorStatus(Exception e) {
    var sb = new StringBuilder("Failed:");
    for (Throwable t = e; t != null; t = t.getCause()) {
      sb.append(" [")
          .append(t.getClass().getSimpleName())
          .append(": ")
          .append(t.getMessage())
          .append("]");
      if (t.getCause() == t) break;
    }
    return DiscoveryStatus.newBuilder().setOk(false).setSummary(sb.toString()).build();
  }
}
