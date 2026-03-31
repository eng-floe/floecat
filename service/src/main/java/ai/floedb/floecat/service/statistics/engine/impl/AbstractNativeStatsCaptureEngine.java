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

package ai.floedb.floecat.service.statistics.engine.impl;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsStore;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

/**
 * Shared connector-backed capture flow for native statistics engines.
 *
 * <p>PR6 scope: resolves TABLE, COLUMN, and FILE targets.
 */
abstract class AbstractNativeStatsCaptureEngine implements StatsCaptureEngine {

  @FunctionalInterface
  interface ConnectorOpener {
    FloecatConnector open(ConnectorConfig config);
  }

  private final Logger log;
  private final TableRepository tableRepository;
  private final ConnectorRepository connectorRepository;
  private final CredentialResolver credentialResolver;
  private final StatsStore statsStore;

  // Package-private test seam.
  ConnectorOpener connectorOpener = ConnectorFactory::create;

  protected AbstractNativeStatsCaptureEngine(
      Logger log,
      TableRepository tableRepository,
      ConnectorRepository connectorRepository,
      CredentialResolver credentialResolver,
      StatsStore statsStore) {
    this.log = log;
    this.tableRepository = tableRepository;
    this.connectorRepository = connectorRepository;
    this.credentialResolver = credentialResolver;
    this.statsStore = statsStore;
  }

  @Override
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    if (request.snapshotId() <= 0L) {
      return Optional.empty();
    }
    try {
      Optional<Table> table = tableRepository.getById(request.tableId());
      if (table.isEmpty() || !table.get().hasUpstream()) {
        return Optional.empty();
      }
      UpstreamRef upstream = table.get().getUpstream();
      if (!upstream.hasConnectorId()
          || upstream.getNamespacePathCount() == 0
          || upstream.getTableDisplayName().isBlank()) {
        return Optional.empty();
      }

      Optional<Connector> connector = connectorRepository.getById(upstream.getConnectorId());
      if (connector.isEmpty()) {
        return Optional.empty();
      }

      ConnectorConfig baseConfig = ConnectorConfigMapper.fromProto(connector.get());
      if (!supportsKind(baseConfig.kind())) {
        return Optional.empty();
      }
      ConnectorConfig resolved =
          resolveCredentials(
              decorateConnectorConfig(baseConfig),
              connector.get().hasAuth()
                  ? connector.get().getAuth()
                  : AuthConfig.getDefaultInstance(),
              upstream.getConnectorId());

      try (FloecatConnector floecatConnector = connectorOpener.open(resolved)) {
        Set<Long> snapshotFilter = Set.of(request.snapshotId());
        // Connector SPI expects namespace as fully-qualified string.
        List<FloecatConnector.SnapshotBundle> bundles =
            floecatConnector.enumerateSnapshotsWithStats(
                String.join(".", upstream.getNamespacePathList()),
                upstream.getTableDisplayName(),
                request.tableId(),
                Set.of(),
                new FloecatConnector.SnapshotEnumerationOptions(
                    true, false, Set.of(), snapshotFilter));
        Optional<FloecatConnector.SnapshotBundle> bundle =
            bundles.stream().filter(b -> b.snapshotId() == request.snapshotId()).findFirst();
        if (bundle.isEmpty()) {
          return Optional.empty();
        }
        Optional<TargetStatsRecord> record =
            findMatchingRecord(bundle.get().targetStats(), request);
        if (record.isEmpty()) {
          return Optional.empty();
        }
        if (request.target().hasTable()) {
          for (TargetStatsRecord bundleRecord : bundle.get().targetStats()) {
            persistRecord(bundleRecord);
          }
        } else {
          persistRecord(record.get());
        }
        return Optional.of(
            StatsCaptureResult.forRecord(id(), record.get(), Map.of("source", "native_connector")));
      }
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "Native stats capture failed engine=%s table=%s snapshot=%s",
          id(),
          request.tableId(),
          request.snapshotId());
      return Optional.empty();
    }
  }

  protected ConnectorConfig decorateConnectorConfig(ConnectorConfig baseConfig) {
    return baseConfig;
  }

  protected abstract boolean supportsKind(ConnectorConfig.Kind kind);

  private ConnectorConfig resolveCredentials(
      ConnectorConfig base, AuthConfig auth, ai.floedb.floecat.common.rpc.ResourceId connectorId) {
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth == null || auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    return credentialResolver
        .resolve(connectorId.getAccountId(), connectorId.getId())
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private Optional<TargetStatsRecord> findMatchingRecord(
      List<TargetStatsRecord> records, StatsCaptureRequest request) {
    StatsTarget target = request.target();
    return switch (target.getTargetCase()) {
      case TABLE -> records.stream().filter(TargetStatsRecord::hasTable).findFirst();
      case COLUMN ->
          records.stream()
              .filter(
                  record ->
                      record.hasScalar()
                          && record.hasTarget()
                          && record.getTarget().hasColumn()
                          && record.getTarget().getColumn().getColumnId()
                              == target.getColumn().getColumnId())
              .findFirst();
      case FILE ->
          records.stream()
              .filter(
                  record ->
                      record.hasFile()
                          && record.hasTarget()
                          && record.getTarget().hasFile()
                          && StatsTargetIdentity.storageId(record.getTarget())
                              .equals(StatsTargetIdentity.storageId(target)))
              .findFirst();
      case TARGET_NOT_SET, EXPRESSION -> Optional.empty();
    };
  }

  private void persistRecord(TargetStatsRecord record) {
    try {
      statsStore.putTargetStats(record);
    } catch (RuntimeException e) {
      log.warnf(
          e,
          "Failed persisting captured stats record engine=%s target=%s",
          id(),
          record.getTarget());
    }
  }

  protected static ConnectorConfig applyIcebergOverrides(ConnectorConfig base) {
    if (base.kind() != ConnectorConfig.Kind.ICEBERG) {
      return base;
    }
    Map<String, String> options = new LinkedHashMap<>(base.options());
    String external = options.get("external.metadata-location");
    if (external == null || external.isBlank()) {
      return base;
    }
    options.putIfAbsent("iceberg.source", "filesystem");
    return options.equals(base.options())
        ? base
        : new ConnectorConfig(base.kind(), base.displayName(), base.uri(), options, base.auth());
  }
}
