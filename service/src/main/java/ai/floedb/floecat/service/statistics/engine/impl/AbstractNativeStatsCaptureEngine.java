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

import ai.floedb.floecat.catalog.rpc.StatsCaptureMode;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
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
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsStore;
import com.google.protobuf.util.Timestamps;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
    // Delta tables can legitimately use snapshot id 0.
    if (request.snapshotId() < 0L) {
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
        String namespaceFq = String.join(".", upstream.getNamespacePathList());
        List<TargetStatsRecord> capturedRecords =
            floecatConnector.captureSnapshotTargetStats(
                namespaceFq,
                upstream.getTableDisplayName(),
                request.tableId(),
                request.snapshotId(),
                request.columnSelectors());
        if (capturedRecords == null || capturedRecords.isEmpty()) {
          return Optional.empty();
        }
        Optional<TargetStatsRecord> record = findMatchingRecord(capturedRecords, request);
        if (record.isEmpty()) {
          return Optional.empty();
        }
        TargetStatsRecord canonicalRecord = withCanonicalMetadata(record.get(), request);
        if (request.target().hasTable()) {
          // Table-target capture persists the full snapshot capture so later column/file lookups
          // hit store without additional capture. Replace the table entry with the matched
          // canonical record.
          Set<String> seenTargets = new LinkedHashSet<>();
          for (TargetStatsRecord bundleRecord : capturedRecords) {
            TargetStatsRecord toPersist =
                bundleRecord.hasTable()
                    ? canonicalRecord
                    : withCanonicalMetadata(bundleRecord, request);
            String storageId = StatsTargetIdentity.storageId(toPersist.getTarget());
            if (seenTargets.add(storageId)) {
              persistRecord(toPersist);
            }
          }
        } else {
          persistRecord(canonicalRecord);
        }
        return Optional.of(
            StatsCaptureResult.forRecord(
                id(), canonicalRecord, Map.of("source", "native_connector")));
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
    if (auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
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

  private TargetStatsRecord withCanonicalMetadata(
      TargetStatsRecord record, StatsCaptureRequest request) {
    long nowMs = System.currentTimeMillis();
    StatsMetadata.Builder metadata =
        (record.hasMetadata() ? record.getMetadata() : StatsMetadata.getDefaultInstance())
            .toBuilder();

    if (metadata.getProducer() == StatsProducer.SPROD_UNSPECIFIED) {
      metadata.setProducer(StatsProducer.SPROD_SOURCE_NATIVE);
    }
    if (metadata.getCompleteness() == StatsCompleteness.SC_UNSPECIFIED) {
      // PR6 baseline policy: native connector captures are treated as complete unless stated
      // otherwise by upstream metadata.
      metadata.setCompleteness(StatsCompleteness.SC_COMPLETE);
    }
    metadata.setCaptureMode(
        request.executionMode() == StatsExecutionMode.SYNC
            ? StatsCaptureMode.SCM_SYNC
            : StatsCaptureMode.SCM_ASYNC);

    if (!metadata.hasConfidenceLevel()) {
      // PR6 baseline policy for callers that do not emit confidence.
      double defaultConfidence =
          metadata.getCompleteness() == StatsCompleteness.SC_COMPLETE ? 1.0d : 0.5d;
      metadata.setConfidenceLevel(defaultConfidence);
    }
    if (!metadata.hasCoverage()) {
      metadata.setCoverage(StatsCoverage.getDefaultInstance());
    }
    if (!metadata.hasCapturedAt()) {
      // Timestamp reflects Floecat normalization/persistence time when source capture time is
      // absent.
      metadata.setCapturedAt(Timestamps.fromMillis(nowMs));
    }
    if (!metadata.hasRefreshedAt()) {
      // Timestamp reflects Floecat refresh/persistence time when source refresh time is absent.
      metadata.setRefreshedAt(Timestamps.fromMillis(nowMs));
    }

    if (!metadata.containsProperties("method")) {
      metadata.putProperties("method", "connector_native");
    }
    if (!metadata.containsProperties("engine_id")) {
      metadata.putProperties("engine_id", id());
    }

    return record.toBuilder().setMetadata(metadata.build()).build();
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
