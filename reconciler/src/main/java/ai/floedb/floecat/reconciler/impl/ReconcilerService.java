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

package ai.floedb.floecat.reconciler.impl;

import static ai.floedb.floecat.reconciler.util.NameParts.split;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.common.auth.CredentialResolverSupport;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.connector.common.resolver.StatsProtoEmitter;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.connector.spi.AuthResolutionContext;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import ai.floedb.floecat.connector.spi.ConnectorConfigMapper;
import ai.floedb.floecat.connector.spi.ConnectorFactory;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.CredentialResolver;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.spi.NameRefNormalizer;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend.TableSpecDescriptor;
import ai.floedb.floecat.reconciler.spi.SnapshotHelpers;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcilerService {
  private static final Logger LOG = Logger.getLogger(ReconcilerService.class);
  private static final int SCHEMA_CACHE_MAX_ENTRIES = 64;

  public enum CaptureMode {
    METADATA_ONLY,
    METADATA_AND_STATS
  }

  @Inject ReconcilerBackend backend;
  @Inject LogicalSchemaMapper schemaMapper;
  @Inject CredentialResolver credentialResolver;

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn) {
    return reconcile(
        principal, connectorId, fullRescan, scopeIn, CaptureMode.METADATA_AND_STATS, null);
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode) {
    return reconcile(principal, connectorId, fullRescan, scopeIn, captureMode, null);
  }

  public Result reconcile(
      PrincipalContext principal,
      ResourceId connectorId,
      boolean fullRescan,
      ReconcileScope scopeIn,
      CaptureMode captureMode,
      String bearerToken) {
    ReconcileScope scope = scopeIn == null ? ReconcileScope.empty() : scopeIn;
    long scanned = 0;
    long changed = 0;
    long errors = 0;
    ReconcileContext ctx = buildContext(principal, Optional.ofNullable(bearerToken));
    String corr = ctx.correlationId();

    final ArrayList<String> errSummaries = new ArrayList<>();

    final Connector stored;
    try {
      stored = backend.lookupConnector(ctx, connectorId);
    } catch (RuntimeException e) {
      return new Result(
          0, 0, 1, new IllegalArgumentException("getConnector failed: " + connectorId.getId(), e));
    }

    if (stored.getState() != ConnectorState.CS_ACTIVE) {
      return new Result(
          0, 0, 1, new IllegalStateException("Connector not ACTIVE: " + connectorId.getId()));
    }

    DestinationTarget.Builder destB =
        stored.hasDestination()
            ? stored.getDestination().toBuilder()
            : DestinationTarget.newBuilder();

    FieldMask.Builder dmaskB = FieldMask.newBuilder();

    final SourceSelector source =
        stored.hasSource() ? stored.getSource() : SourceSelector.getDefaultInstance();
    final DestinationTarget dest =
        stored.hasDestination() ? stored.getDestination() : DestinationTarget.getDefaultInstance();

    var cfg = applyIcebergOverrides(ConnectorConfigMapper.fromProto(stored));
    var resolved = resolveCredentials(cfg, stored.getAuth(), connectorId);

    try (FloecatConnector connector = ConnectorFactory.create(resolved)) {
      final ResourceId destCatalogId = dest.getCatalogId();

      final String sourceNsFq;
      if (source.hasNamespace() && !source.getNamespace().getSegmentsList().isEmpty()) {
        sourceNsFq = fq(source.getNamespace().getSegmentsList());
      } else {
        return new Result(
            0, 0, 1, new IllegalArgumentException("connector.source.namespace is required"));
      }

      final String destNsFq;
      final ResourceId destNamespaceId;

      if (dest.hasNamespaceId()) {
        destNamespaceId = dest.getNamespaceId();
        destNsFq = resolveNamespaceFq(ctx, destNamespaceId);
      } else {
        destNsFq =
            (dest.hasNamespace() && !dest.getNamespace().getSegmentsList().isEmpty())
                ? fq(dest.getNamespace().getSegmentsList())
                : sourceNsFq;

        destNamespaceId = ensureNamespace(ctx, destCatalogId, destNsFq);
      }

      String scopeNamespaceFq = destNsFq != null ? destNsFq : sourceNsFq;
      if (!scope.matchesNamespace(scopeNamespaceFq)) {
        return new Result(
            0,
            0,
            1,
            new IllegalArgumentException(
                "Connector destination namespace "
                    + scopeNamespaceFq
                    + " does not match requested scope"));
      }

      if (!destB.hasNamespaceId()) {
        destB.setNamespaceId(destNamespaceId);
        destB.clearNamespace();
        dmaskB.addAllPaths(List.of("destination.namespace_id", "destination.namespace"));
      }

      final List<String> tables =
          (source.getTable() != null && !source.getTable().isBlank())
              ? List.of(source.getTable())
              : connector.listTables(sourceNsFq);

      if (tables.isEmpty()) {
        return new Result(
            scanned,
            changed,
            1,
            new IllegalStateException("No tables found in source namespace: " + sourceNsFq));
      }

      final boolean singleTableMode = tables.size() == 1;
      final Set<String> includeSelectors = normalizeSelectors(source.getColumnsList());

      final String tableDisplayHint =
          (dest.getTableDisplayName() != null && !dest.getTableDisplayName().isBlank())
              ? dest.getTableDisplayName()
              : null;

      boolean matchedScope = false;
      for (String srcTable : tables) {
        try {
          var upstream = connector.describe(sourceNsFq, srcTable);

          final String destTableDisplay =
              (tableDisplayHint != null) ? tableDisplayHint : upstream.tableName();

          if (!scope.acceptsTable(scopeNamespaceFq, destTableDisplay)) {
            continue;
          }

          matchedScope = true;
          scanned++;

          var effective = overrideDisplay(upstream, destNsFq, destTableDisplay);

          var destTableId =
              ensureTable(
                  ctx,
                  destCatalogId,
                  destNamespaceId,
                  effective,
                  connector.format(),
                  stored.getResourceId(),
                  cfg.uri(),
                  sourceNsFq,
                  srcTable);

          if (singleTableMode && !destB.hasTableId()) {
            destB.setTableId(destTableId);
            destB.clearTableDisplayName();
            dmaskB.addAllPaths(List.of("destination.table_id", "destination.table_display_name"));
          }

          boolean includeStats = captureMode == CaptureMode.METADATA_AND_STATS;
          var bundles =
              connector.enumerateSnapshotsWithStats(
                  sourceNsFq, srcTable, destTableId, includeSelectors, includeStats);

          ingestAllSnapshotsAndStatsFiltered(
              ctx, destTableId, connector, effective, bundles, includeSelectors, includeStats);
          changed++;
        } catch (Exception e) {
          errors++;
          e.printStackTrace();
          errSummaries.add(
              "ns="
                  + scopeNamespaceFq
                  + " table="
                  + sourceNsFq
                  + "."
                  + srcTable
                  + " : "
                  + rootCauseMessage(e));
        }
      }

      if (!matchedScope && scope.hasTableFilter()) {
        return new Result(
            0,
            0,
            1,
            new IllegalArgumentException(
                "No tables matched scope: " + scope.destinationTableDisplayName()));
      }

      DestinationTarget updated = destB.build();
      FieldMask dMask = dmaskB.build();
      if (!updated.equals(stored.getDestination()) && dMask.getPathsCount() > 0) {
        try {
          backend.updateConnectorDestination(ctx, stored.getResourceId(), updated);
        } catch (RuntimeException e) {
          errors++;
          errSummaries.add("updateConnector(destination): " + rootCauseMessage(e));
        }
      }

      if (errors == 0) {
        return new Result(scanned, changed, 0, null);
      } else {
        var summary = new StringBuilder();
        summary.append("Partial failure (errors=").append(errors).append("):");
        for (String s : errSummaries) {
          summary.append("\n - ").append(s);
        }
        return new Result(scanned, changed, errors, new RuntimeException(summary.toString()));
      }

    } catch (Exception e) {
      return new Result(scanned, changed, errors, e);
    }
  }

  private ResourceId ensureNamespace(
      ReconcileContext ctx, ResourceId catalogId, String namespaceFq) {
    var parts = split(namespaceFq);
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    NameRef nameRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(parts.parents)
            .setName(parts.leaf)
            .build();
    NameRef normalized = NameRefNormalizer.normalize(nameRef);
    return backend.ensureNamespace(ctx, catalogId, normalized);
  }

  private ResourceId ensureTable(
      ReconcileContext ctx,
      ResourceId catalogId,
      ResourceId destNamespaceId,
      FloecatConnector.TableDescriptor landingView,
      ConnectorFormat format,
      ResourceId connectorRid,
      String connectorUri,
      String sourceNsFq,
      String sourceTable) {
    String catalogName = backend.lookupCatalogName(ctx, catalogId);
    TableSpecDescriptor descriptor =
        new TableSpecDescriptor(
            landingView.namespaceFq(),
            landingView.tableName(),
            landingView.schemaJson(),
            landingView.properties(),
            landingView.partitionKeys(),
            landingView.columnIdAlgorithm(),
            format,
            connectorRid,
            connectorUri,
            sourceNsFq,
            sourceTable);

    NameRef tableRef =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(List.of(landingView.namespaceFq().split("\\.")))
            .setName(landingView.tableName())
            .build();
    return backend.ensureTable(
        ctx, destNamespaceId, NameRefNormalizer.normalize(tableRef), descriptor);
  }

  private void ensureSnapshot(
      ReconcileContext ctx, ResourceId tableId, FloecatConnector.SnapshotBundle snapshotBundle) {
    if (snapshotBundle == null || snapshotBundle.snapshotId() < 0) {
      return;
    }
    Snapshot existing =
        backend.fetchSnapshot(ctx, tableId, snapshotBundle.snapshotId()).orElse(null);
    buildSnapshot(ctx, tableId, snapshotBundle, existing)
        .ifPresent(snapshot -> backend.ingestSnapshot(ctx, tableId, snapshot));
  }

  Optional<Snapshot> buildSnapshot(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector.SnapshotBundle bundle,
      Snapshot existing) {
    long parentSnapshotId = bundle.parentId();
    if (parentSnapshotId <= 0 && existing != null) {
      parentSnapshotId = existing.getParentSnapshotId();
    }

    Timestamp upstreamTimestamp;
    if (bundle.upstreamCreatedAtMs() > 0) {
      upstreamTimestamp = Timestamps.fromMillis(bundle.upstreamCreatedAtMs());
    } else if (existing != null && existing.hasUpstreamCreatedAt()) {
      upstreamTimestamp = existing.getUpstreamCreatedAt();
    } else {
      upstreamTimestamp = Timestamps.fromMillis(ctx.now().toEpochMilli());
    }

    Snapshot.Builder builder =
        Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(bundle.snapshotId())
            .setUpstreamCreatedAt(upstreamTimestamp);
    if (parentSnapshotId > 0) {
      builder.setParentSnapshotId(parentSnapshotId);
    }
    applyField(
        () -> bundle.schemaJson(),
        () -> existing != null ? existing.getSchemaJson() : null,
        builder::setSchemaJson,
        str -> str != null && !str.isBlank());
    applyField(
        () -> bundle.partitionSpec(),
        () ->
            (existing != null && existing.hasPartitionSpec()) ? existing.getPartitionSpec() : null,
        builder::setPartitionSpec,
        spec -> spec != null);
    applyLongField(
        () -> bundle.sequenceNumber(),
        () -> existing != null ? existing.getSequenceNumber() : 0L,
        builder::setSequenceNumber,
        value -> value > 0);
    applyField(
        () -> bundle.manifestList(),
        () -> existing != null ? existing.getManifestList() : null,
        builder::setManifestList,
        str -> str != null && !str.isBlank());
    if (bundle.summary() != null && !bundle.summary().isEmpty()) {
      var merged = new LinkedHashMap<>(bundle.summary());
      if (existing != null && !existing.getSummaryMap().isEmpty()) {
        existing.getSummaryMap().forEach(merged::putIfAbsent);
      }
      builder.putAllSummary(merged);
    }
    applyLongField(
        () -> (long) bundle.schemaId(),
        () -> existing != null ? existing.getSchemaId() : 0L,
        value -> builder.setSchemaId((int) value),
        value -> value > 0);
    Map<String, ByteString> mergedMetadata = new LinkedHashMap<>();
    if (existing != null && !existing.getFormatMetadataMap().isEmpty()) {
      mergedMetadata.putAll(existing.getFormatMetadataMap());
    }
    if (bundle.metadata() != null && !bundle.metadata().isEmpty()) {
      bundle
          .metadata()
          .forEach(
              (key, value) -> mergedMetadata.put(key, value != null ? value : ByteString.EMPTY));
    }
    if (!mergedMetadata.isEmpty()) {
      // Null metadata values from connectors are stored as empty bytes rather than removing keys.
      builder.putAllFormatMetadata(mergedMetadata);
    }
    Snapshot candidate = builder.build();
    if (existing != null && SnapshotHelpers.equalsIgnoringIngested(candidate, existing)) {
      return Optional.empty();
    }
    return Optional.of(
        candidate.toBuilder()
            .setIngestedAt(Timestamps.fromMillis(ctx.now().toEpochMilli()))
            .build());
  }

  private static <T> void applyField(
      Supplier<T> bundleValue,
      Supplier<T> existingValue,
      Consumer<T> setter,
      Predicate<T> hasValue) {
    T value = bundleValue.get();
    if (hasValue.test(value)) {
      setter.accept(value);
      return;
    }
    T existing = existingValue.get();
    if (hasValue.test(existing)) {
      setter.accept(existing);
    }
  }

  private static void applyLongField(
      Supplier<Long> bundleValue,
      Supplier<Long> existingValue,
      LongConsumer setter,
      LongPredicate hasValue) {
    long value = bundleValue.get();
    if (hasValue.test(value)) {
      setter.accept(value);
      return;
    }
    long existing = existingValue.get();
    if (hasValue.test(existing)) {
      setter.accept(existing);
    }
  }

  private void ingestAllSnapshotsAndStatsFiltered(
      ReconcileContext ctx,
      ResourceId tableId,
      FloecatConnector connector,
      FloecatConnector.TableDescriptor tableDesc,
      List<FloecatConnector.SnapshotBundle> bundles,
      Set<String> includeSelectors,
      boolean includeStats) {

    var seen = new HashSet<Long>();
    var schemaCache =
        new LinkedHashMap<String, SchemaDescriptor>(16, 0.75f, true) {
          private static final long serialVersionUID = 1L;

          @Override
          protected boolean removeEldestEntry(Map.Entry<String, SchemaDescriptor> eldest) {
            return size() > SCHEMA_CACHE_MAX_ENTRIES;
          }
        };

    for (var snapshotBundle : bundles) {
      if (snapshotBundle == null) {
        continue;
      }

      long snapshotId = snapshotBundle.snapshotId();
      if (snapshotId < 0 || !seen.add(snapshotId)) {
        continue;
      }

      ensureSnapshot(ctx, tableId, snapshotBundle);

      if (!includeStats) {
        continue;
      }

      if (backend.statsAlreadyCaptured(ctx, tableId, snapshotId)) {
        continue;
      }

      var tsIn = snapshotBundle.tableStats();
      if (tsIn != null) {
        var tStats = tsIn.toBuilder().setTableId(tableId).setSnapshotId(snapshotId).build();
        backend.putTableStats(ctx, tableId, tStats);
      }

      var colsIn = snapshotBundle.columnStats();
      var fileStatsIn = snapshotBundle.fileStats();

      SchemaDescriptor schema = null;
      if ((colsIn != null && !colsIn.isEmpty())
          || (fileStatsIn != null && !fileStatsIn.isEmpty())) {
        String schemaJson =
            (snapshotBundle.schemaJson() != null && !snapshotBundle.schemaJson().isBlank())
                ? snapshotBundle.schemaJson()
                : tableDesc.schemaJson();
        if (schemaJson == null) {
          schemaJson = "";
        }

        final String schemaJsonForMapping = schemaJson;
        schema =
            schemaCache.computeIfAbsent(
                schemaJson,
                key ->
                    schemaMapper.mapRaw(
                        tableDesc.columnIdAlgorithm(),
                        toTableFormat(connector.format()),
                        schemaJsonForMapping,
                        new HashSet<>(tableDesc.partitionKeys())));
      }

      if (colsIn != null && !colsIn.isEmpty()) {
        List<ColumnStats> colsOut =
            StatsProtoEmitter.toColumnStats(
                tableId,
                snapshotId,
                snapshotBundle.upstreamCreatedAtMs(),
                connector.format(),
                tableDesc.columnIdAlgorithm(),
                (schema == null ? SchemaDescriptor.getDefaultInstance() : schema),
                colsIn);

        List<ColumnStats> filtered =
            (includeSelectors == null || includeSelectors.isEmpty())
                ? colsOut
                : colsOut.stream().filter(c -> matchesSelector(c, includeSelectors)).toList();

        if (!filtered.isEmpty()) {
          backend.putColumnStats(ctx, filtered);
        }
      }

      if (fileStatsIn != null && !fileStatsIn.isEmpty()) {
        var fileStatsOut =
            StatsProtoEmitter.toFileColumnStats(
                tableId,
                snapshotId,
                snapshotBundle.upstreamCreatedAtMs(),
                connector.format(),
                tableDesc.columnIdAlgorithm(),
                (schema == null ? SchemaDescriptor.getDefaultInstance() : schema),
                fileStatsIn);
        backend.putFileColumnStats(ctx, fileStatsOut);
      }
    }
  }

  private static TableFormat toTableFormat(ConnectorFormat format) {
    if (format == null) {
      return TableFormat.TF_UNSPECIFIED;
    }

    String name = format.name();
    int i = name.indexOf('_');
    String stem = (i >= 0 && i + 1 < name.length()) ? name.substring(i + 1) : name;
    String target = "TF_" + stem;
    try {
      return TableFormat.valueOf(target);
    } catch (IllegalArgumentException ignored) {
      return TableFormat.TF_UNKNOWN;
    }
  }

  private FloecatConnector.TableDescriptor overrideDisplay(
      FloecatConnector.TableDescriptor upstream, String destNamespace, String destTable) {
    if (destNamespace == null && destTable == null) {
      return upstream;
    }

    return new FloecatConnector.TableDescriptor(
        destNamespace != null ? destNamespace : upstream.namespaceFq(),
        destTable != null ? destTable : upstream.tableName(),
        upstream.location(),
        upstream.schemaJson(),
        upstream.partitionKeys(),
        upstream.columnIdAlgorithm(),
        upstream.properties());
  }

  private static boolean matchesSelector(ColumnStats c, Set<String> selectors) {
    if (selectors == null || selectors.isEmpty()) {
      return true;
    }

    if (selectors.contains(c.getColumnName())) {
      return true;
    }

    if (selectors.contains("#" + c.getColumnId())) {
      return true;
    }

    return false;
  }

  private static String rootCauseMessage(Throwable t) {
    if (t == null) {
      return "unknown error";
    }
    var seen = new HashSet<Throwable>();
    var parts = new ArrayList<String>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      seen.add(cur);
      parts.add(renderThrowable(cur));
      cur = cur.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private String resolveNamespaceFq(ReconcileContext ctx, ResourceId namespaceId) {
    return backend.resolveNamespaceFq(ctx, namespaceId);
  }

  private static String renderThrowable(Throwable t) {
    if (t instanceof StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + desc;
    }
    String m = t.getMessage();
    String cls = t.getClass().getSimpleName();
    if (m == null || m.isBlank()) {
      return cls;
    }
    return cls + ": " + m;
  }

  private static String fq(List<String> segments) {
    return String.join(".", segments);
  }

  private ReconcileContext buildContext(PrincipalContext principal, Optional<String> bearerToken) {
    String correlationId = principal.getCorrelationId();
    if (correlationId == null || correlationId.isBlank()) {
      correlationId = UUID.randomUUID().toString();
    }
    String source = principal.getSubject();
    if (source == null || source.isBlank()) {
      source = "reconciler-service";
    }
    return new ReconcileContext(correlationId, principal, source, Instant.now(), bearerToken);
  }

  private static Set<String> normalizeSelectors(List<String> in) {
    if (in == null || in.isEmpty()) {
      return Set.of();
    }

    var out = new LinkedHashSet<String>();
    for (var s : in) {
      if (s == null) {
        continue;
      }

      var t = s.trim();
      if (t.isEmpty()) {
        continue;
      }

      out.add(t.startsWith("#") ? "#" + t.substring(1).trim() : t);
    }
    return out;
  }

  private ConnectorConfig resolveCredentials(
      ConnectorConfig base,
      ai.floedb.floecat.connector.rpc.AuthConfig auth,
      ResourceId connectorId) {
    if (auth.hasCredentials()
        && auth.getCredentials().getCredentialCase()
            != ai.floedb.floecat.connector.rpc.AuthCredentials.CredentialCase.CREDENTIAL_NOT_SET) {
      return CredentialResolverSupport.apply(base, auth.getCredentials());
    }
    if (auth == null || auth.getScheme().isBlank() || "none".equalsIgnoreCase(auth.getScheme())) {
      return base;
    }
    var credential = credentialResolver.resolve(connectorId.getAccountId(), connectorId.getId());
    return credential
        .map(c -> CredentialResolverSupport.apply(base, c, AuthResolutionContext.empty()))
        .orElse(base);
  }

  private static ConnectorConfig applyIcebergOverrides(ConnectorConfig base) {
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

  public static final class Result {
    public final long scanned, changed, errors;
    public final Exception error;

    public Result(long scanned, long changed, long errors, Exception error) {
      this.scanned = scanned;
      this.changed = changed;
      this.errors = errors;
      this.error = error;
    }

    public boolean ok() {
      return error == null;
    }

    public String message() {
      return ok() ? "OK" : rootCauseMessage(error);
    }
  }
}
