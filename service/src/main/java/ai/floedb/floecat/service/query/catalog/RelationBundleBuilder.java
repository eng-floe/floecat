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

package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.connector.common.resolver.LogicalSchemaMapper;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.ColumnFailure;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.FlightEndpointRef;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SqlDefinition;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecoratorProvider;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.ViewDecoration;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * Owns all "ResolvedRelation + config → RelationInfo" assembly. Final, immutable, and thread-safe:
 * constructed once by {@link UserObjectBundleService} and reused across the parallel build fan-out.
 * All per-relation state is passed as arguments or lives in locals; the instance holds only the
 * collaborators and config the assembly reads.
 *
 * <p>The driver ({@link UserObjectBundleService}) keeps the pin-identity orchestration and the
 * {@code knownBlobVersions} slim-payload DECISION; this builder only assembles payloads. It exposes
 * {@link #build} (full payload), {@link #buildIdentityOnly} (slim payload), and a few package-
 * visible helpers the driver's pin-identity code needs ({@link #systemExecution}, {@link
 * #decorationRequired}, {@link #currentDecorator}).
 */
final class RelationBundleBuilder {

  private static final Logger LOG = Logger.getLogger(RelationBundleBuilder.class);

  static final String BUILD_FAILED_CODE = "catalog_bundle.build_failed";

  private static final String SYSTEM_FLIGHT_ENDPOINTS_PREFIX = "floedb.system-flight.endpoints.";
  private static final String RELATION_HINT_PERSIST_NANOS_KEY =
      "decorator.relation_hint_persist_nanos";
  private static final String COLUMN_HINT_PERSIST_NANOS_KEY = "decorator.column_hint_persist_nanos";
  private static final String COLUMN_WARM_HIT_COUNT_KEY = "decorator.column_warm_hits";

  private final CatalogOverlay overlay;
  private final EngineMetadataDecoratorProvider decoratorProvider;
  private final boolean engineSpecificEnabled;
  private final FlightEndpointRef floecatFlightEndpoint;
  private final PinValidator pinValidator;
  private final LogicalSchemaMapper logicalSchemaMapper = new LogicalSchemaMapper();

  RelationBundleBuilder(
      CatalogOverlay overlay,
      EngineMetadataDecoratorProvider decoratorProvider,
      boolean engineSpecificEnabled,
      FlightEndpointRef floecatFlightEndpoint,
      PinValidator pinValidator) {
    this.overlay = overlay;
    this.decoratorProvider = decoratorProvider;
    this.engineSpecificEnabled = engineSpecificEnabled;
    this.floecatFlightEndpoint = floecatFlightEndpoint;
    this.pinValidator = pinValidator;
  }

  /** A build error for one relation. Never sinks the whole bundle; the driver maps it to ERROR. */
  record BuildError(String code, String message, String resourceId) {}

  /**
   * The outcome of a full build: exactly one of {@code info} / {@code error} is non-null, together
   * with the per-task {@link UserObjectBundleService.TimingAccumulator} the driver folds back in.
   */
  static final class BuildResult {
    private final RelationInfo info;
    private final BuildError error;
    private final UserObjectBundleService.TimingAccumulator timings;

    private BuildResult(
        RelationInfo info, BuildError error, UserObjectBundleService.TimingAccumulator timings) {
      this.info = info;
      this.error = error;
      this.timings = timings;
    }

    static BuildResult success(
        RelationInfo info, UserObjectBundleService.TimingAccumulator timings) {
      return new BuildResult(info, null, timings);
    }

    static BuildResult failure(
        BuildError error, UserObjectBundleService.TimingAccumulator timings) {
      return new BuildResult(null, error, timings);
    }

    boolean isSuccess() {
      return info != null;
    }

    RelationInfo info() {
      return info;
    }

    BuildError error() {
      return error;
    }

    UserObjectBundleService.TimingAccumulator timings() {
      return timings;
    }
  }

  /**
   * Assemble one relation's full payload. Times the stats and decoration sub-phases into a fresh
   * per-task {@link UserObjectBundleService.TimingAccumulator}, and isolates a build fault to this
   * relation as a {@link BuildError} — one relation's decoration/schema/stats fault must not sink
   * the whole bundle.
   */
  BuildResult build(
      String correlationId,
      UserObjectBundleService.ResolvedRelation relation,
      QueryContext liveCtx,
      MetadataResolutionContext resolutionContext,
      StatsProvider stats,
      Optional<RelationPinIdentity> scopedIdentity) {
    UserObjectBundleService.TimingAccumulator timings =
        new UserObjectBundleService.TimingAccumulator();
    try {
      RelationInfo info =
          buildRelation(
              correlationId, relation, liveCtx, resolutionContext, stats, timings, scopedIdentity);
      return BuildResult.success(info, timings);
    } catch (StatusRuntimeException e) {
      // A structured gRPC error carries a specific code and diagnostic fields — notably
      // pinValidator.validate on genuine catalog-integrity breakage (QUERY_PINNED_ROOT_MISSING,
      // QUERY_PINNED_BLOB_VERSION_MISMATCH, with table_id/pinned_version/found_version). Preserve
      // that structured code and its params instead of flattening to build_failed, so a hard
      // integrity fault is distinguishable from an ordinary decoration/schema fault in metrics and
      // logs. Falls back to build_failed for a status without a structured payload.
      FloecatStatus status = FloecatStatus.fromThrowable(e);
      if (status != null && !status.messageKey().isBlank()) {
        String message =
            status.params().isEmpty() ? status.message() : status.message() + " " + status.params();
        return BuildResult.failure(
            new BuildError(status.messageKey(), message, relation.relationId().getId()), timings);
      }
      String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      return BuildResult.failure(
          new BuildError(BUILD_FAILED_CODE, message, relation.relationId().getId()), timings);
    } catch (RuntimeException e) {
      String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
      return BuildResult.failure(
          new BuildError(BUILD_FAILED_CODE, message, relation.relationId().getId()), timings);
    }
  }

  /**
   * The slim identity-only payload: identity fields, table stats, and the pin identity, with no
   * columns. The driver keeps the {@code knownBlobVersions} DECISION and calls this only when
   * serving slim. Times the stats lookup exactly as the full path does — the slim path still hits
   * the stats provider, which can block on its latency budget.
   */
  RelationInfo buildIdentityOnly(
      UserObjectBundleService.ResolvedRelation relation,
      Optional<RelationPinIdentity> scopedIdentity,
      StatsProvider statsProvider,
      UserObjectBundleService.TimingAccumulator timings) {
    RelationInfo.Builder slim = baseRelationInfo(relation);
    scopedIdentity.ifPresent(slim::setPinIdentity);
    long statsLookupStartNs = System.nanoTime();
    attachTableStats(slim, relation.relationId(), statsProvider);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);
    return slim.build();
  }

  private RelationInfo buildRelation(
      String correlationId,
      UserObjectBundleService.ResolvedRelation relation,
      QueryContext queryContext,
      MetadataResolutionContext resolutionContext,
      StatsProvider statsProvider,
      UserObjectBundleService.TimingAccumulator timings,
      Optional<RelationPinIdentity> scopedIdentity) {
    if (LOG.isTraceEnabled()) {
      LOG.tracef(
          "Building relation bundle query_id=%s relation=%s kind=%s origin=%s",
          queryContext.getQueryId(),
          relation.relationId(),
          relation.node().kind(),
          relation.node().origin());
    }

    // origin is needed below for columnsFor; kind and name are set via baseRelationInfo.
    Origin origin = mapOrigin(relation.node().origin());

    List<SchemaColumn> schemaColumns =
        relation.node() instanceof ViewNode view
            ? view.outputColumns()
            : relation.node() instanceof UserTableNode userTable
                ? UserObjectBundleUtils.qualifyNestedColumnNames(
                    logicalSchemaForRelation(
                            correlationId, relation.relationId(), userTable, queryContext)
                        .getColumnsList())
                : Optional.ofNullable(overlay.tableSchema(relation.node().id()))
                    .orElseGet(List::of);

    List<SchemaColumn> pruned =
        UserObjectBundleUtils.pruneSchema(schemaColumns, relation.candidate(), correlationId);

    List<ColumnInfo> columns =
        UserObjectBundleUtils.columnsFor(schemaColumns, pruned, origin, correlationId);

    RelationInfo.Builder builder = baseRelationInfo(relation);

    /*
     * Populate the bundled endpoint metadata so workers know how to reach the table. FLOECAT
     * tables always use our built-in Flight server, and STORAGE tables can either point at their
     * own Flight endpoint, use an endpoint key resolved from service config, or expose a storage
     * path fallback. ENGINE tables never set an endpoint.
     */
    if (relation.node() instanceof SystemTableNode systemTableNode) {
      // Resolve through the shared helper — the SAME implementation pinIdentityFor uses to fold
      // routing into the token — so the served routing and the token that covers it cannot drift.
      // It is invoked independently at each site (a cheap in-memory config lookup), not memoized
      // across them; both resolve deterministically from the same node, so they always agree.
      SystemExecution exec = systemExecution(systemTableNode);
      builder.setBackendKind(systemTableNode.backendKind());
      if (exec.flightEndpoint() != null) {
        builder.setFlightEndpoint(exec.flightEndpoint());
      } else if (!exec.storagePath().isBlank()) {
        builder.setStoragePath(exec.storagePath());
      }
    }

    long statsLookupStartNs = System.nanoTime();
    attachTableStats(builder, relation.relationId(), statsProvider);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);

    // If this is a view, keep a mutable builder around for decoration.
    ViewDefinition.Builder viewBuilder = null;
    if (relation.node() instanceof ViewNode view) {
      viewBuilder = viewDefinitionBuilder(view);
    }

    // The engine captured at iterator construction, not a live provider re-read: this runs on
    // executor threads where the request context is unreliable, and a silently empty engine would
    // skip engine-specific decoration with no log line (eng-floe/floecat#361).
    EngineContext ctx = resolutionContext.engineContext();
    boolean decorationRequired = decorationRequired(ctx);
    Optional<EngineMetadataDecorator> decorator = currentDecorator(ctx);
    RelationDecoration relationDecoration = null;
    boolean relationDecorationSucceeded = true;
    // Per-phase payload-decoration success, tracked separately from
    // relationDecorationSucceeded (which gates hint commits below). The possession-token stamp
    // needs EVERY payload phase to have succeeded — a view or completion failure leaves the served
    // payload incomplete, and stamping a token for it would lock that incomplete payload into a
    // caching client until it happened to re-miss, instead of self-healing on the next query.
    boolean viewDecorationSucceeded = true;
    boolean completeRelationSucceeded = true;
    long relationDecorationBeforeNanos = timings.decorationTotalNanos();

    if (decorationRequired && decorator.isPresent()) {
      relationDecoration =
          new RelationDecoration(
              builder,
              relation.relationId(),
              relation.node(),
              requireSchema(schemaColumns),
              requireSchema(pruned),
              resolutionContext);

      try {
        long decorateRelationStartNs = System.nanoTime();
        try {
          decorator.get().decorateRelation(ctx, relationDecoration);
        } finally {
          timings.addDecorateRelationNanos(System.nanoTime() - decorateRelationStartNs);
        }
      } catch (RuntimeException e) {
        relationDecorationSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while decorating relation %s (engine=%s)",
            relation.relationId(),
            ctx.normalizedKind());
      }

      // Decorate columns
      // handled below so columns can always emit READY/FAILED status

      // decorate view
      if (viewBuilder != null) {
        ViewDecoration viewDecoration =
            new ViewDecoration(
                builder, viewBuilder, relation.relationId(), relation.node(), resolutionContext);

        try {
          long decorateViewStartNs = System.nanoTime();
          try {
            decorator.get().decorateView(ctx, viewDecoration);
          } finally {
            timings.addDecorateViewNanos(System.nanoTime() - decorateViewStartNs);
          }
        } catch (RuntimeException e) {
          viewDecorationSucceeded = false;
          LOG.debugf(
              e,
              "Decorator threw while decorating view %s (engine=%s)",
              relation.relationId(),
              ctx.normalizedKind());
        }
      }
    }

    if (viewBuilder != null) {
      builder.setViewDefinition(viewBuilder);
    }

    List<ColumnResult> columnResults =
        decorateColumns(
            columns,
            pruned,
            relationDecoration,
            decorator,
            ctx,
            decorationRequired,
            relation.relationId(),
            timings);
    long relationWarmHitCount = decorationCounter(relationDecoration, COLUMN_WARM_HIT_COUNT_KEY);
    timings.addDecorateColumnWarmHits(relationWarmHitCount);

    if (relationDecoration != null && decorator.isPresent()) {
      boolean commitRelationHints = relationDecorationSucceeded;
      boolean commitColumnHints =
          relationDecorationSucceeded && shouldCommitColumnDecorations(columnResults);
      Set<Long> readyColumnIds = commitColumnHints ? readyColumnIds(columnResults) : Set.of();
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Decorator completion decisions relation=%s relation_succeeded=%s"
                + " commit_relation_hints=%s commit_column_hints=%s ready_column_ids=%d",
            relation.relationId(),
            relationDecorationSucceeded,
            commitRelationHints,
            commitColumnHints,
            readyColumnIds.size());
      }
      try {
        long decorateCompleteStartNs = System.nanoTime();
        try {
          decorator
              .get()
              .completeRelation(
                  ctx, relationDecoration, commitRelationHints, commitColumnHints, readyColumnIds);
        } finally {
          timings.addDecorateCompleteNanos(System.nanoTime() - decorateCompleteStartNs);
          timings.addDecoratePersistRelationNanos(
              decorationTimingNanos(relationDecoration, RELATION_HINT_PERSIST_NANOS_KEY));
          timings.addDecoratePersistColumnsNanos(
              decorationTimingNanos(relationDecoration, COLUMN_HINT_PERSIST_NANOS_KEY));
        }
      } catch (RuntimeException e) {
        completeRelationSucceeded = false;
        LOG.debugf(
            e,
            "Decorator threw while completing relation %s (engine=%s)",
            relation.relationId(),
            ctx == null ? "" : ctx.normalizedKind());
      }
    }

    if (LOG.isDebugEnabled()) {
      long relationDecorationNanos =
          Math.max(0L, timings.decorationTotalNanos() - relationDecorationBeforeNanos);
      long relationColdMissCount = Math.max(0L, columnResults.size() - relationWarmHitCount);
      LOG.debugf(
          "Built relation bundle relation=%s columns=%d ready=%d failed=%d warm=%d cold=%d"
              + " decorationMs=%.1f",
          relation.relationId(),
          columnResults.size(),
          countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_OK),
          countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_FAILED),
          relationWarmHitCount,
          relationColdMissCount,
          relationDecorationNanos / 1_000_000.0);
    }

    // Stamp the pin identity. Two distinct concerns share the message and must NOT share a gate:
    //
    //   - The DATA identity (pin_fingerprint, snapshot id, AS-OF provenance,
    // constraints_ref_version)
    //     is a property of the pinned relation, not of the served payload shape. Callers rely on it
    //     to tell a current pin from a historical one and to skip the constraints RPC, so it is
    //     stamped UNCONDITIONALLY whenever the relation is pinned — including on projected or
    //     decoration-incomplete replies, which previously lost it entirely.
    //
    //   - The possession token (table_blob_version) is payload-scoped: a client that advertises it
    //     is later served identity-only and reuses its cached payload verbatim. It is kept only
    // when
    //     the served payload is complete and cacheable, and blanked otherwise:
    //       * full schema — a projected subset must never advertise "I hold every column", or a
    //         later request would be starved of columns it never received;
    //       * every payload-decoration phase succeeded (relation, view, completion) and no column
    //         ended up FAILED — else a transient decoration failure would lock into a caching
    //         client instead of self-healing next query;
    //       * non-blank — a blank version can never prove possession (the match path rejects it).
    //
    // scopedIdentity is computed once by the caller and threaded into both the identity-only match
    // and this stamp, so a cache miss under a populated hint set does not hash the relation twice.
    boolean payloadCacheable =
        servesFullSchema(relation.candidate())
            && relationDecorationSucceeded
            && viewDecorationSucceeded
            && completeRelationSucceeded
            && countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_FAILED) == 0;
    scopedIdentity.ifPresent(
        id ->
            builder.setPinIdentity(
                payloadCacheable && !id.getTableBlobVersion().isBlank()
                    ? id
                    : id.toBuilder().clearTableBlobVersion().build()));

    builder.addAllColumns(columnResults);
    return builder.build();
  }

  /**
   * True when the payload built for this candidate carries the relation's complete column set (no
   * projection). Mirrors {@link UserObjectBundleUtils#pruneSchema} exactly: a candidate that wants
   * all columns, or names none, is served the full schema. The pin-identity token is only stamped
   * for such responses (see buildRelation), so a cached version always denotes the full schema.
   */
  private static boolean servesFullSchema(
      ai.floedb.floecat.query.rpc.TableReferenceCandidate candidate) {
    return candidate.getWantsAllColumns() || candidate.getInitialColumnsCount() == 0;
  }

  /**
   * A {@link RelationInfo} builder carrying the identity fields every response sets — id, canonical
   * name, kind, and origin. Both the slim identity-only reply and the full payload start here, so
   * the two can never disagree on a relation's identity.
   */
  private RelationInfo.Builder baseRelationInfo(UserObjectBundleService.ResolvedRelation relation) {
    return RelationInfo.newBuilder()
        .setRelationId(relation.relationId())
        .setName(relation.canonicalName())
        .setKind(mapKind(relation.node().kind(), relation.node().origin()))
        .setOrigin(mapOrigin(relation.node().origin()));
  }

  /**
   * Attach the relation's live snapshot-scoped estimates (row count, size) when the stats provider
   * has them. Both response paths keep these on the wire: they move with every ingest, so a caching
   * client relies on the reply to refresh them even when the schema payload is omitted.
   */
  private static void attachTableStats(
      RelationInfo.Builder builder, ResourceId relationId, StatsProvider statsProvider) {
    statsProvider
        .tableStats(relationId)
        .map(StatsProviderFactory::toRelationStats)
        .ifPresent(builder::setStats);
  }

  /**
   * A non-negative {@code long} decoration attribute ({@code 0} when absent, non-numeric, or
   * negative). The extraction/clamping lives here once; {@link #decorationTimingNanos} and {@link
   * #decorationCounter} are named wrappers that keep the intent (a nanos timing vs. a warm-hit
   * count) legible at their call sites.
   */
  private static long nonNegativeLongAttribute(
      RelationDecoration relationDecoration, String attributeKey) {
    if (relationDecoration == null || attributeKey == null || attributeKey.isBlank()) {
      return 0L;
    }
    Object value = relationDecoration.attribute(attributeKey);
    if (!(value instanceof Number number)) {
      return 0L;
    }
    return Math.max(0L, number.longValue());
  }

  private static long decorationTimingNanos(
      RelationDecoration relationDecoration, String attributeKey) {
    return nonNegativeLongAttribute(relationDecoration, attributeKey);
  }

  private static long decorationCounter(
      RelationDecoration relationDecoration, String attributeKey) {
    return nonNegativeLongAttribute(relationDecoration, attributeKey);
  }

  private Optional<FlightEndpointRef> configuredEndpointForKey(String endpointKey) {
    if (endpointKey == null || endpointKey.isBlank()) {
      return Optional.empty();
    }

    String normalizedKey = endpointKey.trim();
    String prefix = SYSTEM_FLIGHT_ENDPOINTS_PREFIX + normalizedKey + ".";
    Config config = ConfigProvider.getConfig();
    Optional<String> host =
        config
            .getOptionalValue(prefix + "host", String.class)
            .map(String::trim)
            .filter(value -> !value.isBlank());
    Optional<Integer> port =
        config.getOptionalValue(prefix + "port", Integer.class).filter(value -> value > 0);
    if (host.isEmpty() || port.isEmpty()) {
      LOG.debugf(
          "Storage endpoint key '%s' has no config at %shost/%sport; falling back to storage path",
          normalizedKey, prefix, prefix);
      return Optional.empty();
    }

    boolean tls = config.getOptionalValue(prefix + "tls", Boolean.class).orElse(false);
    return Optional.of(
        FlightEndpointRef.newBuilder().setHost(host.get()).setPort(port.get()).setTls(tls).build());
  }

  /**
   * A system table's resolved execution metadata: the backend kind plus the concrete endpoint the
   * bundle serves (a Flight endpoint, whether built-in, node-declared, or config-resolved, or a
   * storage-path fallback). Resolved in ONE place so buildRelation (which stamps these fields) and
   * the driver's pinIdentityFor (which folds them into the possession token) can never disagree —
   * the token must cover exactly the routing an identity-only reply omits.
   */
  record SystemExecution(String backendKind, FlightEndpointRef flightEndpoint, String storagePath) {
    String tokenMaterial() {
      // Build from the endpoint's explicit, contractual fields (host/port/tls) rather than
      // FlightEndpointRef.toString(): protobuf documents Message.toString() as non-contractual and
      // subject to change, and this token is persisted by clients and matched across queries. The
      // reserved `ticket` field is deliberately excluded — workers must not inspect it, and it is
      // not routing identity. The token then moves exactly when the routing it covers moves.
      String endpoint =
          flightEndpoint != null
              ? flightEndpoint.getHost()
                  + ':'
                  + flightEndpoint.getPort()
                  + ':'
                  + flightEndpoint.getTls()
              : "";
      return backendKind + '\0' + endpoint + '\0' + storagePath;
    }
  }

  SystemExecution systemExecution(SystemTableNode node) {
    String backendKind = String.valueOf(node.backendKind());
    if (node instanceof SystemTableNode.FloeCatSystemTableNode) {
      return new SystemExecution(backendKind, floecatFlightEndpoint, "");
    }
    if (node instanceof SystemTableNode.StorageSystemTableNode storage) {
      if (storage.flightEndpoint() != null) {
        return new SystemExecution(backendKind, storage.flightEndpoint(), "");
      }
      Optional<FlightEndpointRef> configured =
          configuredEndpointForKey(storage.storageEndpointKey());
      if (configured.isPresent()) {
        return new SystemExecution(backendKind, configured.get(), "");
      }
      if (!storage.storagePath().isBlank()) {
        return new SystemExecution(backendKind, null, storage.storagePath());
      }
    }
    return new SystemExecution(backendKind, null, "");
  }

  /**
   * Decorate columns with engine-specific metadata, always emitting a READY/FAILED status per
   * column. Convenience overload timing into a throwaway accumulator; the build path passes its
   * per-task accumulator.
   */
  List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext ctx,
      boolean decorationRequired,
      ResourceId relationId) {
    return decorateColumns(
        columns,
        pruned,
        relationDecoration,
        decorator,
        ctx,
        decorationRequired,
        relationId,
        new UserObjectBundleService.TimingAccumulator());
  }

  private List<ColumnResult> decorateColumns(
      List<ColumnInfo> columns,
      List<SchemaColumn> pruned,
      RelationDecoration relationDecoration,
      Optional<EngineMetadataDecorator> decorator,
      EngineContext ctx,
      boolean decorationRequired,
      ResourceId relationId,
      UserObjectBundleService.TimingAccumulator timings) {

    if (pruned == null || pruned.size() != columns.size()) {
      String msg =
          String.format(
              "Column/schema mismatch columns=%d pruned=%s",
              columns.size(), pruned == null ? "null" : Integer.toString(pruned.size()));
      LOG.debugf("Column decoration mismatch relation=%s %s", relationId, msg);
      if (!decorationRequired) {
        return columns.stream().map(RelationBundleBuilder::readyColumn).toList();
      }
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_SCHEMA_MISMATCH,
                msg,
                Map.of("relation_id", relationId.getId())));
      }
      return failed;
    }

    if (!decorationRequired) {
      return columns.stream().map(RelationBundleBuilder::readyColumn).toList();
    }

    if (decorator.isEmpty() || relationDecoration == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Column decoration unavailable relation=%s engine_kind=%s engine_version=%s",
            relationId,
            safe(ctx == null ? null : ctx.normalizedKind()),
            safe(ctx == null ? null : ctx.normalizedVersion()));
      }
      List<ColumnResult> failed = new ArrayList<>(columns.size());
      for (ColumnInfo column : columns) {
        failed.add(
            failedColumn(
                column,
                ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE,
                "Engine-specific column decorator is unavailable",
                Map.of(
                    "engine_kind", safe(ctx == null ? null : ctx.normalizedKind()),
                    "engine_version", safe(ctx == null ? null : ctx.normalizedVersion()))));
      }
      return failed;
    }

    List<ColumnResult> decorated = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      long decorateColumnTotalStartNs = System.nanoTime();
      ColumnInfo column = columns.get(i);
      SchemaColumn schema = pruned.get(i);
      ColumnInfo.Builder builder = column.toBuilder();
      LogicalType logicalType = parseLogicalType(schema);
      ColumnDecoration columnDecoration =
          new ColumnDecoration(
              builder, schema, logicalType, column.getOrdinal(), relationDecoration);
      try {
        long decorateColumnInvokeStartNs = System.nanoTime();
        try {
          decorator.get().decorateColumn(ctx, columnDecoration);
        } finally {
          timings.addDecorateColumnInvokeNanos(System.nanoTime() - decorateColumnInvokeStartNs);
        }
        ColumnInfo decoratedColumn = columnDecoration.builder().build();
        if (hasRequiredEnginePayload(decoratedColumn, ctx)) {
          decorated.add(readyColumn(decoratedColumn));
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debugf(
                "Column decoration missing required payload relation=%s column=%s ordinal=%d"
                    + " engine_kind=%s",
                relationId,
                column.getName(),
                column.getOrdinal(),
                safe(ctx == null ? null : ctx.normalizedKind()));
          }
          decorated.add(
              failedColumn(
                  decoratedColumn,
                  ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING,
                  "Engine-specific payload is required but missing",
                  Map.of(
                      "engine_kind", safe(ctx == null ? null : ctx.normalizedKind()),
                      "engine_version", safe(ctx == null ? null : ctx.normalizedVersion()))));
        }
      } catch (RuntimeException e) {
        ColumnFailure failure = mapFailure(e, ctx);
        LOG.debugf(
            e,
            "Decorator threw while decorating column %s.%s (engine=%s mapped_code=%s"
                + " extension_code=%d)",
            relationId,
            column.getName(),
            ctx == null ? "" : ctx.normalizedKind(),
            failure.getCode(),
            failure.hasExtensionCodeValue() ? failure.getExtensionCodeValue() : 0);
        decorated.add(failedColumn(column, failure));
      } finally {
        timings.addDecorateColumnsNanos(System.nanoTime() - decorateColumnTotalStartNs);
      }
    }
    return decorated;
  }

  private static ColumnResult readyColumn(ColumnInfo column) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_OK)
        .setColumn(column)
        .build();
  }

  private static ColumnResult failedColumn(
      ColumnInfo column, ColumnFailureCode code, String message, Map<String, String> details) {
    ColumnFailure.Builder failure = ColumnFailure.newBuilder().setCode(code).setMessage(message);
    if (details != null && !details.isEmpty()) {
      failure.putAllDetails(details);
    }
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_FAILED)
        .setFailure(failure)
        .build();
  }

  private static ColumnResult failedColumn(ColumnInfo column, ColumnFailure failure) {
    return ColumnResult.newBuilder()
        .setColumnId(column.getId())
        .setColumnName(column.getName())
        .setOrdinal(column.getOrdinal())
        .setStatus(ColumnStatus.COLUMN_STATUS_FAILED)
        .setFailure(failure)
        .build();
  }

  private ColumnFailure mapFailure(RuntimeException e, EngineContext ctx) {
    if (e instanceof DecorationException de) {
      ColumnFailureCode code =
          de.hasExtensionCodeValue()
              ? ColumnFailureCode.COLUMN_FAILURE_CODE_ENGINE_EXTENSION
              : de.code();
      String message = userFacingFailureMessage(code);
      if (de.hasExtensionCodeValue()) {
        String extensionMessage = safe(de.getMessage()).trim();
        if (!extensionMessage.isBlank()) {
          message = extensionMessage;
        }
      }
      ColumnFailure.Builder builder = ColumnFailure.newBuilder().setCode(code).setMessage(message);
      if (!de.details().isEmpty()) {
        builder.putAllDetails(de.details());
      }
      if (de.hasExtensionCodeValue()) {
        builder.setExtensionCodeValue(de.extensionCodeValue());
      }
      addEngineDetails(builder, ctx);
      if (LOG.isDebugEnabled()) {
        LOG.debugf(
            "Mapped DecorationException to column failure code=%s extension_code=%d engine_kind=%s",
            code,
            de.hasExtensionCodeValue() ? de.extensionCodeValue() : 0,
            safe(ctx == null ? null : ctx.normalizedKind()));
      }
      return builder.build();
    }

    ColumnFailureCode code = ColumnFailureCode.COLUMN_FAILURE_CODE_INTERNAL_ERROR;
    if (e instanceof SecurityException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_PERMISSION_DENIED;
    } else if (e instanceof UnsupportedOperationException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED;
    } else if (e instanceof java.util.NoSuchElementException) {
      code = ColumnFailureCode.COLUMN_FAILURE_CODE_NOT_FOUND;
    }

    ColumnFailure.Builder builder =
        ColumnFailure.newBuilder().setCode(code).setMessage(userFacingFailureMessage(code));
    addEngineDetails(builder, ctx);
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "Mapped RuntimeException to column failure exception=%s code=%s engine_kind=%s",
          e.getClass().getSimpleName(), code, safe(ctx == null ? null : ctx.normalizedKind()));
    }
    return builder.build();
  }

  private static boolean hasRequiredEnginePayload(ColumnInfo column, EngineContext ctx) {
    String normalizedKind = ctx == null ? "" : safe(ctx.normalizedKind());
    for (EngineSpecific spec : column.getEngineSpecificList()) {
      String specKind = safe(spec.getEngineKind());
      boolean kindMatches =
          specKind.isBlank() || normalizedKind.isBlank() || specKind.equals(normalizedKind);
      if (!kindMatches) {
        continue;
      }
      if (!safe(spec.getPayloadType()).isBlank() && !spec.getPayload().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private static String userFacingFailureMessage(ColumnFailureCode code) {
    if (code == null) {
      return "Column resolution failed.";
    }
    return switch (code) {
      case COLUMN_FAILURE_CODE_SCHEMA_MISMATCH ->
          "Column metadata does not match the relation schema.";
      case COLUMN_FAILURE_CODE_DECORATOR_UNAVAILABLE ->
          "Engine-specific column metadata is unavailable.";
      case COLUMN_FAILURE_CODE_ENGINE_PAYLOAD_REQUIRED_MISSING ->
          "Required engine-specific metadata is missing for this column.";
      case COLUMN_FAILURE_CODE_PERMISSION_DENIED ->
          "Permission denied while decorating this column.";
      case COLUMN_FAILURE_CODE_TYPE_NOT_SUPPORTED ->
          "This column type is not supported by the engine metadata decorator.";
      case COLUMN_FAILURE_CODE_LOGICAL_TYPE_INVALID ->
          "The column logical type is invalid for engine metadata decoration.";
      case COLUMN_FAILURE_CODE_NOT_FOUND -> "Column metadata was not found during decoration.";
      case COLUMN_FAILURE_CODE_ENGINE_EXTENSION ->
          "Engine extension failed to provide column metadata.";
      default -> "Column resolution failed.";
    };
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }

  private static void addEngineDetails(ColumnFailure.Builder failure, EngineContext ctx) {
    if (ctx == null) {
      return;
    }
    failure.putDetails("engine_kind", safe(ctx.normalizedKind()));
    failure.putDetails("engine_version", safe(ctx.normalizedVersion()));
  }

  private static boolean shouldCommitColumnDecorations(List<ColumnResult> columnResults) {
    if (columnResults == null || columnResults.isEmpty()) {
      return true;
    }
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == ColumnStatus.COLUMN_STATUS_OK) {
        return true;
      }
    }
    return false;
  }

  private static Set<Long> readyColumnIds(List<ColumnResult> columnResults) {
    if (columnResults == null || columnResults.isEmpty()) {
      return Set.of();
    }
    Set<Long> ids = new java.util.HashSet<>();
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == ColumnStatus.COLUMN_STATUS_OK && result.getColumnId() > 0) {
        ids.add(result.getColumnId());
      }
    }
    return ids;
  }

  private static int countColumnsWithStatus(List<ColumnResult> columnResults, ColumnStatus status) {
    int count = 0;
    if (columnResults == null || status == null) {
      return count;
    }
    for (ColumnResult result : columnResults) {
      if (result.getStatus() == status) {
        count++;
      }
    }
    return count;
  }

  private LogicalType parseLogicalType(SchemaColumn column) {
    if (column == null) {
      return null;
    }
    String logical = column.getLogicalType();
    if (logical == null || logical.isBlank()) {
      return null;
    }
    try {
      return LogicalTypeFormat.parse(logical);
    } catch (IllegalArgumentException e) {
      LOG.debugf(e, "Failed to parse logical type '%s'", logical);
      return null;
    }
  }

  Optional<EngineMetadataDecorator> currentDecorator(EngineContext ctx) {
    if (!decorationRequired(ctx)) {
      return Optional.empty();
    }
    return decoratorProvider.decorator(ctx);
  }

  boolean decorationRequired(EngineContext ctx) {
    return engineSpecificEnabled && ctx != null && ctx.enginePluginOverlaysEnabled();
  }

  private static List<SchemaColumn> requireSchema(List<SchemaColumn> schema) {
    if (schema == null) {
      return List.of();
    }
    return List.copyOf(schema);
  }

  private ai.floedb.floecat.query.rpc.SchemaDescriptor logicalSchemaForRelation(
      String correlationId,
      ResourceId relationId,
      UserTableNode userTable,
      QueryContext queryContext) {
    Optional<TablePin> pin = queryContext.findTablePin(relationId, correlationId);
    if (pin.isEmpty()) {
      // Not yet pinned (e.g. a relation resolved outside the pinned set): fall back to the table's
      // default schema.
      return logicalSchemaMapper.map(userTable);
    }
    // Consume the pinned snapshot identity, validating the pinned blobs; a bad pinned blob fails
    // hard rather than falling back to current catalog state.
    pinValidator.validate(correlationId, pin.get());
    SnapshotRef snapshotRef =
        SnapshotRef.newBuilder().setSnapshotId(pin.get().getSnapshotId()).build();
    CatalogOverlay.SchemaResolution resolved =
        overlay.schemaFor(
            correlationId,
            relationId,
            snapshotRef,
            pin.get().getTableBlobUri(),
            pin.get().getSnapshotBlobUri());
    return logicalSchemaMapper.map(resolved.table(), resolved.schemaJson());
  }

  private ViewDefinition.Builder viewDefinitionBuilder(ViewNode view) {
    ViewDefinition.Builder builder =
        ViewDefinition.newBuilder().setCanonicalSql(view.sql()).setDialect(view.dialect());
    builder.addAllSqlDefinitions(
        view.sqlDefinitions().stream()
            .map(
                def ->
                    SqlDefinition.newBuilder()
                        .setSql(def.getSql())
                        .setDialect(def.getDialect())
                        .build())
            .toList());
    builder.addAllBaseRelations(view.baseRelations());
    builder.addAllCreationSearchPath(view.creationSearchPath());
    return builder;
  }

  private RelationKind mapKind(GraphNodeKind kind, GraphNodeOrigin origin) {
    if (kind == GraphNodeKind.VIEW && origin == GraphNodeOrigin.SYSTEM) {
      return RelationKind.RELATION_KIND_SYSTEM_VIEW;
    }
    return switch (kind) {
      case TABLE -> RelationKind.RELATION_KIND_TABLE;
      case VIEW -> RelationKind.RELATION_KIND_VIEW;
      default -> RelationKind.RELATION_KIND_UNSPECIFIED;
    };
  }

  private Origin mapOrigin(GraphNodeOrigin origin) {
    return origin == GraphNodeOrigin.SYSTEM ? Origin.ORIGIN_BUILTIN : Origin.ORIGIN_USER;
  }
}
