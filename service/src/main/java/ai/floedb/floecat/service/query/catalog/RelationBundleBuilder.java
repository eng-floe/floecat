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
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.ColumnResult;
import ai.floedb.floecat.query.rpc.ColumnStatus;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationKind;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SqlDefinition;
import ai.floedb.floecat.query.rpc.TablePin;
import ai.floedb.floecat.query.rpc.ViewDefinition;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.query.PinValidator;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.SchemaColumns;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Owns all "ResolvedRelation + config → RelationInfo" assembly. Final, immutable, and thread-safe:
 * constructed once by {@link UserObjectBundleService} and reused across the parallel build fan-out.
 * All per-relation state is passed as arguments or lives in locals; the instance holds only the
 * collaborators and config the assembly reads.
 *
 * <p>The driver ({@link UserObjectBundleService}) keeps the pin-identity orchestration and the
 * {@code knownPayloadTokens} slim-payload DECISION; this builder only assembles payloads. It
 * exposes {@link #build} (full payload) and {@link #buildIdentityOnly} (slim payload).
 */
final class RelationBundleBuilder {

  private static final Logger LOG = Logger.getLogger(RelationBundleBuilder.class);

  static final String BUILD_FAILED_CODE = "catalog_bundle.build_failed";

  private final CatalogGraphView graphView;
  private final EngineRelationDecorator engineRelationDecorator;
  private final SystemExecutionResolver systemExecutionResolver;
  private final PinValidator pinValidator;
  private final LogicalSchemaMapper logicalSchemaMapper = new LogicalSchemaMapper();

  RelationBundleBuilder(
      CatalogGraphView graphView,
      EngineRelationDecorator engineRelationDecorator,
      SystemExecutionResolver systemExecutionResolver,
      PinValidator pinValidator) {
    this.graphView = graphView;
    this.engineRelationDecorator = engineRelationDecorator;
    this.systemExecutionResolver = systemExecutionResolver;
    this.pinValidator = pinValidator;
  }

  /** A build error for one relation. Never sinks the whole bundle; the driver maps it to ERROR. */
  record BuildError(String code, String message, String resourceId) {}

  /**
   * The outcome of a full build: exactly one of {@code info} / {@code error} is non-null, together
   * with the per-task {@link TimingAccumulator} the driver folds back in.
   */
  static final class BuildResult {
    private final RelationInfo info;
    private final BuildError error;
    private final TimingAccumulator timings;

    private BuildResult(RelationInfo info, BuildError error, TimingAccumulator timings) {
      this.info = info;
      this.error = error;
      this.timings = timings;
    }

    static BuildResult success(RelationInfo info, TimingAccumulator timings) {
      return new BuildResult(info, null, timings);
    }

    static BuildResult failure(BuildError error, TimingAccumulator timings) {
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

    TimingAccumulator timings() {
      return timings;
    }
  }

  /**
   * Validate the pinned root for a user-table build on the caller's thread. Returns the same
   * per-relation error that {@link #build} uses for later assembly failures; cancellation still
   * aborts the whole stream.
   */
  Optional<BuildError> validatePin(
      String correlationId, ResolvedRelation relation, QueryContext queryContext) {
    if (!(relation.node() instanceof UserTableNode)) {
      return Optional.empty();
    }
    Optional<TablePin> pin = queryContext.findTablePin(relation.relationId(), correlationId);
    if (pin.isEmpty()) {
      return Optional.empty();
    }
    try {
      pinValidator.validate(correlationId, pin.get());
      return Optional.empty();
    } catch (java.util.concurrent.CancellationException e) {
      throw e;
    } catch (RuntimeException e) {
      return Optional.of(buildError(relation, e));
    }
  }

  /**
   * Assemble one relation's full payload. Times the stats and decoration sub-phases into a fresh
   * per-task {@link TimingAccumulator}, and isolates a build fault to this relation as a {@link
   * BuildError} — one relation's decoration/schema/stats fault must not sink the whole bundle.
   */
  BuildResult build(
      String correlationId,
      ResolvedRelation relation,
      QueryContext liveCtx,
      MetadataResolutionContext resolutionContext,
      EngineRelationDecorator.Selection decorationSelection,
      Optional<StatsProvider.TableStatsView> tableStats,
      Optional<RelationPinIdentity> payloadIdentity) {
    TimingAccumulator timings = new TimingAccumulator();
    try {
      RelationInfo info =
          buildRelation(
              correlationId,
              relation,
              liveCtx,
              resolutionContext,
              decorationSelection,
              tableStats,
              timings,
              payloadIdentity);
      return BuildResult.success(info, timings);
    } catch (java.util.concurrent.CancellationException e) {
      throw e;
    } catch (RuntimeException e) {
      return BuildResult.failure(buildError(relation, e), timings);
    }
  }

  private static BuildError buildError(ResolvedRelation relation, RuntimeException failure) {
    if (failure instanceof StatusRuntimeException e) {
      // Preserve structured catalog-integrity codes and diagnostics instead of flattening them to
      // build_failed. Statuses without structured payloads use the ordinary fallback below.
      FloecatStatus status = FloecatStatus.fromThrowable(e);
      if (status != null && !status.messageKey().isBlank()) {
        String message =
            status.params().isEmpty() ? status.message() : status.message() + " " + status.params();
        return new BuildError(status.messageKey(), message, relation.relationId().getId());
      }
    }
    String message =
        failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    return new BuildError(BUILD_FAILED_CODE, message, relation.relationId().getId());
  }

  /**
   * The slim identity-only payload: identity fields, table stats, and the pin identity, with no
   * columns. The driver keeps the {@code knownPayloadTokens} decision and calls this only when
   * serving slim. Stats were resolved on the producer thread before assembly.
   */
  RelationInfo buildIdentityOnly(
      ResolvedRelation relation,
      Optional<RelationPinIdentity> payloadIdentity,
      Optional<StatsProvider.TableStatsView> tableStats,
      TimingAccumulator timings) {
    RelationInfo.Builder slim = baseRelationInfo(relation);
    payloadIdentity.ifPresent(slim::setPinIdentity);
    long statsLookupStartNs = System.nanoTime();
    attachTableStats(slim, tableStats);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);
    return slim.build();
  }

  private RelationInfo buildRelation(
      String correlationId,
      ResolvedRelation relation,
      QueryContext queryContext,
      MetadataResolutionContext resolutionContext,
      EngineRelationDecorator.Selection decorationSelection,
      Optional<StatsProvider.TableStatsView> tableStats,
      TimingAccumulator timings,
      Optional<RelationPinIdentity> payloadIdentity) {
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

    // Relation payloads carry TOP-LEVEL columns only: ordinals are 1-based within the parent,
    // so any nested row — synthetic placeholder or struct child — shares its ordinal (and
    // therefore its engine attnum) with some top-level column. Nested typing reaches the engine
    // via the per-column type tree; the flattened node set remains available for stats and
    // catalog traversal.
    List<SchemaColumn> schemaColumns =
        relation.node() instanceof ViewNode view
            ? view.outputColumns()
            : relation.node() instanceof UserTableNode userTable
                ? SchemaColumns.topLevelOnly(
                    logicalSchemaForRelation(
                            correlationId, relation.relationId(), userTable, queryContext)
                        .getColumnsList())
                : Optional.ofNullable(graphView.tableSchema(relation.node().id()))
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
      SystemExecutionResolver.SystemExecution exec =
          systemExecutionResolver.resolve(systemTableNode);
      builder.setBackendKind(systemTableNode.backendKind());
      if (exec.flightEndpoint() != null) {
        builder.setFlightEndpoint(exec.flightEndpoint());
      } else if (!exec.storagePath().isBlank()) {
        builder.setStoragePath(exec.storagePath());
      }
    }

    long statsLookupStartNs = System.nanoTime();
    attachTableStats(builder, tableStats);
    timings.addStatsLookupNanos(System.nanoTime() - statsLookupStartNs);

    // If this is a view, keep a mutable builder around for decoration.
    ViewDefinition.Builder viewBuilder = null;
    if (relation.node() instanceof ViewNode view) {
      viewBuilder = viewDefinitionBuilder(view);
    }

    long relationDecorationBeforeNanos = timings.decorationTotalNanos();
    EngineRelationDecorator.Outcome decoration =
        engineRelationDecorator.decorate(
            relation,
            builder,
            viewBuilder,
            columns,
            pruned,
            schemaColumns,
            resolutionContext,
            decorationSelection,
            timings);
    List<ColumnResult> columnResults = decoration.columnResults();
    long relationWarmHitCount = decoration.relationWarmHitCount();
    boolean relationDecorationSucceeded = decoration.relationDecorationSucceeded();
    boolean viewDecorationSucceeded = decoration.viewDecorationSucceeded();
    boolean completeRelationSucceeded = decoration.completeRelationSucceeded();

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

    // Stamp the pin identity. Two distinct concerns share the message and must NOT share a
    // condition:
    //
    //   - The DATA identity (pin_fingerprint, snapshot id, AS-OF provenance,
    // constraints_ref_version)
    //     is a property of the pinned relation, not of the served payload shape. Callers rely on it
    //     to tell a current pin from a historical one and to skip the constraints RPC, so it is
    //     stamped UNCONDITIONALLY whenever the relation is pinned, including on projected or
    //     decoration-incomplete replies.
    //
    //   - The payload token (table_blob_version) is payload-scoped: a client that advertises it
    //     is later served identity-only and reuses its cached payload verbatim. It is kept only
    // when
    //     the served payload is complete and cacheable, and blanked otherwise:
    //       * full schema — a projected subset must never advertise "I hold every column", or a
    //         later request would be starved of columns it never received;
    //       * every payload-decoration phase succeeded (relation, view, completion) and no column
    //         ended up FAILED — else a transient decoration failure would lock into a caching
    //         client instead of self-healing next query;
    //       * non-blank — a blank version cannot prove the client already has the payload (the
    //         match path rejects it).
    //
    // payloadIdentity is computed once by the caller and threaded into both the identity-only match
    // and this stamp, so a cache miss under a populated hint set does not hash the relation twice.
    boolean payloadCacheable =
        servesFullSchema(relation.candidate())
            && relationDecorationSucceeded
            && viewDecorationSucceeded
            && completeRelationSucceeded
            && countColumnsWithStatus(columnResults, ColumnStatus.COLUMN_STATUS_FAILED) == 0;
    payloadIdentity.ifPresent(
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
  private RelationInfo.Builder baseRelationInfo(ResolvedRelation relation) {
    return RelationInfo.newBuilder()
        .setRelationId(relation.relationId())
        .setName(relation.canonicalName())
        .setKind(mapKind(relation.node().kind(), relation.node().origin()))
        .setOrigin(mapOrigin(relation.node().origin()));
  }

  /**
   * Attach the relation's pre-resolved snapshot-scoped estimates (row count, size) when available.
   * Both response paths keep these on the wire: they move with every ingest, so a caching client
   * relies on the reply to refresh them even when the schema payload is omitted.
   */
  private static void attachTableStats(
      RelationInfo.Builder builder, Optional<StatsProvider.TableStatsView> tableStats) {
    tableStats.map(StatsProviderFactory::toRelationStats).ifPresent(builder::setStats);
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
    // Consume the pinned snapshot identity. The stream's producer-thread pre-pass validated this
    // pin before the relation entered worker fan-out.
    SnapshotRef snapshotRef =
        SnapshotRef.newBuilder().setSnapshotId(pin.get().getSnapshotId()).build();
    CatalogGraphView.SchemaResolution resolved =
        graphView.schemaFor(
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
