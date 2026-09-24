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
package ai.floedb.floecat.service.catalog.impl.surface;

import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.KIND;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.RELATION_NAMES_TOO_MANY;
import static ai.floedb.floecat.service.error.impl.GeneratedErrorMessages.MessageKey.TABLE;

import ai.floedb.floecat.catalog.rpc.GetRelationRequest;
import ai.floedb.floecat.catalog.rpc.GetRelationResponse;
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationListError;
import ai.floedb.floecat.catalog.rpc.RelationListResult;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.ResolveRelationResult;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsResponse;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.service.catalog.impl.surface.RelationScope.Segment;
import ai.floedb.floecat.service.common.MutationOps;
import ai.floedb.floecat.service.error.impl.FloecatStatus;
import ai.floedb.floecat.service.error.impl.GrpcErrors;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Kind-neutral read surface for engine adapters.
 *
 * <p>A listing walks lightweight graph-reference segments. Schema reads are explicit and use the
 * existing typed table/view surfaces only when requested.
 */
public final class CatalogSurfaceRelations {

  private final CatalogGraphView graphView;
  private final CatalogContext context;
  private final CatalogSurfaceTables tables;
  private final CatalogSurfaceViews views;
  private final RelationScope scope;
  private final RelationMapper mapper;
  private final int maxPageSize;

  public CatalogSurfaceRelations(
      TableRepository tableRepo,
      ViewRepository viewRepo,
      CurrentSnapshotView currentSnapshot,
      CatalogGraphView graphView,
      CatalogContext context,
      int maxPageSize) {
    this.maxPageSize = maxPageSize;
    this.graphView = Objects.requireNonNull(graphView, "catalog graph view is required");
    this.context = Objects.requireNonNull(context, "catalog context is required");
    var writePolicy = new CatalogSurfaceWritePolicy(graphView, context);
    this.tables =
        new CatalogSurfaceTables(
            Objects.requireNonNull(tableRepo, "table repository is required"), graphView, context);
    this.views =
        new CatalogSurfaceViews(
            Objects.requireNonNull(viewRepo, "view repository is required"), graphView, context);
    this.scope = new RelationScope(graphView, context, writePolicy);
    this.mapper =
        new RelationMapper(
            graphView,
            context,
            Objects.requireNonNull(currentSnapshot, "current snapshot view is required"));
  }

  public ListRelationsResponse listRelations(
      ListRelationsRequest request, String accountId, String corr) {
    List<ResourceKind> kinds = RelationScope.requestedKinds(request.getKindsList(), corr);
    List<Segment> segments = scope.segments(request, kinds, corr);

    var pageIn = MutationOps.pageIn(request.hasPage() ? request.getPage() : null);
    // The page buffer is sized from this, so an uncapped request size would allocate before a
    // single relation is read.
    int want = Math.min(Math.max(1, pageIn.limit), maxPageSize);
    RelationPageCursor cursor = RelationPageCursor.decode(pageIn.token, corr);
    String scopeFingerprint = scopeFingerprint(request, kinds, accountId, context);
    if (!pageIn.token.isBlank()) {
      cursor.requireScope(scopeFingerprint, pageIn.token, corr);
    }

    var results = new ArrayList<RelationListResult>(want);
    var catalogNames = new HashMap<ResourceId, String>();
    int total = total(segments, request.getIncludeTotal(), cursor.total(), accountId);
    int start = RelationScope.indexAtOrAfter(segments, cursor.segmentKey(), kinds, corr);
    // The inner token is the resumed segment's own pager token. When that segment is gone the walk
    // continues at the next one, which must start from its beginning rather than inherit a token
    // minted for a different source.
    String innerToken =
        start < segments.size() && segments.get(start).key().equals(cursor.segmentKey())
            ? cursor.innerToken()
            : "";
    String nextToken = "";

    for (int i = start; i < segments.size(); i++) {
      Segment segment = segments.get(i);
      var page =
          pageSegment(
              segment,
              accountId,
              catalogNames.computeIfAbsent(
                  segment.namespace().catalogId(),
                  id -> graphView.catalogName(id, context).orElse("")),
              want - results.size(),
              innerToken,
              request.getIncludeSchema(),
              request.getIncludeStatus(),
              corr);
      results.addAll(page.results());
      innerToken = "";

      if (!page.nextToken().isBlank()) {
        nextToken =
            new RelationPageCursor(scopeFingerprint, segment.key(), total, page.nextToken())
                .encode();
        break;
      }
      if (results.size() >= want) {
        nextToken =
            i + 1 < segments.size()
                ? new RelationPageCursor(scopeFingerprint, segments.get(i + 1).key(), total, "")
                    .encode()
                : "";
        break;
      }
    }

    return ListRelationsResponse.newBuilder()
        .addAllResults(results)
        .setPage(MutationOps.pageOut(nextToken, total))
        .build();
  }

  private static String scopeFingerprint(
      ListRelationsRequest request,
      List<ResourceKind> kinds,
      String accountId,
      CatalogContext context) {
    StringBuilder canonical = new StringBuilder(accountId).append('\0');
    if (request.hasCatalogId()) {
      appendScope(canonical, "catalog", request.getCatalogId());
    } else {
      appendScope(canonical, "namespace", request.getNamespaceId());
    }
    canonical
        .append('\0')
        .append(request.getRecursive())
        .append('\0')
        .append(request.getIncludeSchema())
        .append('\0')
        .append(request.getIncludeStatus())
        .append('\0')
        .append(request.getIncludeTotal());
    appendContext(canonical, context);
    for (ResourceKind kind : kinds) {
      canonical.append('\0').append(kind.getNumber());
    }
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256")
                  .digest(canonical.toString().getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError("SHA-256 is required", impossible);
    }
  }

  private static void appendContext(StringBuilder canonical, CatalogContext context) {
    canonical
        .append('\0')
        .append(context.environment().normalizedKind())
        .append('\0')
        .append(context.environment().normalizedVersion())
        .append('\0')
        .append(context.engine().normalizedKind())
        .append('\0')
        .append(context.engine().normalizedVersion());
  }

  private static void appendScope(StringBuilder canonical, String type, ResourceId id) {
    canonical
        .append(type)
        .append('\0')
        .append(id.getAccountId())
        .append('\0')
        .append(id.getId())
        .append('\0')
        .append(id.getKindValue());
  }

  public ResolveRelationsResponse resolveRelations(
      ResolveRelationsRequest request, int maxNames, String corr) {
    int candidateCount =
        request.getReferencesList().stream().mapToInt(RelationReference::getCandidatesCount).sum();
    if (candidateCount > maxNames) {
      throw GrpcErrors.invalidArgument(
          corr,
          RELATION_NAMES_TOO_MANY,
          Map.of(
              "requested", Integer.toString(candidateCount),
              "limit", Integer.toString(maxNames)));
    }

    // One graph round trip for every candidate, then precedence per reference.
    List<NameRef> allCandidates =
        request.getReferencesList().stream()
            .flatMap(reference -> reference.getCandidatesList().stream())
            .toList();
    Map<NameRef, Optional<ResourceId>> resolved =
        graphView.resolveNames(corr, allCandidates, context);

    var out = ResolveRelationsResponse.newBuilder();
    for (RelationReference reference : request.getReferencesList()) {
      out.addResults(
          resolveOne(
              reference, resolved, request.getIncludeSchema(), request.getIncludeStatus(), corr));
    }
    return out.build();
  }

  public GetRelationResponse getRelation(GetRelationRequest request, String corr) {
    return GetRelationResponse.newBuilder()
        .setRelation(
            relationById(
                request.getRelationId(),
                request.getIncludeSchema(),
                request.getIncludeStatus(),
                corr))
        .build();
  }

  /**
   * The first candidate that resolves wins; a candidate that resolves but cannot be read fails only
   * this reference.
   */
  private ResolveRelationResult resolveOne(
      RelationReference reference,
      Map<NameRef, Optional<ResourceId>> resolved,
      boolean includeSchema,
      boolean includeStatus,
      String corr) {
    var result = ResolveRelationResult.newBuilder();
    for (NameRef candidate : reference.getCandidatesList()) {
      ResourceId id = resolved.getOrDefault(candidate, Optional.empty()).orElse(null);
      if (id == null) {
        continue;
      }
      NameRef name = resolvedNameOf(id, candidate);
      result.setResolvedName(name);
      try {
        Relation relation =
            includeSchema
                ? relationById(id, true, includeStatus, corr)
                : mapper.fromRef(
                    new CatalogGraphView.RelationRef(id, name.getName(), id.getKind()),
                    name,
                    includeStatus);
        return result.setRelation(relation).build();
      } catch (StatusRuntimeException failure) {
        if (!GrpcErrors.isRelationScoped(failure)) {
          throw failure;
        }
        return result.setError(toError(name, failure, corr)).build();
      }
    }
    return result.setError(noCandidateResolved(reference, corr)).build();
  }

  /**
   * The name a reference resolved to, in the shape a hydrated read reports.
   *
   * <p>A user relation is matched on a catalog-qualified key, so the candidate that matched is that
   * name. A builtin is matched without its catalog, so a candidate can resolve while naming no
   * catalog or another one; the graph holds the name and answers from the snapshot.
   */
  private NameRef resolvedNameOf(ResourceId id, NameRef candidate) {
    if (!SystemResourceIdGenerator.isSystemId(id)) {
      return candidate;
    }
    return graphView.resolveSystemRelationName(id, context).orElse(candidate);
  }

  private Relation relationById(
      ResourceId relationId, boolean includeSchema, boolean includeStatus, String corr) {
    if (relationId == null) {
      throw GrpcErrors.notFound(corr, TABLE, Map.of("id", "<missing_relation_id>"));
    }
    return switch (relationId.getKind()) {
      case RK_TABLE ->
          mapper.fromTable(tables.byId(relationId, corr), includeSchema, includeStatus);
      case RK_VIEW -> mapper.fromView(views.byId(relationId, corr), includeSchema, includeStatus);
      default -> throw GrpcErrors.invalidArgument(corr, KIND, Map.of("field", "relation_id"));
    };
  }

  private record Page(List<RelationListResult> results, String nextToken) {}

  /** One segment's page of relations, mapped from lightweight graph references. */
  private Page pageSegment(
      Segment segment,
      String accountId,
      String catalogName,
      int want,
      String innerToken,
      boolean includeSchema,
      boolean includeStatus,
      String corr) {
    CatalogSurfaceRelationPager.RefSource source = refSource(segment, accountId);
    var page = CatalogSurfaceRelationPager.listRefs(want, innerToken, source, corr);
    var results = new ArrayList<RelationListResult>(page.relations().size());
    for (var ref : page.relations()) {
      NameRef name = mapper.namespaceName(segment.namespace(), ref.name(), catalogName);
      try {
        Relation relation =
            includeSchema
                ? source.hydrateRelation(ref, name, includeStatus, corr, mapper)
                : mapper.fromRef(ref, name, includeStatus);
        results.add(RelationListResult.newBuilder().setRelation(relation).build());
      } catch (StatusRuntimeException failure) {
        if (!GrpcErrors.isRelationScoped(failure)) {
          throw failure;
        }
        results.add(
            RelationListResult.newBuilder()
                .setError(toListError(ref.id(), name, failure, corr))
                .build());
      }
    }
    return new Page(results, page.nextToken());
  }

  private CatalogSurfaceRelationPager.RefSource refSource(Segment segment, String accountId) {
    return switch (segment.kind()) {
      case RK_TABLE -> tables.pageSource(segment.namespace(), accountId);
      case RK_VIEW -> views.pageSource(segment.namespace(), accountId);
      default -> throw GrpcErrors.invalidArgument("", KIND, Map.of("field", "kinds"));
    };
  }

  private static RelationListError toListError(
      ResourceId relationId, NameRef name, StatusRuntimeException failure, String corr) {
    Error error = toError(name, failure, corr);
    return RelationListError.newBuilder()
        .setRelationId(relationId)
        .setName(name)
        .setError(error)
        .build();
  }

  private int total(List<Segment> segments, boolean requested, int carried, String accountId) {
    if (!requested) {
      return 0;
    }
    if (carried != RelationPageCursor.UNCOUNTED) {
      return carried;
    }
    int total = 0;
    for (Segment segment : segments) {
      total += CatalogSurfaceRelationPager.total(refSource(segment, accountId));
    }
    return total;
  }

  private static Error noCandidateResolved(RelationReference reference, String corr) {
    String tried =
        reference.getCandidatesList().stream()
            .map(NameRefUtil::identityKey)
            .collect(Collectors.joining(", "));
    return Error.newBuilder()
        .setCode(ErrorCode.MC_NOT_FOUND)
        .setMessage("relation not found for any candidate: " + tried)
        .setCorrelationId(corr)
        .build();
  }

  /**
   * The error the failure already carries, so a per-item result keeps the code, message key and
   * params the rest of the error surface reports. A throwable with no error detail attached is
   * summarised instead.
   */
  private static Error toError(NameRef name, StatusRuntimeException failure, String corr) {
    FloecatStatus decoded = FloecatStatus.fromThrowable(failure);
    if (decoded == null) {
      return Error.newBuilder()
          .setCode(ErrorCode.MC_INTERNAL)
          .setMessage("relation unreadable: " + name.getName())
          .setCorrelationId(corr)
          .build();
    }
    return Error.newBuilder()
        .setCode(decoded.errorCode())
        .setMessage(decoded.message())
        .setMessageKey(decoded.messageKey())
        .putAllParams(decoded.params())
        .setCorrelationId(decoded.correlationId().isBlank() ? corr : decoded.correlationId())
        .build();
  }
}
