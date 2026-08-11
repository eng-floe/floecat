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
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.RelationPinIdentity;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.query.QueryPins;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.types.Hashing;
import java.util.Optional;
import java.util.Set;

/**
 * Computes the payload identity a resolved relation advertises and decides whether the client
 * already has the served payload (identity-only fast path). Stateless per call and used on the
 * driver thread: it holds a {@link RelationBundleBuilder} for slim assembly, the system execution
 * resolver whose routing the token covers, and the decoration epoch. The cacheable/blank payload
 * stamp for a FULL payload stays in the builder; this policy owns identity creation and the
 * identity-only decision.
 */
final class RelationPayloadPolicy {

  private final RelationBundleBuilder relationBuilder;
  private final SystemExecutionResolver systemExecutionResolver;
  private final EngineRelationDecorator engineRelationDecorator;

  // Bumped when the engine decorator's behavior changes WITHOUT moving the engine version; folded
  // into the identity-only payload token so a decorator change invalidates cached decoration.
  private final String decorationEpoch;

  RelationPayloadPolicy(
      RelationBundleBuilder relationBuilder,
      SystemExecutionResolver systemExecutionResolver,
      EngineRelationDecorator engineRelationDecorator,
      String decorationEpoch) {
    this.relationBuilder = relationBuilder;
    this.systemExecutionResolver = systemExecutionResolver;
    this.engineRelationDecorator = engineRelationDecorator;
    this.decorationEpoch = safe(decorationEpoch);
  }

  /*
   * The opaque pin identity for a resolved relation. Tables carry the query
   * pin's identity, frozen at first touch. Views and system relations have no
   * query pin in V1; they carry a derived content token — the SHA-256 of the
   * relation id and the node's cache identity (see below for why the id is
   * required) — which is immutable per content version, leaks no URI or
   * storage authority, and (with an empty constraints ref) states the
   * deterministic truth that no constraints bundle exists for them.
   */
  private Optional<PinIdentitySource> pinIdentityFor(
      String correlationId, ResolvedRelation relation, QueryContext queryContext) {
    // Only a USER table carries a per-query snapshot pin; route it through the pin's identity.
    // Views AND system tables have no query pin, so they take the derived content token below;
    // system tables need that token for clients to cache them.
    // Discriminate on kind+origin (a system table is also a TABLE node, and a view may be USER
    // origin) rather than the concrete node class, so the routing holds for every node backing.
    if (relation.node().kind() == GraphNodeKind.TABLE
        && relation.node().origin() == GraphNodeOrigin.USER) {
      return queryContext
          .findTablePin(relation.relationId(), correlationId)
          .map(pin -> new PinIdentitySource(QueryPins.identity(pin), schemaScope(pin)));
    }
    String cacheIdentity = relation.node().cacheIdentity();
    if (cacheIdentity == null || cacheIdentity.isBlank()) {
      return Optional.empty();
    }
    // Derived content token for views and system relations: a hash of the relation id plus the
    // node's registry cacheIdentity. The relation id is ESSENTIAL, not decoration: SystemTableNode
    // does not override GraphNode.cacheIdentity(), which returns the bare catalog-fingerprint
    // version (SystemNodeRegistry hands every system table in a catalog the same value), so hashing
    // cacheIdentity alone would collide across all system tables — a client that cached one would
    // be served another identity-only under the shared token and reuse the wrong schema. Mixing the
    // id in makes the token unique per relation while still moving with engine content (the version
    // changes on catalog upgrade). It also folds in a system table's resolved EXECUTION metadata
    // (backend kind + the resolved Flight/storage endpoint) — an identity-only reply omits that
    // metadata, and a config-resolved endpoint (configuredEndpointForKey) can change without moving
    // cacheIdentity. A floecat redeploy does NOT reset an external caching client, so without this
    // the client would match the token, get no endpoint, and route to the stale one. The endpoint
    // is
    // resolved through the shared SystemExecutionResolver used by payload assembly, so the token
    // cannot drift from the served routing.
    ResourceId relId = relation.relationId();
    StringBuilder keyMaterial =
        new StringBuilder()
            .append(relId.getAccountId())
            .append('\0')
            .append(relId.getId())
            .append('\0')
            .append(relId.getKindValue())
            .append('\0')
            .append(cacheIdentity);
    if (relation.node() instanceof SystemTableNode systemTableNode) {
      keyMaterial
          .append('\0')
          .append(systemExecutionResolver.resolve(systemTableNode).tokenMaterial());
    }
    // A CONTENT-derived identity: only table_blob_version is meaningful. A view or system relation
    // has no query snapshot pin, so snapshot_id, pin_kind, pin_fingerprint, and constraints_ref
    // stay unset (0 / UNSPECIFIED / empty) — deliberately, not as a placeholder. Consumers must key
    // such a relation on table_blob_version alone and MUST NOT read the snapshot-pin fields off it
    // (there is no snapshot to describe). The in-repo planner does exactly this — it reads only
    // table_blob_version, constraints_ref_version, and snapshot_id off pin_identity and never
    // branches on pin_kind (see RPC_parsing.cpp) — so the present-but-defaulted fields are inert.
    // No schema scope either: the content hash above IS the schema identity.
    return Optional.of(
        new PinIdentitySource(
            RelationPinIdentity.newBuilder()
                .setTableBlobVersion(Hashing.sha256Hex(keyMaterial.toString()))
                .build(),
            ""));
  }

  /**
   * A wire-facing pin identity plus the server-side schema-scope material its payload token folds
   * in. The scope stays OFF the identity (RelationPinIdentity is planner-facing; the fingerprint is
   * internal pin state) — this pair is how it travels from pinIdentityFor to payloadToken without
   * widening the wire message.
   */
  private record PinIdentitySource(RelationPinIdentity identity, String schemaScope) {}

  /**
   * The schema-scope material a table pin contributes to the payload token: the read-schema
   * fingerprint stamped on the pinned manifest entry, or — for pins built from pre-fingerprint
   * entries — the snapshot blob version (correct but coarser: it also moves on data-only ingests,
   * so legacy entries run cold on ingest until their next snapshot write stamps a fingerprint).
   */
  private static String schemaScope(ai.floedb.floecat.query.rpc.TablePin pin) {
    return pin.getSchemaFingerprint().isBlank()
        ? pin.getSnapshotBlobVersion()
        : pin.getSchemaFingerprint();
  }

  /**
   * The pin identity as stamped on the wire, with its {@code table_blob_version} scoped to the
   * SERVED PAYLOAD rather than the bare content version (see {@link #payloadToken}). Both the
   * full-response stamp and the identity-only match go through here, so the token a client
   * advertises and the token this policy compares can never drift.
   */
  Optional<RelationPinIdentity> payloadIdentity(
      String correlationId,
      ResolvedRelation relation,
      QueryContext queryContext,
      EngineContext ctx) {
    return pinIdentityFor(correlationId, relation, queryContext)
        .map(
            src ->
                src.identity().toBuilder()
                    .setTableBlobVersion(
                        payloadToken(src.identity().getTableBlobVersion(), src.schemaScope(), ctx))
                    .build());
  }

  /**
   * The payload token a caching client advertises (GetUserObjectsRequest.known_table_blob_versions)
   * and this policy matches on. It must identify the WITHHELD PAYLOAD, not merely the content
   * version: withheld columns carry engine-keyed payload (decorateColumns /
   * hasRequiredEnginePayload), so a bare content version would let a client that shares one catalog
   * cache across engines — or that spans an engine-version or decorator upgrade — advertise a
   * version decorated for engine A, be served identity-only under engine B, and reuse engine-A
   * decoration for an engine-B query. The requesting engine is already on the wire (EngineContext),
   * so we fold it in server-side at both mint sites; the client stays engine-agnostic and
   * correctness no longer depends on it keying its own cache by engine.
   *
   * <p>The token folds in a SCHEMA scope ({@code schemaScope}), because the served column schema is
   * read from the pinned snapshot (schema-on-read) and CreateSnapshot/UpdateSnapshot can change
   * that schema WITHOUT moving the definition ref (table_blob_version). A definition-only token
   * would therefore let a client that holds an old schema be served identity-only for a NEW schema
   * and reuse stale columns/types. The scope is the read-schema fingerprint stamped on the pinned
   * manifest entry (SnapshotManifestEntry.schema_fingerprint): identical read schemas share it, so
   * a data-only ingest keeps the token — and the client's schema — warm, while a snapshot-backed
   * schema change moves it. Pins built from pre-fingerprint manifest entries fall back to the
   * snapshot blob version (see {@link #schemaScope}): still never stale, just cold on every ingest
   * until the table's next snapshot write stamps a fingerprint. Views and system relations pass an
   * empty scope — their content hash is already the schema identity.
   *
   * <p>{@code decorationEpoch} additionally invalidates cached decoration when the decorator's
   * behavior changes without moving the engine version. When there is nothing to fold in — no
   * schema scope (views/system) AND no engine decoration — the token IS the content version,
   * byte-identical to the unscoped behavior.
   */
  private String payloadToken(String contentVersion, String schemaScope, EngineContext ctx) {
    if (contentVersion == null || contentVersion.isBlank()) {
      return contentVersion;
    }
    String scope = safe(schemaScope);
    boolean decorate = engineRelationDecorator.isRequired(ctx);
    if (scope.isBlank() && !decorate) {
      return contentVersion;
    }
    StringBuilder material = new StringBuilder(contentVersion).append('\0').append(scope);
    if (decorate) {
      material
          .append('\0')
          .append(safe(ctx.normalizedKind()))
          .append('\0')
          .append(safe(ctx.normalizedVersion()))
          .append('\0')
          .append(decorationEpoch);
    }
    return Hashing.sha256Hex(material.toString());
  }

  /*
   * Identity-only response when the request already has the exact content version
   * this resolution serves: the payload (schema, columns,
   * view definition, decoration) is omitted — the identity plus the
   * lightweight stats are all a caching client needs, and the omitted bytes
   * are provably identical to what it holds. A generic conditional-request
   * feature, never client-special-casing: servers MAY ignore the hint and
   * clients MUST treat a full payload as equally correct. Returns null when
   * the relation must be built in full.
   */
  RelationInfo identityOnly(
      ResolvedRelation relation,
      Optional<RelationPinIdentity> payloadIdentity,
      Optional<StatsProvider.TableStatsView> tableStats,
      Set<String> knownPayloadTokens,
      TimingAccumulator timings) {
    // The token is the engine-scoped payload token (payloadIdentity), not the bare content version,
    // so a client that proved it has the payload under a different engine cannot be served
    // identity-only. A blank version can never prove the client has a payload: a user table whose
    // definition blob had no etag
    // resolves to table_blob_version="" (the repository defaults a missing etag to empty), and
    // every such table would otherwise share that key — one cached, the rest served the wrong
    // schema identity-only. Force the full payload rather than match on the empty string.
    if (knownPayloadTokens.isEmpty()
        || payloadIdentity.isEmpty()
        || payloadIdentity.get().getTableBlobVersion().isBlank()
        || !knownPayloadTokens.contains(payloadIdentity.get().getTableBlobVersion())) {
      return null;
    }
    // The slim payload assembly (baseRelationInfo + attachTableStats + setPinIdentity, no columns)
    // lives in the builder; this policy keeps only the payload-reuse decision above. Its stats
    // lookup
    // is timed into the passed accumulator there, exactly as the full build path times it.
    return relationBuilder.buildIdentityOnly(relation, payloadIdentity, tableStats, timings);
  }

  private static String safe(String value) {
    return value == null ? "" : value;
  }
}
