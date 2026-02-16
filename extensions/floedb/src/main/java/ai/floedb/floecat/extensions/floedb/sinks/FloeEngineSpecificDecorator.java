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

package ai.floedb.floecat.extensions.floedb.sinks;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.engine.FloeTypeMapper;
import ai.floedb.floecat.extensions.floedb.hints.FloeHintResolver;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence.ColumnHint;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/** Decorator that attaches Floe engine-specific column metadata. */
public final class FloeEngineSpecificDecorator implements EngineMetadataDecorator {

  private static final Logger LOG = Logger.getLogger(FloeEngineSpecificDecorator.class);
  private static final String RESOLVER_KEY = "floe.typeResolver";
  private static final String RELATION_OID_KEY = "floe.relOid";

  private final EngineTypeMapper typeMapper = new FloeTypeMapper();
  private static final String COLUMN_HINTS_KEY =
      "ai.floedb.floecat.extensions.floedb.sinks.FloeEngineSpecificDecorator.columnHints";
  private final EngineHintPersistence persistence;

  public FloeEngineSpecificDecorator(EngineHintPersistence persistence) {
    this.persistence = persistence == null ? EngineHintPersistence.NOOP : persistence;
  }

  @Override
  public void decorateColumn(EngineContext engineContext, ColumnDecoration column) {
    String normalizedKind = engineContext.normalizedKind();
    if (normalizedKind == null || normalizedKind.isBlank()) {
      return;
    }
    RelationDecoration relation = column.relation();
    MetadataResolutionContext context = relation.resolutionContext();
    SchemaColumn schema = column.schemaColumn();
    if (context == null || schema == null) {
      return;
    }
    try {
      TypeResolver resolver = relation.attribute(RESOLVER_KEY);
      if (resolver == null) {
        resolver = new TypeResolver(context, typeMapper);
        relation.attribute(RESOLVER_KEY, resolver);
      }
      Optional<EngineSpecific> existing =
          findExistingMetadata(column.builder(), normalizedKind, COLUMN.type());
      if (existing.isPresent()) {
        return;
      }
      int relOid = relationOid(relation);
      FloeColumnSpecific attribute =
          FloeHintResolver.columnSpecific(
              context,
              resolver,
              relation.relationId(),
              relOid,
              column.ordinal(),
              schema,
              column.logicalType());
      column.builder().addEngineSpecific(toEngineSpecific(normalizedKind, attribute));
      bufferColumnHint(
          relation, relation.relationId(), column.id(), COLUMN.type(), attribute.toByteArray());
    } catch (MissingSystemOidException e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed to decorate column %s.%s for engine %s@%s",
          relation.relationId(),
          schema.getName(),
          engineContext.engineKind(),
          engineContext.engineVersion());
    }
  }

  @Override
  public void decorateRelation(EngineContext engineContext, RelationDecoration relation) {
    String normalizedKind = engineContext.normalizedKind();
    if (normalizedKind == null || normalizedKind.isBlank()) {
      return;
    }
    RelationInfo.Builder builder = relation.builder();
    if (findExistingMetadata(builder, normalizedKind, RELATION.type()).isPresent()) {
      return;
    }
    try {
      FloeRelationSpecific relationSpecific =
          FloeHintResolver.relationSpecific(relation.resolutionContext(), relation.node());
      builder.addEngineSpecific(toEngineSpecific(normalizedKind, relationSpecific));
      persistRelationHint(engineContext, relation.relationId(), relationSpecific, RELATION.type());
    } catch (MissingSystemOidException e) {
      throw e; // SYSTEM objects must not silently degrade
    } catch (RuntimeException e) {
      LOG.warnf(
          e,
          "Failed to decorate relation %s for engine %s@%s",
          relation.relationId(),
          engineContext.engineKind(),
          engineContext.engineVersion());
    } finally {
      flushColumnHints(engineContext, relation, relation.relationId());
    }
  }

  private static int relationOid(RelationDecoration relation) {
    Integer cached = relation.attribute(RELATION_OID_KEY);
    if (cached != null) {
      return cached;
    }

    MetadataResolutionContext context = relation.resolutionContext();
    if (context == null) {
      throw new IllegalStateException(
          "Missing resolution context for relation=" + relation.relationId());
    }

    var node = relation.node();
    if (node == null) {
      throw new IllegalStateException(
          "Missing resolved node for relation=" + relation.relationId());
    }

    // Policy is based on the relation's origin: SYSTEM must have a persisted hint; USER may fall
    // back.
    ScannerUtils.OidPolicy policy = ScannerUtils.oidPolicy(node.origin());
    int oid =
        ScannerUtils.oid(
            context.overlay(),
            node.id(),
            RELATION,
            FloeRelationSpecific.class,
            FloeRelationSpecific::getOid,
            context.engineContext(),
            policy);

    relation.attribute(RELATION_OID_KEY, oid);
    return oid;
  }

  private static Optional<EngineSpecific> findExistingMetadata(
      ColumnInfo.Builder builder, String engineKind, String payloadType) {
    if (builder == null || payloadType == null) {
      return Optional.empty();
    }
    for (int i = 0; i < builder.getEngineSpecificCount(); i++) {
      EngineSpecific spec = builder.getEngineSpecific(i);
      if (!payloadType.equals(spec.getPayloadType())) {
        continue;
      }
      String specKind = spec.getEngineKind();
      if (specKind == null || specKind.isBlank() || specKind.equals(engineKind)) {
        return Optional.of(spec);
      }
    }
    return Optional.empty();
  }

  private static EngineSpecific toEngineSpecific(String engineKind, FloeColumnSpecific column) {
    if (column == null) {
      return EngineSpecific.newBuilder().setEngineKind(engineKind).build();
    }
    return EngineSpecific.newBuilder()
        .setEngineKind(engineKind)
        .setPayloadType(COLUMN.type())
        .setPayload(column.toByteString())
        .build();
  }

  private static Optional<EngineSpecific> findExistingMetadata(
      RelationInfo.Builder builder, String engineKind, String payloadType) {
    if (builder == null || payloadType == null) {
      return Optional.empty();
    }
    for (int i = 0; i < builder.getEngineSpecificCount(); i++) {
      EngineSpecific spec = builder.getEngineSpecific(i);
      if (!payloadType.equals(spec.getPayloadType())) {
        continue;
      }
      String specKind = spec.getEngineKind();
      if (specKind == null || specKind.isBlank() || specKind.equals(engineKind)) {
        return Optional.of(spec);
      }
    }
    return Optional.empty();
  }

  private static EngineSpecific toEngineSpecific(
      String engineKind, FloeRelationSpecific relationSpecific) {
    if (relationSpecific == null) {
      return EngineSpecific.newBuilder().setEngineKind(engineKind).build();
    }
    return EngineSpecific.newBuilder()
        .setEngineKind(engineKind)
        .setPayloadType(RELATION.type())
        .setPayload(relationSpecific.toByteString())
        .build();
  }

  private void persistRelationHint(
      EngineContext ctx,
      ResourceId relationId,
      FloeRelationSpecific relationSpecific,
      String payloadType) {
    persistHint(ctx, relationId, relationSpecific.toByteArray(), payloadType);
  }

  private void persistHint(
      EngineContext ctx, ResourceId relationId, byte[] payload, String payloadType) {
    if (persistence == null || persistence == EngineHintPersistence.NOOP) {
      return;
    }
    try {
      persistence.persistRelationHint(
          relationId, payloadType, ctx.normalizedKind(), ctx.normalizedVersion(), payload);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to persist relation engine hint for relation %s", relationId);
    }
  }

  private void bufferColumnHint(
      RelationDecoration relation,
      ResourceId relationId,
      long columnId,
      String payloadType,
      byte[] payload) {
    if (relationId == null || columnId <= 0) {
      if (columnId <= 0 && LOG.isDebugEnabled()) {
        LOG.debugf("Skipping column hint for %s: column id missing or zero", relationId);
      }
      return;
    }
    @SuppressWarnings("unchecked")
    List<ColumnHint> hints = (List<ColumnHint>) relation.attribute(COLUMN_HINTS_KEY);
    if (hints == null) {
      hints = new ArrayList<>();
      relation.attribute(COLUMN_HINTS_KEY, hints);
    }
    hints.add(new ColumnHint(payloadType, columnId, payload));
  }

  private void flushColumnHints(
      EngineContext ctx, RelationDecoration relation, ResourceId relationId) {
    if (relationId == null) {
      return;
    }
    @SuppressWarnings("unchecked")
    List<EngineHintPersistence.ColumnHint> hints =
        (List<EngineHintPersistence.ColumnHint>) relation.attribute(COLUMN_HINTS_KEY);
    if (hints == null || hints.isEmpty()) {
      return;
    }
    persistColumnHints(ctx, relationId, hints);
    relation.attribute(COLUMN_HINTS_KEY, null);
  }

  private void persistColumnHints(
      EngineContext ctx,
      ResourceId relationId,
      List<EngineHintPersistence.ColumnHint> columnHints) {
    if (columnHints == null || columnHints.isEmpty()) {
      return;
    }
    if (persistence == null || persistence == EngineHintPersistence.NOOP) {
      return;
    }
    try {
      persistence.persistColumnHints(
          relationId, ctx.normalizedKind(), ctx.normalizedVersion(), columnHints);
    } catch (RuntimeException e) {
      LOG.debugf(e, "Failed to persist column engine hints for relation %s", relationId);
    }
  }
}
