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
import ai.floedb.floecat.extensions.floedb.proto.FloeDecorationFailureCode;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.ColumnTypeResolutionException;
import ai.floedb.floecat.extensions.floedb.utils.InvalidColumnLogicalTypeException;
import ai.floedb.floecat.extensions.floedb.utils.MissingColumnLogicalTypeException;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemMetadataException;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.extensions.floedb.utils.UnmappedColumnTypeException;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence;
import ai.floedb.floecat.metagraph.hint.EngineHintPersistence.ColumnHint;
import ai.floedb.floecat.query.rpc.ColumnFailureCode;
import ai.floedb.floecat.query.rpc.ColumnInfo;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.RelationInfo;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.spi.decorator.ColumnDecoration;
import ai.floedb.floecat.systemcatalog.spi.decorator.DecorationException;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.decorator.RelationDecoration;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;

/** Decorator that attaches Floe engine-specific column metadata. */
public final class FloeEngineSpecificDecorator implements EngineMetadataDecorator {

  private static final Logger LOG = Logger.getLogger(FloeEngineSpecificDecorator.class);
  private static final String RESOLVER_KEY = "floe.typeResolver";
  private static final String RELATION_OID_KEY = "floe.relOid";
  private static final String RELATION_HINT_KEY =
      "ai.floedb.floecat.extensions.floedb.sinks.FloeEngineSpecificDecorator.relationHint";
  private static final String COLUMN_HINTS_KEY =
      "ai.floedb.floecat.extensions.floedb.sinks.FloeEngineSpecificDecorator.columnHints";

  private final EngineTypeMapper typeMapper = new FloeTypeMapper();
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
      if (column.id() > 0) {
        bufferColumnHint(
            relation, relation.relationId(), column.id(), COLUMN.type(), attribute.toByteArray());
      } else if (LOG.isDebugEnabled()) {
        LOG.debugf("Skipping column hint for %s: column id missing or zero", relation.relationId());
      }
    } catch (MissingColumnLogicalTypeException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_LOGICAL_TYPE_MISSING, e);
    } catch (InvalidColumnLogicalTypeException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_LOGICAL_TYPE_INVALID, e);
    } catch (UnmappedColumnTypeException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_TYPE_MAPPING_MISSING, e);
    } catch (ColumnTypeResolutionException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
    } catch (MissingSystemOidException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_MISSING_OID, e);
    } catch (MissingSystemMetadataException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
    } catch (SecurityException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_PERMISSION_DENIED, e);
    } catch (RuntimeException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
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
      stageRelationHint(relation, RELATION.type(), relationSpecific.toByteArray());
    } catch (MissingColumnLogicalTypeException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_LOGICAL_TYPE_MISSING, e);
    } catch (InvalidColumnLogicalTypeException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_LOGICAL_TYPE_INVALID, e);
    } catch (UnmappedColumnTypeException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_TYPE_MAPPING_MISSING, e);
    } catch (ColumnTypeResolutionException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
    } catch (MissingSystemOidException e) {
      throw decorationFailure(
          FloeDecorationFailureCode.FLOE_DECORATION_FAILURE_CODE_MISSING_OID, e);
    } catch (MissingSystemMetadataException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
    } catch (SecurityException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_PERMISSION_DENIED, e);
    } catch (RuntimeException e) {
      throw decorationFailure(ColumnFailureCode.COLUMN_FAILURE_CODE_DECORATION_ERROR, e);
    }
  }

  @Override
  public void completeRelation(
      EngineContext ctx,
      RelationDecoration relation,
      boolean commitRelationHints,
      boolean commitColumnHints,
      Set<Long> committedColumnIds) {
    if (relation == null) {
      return;
    }
    ResourceId relationId = relation.relationId();
    if (!commitRelationHints && !commitColumnHints) {
      clearStagedHints(relation);
      return;
    }

    StagedRelationHint relationHint = relation.attribute(RELATION_HINT_KEY);
    if (commitRelationHints && relationHint != null) {
      persistHint(ctx, relationId, relationHint.payload(), relationHint.payloadType());
    }

    @SuppressWarnings("unchecked")
    List<ColumnHint> columnHints = (List<ColumnHint>) relation.attribute(COLUMN_HINTS_KEY);
    if (commitColumnHints && columnHints != null && !columnHints.isEmpty()) {
      List<ColumnHint> toPersist = columnHints;
      if (committedColumnIds != null && !committedColumnIds.isEmpty()) {
        toPersist =
            columnHints.stream()
                .filter(hint -> committedColumnIds.contains(hint.columnId()))
                .toList();
      }
      persistColumnHints(ctx, relationId, toPersist);
    }

    clearStagedHints(relation);
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

  private static void stageRelationHint(
      RelationDecoration relation, String payloadType, byte[] payload) {
    relation.attribute(RELATION_HINT_KEY, new StagedRelationHint(payloadType, payload));
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

  private static void clearStagedHints(RelationDecoration relation) {
    relation.attribute(RELATION_HINT_KEY, null);
    relation.attribute(COLUMN_HINTS_KEY, null);
  }

  private static DecorationException decorationFailure(
      ColumnFailureCode coreCode, RuntimeException cause) {
    return new DecorationException(coreCode, cause.getMessage(), cause);
  }

  private static DecorationException decorationFailure(
      FloeDecorationFailureCode extensionCode, RuntimeException cause) {
    return new DecorationException(extensionCode.getNumber(), cause.getMessage(), cause);
  }

  private record StagedRelationHint(String payloadType, byte[] payload) {}
}
