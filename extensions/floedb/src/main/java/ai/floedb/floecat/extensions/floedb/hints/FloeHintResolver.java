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

package ai.floedb.floecat.extensions.floedb.hints;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.extensions.floedb.pgcatalog.PgCatalogProvider;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.extensions.floedb.utils.MissingSystemOidException;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils.OidPolicy;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.scanner.spi.MetadataResolutionContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import com.google.protobuf.Message;
import java.util.Arrays;
import java.util.Optional;

/** Computation-only helper that produces Floe-specific metadata hints. */
public final class FloeHintResolver {
  static final int VARHDRSZ = 4;

  private FloeHintResolver() {}

  private static ScannerUtils.OidPolicy oidPolicy(GraphNode node) {
    if (node == null) {
      return ScannerUtils.OidPolicy.FALLBACK;
    }
    return node.origin() == GraphNodeOrigin.SYSTEM
        ? ScannerUtils.OidPolicy.REQUIRE
        : ScannerUtils.OidPolicy.FALLBACK;
  }

  private static <T extends Message> int resolveOid(
      MetadataResolutionContext ctx,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.ToIntFunction<T> extractor,
      ScannerUtils.OidPolicy policy,
      String missingMsgPrefix) {
    if (id == null) {
      throw new IllegalArgumentException(
          "Cannot resolve OID: id is null (" + missingMsgPrefix + ")");
    }
    if (ctx == null) {
      if (policy == ScannerUtils.OidPolicy.REQUIRE) {
        throw new MissingSystemOidException(
            missingMsgPrefix + " id=" + id + " (no resolution context)");
      }
      return ScannerUtils.fallbackOid(id, descriptor.type());
    }

    return ScannerUtils.oid(
        ctx.overlay(), id, descriptor, messageClass, extractor, ctx.engineContext(), policy);
  }

  private static <T extends Message> int resolveOid(
      MetadataResolutionContext ctx,
      GraphNode node,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass,
      java.util.function.ToIntFunction<T> extractor,
      String missingMsgPrefix) {

    return resolveOid(
        ctx,
        node == null ? null : node.id(),
        descriptor,
        messageClass,
        extractor,
        oidPolicy(node),
        missingMsgPrefix);
  }

  public static FloeNamespaceSpecific namespaceSpecific(
      MetadataResolutionContext ctx, NamespaceNode namespace) {
    FloeNamespaceSpecific.Builder builder =
        payload(ctx, namespace.id(), NAMESPACE, FloeNamespaceSpecific.class)
            .map(FloeNamespaceSpecific::toBuilder)
            .orElse(FloeNamespaceSpecific.newBuilder());
    applyNamespaceDefaults(builder, ctx, namespace);
    return builder.build();
  }

  public static FloeRelationSpecific relationSpecific(
      MetadataResolutionContext ctx, RelationNode node) {
    FloeRelationSpecific.Builder builder =
        payload(ctx, node.id(), RELATION, FloeRelationSpecific.class)
            .map(FloeRelationSpecific::toBuilder)
            .orElse(FloeRelationSpecific.newBuilder());
    applyRelationDefaults(builder, ctx, node);
    return builder.build();
  }

  public static FloeFunctionSpecific functionSpecific(
      MetadataResolutionContext ctx, ResourceId namespaceId, FunctionNode function) {
    FloeFunctionSpecific.Builder builder =
        payload(ctx, function.id(), FUNCTION, FloeFunctionSpecific.class)
            .map(FloeFunctionSpecific::toBuilder)
            .orElse(FloeFunctionSpecific.newBuilder());
    applyFunctionDefaults(builder, ctx, namespaceId, function);
    return builder.build();
  }

  public static FloeTypeSpecific typeSpecific(MetadataResolutionContext ctx, TypeNode type) {
    FloeTypeSpecific.Builder builder =
        payload(ctx, type.id(), TYPE, FloeTypeSpecific.class)
            .map(FloeTypeSpecific::toBuilder)
            .orElse(FloeTypeSpecific.newBuilder());
    applyTypeDefaults(builder, type);

    if (!builder.hasOid()) {
      builder.setOid(
          resolveOid(
              ctx,
              type,
              TYPE,
              FloeTypeSpecific.class,
              FloeTypeSpecific::getOid,
              "Missing OID for SYSTEM type"));
    }
    return builder.build();
  }

  public static FloeColumnSpecific columnSpecific(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      ResourceId tableId,
      int relationOid,
      int attnum,
      SchemaColumn column,
      LogicalType logicalType) {

    if (ctx != null && tableId != null) {
      Optional<FloeColumnSpecific> stored =
          ScannerUtils.columnPayload(
              ctx.overlay(),
              tableId,
              column.getId(),
              COLUMN,
              FloeColumnSpecific.class,
              ctx.engineContext());
      if (stored.isPresent()) {
        return stored.get();
      }
    }

    ColumnMetadata metadata = columnMetadata(ctx, resolver, column, logicalType);
    return buildColumnSpecific(column, attnum, metadata);
  }

  public static ColumnMetadata columnMetadata(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      SchemaColumn column,
      LogicalType logicalType) {

    LogicalType resolved = logicalType == null ? parseLogicalType(column) : logicalType;

    String columnName = column == null ? "<unknown>" : column.getName();
    if (resolved == null) {
      throw new MissingSystemOidException("Missing/invalid logical type for column " + columnName);
    }

    ColumnMetadata metadata = resolveColumnMetadata(ctx, resolver, column, resolved);
    if (metadata == null) {
      throw new MissingSystemOidException(
          "Failed to derive metadata for column " + columnName + " logicalType=" + resolved);
    }
    return metadata;
  }

  public static FloeColumnSpecific buildColumnSpecific(
      SchemaColumn column, int attnum, ColumnMetadata metadata) {
    return FloeColumnSpecific.newBuilder()
        .setAttname(column == null ? "" : column.getName())
        .setAttnum(attnum)
        .setAttnotnull(column != null && !column.getNullable())
        .setAttisdropped(false)
        .setAtttypid(metadata.typeOid())
        .setAtttypmod(metadata.typmod())
        .setAttcollation(metadata.attcollation())
        .setAtthasdef(false)
        .build();
  }

  private static void applyNamespaceDefaults(
      FloeNamespaceSpecific.Builder builder,
      MetadataResolutionContext ctx,
      NamespaceNode namespace) {

    if (!builder.hasOid()) {
      builder.setOid(
          resolveOid(
              ctx,
              namespace,
              NAMESPACE,
              FloeNamespaceSpecific.class,
              FloeNamespaceSpecific::getOid,
              "Missing OID for SYSTEM namespace"));
    }
    if (!builder.hasNspname()) {
      builder.setNspname(namespace.displayName());
    }
  }

  private static void applyRelationDefaults(
      FloeRelationSpecific.Builder builder, MetadataResolutionContext ctx, RelationNode relnode) {

    if (!builder.hasOid()) {
      builder.setOid(
          resolveOid(
              ctx,
              relnode,
              RELATION,
              FloeRelationSpecific.class,
              FloeRelationSpecific::getOid,
              "Missing OID for SYSTEM relation"));
    }
    if (!builder.hasRelname()) {
      builder.setRelname(relnode.displayName());
    }
    if (!builder.hasRelnamespace()) {
      ResourceId namespaceId = relnode.namespaceId();
      assert namespaceId != null;
      builder.setRelnamespace(
          ScannerUtils.oid(
              ctx.overlay(),
              namespaceId,
              NAMESPACE,
              FloeNamespaceSpecific.class,
              FloeNamespaceSpecific::getOid,
              ctx.engineContext(),
              ScannerUtils.oidPolicy(namespaceId)));
    }
    if (!builder.hasRelkind()) {
      builder.setRelkind(relnode instanceof ViewNode ? "v" : "r");
    }
  }

  private static void applyFunctionDefaults(
      FloeFunctionSpecific.Builder builder,
      MetadataResolutionContext ctx,
      ResourceId namespaceId,
      FunctionNode function) {

    // IMPORTANT: we only allow fallback for USER objects; SYSTEM must have persisted hints.
    ScannerUtils.OidPolicy policy = oidPolicy(function);

    if (ctx == null) {
      if (policy == ScannerUtils.OidPolicy.REQUIRE) {
        throw new MissingSystemOidException(
            "Missing required SYSTEM function hints for function id="
                + function.id()
                + " (no resolution context)");
      }
      return;
    }

    CatalogOverlay overlay = ctx.overlay();
    EngineContext engineContext = ctx.engineContext();

    if (!builder.hasOid()) {
      builder.setOid(
          ScannerUtils.oid(
              overlay,
              function.id(),
              FUNCTION,
              FloeFunctionSpecific.class,
              FloeFunctionSpecific::getOid,
              engineContext,
              policy));
    }

    builder.setPronamespace(
        ScannerUtils.oid(
            overlay,
            namespaceId,
            NAMESPACE,
            FloeNamespaceSpecific.class,
            FloeNamespaceSpecific::getOid,
            engineContext,
            ScannerUtils.oidPolicy(namespaceId)));

    if (!builder.hasProrettype()) {
      builder.setProrettype(
          ScannerUtils.oid(
              overlay,
              function.id(),
              FUNCTION,
              FloeFunctionSpecific.class,
              FloeFunctionSpecific::getProrettype,
              engineContext,
              OidPolicy.REQUIRE));
    }

    if (builder.getProargtypesCount() == 0) {
      int[] types =
          ScannerUtils.array(
              overlay,
              function.id(),
              FUNCTION,
              FloeFunctionSpecific.class,
              s -> s.getProargtypesList().stream().mapToInt(Integer::intValue).toArray(),
              engineContext,
              OidPolicy.REQUIRE);
      Arrays.stream(types).forEach(builder::addProargtypes);
    }
  }

  private static void applyTypeDefaults(FloeTypeSpecific.Builder builder, TypeNode type) {
    if (!builder.hasTypname()) {
      builder.setTypname(type.displayName());
    }
    if (!builder.hasTypnamespace() && type.origin() == GraphNodeOrigin.SYSTEM) {
      builder.setTypnamespace(PgCatalogProvider.PG_CATALOG_OID);
    }
    if (!builder.hasTyplen()) {
      builder.setTyplen(-1);
    }
    if (!builder.hasTypalign()) {
      builder.setTypalign("i");
    }
    if (!builder.hasTypdelim()) {
      builder.setTypdelim(",");
    }
  }

  private static ColumnMetadata resolveColumnMetadata(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      SchemaColumn column,
      LogicalType logical) {

    if (ctx == null || resolver == null || column == null) {
      return null;
    }

    String columnName = column.getName() == null ? "<unknown>" : column.getName();
    LogicalType logicalType = logical == null ? parseLogicalType(column) : logical;
    if (logicalType == null) {
      return null;
    }

    int typmod = deriveTypmod(logicalType);
    TypeNode type;
    try {
      type = resolver.resolveOrThrow(logicalType);
    } catch (IllegalStateException e) {
      throw new MissingSystemOidException(
          "Failed to resolve type for column " + columnName + " logicalType=" + logicalType, e);
    }

    Optional<FloeTypeSpecific> specificOpt = payload(ctx, type.id(), TYPE, FloeTypeSpecific.class);

    int typeOid =
        ScannerUtils.oid(
            ctx.overlay(),
            type.id(),
            TYPE,
            FloeTypeSpecific.class,
            FloeTypeSpecific::getOid,
            ctx.engineContext(),
            oidPolicy(type));

    int attlen = specificOpt.map(s -> s.hasTyplen() ? s.getTyplen() : -1).orElse(-1);
    String attalign =
        (specificOpt.isPresent()
                && specificOpt.get().hasTypalign()
                && !specificOpt.get().getTypalign().isBlank())
            ? specificOpt.get().getTypalign()
            : "i";
    boolean attbyval =
        (specificOpt.isPresent() && specificOpt.get().hasTypbyval())
            ? specificOpt.get().getTypbyval()
            : passByValue(logicalType);
    int attcollation =
        (specificOpt.isPresent() && specificOpt.get().hasTypcollation())
            ? specificOpt.get().getTypcollation()
            : 0;

    return new ColumnMetadata(typeOid, typmod, attlen, attalign, attbyval, "p", 0, attcollation);
  }

  public static LogicalType parseLogicalType(SchemaColumn column) {
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
      return null;
    }
  }

  static int deriveTypmod(LogicalType logical) {
    if (logical == null) {
      return -1;
    }

    switch (logical.kind()) {
      case DECIMAL:
        return (logical.precision() << 16) | logical.scale();
      case TIMESTAMP, TIMESTAMPTZ, TIME:
        {
          // For temporal types, we encode the precision in typmod as (precision + VARHDRSZ)
          Integer p = logical.temporalPrecision();
          return (p == null) ? -1 : (p + VARHDRSZ);
        }

      default:
        return -1;
    }
  }

  private static <T extends Message> Optional<T> payload(
      MetadataResolutionContext ctx,
      ResourceId id,
      FloePayloads.Descriptor descriptor,
      Class<T> messageClass) {
    if (ctx == null) {
      return Optional.empty();
    }
    return ScannerUtils.payload(ctx.overlay(), id, descriptor, messageClass, ctx.engineContext());
  }

  // package-private for testing
  static boolean passByValue(LogicalType logical) {
    if (logical == null) {
      return false;
    }
    return switch (logical.kind()) {
      case BOOLEAN, INT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, TIMESTAMPTZ -> true;
      default -> false;
    };
  }

  public static record ColumnMetadata(
      int typeOid,
      int typmod,
      int attlen,
      String attalign,
      boolean attbyval,
      String attstorage,
      int attndims,
      int attcollation) {}
}
