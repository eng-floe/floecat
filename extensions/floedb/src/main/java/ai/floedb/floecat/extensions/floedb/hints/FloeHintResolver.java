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

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.pgcatalog.PgCatalogProvider;
import ai.floedb.floecat.extensions.floedb.proto.FloeColumnSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.PayloadDescriptor;
import ai.floedb.floecat.extensions.floedb.utils.ScannerUtils;
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TypeNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.systemcatalog.spi.scanner.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.spi.scanner.MetadataResolutionContext;
import ai.floedb.floecat.systemcatalog.spi.types.TypeResolver;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import java.util.Arrays;
import java.util.Optional;

/** Computation-only helper that produces Floe-specific metadata hints. */
public final class FloeHintResolver {

  private FloeHintResolver() {}

  public static FloeNamespaceSpecific namespaceSpecific(
      MetadataResolutionContext ctx, NamespaceNode namespace) {
    FloeNamespaceSpecific.Builder builder =
        payload(ctx, namespace.id(), NAMESPACE)
            .map(FloeNamespaceSpecific::toBuilder)
            .orElse(FloeNamespaceSpecific.newBuilder());
    applyNamespaceDefaults(builder, namespace);
    return builder.build();
  }

  public static FloeRelationSpecific relationSpecific(
      MetadataResolutionContext ctx, GraphNode node) {
    FloeRelationSpecific.Builder builder =
        payload(ctx, node.id(), RELATION)
            .map(FloeRelationSpecific::toBuilder)
            .orElse(FloeRelationSpecific.newBuilder());
    applyRelationDefaults(builder, node);
    return builder.build();
  }

  public static FloeFunctionSpecific functionSpecific(
      MetadataResolutionContext ctx, ResourceId namespaceId, FunctionNode function) {
    FloeFunctionSpecific.Builder builder =
        payload(ctx, function.id(), FUNCTION)
            .map(FloeFunctionSpecific::toBuilder)
            .orElse(FloeFunctionSpecific.newBuilder());
    applyFunctionDefaults(builder, ctx, namespaceId, function);
    return builder.build();
  }

  public static FloeTypeSpecific typeSpecific(MetadataResolutionContext ctx, TypeNode type) {
    FloeTypeSpecific.Builder builder =
        payload(ctx, type.id(), TYPE)
            .map(FloeTypeSpecific::toBuilder)
            .orElse(FloeTypeSpecific.newBuilder());
    if (!builder.hasOid()) {
      builder.setOid(ScannerUtils.fallbackOid(type.id()));
    }
    applyTypeDefaults(builder, type);
    return builder.build();
  }

  public static FloeColumnSpecific columnSpecific(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      int relationOid,
      int attnum,
      SchemaColumn column,
      LogicalType logicalType) {
    FloeColumnSpecific.Builder builder = FloeColumnSpecific.newBuilder();
    builder
        .setAttrelid(relationOid)
        .setAttnum(attnum)
        .setAttname(column == null ? "" : column.getName())
        .setAttnotnull(column != null && !column.getNullable())
        .setAttisdropped(false);

    ColumnMetadata metadata = resolveColumnMetadata(ctx, resolver, column, logicalType);
    int typeOid =
        metadata == null ? fallbackTypeOid(ctx, resolver, column, logicalType) : metadata.typeOid;
    int typmod = metadata == null ? -1 : metadata.typmod;
    int attlen = metadata == null ? -1 : metadata.attlen;
    String attalign = metadata == null ? "i" : metadata.attalign;
    String attstorage = metadata == null ? "p" : metadata.attstorage;
    int attndims = metadata == null ? 0 : metadata.attndims;
    int attcollation = metadata == null ? 0 : metadata.attcollation;
    boolean attbyval = metadata == null ? false : metadata.typeSpecific.getTypbyval();

    builder
        .setAtttypid(typeOid)
        .setAtttypmod(typmod)
        .setAttlen(attlen)
        .setAttalign(attalign)
        .setAttstorage(attstorage)
        .setAttndims(attndims)
        .setAttcollation(attcollation)
        .setAttbyval(attbyval);

    return builder.build();
  }

  private static void applyNamespaceDefaults(
      FloeNamespaceSpecific.Builder builder, NamespaceNode namespace) {
    if (!builder.hasOid()) {
      builder.setOid(ScannerUtils.fallbackOid(namespace.id()));
    }
    if (!builder.hasNspname()) {
      builder.setNspname(namespace.displayName());
    }
    if (!builder.hasNspowner()) {
      builder.setNspowner(ScannerUtils.defaultOwnerOid());
    }
  }

  private static void applyRelationDefaults(FloeRelationSpecific.Builder builder, GraphNode node) {
    if (!builder.hasOid()) {
      builder.setOid(ScannerUtils.fallbackOid(node.id()));
    }
    if (!builder.hasRelname()) {
      builder.setRelname(node.displayName());
    }
    if (!builder.hasRelnamespace()) {
      builder.setRelnamespace(PgCatalogProvider.PG_CATALOG_OID);
    }
    if (!builder.hasRelkind()) {
      builder.setRelkind(node instanceof ViewNode ? "v" : "r");
    }
    if (!builder.hasRelowner()) {
      builder.setRelowner(ScannerUtils.defaultOwnerOid());
    }
  }

  private static void applyFunctionDefaults(
      FloeFunctionSpecific.Builder builder,
      MetadataResolutionContext ctx,
      ResourceId namespaceId,
      FunctionNode function) {
    if (ctx == null) {
      return;
    }
    CatalogOverlay overlay = ctx.overlay();
    EngineContext engineContext = ctx.engineContext();
    if (!builder.hasOid()) {
      builder.setOid(
          ScannerUtils.oid(
              overlay, function.id(), FUNCTION, FloeFunctionSpecific::getOid, engineContext));
    }
    builder.setPronamespace(
        ScannerUtils.oid(
            overlay, namespaceId, NAMESPACE, FloeNamespaceSpecific::getOid, engineContext));
    if (!builder.hasProrettype()) {
      builder.setProrettype(
          ScannerUtils.oid(
              overlay,
              function.id(),
              FUNCTION,
              FloeFunctionSpecific::getProrettype,
              engineContext));
    }
    if (builder.getProargtypesCount() == 0) {
      int[] types =
          ScannerUtils.array(
              overlay,
              function.id(),
              FUNCTION,
              s -> s.getProargtypesList().stream().mapToInt(Integer::intValue).toArray(),
              engineContext);
      Arrays.stream(types).forEach(builder::addProargtypes);
    }
  }

  private static void applyTypeDefaults(FloeTypeSpecific.Builder builder, TypeNode type) {
    if (!builder.hasTypname()) {
      builder.setTypname(type.displayName());
    }
    if (!builder.hasTypnamespace()) {
      builder.setTypnamespace(
          ai.floedb.floecat.extensions.floedb.pgcatalog.PgCatalogProvider.PG_CATALOG_OID);
    }
    if (!builder.hasTypowner()) {
      builder.setTypowner(ScannerUtils.defaultOwnerOid());
    }
    if (!builder.hasTyplen()) {
      builder.setTyplen(-1);
    }
    if (!builder.hasTyptype()) {
      builder.setTyptype("b");
    }
    if (!builder.hasTypcategory()) {
      builder.setTypcategory("U");
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
    LogicalType logicalType = logical == null ? parseLogicalType(column) : logical;
    if (logicalType == null) {
      return null;
    }
    CatalogOverlay overlay = ctx.overlay();
    EngineContext engineContext = ctx.engineContext();
    try {
      int typmod = deriveTypmod(logical);
      TypeNode type = resolver.resolveOrThrow(logical);
      int typeOid =
          ScannerUtils.oid(overlay, type.id(), TYPE, FloeTypeSpecific::getOid, engineContext);
      Optional<FloeTypeSpecific> typeSpec =
          ScannerUtils.payload(overlay, type.id(), TYPE, engineContext);
      FloeTypeSpecific specific =
          typeSpec.isPresent()
              ? typeSpec.get()
              : FloeTypeSpecific.newBuilder().setOid(typeOid).build();
      int attlen = typeSpec.map(FloeTypeSpecific::getTyplen).orElse(-1);
      String attalign = specific.getTypalign().isBlank() ? "i" : specific.getTypalign();
      String attstorage = specific.getTypstorage().isBlank() ? "p" : specific.getTypstorage();
      int attndims = typeSpec.map(FloeTypeSpecific::getTypndims).orElse(0);
      int attcollation = typeSpec.map(FloeTypeSpecific::getTypcollation).orElse(0);
      return new ColumnMetadata(
          logical,
          type,
          typeOid,
          specific,
          typmod,
          attlen,
          attalign,
          attstorage,
          attndims,
          attcollation);
    } catch (RuntimeException e) {
      return null;
    }
  }

  private static int fallbackTypeOid(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      SchemaColumn column,
      LogicalType logical) {
    if (ctx == null || resolver == null) {
      return ScannerUtils.fallbackOid(fallbackTypeId(column));
    }
    LogicalType logicalType = logical == null ? parseLogicalType(column) : logical;
    if (logicalType == null) {
      return ScannerUtils.fallbackOid(fallbackTypeId(column));
    }
    Optional<TypeNode> resolved = resolver.resolve(logical);
    return resolved
        .map(
            type ->
                ScannerUtils.oid(
                    ctx.overlay(), type.id(), TYPE, FloeTypeSpecific::getOid, ctx.engineContext()))
        .orElseGet(() -> ScannerUtils.fallbackOid(fallbackTypeId(column)));
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

  private static int deriveTypmod(LogicalType logical) {
    if (logical == null) {
      return -1;
    }
    if (logical.isDecimal()) {
      return (logical.precision() << 16) | logical.scale();
    }
    return -1;
  }

  private static ResourceId fallbackTypeId(SchemaColumn column) {
    String key = column == null ? "unknown" : column.getLogicalType();
    if (key == null || key.isBlank()) {
      key = column == null ? "column" : column.getName();
    }
    return ResourceId.newBuilder()
        .setAccountId("floecat")
        .setKind(ResourceKind.RK_TYPE)
        .setId("fallback:" + key)
        .build();
  }

  private static <T> Optional<T> payload(
      MetadataResolutionContext ctx, ResourceId id, PayloadDescriptor<T> descriptor) {
    if (ctx == null) {
      return Optional.empty();
    }
    return ScannerUtils.payload(ctx.overlay(), id, descriptor, ctx.engineContext());
  }

  private record ColumnMetadata(
      LogicalType logicalType,
      TypeNode typeNode,
      int typeOid,
      FloeTypeSpecific typeSpecific,
      int typmod,
      int attlen,
      String attalign,
      String attstorage,
      int attndims,
      int attcollation) {}
}
