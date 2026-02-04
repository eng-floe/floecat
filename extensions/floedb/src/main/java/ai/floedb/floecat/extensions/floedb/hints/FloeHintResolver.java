/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
      builder.setOid(ScannerUtils.fallbackOid(type.id(), TYPE.type()));
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
    ColumnMetadata metadata = columnMetadata(ctx, resolver, column, logicalType);
    return buildColumnSpecific(column, attnum, metadata);
  }

  public static ColumnMetadata columnMetadata(
      MetadataResolutionContext ctx,
      TypeResolver resolver,
      SchemaColumn column,
      LogicalType logicalType) {
    LogicalType resolved = logicalType == null ? parseLogicalType(column) : logicalType;

    // If we cannot parse/resolve a logical type, still return a stable, safe metadata bundle.
    // This keeps pg_attribute rows well-formed even when upstream logical types are
    // missing/invalid.
    if (resolved == null) {
      int typeOid = ScannerUtils.fallbackOid(fallbackTypeId(column), TYPE.type());
      return new ColumnMetadata(typeOid, -1, -1, "i", false, "p", 0, 0);
    }

    ColumnMetadata metadata = resolveColumnMetadata(ctx, resolver, column, resolved);
    if (metadata != null) {
      return metadata;
    }

    int typeOid = fallbackTypeOid(ctx, resolver, column, resolved);
    int typmod = deriveTypmod(resolved);
    return new ColumnMetadata(typeOid, typmod, -1, "i", passByValue(resolved), "p", 0, 0);
  }

  public static FloeColumnSpecific buildColumnSpecific(
      SchemaColumn column, int attnum, ColumnMetadata metadata) {
    FloeColumnSpecific.Builder builder = FloeColumnSpecific.newBuilder();
    builder
        .setAttname(column == null ? "" : column.getName())
        .setAttnum(attnum)
        .setAttnotnull(column != null && !column.getNullable())
        .setAttisdropped(false)
        .setAtttypid(metadata.typeOid())
        .setAtttypmod(metadata.typmod())
        .setAttcollation(metadata.attcollation())
        .setAtthasdef(false);
    return builder.build();
  }

  private static void applyNamespaceDefaults(
      FloeNamespaceSpecific.Builder builder, NamespaceNode namespace) {
    if (!builder.hasOid()) {
      builder.setOid(ScannerUtils.fallbackOid(namespace.id(), NAMESPACE.type()));
    }
    if (!builder.hasNspname()) {
      builder.setNspname(namespace.displayName());
    }
  }

  private static void applyRelationDefaults(FloeRelationSpecific.Builder builder, GraphNode node) {
    if (!builder.hasOid()) {
      builder.setOid(ScannerUtils.fallbackOid(node.id(), RELATION.type()));
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
    LogicalType logicalType = logical == null ? parseLogicalType(column) : logical;
    if (logicalType == null) {
      return null;
    }
    CatalogOverlay overlay = ctx.overlay();
    EngineContext engineContext = ctx.engineContext();
    try {
      int typmod = deriveTypmod(logicalType);
      TypeNode type = resolver.resolveOrThrow(logicalType);
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
      boolean attbyval =
          typeSpec
              .filter(FloeTypeSpecific::hasTypbyval)
              .map(FloeTypeSpecific::getTypbyval)
              .orElse(passByValue(logicalType));
      int attcollation = typeSpec.map(FloeTypeSpecific::getTypcollation).orElse(0);
      return new ColumnMetadata(typeOid, typmod, attlen, attalign, attbyval, "p", 0, attcollation);
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
      return ScannerUtils.fallbackOid(fallbackTypeId(column), TYPE.type());
    }
    LogicalType logicalType = logical == null ? parseLogicalType(column) : logical;
    if (logicalType == null) {
      return ScannerUtils.fallbackOid(fallbackTypeId(column), TYPE.type());
    }
    Optional<TypeNode> resolved = resolver.resolve(logicalType);
    return resolved
        .map(
            type ->
                ScannerUtils.oid(
                    ctx.overlay(), type.id(), TYPE, FloeTypeSpecific::getOid, ctx.engineContext()))
        .orElseGet(() -> ScannerUtils.fallbackOid(fallbackTypeId(column), TYPE.type()));
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

  private static boolean passByValue(LogicalType logical) {
    if (logical == null) {
      return false;
    }
    switch (logical.kind()) {
      case BOOLEAN, INT16, INT32, INT64, FLOAT32, FLOAT64, DATE, TIME, TIMESTAMP, UUID:
        return true;
      default:
        return false;
    }
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
