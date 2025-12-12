package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.*;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Objects;

/**
 * Maps between engine-neutral builtin definitions (Builtin*Def using NameRef) and the wire protocol
 * (SqlFunction, SqlOperator, etc.) which also uses NameRef.
 */
public final class SystemCatalogProtoMapper {

  private SystemCatalogProtoMapper() {}

  // =========================================================================
  //  Top-level Registry
  // =========================================================================

  public static BuiltinRegistry toProto(SystemCatalogData catalog) {
    Objects.requireNonNull(catalog, "catalog");
    var builder = BuiltinRegistry.newBuilder();

    catalog.functions().forEach(f -> builder.addFunctions(toProtoFunction(f)));
    catalog.operators().forEach(o -> builder.addOperators(toProtoOperator(o)));
    catalog.types().forEach(t -> builder.addTypes(toProtoType(t)));
    catalog.casts().forEach(c -> builder.addCasts(toProtoCast(c)));
    catalog.collations().forEach(c -> builder.addCollations(toProtoCollation(c)));
    catalog.aggregates().forEach(a -> builder.addAggregates(toProtoAggregate(a)));

    return builder.build();
  }

  public static SystemCatalogData fromProto(BuiltinRegistry proto) {
    return fromProto(proto, "");
  }

  public static SystemCatalogData fromProto(BuiltinRegistry proto, String defaultEngine) {
    Objects.requireNonNull(proto, "proto");

    return new SystemCatalogData(
        proto.getFunctionsList().stream().map(f -> fromProtoFunction(f, defaultEngine)).toList(),
        proto.getOperatorsList().stream().map(o -> fromProtoOperator(o, defaultEngine)).toList(),
        proto.getTypesList().stream().map(t -> fromProtoType(t, defaultEngine)).toList(),
        proto.getCastsList().stream().map(c -> fromProtoCast(c, defaultEngine)).toList(),
        proto.getCollationsList().stream().map(c -> fromProtoCollation(c, defaultEngine)).toList(),
        proto.getAggregatesList().stream().map(a -> fromProtoAggregate(a, defaultEngine)).toList(),
        List.of(),
        List.of(),
        List.of());
  }

  // =========================================================================
  //  SqlFunction
  // =========================================================================

  private static SqlFunction toProtoFunction(SystemFunctionDef def) {
    var builder =
        SqlFunction.newBuilder()
            .setName(def.name())
            .addAllArgumentTypes(def.argumentTypes())
            .setReturnType(def.returnType())
            .setIsAggregate(def.isAggregate())
            .setIsWindow(def.isWindow())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemFunctionDef fromProtoFunction(SqlFunction proto, String defaultEngine) {
    return new SystemFunctionDef(
        proto.getName(),
        proto.getArgumentTypesList(),
        proto.getReturnType(),
        proto.getIsAggregate(),
        proto.getIsWindow(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  SqlOperator
  // =========================================================================

  private static SqlOperator toProtoOperator(SystemOperatorDef def) {
    var builder =
        SqlOperator.newBuilder()
            .setName(def.name())
            .setLeftType(def.leftType())
            .setRightType(def.rightType())
            .setReturnType(def.returnType())
            .setIsCommutative(def.isCommutative())
            .setIsAssociative(def.isAssociative())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemOperatorDef fromProtoOperator(SqlOperator proto, String defaultEngine) {
    return new SystemOperatorDef(
        proto.getName(),
        proto.getLeftType(),
        proto.getRightType(),
        proto.getReturnType(),
        proto.getIsCommutative(),
        proto.getIsAssociative(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  SqlType
  // =========================================================================

  private static SqlType toProtoType(SystemTypeDef def) {
    var builder =
        SqlType.newBuilder()
            .setName(def.name())
            .setCategory(def.category())
            .setIsArray(def.array())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    if (def.array() && def.elementType() != null) {
      builder.setElementType(def.elementType());
    }

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemTypeDef fromProtoType(SqlType proto, String defaultEngine) {
    NameRef elem =
        proto.getIsArray() && proto.hasElementType() && !proto.getElementType().getName().isBlank()
            ? proto.getElementType()
            : null;

    return new SystemTypeDef(
        proto.getName(),
        proto.getCategory(),
        proto.getIsArray(),
        elem,
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  SqlCast
  // =========================================================================

  private static SqlCast toProtoCast(SystemCastDef def) {
    var builder =
        SqlCast.newBuilder()
            .setName(def.name())
            .setSourceType(def.sourceType())
            .setTargetType(def.targetType())
            .setMethod(def.method().wireValue())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemCastDef fromProtoCast(SqlCast proto, String defaultEngine) {
    return new SystemCastDef(
        proto.getName(),
        proto.getSourceType(),
        proto.getTargetType(),
        SystemCastMethod.fromWireValue(proto.getMethod()),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  SqlCollation
  // =========================================================================

  private static SqlCollation toProtoCollation(SystemCollationDef def) {
    var builder =
        SqlCollation.newBuilder()
            .setName(def.name())
            .setLocale(def.locale())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemCollationDef fromProtoCollation(SqlCollation proto, String defaultEngine) {
    return new SystemCollationDef(
        proto.getName(),
        proto.getLocale(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  SqlAggregate
  // =========================================================================

  private static SqlAggregate toProtoAggregate(SystemAggregateDef def) {
    var builder =
        SqlAggregate.newBuilder()
            .setName(def.name())
            .addAllArgumentTypes(def.argumentTypes())
            .setStateType(def.stateType())
            .setReturnType(def.returnType())
            .setOrigin(Origin.ORIGIN_BUILTIN);

    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemAggregateDef fromProtoAggregate(SqlAggregate proto, String defaultEngine) {
    return new SystemAggregateDef(
        proto.getName(),
        proto.getArgumentTypesList(),
        proto.getStateType(),
        proto.getReturnType(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
  }

  // =========================================================================
  //  EngineSpecific <-> EngineSpecificRule
  // =========================================================================

  private static EngineSpecific toProtoRule(EngineSpecificRule rule) {
    var builder =
        EngineSpecific.newBuilder()
            .setEngineKind(rule.engineKind())
            .setMinVersion(rule.minVersion())
            .setMaxVersion(rule.maxVersion())
            .putAllProperties(rule.properties());

    if (rule.hasExtensionPayload()) {
      builder
          .setPayloadType(rule.payloadType())
          .setPayload(ByteString.copyFrom(rule.extensionPayload()));
    }

    return builder.build();
  }

  private static EngineSpecificRule fromProtoRule(EngineSpecific proto, String defaultEngineKind) {
    String engineKind = proto.getEngineKind().isBlank() ? defaultEngineKind : proto.getEngineKind();

    byte[] payload = proto.getPayload().isEmpty() ? new byte[0] : proto.getPayload().toByteArray();

    return new EngineSpecificRule(
        engineKind,
        proto.getMinVersion(),
        proto.getMaxVersion(),
        proto.getPayloadType(),
        payload,
        proto.getPropertiesMap());
  }
}
