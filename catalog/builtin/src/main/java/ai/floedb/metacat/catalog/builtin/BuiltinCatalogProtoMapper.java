package ai.floedb.metacat.catalog.builtin;

import ai.floedb.metacat.catalog.rpc.BuiltinAggregate;
import ai.floedb.metacat.catalog.rpc.BuiltinCast;
import ai.floedb.metacat.catalog.rpc.BuiltinCatalog;
import ai.floedb.metacat.catalog.rpc.BuiltinCollation;
import ai.floedb.metacat.catalog.rpc.BuiltinEngineSpecific;
import ai.floedb.metacat.catalog.rpc.BuiltinFunction;
import ai.floedb.metacat.catalog.rpc.BuiltinOperator;
import ai.floedb.metacat.catalog.rpc.BuiltinType;
import java.util.Objects;

/** Helper to convert between protobuf `BuiltinCatalog` messages and in-memory records. */
public final class BuiltinCatalogProtoMapper {

  private BuiltinCatalogProtoMapper() {}

  public static BuiltinCatalog toProto(BuiltinCatalogData catalog) {
    Objects.requireNonNull(catalog, "catalog");

    var builder = BuiltinCatalog.newBuilder().setVersion(catalog.version());
    catalog.functions().forEach(fn -> builder.addFunctions(toProto(fn)));
    catalog.operators().forEach(op -> builder.addOperators(toProto(op)));
    catalog.types().forEach(t -> builder.addTypes(toProto(t)));
    catalog.casts().forEach(c -> builder.addCasts(toProto(c)));
    catalog.collations().forEach(c -> builder.addCollations(toProto(c)));
    catalog.aggregates().forEach(a -> builder.addAggregates(toProto(a)));
    return builder.build();
  }

  public static BuiltinCatalogData fromProto(BuiltinCatalog proto) {
    Objects.requireNonNull(proto, "proto");
    return new BuiltinCatalogData(
        proto.getVersion(),
        proto.getFunctionsList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoFunction)
            .toList(),
        proto.getOperatorsList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoOperator)
            .toList(),
        proto.getTypesList().stream().map(BuiltinCatalogProtoMapper::fromProtoType).toList(),
        proto.getCastsList().stream().map(BuiltinCatalogProtoMapper::fromProtoCast).toList(),
        proto.getCollationsList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoCollation)
            .toList(),
        proto.getAggregatesList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoAggregate)
            .toList());
  }

  private static BuiltinFunction toProto(BuiltinFunctionDef def) {
    var builder =
        BuiltinFunction.newBuilder()
            .setName(def.name())
            .addAllArgumentTypes(def.argumentTypes())
            .setReturnType(def.returnType())
            .setIsAggregate(def.aggregate())
            .setIsWindow(def.window())
            .setIsStrict(def.strict())
            .setIsImmutable(def.immutable());
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinFunctionDef fromProtoFunction(BuiltinFunction proto) {
    return new BuiltinFunctionDef(
        proto.getName(),
        proto.getArgumentTypesList(),
        proto.getReturnType(),
        proto.getIsAggregate(),
        proto.getIsWindow(),
        proto.getIsStrict(),
        proto.getIsImmutable(),
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinOperator toProto(BuiltinOperatorDef def) {
    var builder =
        BuiltinOperator.newBuilder()
            .setName(def.name())
            .setLeftType(def.leftType())
            .setRightType(def.rightType())
            .setFunctionName(def.functionName());
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinOperatorDef fromProtoOperator(BuiltinOperator proto) {
    return new BuiltinOperatorDef(
        proto.getName(),
        proto.getLeftType(),
        proto.getRightType(),
        proto.getFunctionName(),
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinType toProto(BuiltinTypeDef def) {
    var builder =
        BuiltinType.newBuilder()
            .setName(def.name())
            .setCategory(def.category())
            .setIsArray(def.array());
    if (def.oid() != null) {
      builder.setOid(def.oid());
    }
    if (def.array() && def.elementType() != null && !def.elementType().isBlank()) {
      builder.setElementType(def.elementType());
    }
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinTypeDef fromProtoType(BuiltinType proto) {
    Integer oid = proto.getOid() == 0 ? null : proto.getOid();
    String elementType =
        proto.getIsArray() && !proto.getElementType().isBlank() ? proto.getElementType() : null;
    return new BuiltinTypeDef(
        proto.getName(),
        oid,
        proto.getCategory(),
        proto.getIsArray(),
        elementType,
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinCast toProto(BuiltinCastDef def) {
    var builder =
        BuiltinCast.newBuilder()
            .setSourceType(def.sourceType())
            .setTargetType(def.targetType())
            .setMethod(def.method().wireValue());
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinCastDef fromProtoCast(BuiltinCast proto) {
    return new BuiltinCastDef(
        proto.getSourceType(),
        proto.getTargetType(),
        BuiltinCastMethod.fromWireValue(proto.getMethod()),
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinCollation toProto(BuiltinCollationDef def) {
    var builder = BuiltinCollation.newBuilder().setName(def.name()).setLocale(def.locale());
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinCollationDef fromProtoCollation(BuiltinCollation proto) {
    return new BuiltinCollationDef(
        proto.getName(),
        proto.getLocale(),
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinAggregate toProto(BuiltinAggregateDef def) {
    var builder =
        BuiltinAggregate.newBuilder()
            .setName(def.name())
            .addAllArgumentTypes(def.argumentTypes())
            .setStateType(def.stateType())
            .setReturnType(def.returnType());
    if (def.stateFunction() != null && !def.stateFunction().isBlank()) {
      builder.setStateFn(def.stateFunction());
    }
    if (def.finalFunction() != null && !def.finalFunction().isBlank()) {
      builder.setFinalFn(def.finalFunction());
    }
    def.engineSpecific().forEach(rule -> builder.addEngineSpecific(toProto(rule)));
    return builder.build();
  }

  private static BuiltinAggregateDef fromProtoAggregate(BuiltinAggregate proto) {
    String stateFn = proto.getStateFn().isBlank() ? null : proto.getStateFn();
    String finalFn = proto.getFinalFn().isBlank() ? null : proto.getFinalFn();
    return new BuiltinAggregateDef(
        proto.getName(),
        proto.getArgumentTypesList(),
        proto.getStateType(),
        proto.getReturnType(),
        stateFn,
        finalFn,
        proto.getEngineSpecificList().stream()
            .map(BuiltinCatalogProtoMapper::fromProtoRule)
            .toList());
  }

  private static BuiltinEngineSpecific toProto(EngineSpecificRule def) {
    var builder = BuiltinEngineSpecific.newBuilder();
    if (!def.engineKind().isBlank()) {
      builder.setEngineKind(def.engineKind());
    }
    if (!def.minVersion().isBlank()) {
      builder.setMinVersion(def.minVersion());
    }
    if (!def.maxVersion().isBlank()) {
      builder.setMaxVersion(def.maxVersion());
    }
    builder.putAllProperties(def.properties());
    return builder.build();
  }

  private static EngineSpecificRule fromProtoRule(BuiltinEngineSpecific proto) {
    return new EngineSpecificRule(
        proto.getEngineKind(),
        proto.getMinVersion(),
        proto.getMaxVersion(),
        proto.getPropertiesMap());
  }
}
