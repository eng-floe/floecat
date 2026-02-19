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

package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.query.rpc.EngineSpecific;
import ai.floedb.floecat.query.rpc.FloeCatTableDetails;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.SqlAggregate;
import ai.floedb.floecat.query.rpc.SqlCast;
import ai.floedb.floecat.query.rpc.SqlCollation;
import ai.floedb.floecat.query.rpc.SqlFunction;
import ai.floedb.floecat.query.rpc.SqlOperator;
import ai.floedb.floecat.query.rpc.SqlType;
import ai.floedb.floecat.query.rpc.StorageTableDetails;
import ai.floedb.floecat.query.rpc.SystemColumn;
import ai.floedb.floecat.query.rpc.SystemNamespace;
import ai.floedb.floecat.query.rpc.SystemObjectsRegistry;
import ai.floedb.floecat.query.rpc.SystemTable;
import ai.floedb.floecat.query.rpc.SystemView;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastMethod;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import com.google.protobuf.ByteString;
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

  public static SystemObjectsRegistry toProto(SystemCatalogData catalog) {
    Objects.requireNonNull(catalog, "catalog");
    var builder = SystemObjectsRegistry.newBuilder();

    catalog.functions().forEach(f -> builder.addFunctions(toProtoFunction(f)));
    catalog.operators().forEach(o -> builder.addOperators(toProtoOperator(o)));
    catalog.types().forEach(t -> builder.addTypes(toProtoType(t)));
    catalog.casts().forEach(c -> builder.addCasts(toProtoCast(c)));
    catalog.collations().forEach(c -> builder.addCollations(toProtoCollation(c)));
    catalog.aggregates().forEach(a -> builder.addAggregates(toProtoAggregate(a)));
    catalog.namespaces().forEach(ns -> builder.addSystemNamespaces(toProtoNamespace(ns)));
    catalog.tables().forEach(tbl -> builder.addSystemTables(toProtoTable(tbl)));
    catalog.views().forEach(view -> builder.addSystemViews(toProtoView(view)));
    catalog.registryEngineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));

    return builder.build();
  }

  public static SystemCatalogData fromProto(SystemObjectsRegistry proto) {
    return fromProto(proto, "");
  }

  public static SystemCatalogData fromProto(SystemObjectsRegistry proto, String defaultEngine) {
    Objects.requireNonNull(proto, "proto");

    return new SystemCatalogData(
        proto.getFunctionsList().stream().map(f -> fromProtoFunction(f, defaultEngine)).toList(),
        proto.getOperatorsList().stream().map(o -> fromProtoOperator(o, defaultEngine)).toList(),
        proto.getTypesList().stream().map(t -> fromProtoType(t, defaultEngine)).toList(),
        proto.getCastsList().stream().map(c -> fromProtoCast(c, defaultEngine)).toList(),
        proto.getCollationsList().stream().map(c -> fromProtoCollation(c, defaultEngine)).toList(),
        proto.getAggregatesList().stream().map(a -> fromProtoAggregate(a, defaultEngine)).toList(),
        proto.getSystemNamespacesList().stream()
            .map(ns -> fromProtoNamespace(ns, defaultEngine))
            .toList(),
        proto.getSystemTablesList().stream()
            .map(tbl -> fromProtoTable(tbl, defaultEngine))
            .toList(),
        proto.getSystemViewsList().stream()
            .map(view -> fromProtoView(view, defaultEngine))
            .toList(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngine))
            .toList());
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
  //  Relation mapping
  // =========================================================================

  private static SystemNamespace toProtoNamespace(SystemNamespaceDef def) {
    var builder =
        SystemNamespace.newBuilder().setName(def.name()).setDisplayName(def.displayName());
    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemNamespaceDef fromProtoNamespace(
      SystemNamespace proto, String defaultEngineKind) {
    return new SystemNamespaceDef(
        proto.getName(),
        proto.getDisplayName(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngineKind))
            .toList());
  }

  private static SystemTable toProtoTable(SystemTableDef def) {
    var builder =
        SystemTable.newBuilder()
            .setName(def.name())
            .setDisplayName(def.displayName())
            .setBackendKind(def.backendKind());
    switch (def.backendKind()) {
      case TABLE_BACKEND_KIND_FLOECAT ->
          builder.setFloecat(FloeCatTableDetails.newBuilder().setScannerId(def.scannerId()));
      case TABLE_BACKEND_KIND_STORAGE -> {
        var storageBuilder = StorageTableDetails.newBuilder();
        if (def.flightEndpoint() != null) {
          storageBuilder.setFlightEndpoint(def.flightEndpoint());
        } else if (!def.storagePath().isBlank()) {
          storageBuilder.setPath(def.storagePath());
        }
        builder.setStorage(storageBuilder);
      }
      default -> {}
    }
    def.columns().forEach(col -> builder.addColumns(toProtoColumn(col)));
    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemTableDef fromProtoTable(SystemTable proto, String defaultEngineKind) {
    ai.floedb.floecat.query.rpc.FlightEndpointRef flightEndpoint = null;
    if (proto.hasStorage() && proto.getStorage().hasFlightEndpoint()) {
      flightEndpoint = proto.getStorage().getFlightEndpoint();
    }
    return new SystemTableDef(
        proto.getName(),
        proto.getDisplayName(),
        proto.getColumnsList().stream()
            .map(col -> fromProtoColumn(col, defaultEngineKind))
            .toList(),
        proto.getBackendKind(),
        proto.hasFloecat() ? proto.getFloecat().getScannerId() : "",
        proto.hasStorage() ? proto.getStorage().getPath() : "",
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngineKind))
            .toList(),
        flightEndpoint);
  }

  private static SystemView toProtoView(SystemViewDef def) {
    var builder =
        SystemView.newBuilder()
            .setName(def.name())
            .setDisplayName(def.displayName())
            .setSql(def.sql())
            .setDialect(def.dialect());
    def.columns().forEach(col -> builder.addColumns(toProtoColumn(col)));
    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemViewDef fromProtoView(SystemView proto, String defaultEngineKind) {
    return new SystemViewDef(
        proto.getName(),
        proto.getDisplayName(),
        proto.getSql(),
        proto.getDialect(),
        proto.getColumnsList().stream()
            .map(col -> fromProtoColumn(col, defaultEngineKind))
            .toList(),
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngineKind))
            .toList());
  }

  private static SystemColumn toProtoColumn(SystemColumnDef def) {
    var builder =
        SystemColumn.newBuilder()
            .setName(def.name())
            .setType(def.type())
            .setNullable(def.nullable())
            .setOrdinal(def.ordinal());
    if (def.hasId()) {
      builder.setId(def.id());
    }
    def.engineSpecific().forEach(es -> builder.addEngineSpecific(toProtoRule(es)));
    return builder.build();
  }

  private static SystemColumnDef fromProtoColumn(SystemColumn proto, String defaultEngineKind) {
    return new SystemColumnDef(
        proto.getName(),
        proto.getType(),
        proto.getNullable(),
        proto.getOrdinal(),
        proto.getId() != 0 ? proto.getId() : null,
        proto.getEngineSpecificList().stream()
            .map(es -> fromProtoRule(es, defaultEngineKind))
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
