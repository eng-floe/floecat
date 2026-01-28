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

package ai.floedb.floecat.systemcatalog.validation;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. Uses
 * NameRef everywhere.
 */
public final class SystemCatalogValidator {

  private SystemCatalogValidator() {}

  interface Codes {
    String CATALOG_NULL = "catalog.null";

    interface Type {
      String TYPES_EMPTY = "types.empty";
      String NAME_REQUIRED = "type.name.required";
      String DUPLICATE = "type.duplicate";
      String CATEGORY_REQUIRED = "type.category.required";
      String CATEGORY_INVALID = "type.category.invalid";
      String ELEMENT_REQUIRED = "type.element.required";
      String ELEMENT_SELF = "type.element.self";
      String UNKNOWN = "type.unknown";
    }

    interface Function {
      String EMPTY = "functions.empty";
      String NAME_REQUIRED = "function.name.required";
      String DUPLICATE = "function.duplicate";
      String RETURN_TYPE_REQUIRED = "function.return.type.required";
      String RETURN_TYPE_UNKNOWN = "function.return.type.unknown";
      String ARG_TYPE_REQUIRED = "function.arg.type.required";
      String ARG_TYPE_UNKNOWN = "function.arg.type.unknown";
    }

    interface Operator {
      String NAME_REQUIRED = "operator.name.required";
      String DUPLICATE = "operator.duplicate";
      String LEFT_TYPE_UNKNOWN = "operator.left.type.unknown";
      String RIGHT_TYPE_REQUIRED = "operator.right.type.required";
      String RIGHT_TYPE_UNKNOWN = "operator.right.type.unknown";
      String RETURN_TYPE_REQUIRED = "operator.return.type.required";
      String RETURN_TYPE_UNKNOWN = "operator.return.type.unknown";
    }

    interface Cast {
      String NAME_REQUIRED = "cast.name.required";
      String NAME_DUPLICATE = "cast.name.duplicate";
      String DUPLICATE = "cast.duplicate";
      String SOURCE_TYPE_REQUIRED = "cast.source.type.required";
      String SOURCE_TYPE_UNKNOWN = "cast.source.type.unknown";
      String TARGET_TYPE_REQUIRED = "cast.target.type.required";
      String TARGET_TYPE_UNKNOWN = "cast.target.type.unknown";
    }

    interface Collation {
      String NAME_REQUIRED = "collation.name.required";
      String LOCALE_REQUIRED = "collation.locale.required";
      String DUPLICATE = "collation.duplicate";
    }

    interface Aggregate {
      String NAME_REQUIRED = "agg.name.required";
      String DUPLICATE = "agg.duplicate";
      String STATE_TYPE_REQUIRED = "agg.state.type.required";
      String STATE_TYPE_UNKNOWN = "agg.state.type.unknown";
      String RETURN_TYPE_REQUIRED = "agg.return.type.required";
      String RETURN_TYPE_UNKNOWN = "agg.return.type.unknown";
      String ARG_TYPE_REQUIRED = "agg.arg.type.required";
      String ARG_TYPE_UNKNOWN = "agg.arg.type.unknown";
    }

    interface Namespace {
      String NAME_REQUIRED = "namespace.name.required";
      String UNQUALIFIED_REQUIRED = "namespace.name.unqualified.required";
      String DUPLICATE = "namespace.duplicate";
    }

    interface Relation {
      String TABLE_NAME_REQUIRED = "table.name.required";
      String TABLE_DUPLICATE = "table.duplicate";
      String TABLE_QUALIFIED_REQUIRED = "table.name.qualified.required";
      String TABLE_NAMESPACE_UNKNOWN = "table.namespace.unknown";
      String VIEW_NAME_REQUIRED = "view.name.required";
      String VIEW_DUPLICATE = "view.duplicate";
      String VIEW_QUALIFIED_REQUIRED = "view.name.qualified.required";
      String VIEW_NAMESPACE_UNKNOWN = "view.namespace.unknown";
    }

    interface EngineSpecific {
      String PAYLOAD_REQUIRED = "engine_specific.payload_type.required";
      String REGISTRY_DUPLICATE = "registry.engineSpecific.duplicate";
    }

    interface Column {
      String SCHEMA_REQUIRED = "floe.column.schema.required";
      String ORDINAL_DUPLICATE = "floe.column.ordinal_duplicate";
    }
  }

  /** Runs validation and returns structured issues. */
  public static List<ValidationIssue> validate(SystemCatalogData catalog) {
    List<ValidationIssue> issues = new ArrayList<>();
    if (catalog == null) {
      err(issues, Codes.CATALOG_NULL, "catalog", null);
      return issues;
    }

    Set<NameRef> typeNames = validateTypes(catalog.types(), issues);
    verifyTypeDetails(catalog.types(), typeNames, issues);

    validateFunctions(catalog.functions(), typeNames, issues);
    validateOperators(catalog.operators(), typeNames, issues);
    validateCasts(catalog.casts(), typeNames, issues);
    validateCollations(catalog.collations(), issues);
    validateAggregates(catalog.aggregates(), typeNames, issues);

    validateNamespacesTablesAndViews(catalog, issues);

    validateEngineSpecificPayloadTypes(catalog, issues);

    return issues;
  }

  // ------------------------------------------------------------
  // Types
  // ------------------------------------------------------------

  private static Set<NameRef> validateTypes(
      List<SystemTypeDef> types, List<ValidationIssue> issues) {
    if (types == null || types.isEmpty()) {
      err(issues, Codes.Type.TYPES_EMPTY, "types", null);
      return Set.of();
    }

    Set<NameRef> typeNames = new HashSet<>();
    for (SystemTypeDef type : types) {
      if (type == null) {
        continue;
      }
      NameRef name = type.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Type.NAME_REQUIRED, "type", null);
        continue;
      }
      if (!typeNames.add(name)) {
        err(issues, Codes.Type.DUPLICATE, objectCtx("type", name), null, formatName(name));
      }
    }
    return typeNames;
  }

  private static void verifyTypeDetails(
      List<SystemTypeDef> types, Set<NameRef> typeNames, List<ValidationIssue> issues) {
    if (types == null) {
      return;
    }
    for (SystemTypeDef type : types) {
      if (type == null) {
        continue;
      }
      NameRef name = type.name();
      String ctx = objectCtx("type", name);

      String category = type.category();
      if (isBlank(category)) {
        err(issues, Codes.Type.CATEGORY_REQUIRED, ctx, null);
      } else if (category.length() != 1 || !Character.isUpperCase(category.charAt(0))) {
        err(issues, Codes.Type.CATEGORY_INVALID, ctx, null, category);
      }

      NameRef element = type.elementType();
      if (type.array()) {
        if (element == null || isBlank(element.getName())) {
          err(issues, Codes.Type.ELEMENT_REQUIRED, ctx, null);
        } else {
          requireTypeExists(
              element,
              typeNames,
              issues,
              objectCtx("type", name, "elementType"),
              Codes.Type.UNKNOWN,
              /* optional= */ false);
          if (name != null && element.equals(name)) {
            err(issues, Codes.Type.ELEMENT_SELF, ctx, null, formatName(name));
          }
        }
      } else if (element != null && !isBlank(element.getName())) {
        requireTypeExists(
            element,
            typeNames,
            issues,
            objectCtx("type", name, "elementType"),
            Codes.Type.UNKNOWN,
            /* optional= */ true);
      }
    }
  }

  // ------------------------------------------------------------
  // Functions
  // ------------------------------------------------------------

  private static void validateFunctions(
      List<SystemFunctionDef> functions, Set<NameRef> typeNames, List<ValidationIssue> issues) {

    if (functions == null || functions.isEmpty()) {
      err(issues, Codes.Function.EMPTY, "functions", null);
      return;
    }

    Set<String> signatures = new HashSet<>();
    for (SystemFunctionDef fn : functions) {
      if (fn == null) {
        continue;
      }
      NameRef name = fn.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Function.NAME_REQUIRED, "function", null);
        continue;
      }

      String fnCtx = objectCtx("function", name);
      String signature = functionSignature(fn);
      if (!signatures.add(signature)) {
        err(issues, Codes.Function.DUPLICATE, fnCtx, null, signature);
      }

      // return type must exist
      if (fn.returnType() == null || isBlank(fn.returnType().getName())) {
        err(issues, Codes.Function.RETURN_TYPE_REQUIRED, fnCtx, null);
      } else {
        requireTypeExists(
            fn.returnType(),
            typeNames,
            issues,
            objectCtx("function", name, "returnType"),
            Codes.Function.RETURN_TYPE_UNKNOWN,
            /* optional= */ false);
      }

      // argument types must exist
      List<NameRef> args = fn.argumentTypes();
      if (args != null) {
        for (int i = 0; i < args.size(); i++) {
          NameRef arg = args.get(i);
          String argCtx = objectCtx("function", name, "argTypes[" + i + "]");
          if (arg == null || isBlank(arg.getName())) {
            err(issues, Codes.Function.ARG_TYPE_REQUIRED, argCtx, null);
          } else {
            requireTypeExists(
                arg,
                typeNames,
                issues,
                argCtx,
                Codes.Function.ARG_TYPE_UNKNOWN,
                /* optional= */ false);
          }
        }
      }
    }
  }

  // ------------------------------------------------------------
  // Operators
  // ------------------------------------------------------------

  private static void validateOperators(
      List<SystemOperatorDef> operators, Set<NameRef> typeNames, List<ValidationIssue> issues) {

    if (operators == null) {
      return;
    }

    Set<String> signatures = new HashSet<>();
    for (SystemOperatorDef op : operators) {
      if (op == null) {
        continue;
      }
      NameRef name = op.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Operator.NAME_REQUIRED, "operator", null);
        continue;
      }

      String opCtx = objectCtx("operator", name);
      String signature = operatorSignature(op);
      if (!signatures.add(signature)) {
        err(issues, Codes.Operator.DUPLICATE, opCtx, null, signature);
      }

      // left type is optional (unary operators), but if present must exist
      if (op.leftType() != null && !isBlank(op.leftType().getName())) {
        requireTypeExists(
            op.leftType(),
            typeNames,
            issues,
            objectCtx("operator", name, "leftType"),
            Codes.Operator.LEFT_TYPE_UNKNOWN,
            /* optional= */ true);
      }

      // right + return required
      if (op.rightType() == null || isBlank(op.rightType().getName())) {
        err(
            issues,
            Codes.Operator.RIGHT_TYPE_REQUIRED,
            objectCtx("operator", name, "rightType"),
            null);
      } else {
        requireTypeExists(
            op.rightType(),
            typeNames,
            issues,
            objectCtx("operator", name, "rightType"),
            Codes.Operator.RIGHT_TYPE_UNKNOWN,
            /* optional= */ false);
      }

      if (op.returnType() == null || isBlank(op.returnType().getName())) {
        err(
            issues,
            Codes.Operator.RETURN_TYPE_REQUIRED,
            objectCtx("operator", name, "returnType"),
            null);
      } else {
        requireTypeExists(
            op.returnType(),
            typeNames,
            issues,
            objectCtx("operator", name, "returnType"),
            Codes.Operator.RETURN_TYPE_UNKNOWN,
            /* optional= */ false);
      }
    }
  }

  // ------------------------------------------------------------
  // Casts
  // ------------------------------------------------------------

  private static void validateCasts(
      List<SystemCastDef> casts, Set<NameRef> typeNames, List<ValidationIssue> issues) {

    if (casts == null) {
      return;
    }

    Set<NameRef> names = new HashSet<>();
    Set<String> mappings = new HashSet<>();

    for (SystemCastDef cast : casts) {
      if (cast == null) {
        continue;
      }
      NameRef name = cast.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Cast.NAME_REQUIRED, "cast", null);
        continue;
      }
      String castCtx = objectCtx("cast", name);

      if (!names.add(name)) {
        err(issues, Codes.Cast.NAME_DUPLICATE, castCtx, null, formatName(name));
      }

      NameRef src = cast.sourceType();
      NameRef tgt = cast.targetType();

      if (src == null || isBlank(src.getName())) {
        err(issues, Codes.Cast.SOURCE_TYPE_REQUIRED, objectCtx("cast", name, "sourceType"), null);
      } else {
        requireTypeExists(
            src,
            typeNames,
            issues,
            objectCtx("cast", name, "sourceType"),
            Codes.Cast.SOURCE_TYPE_UNKNOWN,
            false);
      }

      if (tgt == null || isBlank(tgt.getName())) {
        err(issues, Codes.Cast.TARGET_TYPE_REQUIRED, objectCtx("cast", name, "targetType"), null);
      } else {
        requireTypeExists(
            tgt,
            typeNames,
            issues,
            objectCtx("cast", name, "targetType"),
            Codes.Cast.TARGET_TYPE_UNKNOWN,
            false);
      }

      if (src != null && !isBlank(src.getName()) && tgt != null && !isBlank(tgt.getName())) {
        String mapping = castSignature(src, tgt);
        if (!mappings.add(mapping)) {
          err(issues, Codes.Cast.DUPLICATE, castCtx, null, mapping);
        }
      }
    }
  }

  // ------------------------------------------------------------
  // Collations
  // ------------------------------------------------------------

  private static void validateCollations(
      List<SystemCollationDef> collations, List<ValidationIssue> issues) {
    if (collations == null) {
      return;
    }

    Set<String> signatures = new HashSet<>();

    for (SystemCollationDef coll : collations) {
      if (coll == null) {
        continue;
      }
      NameRef name = coll.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Collation.NAME_REQUIRED, "collation", null);
        continue;
      }

      String ctx = objectCtx("collation", name);
      if (isBlank(coll.locale())) {
        err(issues, Codes.Collation.LOCALE_REQUIRED, ctx, null);
        continue;
      }

      String signature = collationSignature(coll);
      if (!signatures.add(signature)) {
        err(issues, Codes.Collation.DUPLICATE, ctx, null, signature);
      }
    }
  }

  // ------------------------------------------------------------
  // Aggregates
  // ------------------------------------------------------------

  private static void validateAggregates(
      List<SystemAggregateDef> aggregates, Set<NameRef> typeNames, List<ValidationIssue> issues) {

    if (aggregates == null) {
      return;
    }

    Set<String> signatures = new HashSet<>();

    for (SystemAggregateDef agg : aggregates) {
      if (agg == null) {
        continue;
      }
      NameRef name = agg.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Aggregate.NAME_REQUIRED, "aggregate", null);
        continue;
      }

      String aggCtx = objectCtx("aggregate", name);

      String signature = aggregateSignature(agg);
      if (!signatures.add(signature)) {
        err(issues, Codes.Aggregate.DUPLICATE, aggCtx, null, signature);
      }

      if (agg.stateType() == null || isBlank(agg.stateType().getName())) {
        err(
            issues,
            Codes.Aggregate.STATE_TYPE_REQUIRED,
            objectCtx("aggregate", name, "stateType"),
            null);
      } else {
        requireTypeExists(
            agg.stateType(),
            typeNames,
            issues,
            objectCtx("aggregate", name, "stateType"),
            Codes.Aggregate.STATE_TYPE_UNKNOWN,
            false);
      }

      if (agg.returnType() == null || isBlank(agg.returnType().getName())) {
        err(
            issues,
            Codes.Aggregate.RETURN_TYPE_REQUIRED,
            objectCtx("aggregate", name, "returnType"),
            null);
      } else {
        requireTypeExists(
            agg.returnType(),
            typeNames,
            issues,
            objectCtx("aggregate", name, "returnType"),
            Codes.Aggregate.RETURN_TYPE_UNKNOWN,
            false);
      }

      List<NameRef> args = agg.argumentTypes();
      if (args != null) {
        for (int i = 0; i < args.size(); i++) {
          NameRef arg = args.get(i);
          String argCtx = objectCtx("aggregate", name, "argTypes[" + i + "]");
          if (arg == null || isBlank(arg.getName())) {
            err(issues, Codes.Aggregate.ARG_TYPE_REQUIRED, argCtx, null);
          } else {
            requireTypeExists(
                arg,
                typeNames,
                issues,
                argCtx,
                Codes.Aggregate.ARG_TYPE_UNKNOWN,
                /* optional= */ false);
          }
        }
      }
    }
  }

  // ------------------------------------------------------------
  // Namespaces / Tables / Views
  // ------------------------------------------------------------

  private static void validateNamespacesTablesAndViews(
      SystemCatalogData catalog, List<ValidationIssue> issues) {
    Set<NameRef> namespaceNames = validateNamespaces(catalog.namespaces(), issues);
    validateTables(catalog.tables(), namespaceNames, issues);
    validateViews(catalog.views(), namespaceNames, issues);
  }

  private static Set<NameRef> validateNamespaces(
      List<SystemNamespaceDef> namespaces, List<ValidationIssue> issues) {
    if (namespaces == null) {
      return Set.of();
    }
    Set<NameRef> names = new HashSet<>();

    for (SystemNamespaceDef ns : namespaces) {
      if (ns == null) {
        continue;
      }
      NameRef name = ns.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Namespace.NAME_REQUIRED, "namespace", null);
        continue;
      }

      String ctx = objectCtx("namespace", name);

      if (!name.getPathList().isEmpty()) {
        err(issues, Codes.Namespace.UNQUALIFIED_REQUIRED, ctx, null, formatName(name));
      }
      if (!names.add(name)) {
        err(issues, Codes.Namespace.DUPLICATE, ctx, null, formatName(name));
      }
    }
    return names;
  }

  private static void validateTables(
      List<SystemTableDef> tables, Set<NameRef> namespaceNames, List<ValidationIssue> issues) {
    if (tables == null) {
      return;
    }

    Set<NameRef> seen = new HashSet<>();
    for (SystemTableDef tbl : tables) {
      if (tbl == null) {
        continue;
      }
      NameRef name = tbl.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Relation.TABLE_NAME_REQUIRED, "table", null);
        continue;
      }

      String ctx = objectCtx("table", name);

      if (!seen.add(name)) {
        err(issues, Codes.Relation.TABLE_DUPLICATE, ctx, null, formatName(name));
      }
      requireQualifiedObjectName("table", name, issues, Codes.Relation.TABLE_QUALIFIED_REQUIRED);
      requireNamespaceExists(
          "table", name, namespaceNames, issues, Codes.Relation.TABLE_NAMESPACE_UNKNOWN);
    }
  }

  private static void validateViews(
      List<SystemViewDef> views, Set<NameRef> namespaceNames, List<ValidationIssue> issues) {
    if (views == null) {
      return;
    }

    Set<NameRef> seen = new HashSet<>();
    for (SystemViewDef view : views) {
      if (view == null) {
        continue;
      }
      NameRef name = view.name();
      if (name == null || isBlank(name.getName())) {
        err(issues, Codes.Relation.VIEW_NAME_REQUIRED, "view", null);
        continue;
      }

      String ctx = objectCtx("view", name);

      if (!seen.add(name)) {
        err(issues, Codes.Relation.VIEW_DUPLICATE, ctx, null, formatName(name));
      }
      requireQualifiedObjectName("view", name, issues, Codes.Relation.VIEW_QUALIFIED_REQUIRED);
      requireNamespaceExists(
          "view", name, namespaceNames, issues, Codes.Relation.VIEW_NAMESPACE_UNKNOWN);
    }
  }

  // ------------------------------------------------------------
  // Engine specific payload types
  // ------------------------------------------------------------

  private static void validateEngineSpecificPayloadTypes(
      SystemCatalogData catalog, List<ValidationIssue> issues) {

    validateEngineSpecificPayloadTypes("function", catalog.functions(), issues);
    validateEngineSpecificPayloadTypes("operator", catalog.operators(), issues);
    validateEngineSpecificPayloadTypes("type", catalog.types(), issues);
    validateEngineSpecificPayloadTypes("cast", catalog.casts(), issues);
    validateEngineSpecificPayloadTypes("collation", catalog.collations(), issues);
    validateEngineSpecificPayloadTypes("aggregate", catalog.aggregates(), issues);
    validateEngineSpecificPayloadTypes("namespace", catalog.namespaces(), issues);
    validateEngineSpecificPayloadTypes("table", catalog.tables(), issues);
    validateEngineSpecificPayloadTypes("view", catalog.views(), issues);

    validateRegistryEngineSpecificPayloads(catalog, issues);
  }

  private static void validateEngineSpecificPayloadTypes(
      String kind, List<? extends SystemObjectDef> defs, List<ValidationIssue> issues) {
    if (defs == null) {
      return;
    }
    for (SystemObjectDef def : defs) {
      if (def == null) {
        continue;
      }
      NameRef name = def.name();
      String baseCtx = objectCtx(kind, name);
      validateEngineSpecificRules(baseCtx, def.engineSpecific(), issues);
    }
  }

  private static void validateEngineSpecificRules(
      String baseCtx, List<EngineSpecificRule> rules, List<ValidationIssue> issues) {
    if (rules == null || rules.isEmpty()) {
      return;
    }
    for (int i = 0; i < rules.size(); i++) {
      EngineSpecificRule rule = rules.get(i);
      if (rule == null) {
        continue;
      }
      if (isBlank(rule.payloadType())) {
        err(
            issues,
            Codes.EngineSpecific.PAYLOAD_REQUIRED,
            baseCtx + ".engineSpecific[" + i + "]",
            null);
      }
    }
  }

  private static void validateRegistryEngineSpecificPayloads(
      SystemCatalogData catalog, List<ValidationIssue> issues) {
    List<EngineSpecificRule> rules = catalog.registryEngineSpecific();
    if (rules == null || rules.isEmpty()) {
      return;
    }
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < rules.size(); i++) {
      EngineSpecificRule rule = rules.get(i);
      if (rule == null) {
        continue;
      }
      if (isBlank(rule.payloadType())) {
        err(
            issues,
            Codes.EngineSpecific.PAYLOAD_REQUIRED,
            "registry.engineSpecific[" + i + "]",
            null);
        continue;
      }
      String key = registryRuleKey(rule);
      if (!seen.add(key)) {
        err(issues, Codes.EngineSpecific.REGISTRY_DUPLICATE, "registry", null, key);
      }
    }
  }

  private static String registryRuleKey(EngineSpecificRule rule) {
    String payloadType = nullToEmpty(rule.payloadType());
    String kind = nullToEmpty(rule.engineKind());
    String min = nullToEmpty(rule.minVersion());
    String max = nullToEmpty(rule.maxVersion());
    return String.join("|", payloadType, kind, min, max);
  }

  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------

  private static void requireQualifiedObjectName(
      String prefix, NameRef name, List<ValidationIssue> issues, String code) {
    if (name == null) {
      return;
    }
    if (name.getPathList().isEmpty()) {
      err(issues, code, objectCtx(prefix, name), null, formatName(name));
    }
  }

  private static void requireNamespaceExists(
      String prefix,
      NameRef objectName,
      Set<NameRef> namespaceNames,
      List<ValidationIssue> issues,
      String code) {

    if (objectName == null || objectName.getPathList().isEmpty()) {
      return;
    }

    NameRef ns = namespaceOf(objectName);
    if (ns != null && !isBlank(ns.getName()) && !namespaceNames.contains(ns)) {
      err(issues, code, objectCtx(prefix, objectName), null, formatName(ns));
    }
  }

  private static NameRef namespaceOf(NameRef objectName) {
    if (objectName == null) {
      return null;
    }
    List<String> path = objectName.getPathList();
    if (path.isEmpty()) {
      return null;
    }
    NameRef.Builder b = NameRef.newBuilder();
    if (path.size() > 1) {
      b.addAllPath(path.subList(0, path.size() - 1));
    }
    b.setName(path.get(path.size() - 1));
    return b.build();
  }

  private static void requireTypeExists(
      NameRef ref,
      Set<NameRef> known,
      List<ValidationIssue> issues,
      String ctx,
      String unknownCode,
      boolean optional) {

    if (ref == null || isBlank(ref.getName())) {
      if (!optional) {
        // caller should emit the appropriate *.required code; keep this helper for unknown checks
        // only.
      }
      return;
    }
    if (known == null || known.isEmpty()) {
      // If we have no known types, avoid spamming "unknown" errors -- types.empty already reported.
      return;
    }
    if (!known.contains(ref)) {
      err(issues, unknownCode, ctx, null, formatName(ref));
    }
  }

  private static String formatName(NameRef ref) {
    if (ref == null) {
      return "<null>";
    }
    if (ref.getPathList().isEmpty()) {
      return nullToEmpty(ref.getName());
    }
    return String.join(".", ref.getPathList()) + "." + nullToEmpty(ref.getName());
  }

  private static String objectCtx(String kind, NameRef name) {
    return kind + ":" + formatName(name);
  }

  private static String objectCtx(String kind, NameRef name, String fieldPath) {
    if (fieldPath == null || fieldPath.isEmpty()) {
      return objectCtx(kind, name);
    }
    return objectCtx(kind, name) + "." + fieldPath;
  }

  private static String operatorSignature(SystemOperatorDef op) {
    return formatName(op.name())
        + "("
        + formatName(op.leftType())
        + ","
        + formatName(op.rightType())
        + ")";
  }

  private static String castSignature(NameRef src, NameRef tgt) {
    return formatName(src) + "->" + formatName(tgt);
  }

  private static String aggregateSignature(SystemAggregateDef agg) {
    StringBuilder b = new StringBuilder();
    b.append(formatName(agg.name())).append("(");
    List<NameRef> args = agg.argumentTypes();
    if (args != null) {
      for (int i = 0; i < args.size(); i++) {
        b.append(formatName(args.get(i)));
        if (i + 1 < args.size()) {
          b.append(",");
        }
      }
    }
    b.append(")");
    b.append("->").append(formatName(agg.returnType()));
    b.append("[").append(formatName(agg.stateType())).append("]");
    return b.toString();
  }

  private static String functionSignature(SystemFunctionDef fn) {
    StringBuilder b = new StringBuilder();
    b.append(formatName(fn.name())).append("(");
    List<NameRef> args = fn.argumentTypes();
    if (args != null) {
      for (int i = 0; i < args.size(); i++) {
        b.append(formatName(args.get(i)));
        if (i + 1 < args.size()) {
          b.append(",");
        }
      }
    }
    b.append(")->").append(formatName(fn.returnType()));
    return b.toString();
  }

  private static String collationSignature(SystemCollationDef coll) {
    String locale = nullToEmpty(coll.locale());
    return formatName(coll.name()) + ":" + locale;
  }

  private static void err(
      List<ValidationIssue> issues,
      String code,
      String ctx,
      VersionIntervals.VersionInterval interval,
      String... args) {
    issues.add(
        new ValidationIssue(
            code,
            Severity.ERROR,
            nullToEmpty(ctx),
            interval,
            args == null ? List.of() : List.of(args)));
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }
}
