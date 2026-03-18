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

import ai.floedb.floecat.catalog.rpc.ConstraintColumnRef;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.ConstraintType;
import ai.floedb.floecat.catalog.rpc.ForeignKeyActionRule;
import ai.floedb.floecat.catalog.rpc.ForeignKeyMatchOption;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
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
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.SignatureUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. Uses
 * NameRef everywhere.
 */
public final class SystemCatalogValidator {

  private SystemCatalogValidator() {}

  interface Codes {
    String CATALOG_NULL = "catalog.null";

    interface Type {
      String NAME_REQUIRED = "type.name.required";
      String DUPLICATE = "type.duplicate";
      String NAME_QUALIFIED_REQUIRED = "type.name.qualified.required";
      String NAMESPACE_UNKNOWN = "type.namespace.unknown";
      String CATEGORY_REQUIRED = "type.category.required";
      String CATEGORY_INVALID = "type.category.invalid";
      String ELEMENT_REQUIRED = "type.element.required";
      String ELEMENT_SELF = "type.element.self";
      String UNKNOWN = "type.unknown";
    }

    interface Function {
      String NAME_REQUIRED = "function.name.required";
      String DUPLICATE = "function.duplicate";
      String NAME_QUALIFIED_REQUIRED = "function.name.qualified.required";
      String NAMESPACE_UNKNOWN = "function.namespace.unknown";
      String RETURN_TYPE_REQUIRED = "function.return.type.required";
      String RETURN_TYPE_UNKNOWN = "function.return.type.unknown";
      String ARG_TYPE_REQUIRED = "function.arg.type.required";
      String ARG_TYPE_UNKNOWN = "function.arg.type.unknown";
    }

    interface Operator {
      String NAME_REQUIRED = "operator.name.required";
      String DUPLICATE = "operator.duplicate";
      String NAME_QUALIFIED_REQUIRED = "operator.name.qualified.required";
      String NAMESPACE_UNKNOWN = "operator.namespace.unknown";
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
      String NAME_QUALIFIED_REQUIRED = "cast.name.qualified.required";
      String NAMESPACE_UNKNOWN = "cast.namespace.unknown";
      String SOURCE_TYPE_REQUIRED = "cast.source.type.required";
      String SOURCE_TYPE_UNKNOWN = "cast.source.type.unknown";
      String TARGET_TYPE_REQUIRED = "cast.target.type.required";
      String TARGET_TYPE_UNKNOWN = "cast.target.type.unknown";
    }

    interface Collation {
      String NAME_REQUIRED = "collation.name.required";
      String LOCALE_REQUIRED = "collation.locale.required";
      String DUPLICATE = "collation.duplicate";
      String NAME_QUALIFIED_REQUIRED = "collation.name.qualified.required";
      String NAMESPACE_UNKNOWN = "collation.namespace.unknown";
    }

    interface Aggregate {
      String NAME_REQUIRED = "agg.name.required";
      String DUPLICATE = "agg.duplicate";
      String NAME_QUALIFIED_REQUIRED = "agg.name.qualified.required";
      String NAMESPACE_UNKNOWN = "agg.namespace.unknown";
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

    interface Constraint {
      String TYPE_REQUIRED = "table.constraint.type.required";
      String NAME_DUPLICATE = "table.constraint.name.duplicate";
      String COLUMNS_REQUIRED = "table.constraint.columns.required";
      String FK_REFERENCED_TABLE_REQUIRED = "table.constraint.fk.referenced_table.required";
      String FK_REFERENCED_TABLE_UNKNOWN = "table.constraint.fk.referenced_table.unknown";
      String REFERENCED_COLUMNS_REQUIRED = "table.constraint.referenced_columns.required";
      String FK_COLUMN_ARITY_MISMATCH = "table.constraint.fk.arity.mismatch";
      String FK_REFERENCED_NOT_ALLOWED = "table.constraint.fk_only.field.not_allowed";
      String FK_BEHAVIOR_NOT_ALLOWED = "table.constraint.fk_only.behavior.not_allowed";
      String CHECK_EXPRESSION_REQUIRED = "table.constraint.check.expression.required";
      String CHECK_EXPRESSION_NOT_ALLOWED = "table.constraint.check_only.expression.not_allowed";
      String NOT_NULL_SINGLE_COLUMN = "table.constraint.not_null.single_column";
      String NOT_NULL_NULLABLE_COLUMN = "table.constraint.not_null.column_is_nullable";
      String COLUMN_REF_TARGET_REQUIRED = "table.constraint.column_ref.target.required";
      String COLUMN_REF_UNKNOWN = "table.constraint.column_ref.unknown";
      String COLUMN_REF_ID_NAME_MISMATCH = "table.constraint.column_ref.id_name.mismatch";
      String COLUMN_REF_ORDINAL_INVALID = "table.constraint.column_ref.ordinal.invalid";
      String COLUMN_REF_ORDINAL_DUPLICATE = "table.constraint.column_ref.ordinal.duplicate";
    }
  }

  /** Controls which object kinds are required to carry a known namespace-qualified name. */
  public record NamespaceScopePolicy(
      boolean functionScoped,
      boolean operatorScoped,
      boolean typeScoped,
      boolean castScoped,
      boolean collationScoped,
      boolean aggregateScoped,
      boolean tableScoped,
      boolean viewScoped) {
    public static NamespaceScopePolicy defaultPolicy() {
      return new NamespaceScopePolicy(
          true, // function
          false, // operator
          true, // type
          false, // cast
          false, // collation
          false, // aggregate
          true, // table
          true // view
          );
    }

    public static NamespaceScopePolicy strictAll() {
      return new NamespaceScopePolicy(true, true, true, true, true, true, true, true);
    }
  }

  /** Runs validation and returns structured issues. */
  public static List<ValidationIssue> validate(SystemCatalogData catalog) {
    return validate(catalog, NamespaceScopePolicy.defaultPolicy());
  }

  /** Runs validation with a caller-provided namespace-scoping policy. */
  public static List<ValidationIssue> validate(
      SystemCatalogData catalog, NamespaceScopePolicy namespaceScopePolicy) {
    NamespaceScopePolicy policy =
        Objects.requireNonNullElse(namespaceScopePolicy, NamespaceScopePolicy.defaultPolicy());
    return validateInternal(catalog, policy);
  }

  /** Runs validation for partial catalog fragments (e.g. namespace/table/view-only overlays). */
  public static List<ValidationIssue> validateFragment(SystemCatalogData catalog) {
    return validateFragment(catalog, NamespaceScopePolicy.defaultPolicy());
  }

  /** Runs fragment validation with a caller-provided namespace-scoping policy. */
  public static List<ValidationIssue> validateFragment(
      SystemCatalogData catalog, NamespaceScopePolicy namespaceScopePolicy) {
    return validate(catalog, namespaceScopePolicy);
  }

  private static List<ValidationIssue> validateInternal(
      SystemCatalogData catalog, NamespaceScopePolicy policy) {
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

    validateNamespacesTablesAndViews(catalog, issues, policy);

    validateEngineSpecificPayloadTypes(catalog, issues);

    return issues;
  }

  // ------------------------------------------------------------
  // Types
  // ------------------------------------------------------------

  private static Set<NameRef> validateTypes(
      List<SystemTypeDef> types, List<ValidationIssue> issues) {
    if (types == null || types.isEmpty()) {
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
      String signature = SignatureUtil.functionSignature(fn);
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
      String signature = SignatureUtil.operatorSignature(op);
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
        String mapping = SignatureUtil.castSignature(cast);
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

      String signature = SignatureUtil.aggregateSignature(agg);
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
      SystemCatalogData catalog, List<ValidationIssue> issues, NamespaceScopePolicy policy) {
    Set<NameRef> namespaceNames = validateNamespaces(catalog.namespaces(), issues);
    if (policy.functionScoped()) {
      validateNamespaceScopedObjectNames(
          "function",
          catalog.functions(),
          SystemFunctionDef::name,
          namespaceNames,
          issues,
          Codes.Function.NAME_QUALIFIED_REQUIRED,
          Codes.Function.NAMESPACE_UNKNOWN);
    }
    if (policy.operatorScoped()) {
      validateNamespaceScopedObjectNames(
          "operator",
          catalog.operators(),
          SystemOperatorDef::name,
          namespaceNames,
          issues,
          Codes.Operator.NAME_QUALIFIED_REQUIRED,
          Codes.Operator.NAMESPACE_UNKNOWN);
    }
    if (policy.typeScoped()) {
      validateNamespaceScopedObjectNames(
          "type",
          catalog.types(),
          SystemTypeDef::name,
          namespaceNames,
          issues,
          Codes.Type.NAME_QUALIFIED_REQUIRED,
          Codes.Type.NAMESPACE_UNKNOWN);
    }
    if (policy.castScoped()) {
      validateNamespaceScopedObjectNames(
          "cast",
          catalog.casts(),
          SystemCastDef::name,
          namespaceNames,
          issues,
          Codes.Cast.NAME_QUALIFIED_REQUIRED,
          Codes.Cast.NAMESPACE_UNKNOWN);
    }
    if (policy.collationScoped()) {
      validateNamespaceScopedObjectNames(
          "collation",
          catalog.collations(),
          SystemCollationDef::name,
          namespaceNames,
          issues,
          Codes.Collation.NAME_QUALIFIED_REQUIRED,
          Codes.Collation.NAMESPACE_UNKNOWN);
    }
    if (policy.aggregateScoped()) {
      validateNamespaceScopedObjectNames(
          "aggregate",
          catalog.aggregates(),
          SystemAggregateDef::name,
          namespaceNames,
          issues,
          Codes.Aggregate.NAME_QUALIFIED_REQUIRED,
          Codes.Aggregate.NAMESPACE_UNKNOWN);
    }
    if (policy.tableScoped()) {
      validateTables(catalog.tables(), namespaceNames, issues);
    }
    if (policy.viewScoped()) {
      validateViews(catalog.views(), namespaceNames, issues);
    }
  }

  private static <T> void validateNamespaceScopedObjectNames(
      String kind,
      List<T> defs,
      Function<T, NameRef> nameFn,
      Set<NameRef> namespaceNames,
      List<ValidationIssue> issues,
      String unqualifiedCode,
      String namespaceUnknownCode) {
    if (defs == null) {
      return;
    }
    for (T def : defs) {
      if (def == null) {
        continue;
      }
      NameRef name = nameFn.apply(def);
      if (name == null || isBlank(name.getName())) {
        continue;
      }
      requireQualifiedObjectName(kind, name, issues, unqualifiedCode);
      requireNamespaceExists(kind, name, namespaceNames, issues, namespaceUnknownCode);
    }
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

    Map<String, TableColumnIndex> tableColumnsByCanonical = new LinkedHashMap<>();
    for (SystemTableDef table : tables) {
      if (table == null || table.name() == null || isBlank(table.name().getName())) {
        continue;
      }
      tableColumnsByCanonical.put(
          NameRefUtil.canonical(table.name()), indexColumns(table.columns()));
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
      validateTableConstraints(tbl, ctx, tableColumnsByCanonical, issues);
    }
  }

  private static void validateTableConstraints(
      SystemTableDef table,
      String tableCtx,
      Map<String, TableColumnIndex> tableColumnsByCanonical,
      List<ValidationIssue> issues) {
    List<ConstraintDefinition> constraints = table.constraints();
    if (constraints == null || constraints.isEmpty()) {
      return;
    }
    Map<String, SystemColumnDef> columnsByName = new HashMap<>();
    Map<Long, SystemColumnDef> columnsById = new HashMap<>();
    for (SystemColumnDef column : table.columns()) {
      columnsByName.put(column.name(), column);
      if (column.hasId() && column.id() > 0) {
        columnsById.put(column.id(), column);
      }
    }
    TableColumnIndex localTableIndex =
        new TableColumnIndex(Map.copyOf(columnsByName), Map.copyOf(columnsById));
    Set<String> constraintNames = new HashSet<>();
    for (int i = 0; i < constraints.size(); i++) {
      ConstraintDefinition constraint = constraints.get(i);
      if (constraint == null) {
        continue;
      }
      String constraintCtx = tableCtx + ".constraints[" + i + "]";
      if (constraint.getType() == ConstraintType.CT_UNSPECIFIED) {
        err(issues, Codes.Constraint.TYPE_REQUIRED, constraintCtx, null);
      }

      String constraintName = nullToEmpty(constraint.getName()).trim();
      if (!constraintName.isEmpty() && !constraintNames.add(constraintName)) {
        err(issues, Codes.Constraint.NAME_DUPLICATE, constraintCtx, null, constraintName);
      }

      if (requiresLocalColumns(constraint.getType()) && constraint.getColumnsCount() == 0) {
        err(issues, Codes.Constraint.COLUMNS_REQUIRED, constraintCtx, null);
      }
      if (constraint.getType() == ConstraintType.CT_NOT_NULL && constraint.getColumnsCount() != 1) {
        err(
            issues,
            Codes.Constraint.NOT_NULL_SINGLE_COLUMN,
            constraintCtx,
            null,
            Integer.toString(constraint.getColumnsCount()));
      } else if (constraint.getType() == ConstraintType.CT_NOT_NULL
          && constraint.getColumnsCount() == 1) {
        ConstraintColumnRef notNullColumn = constraint.getColumns(0);
        SystemColumnDef target = resolveLocalColumn(columnsByName, columnsById, notNullColumn);
        if (target != null && target.nullable()) {
          err(
              issues,
              Codes.Constraint.NOT_NULL_NULLABLE_COLUMN,
              constraintCtx,
              null,
              target.name());
        }
      }
      if (constraint.getType() == ConstraintType.CT_FOREIGN_KEY) {
        if (!constraint.hasReferencedTableId() && isBlank(constraint.getReferencedTable())) {
          err(issues, Codes.Constraint.FK_REFERENCED_TABLE_REQUIRED, constraintCtx, null);
        }
        if (constraint.getReferencedColumnsCount() == 0) {
          err(issues, Codes.Constraint.REFERENCED_COLUMNS_REQUIRED, constraintCtx, null);
        }
        if (constraint.getColumnsCount() != constraint.getReferencedColumnsCount()) {
          err(
              issues,
              Codes.Constraint.FK_COLUMN_ARITY_MISMATCH,
              constraintCtx,
              null,
              Integer.toString(constraint.getColumnsCount()),
              Integer.toString(constraint.getReferencedColumnsCount()));
        }
      } else if (constraint.hasReferencedTableId()
          || constraint.hasReferencedTable()
          || constraint.getReferencedColumnsCount() > 0
          || !isBlank(constraint.getReferencedConstraintName())) {
        err(issues, Codes.Constraint.FK_REFERENCED_NOT_ALLOWED, constraintCtx, null);
      }
      if (constraint.getType() != ConstraintType.CT_FOREIGN_KEY
          && (constraint.getMatchOption() != ForeignKeyMatchOption.FK_MATCH_OPTION_UNSPECIFIED
              || constraint.getUpdateRule() != ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED
              || constraint.getDeleteRule() != ForeignKeyActionRule.FK_ACTION_RULE_UNSPECIFIED)) {
        err(issues, Codes.Constraint.FK_BEHAVIOR_NOT_ALLOWED, constraintCtx, null);
      }

      if (constraint.getType() == ConstraintType.CT_CHECK) {
        if (isBlank(constraint.getCheckExpression())) {
          err(issues, Codes.Constraint.CHECK_EXPRESSION_REQUIRED, constraintCtx, null);
        }
      } else if (!isBlank(constraint.getCheckExpression())) {
        err(issues, Codes.Constraint.CHECK_EXPRESSION_NOT_ALLOWED, constraintCtx, null);
      }

      validateConstraintColumnRefs(
          constraintCtx + ".columns",
          constraint.getColumnsList(),
          columnsByName,
          columnsById,
          issues);
      Map<String, SystemColumnDef> referencedColumnsByName = Map.of();
      Map<Long, SystemColumnDef> referencedColumnsById = Map.of();
      if (constraint.getType() == ConstraintType.CT_FOREIGN_KEY) {
        TableColumnIndex referencedIndex =
            resolveReferencedTableColumns(
                table, constraint, tableColumnsByCanonical, localTableIndex);
        if (referencedIndex != null) {
          referencedColumnsByName = referencedIndex.byName();
          referencedColumnsById = referencedIndex.byId();
        } else if (!constraint.hasReferencedTableId()
            && !isBlank(constraint.getReferencedTable())) {
          err(
              issues,
              Codes.Constraint.FK_REFERENCED_TABLE_UNKNOWN,
              constraintCtx,
              null,
              formatName(constraint.getReferencedTable()));
        }
      }
      validateConstraintColumnRefs(
          constraintCtx + ".referenced_columns",
          constraint.getReferencedColumnsList(),
          referencedColumnsByName,
          referencedColumnsById,
          issues);
    }
  }

  private static TableColumnIndex resolveReferencedTableColumns(
      SystemTableDef localTable,
      ConstraintDefinition constraint,
      Map<String, TableColumnIndex> tableColumnsByCanonical,
      TableColumnIndex localTableIndex) {
    if (localTable == null
        || localTable.name() == null
        || constraint == null
        || isBlank(constraint.getReferencedTable())) {
      return null;
    }
    NameRef referencedTable = constraint.getReferencedTable();
    String referencedRaw = NameRefUtil.canonical(referencedTable);
    if (referencedRaw.isBlank()) {
      return null;
    }

    if (referencedTable.getPathCount() > 0) {
      return tableColumnsByCanonical.get(referencedRaw);
    }

    String localNamespace = namespacePrefix(localTable.name());
    if (!localNamespace.isEmpty()) {
      TableColumnIndex inNamespace =
          tableColumnsByCanonical.get(localNamespace + "." + referencedRaw);
      if (inNamespace != null) {
        return inNamespace;
      }
    }

    if (referencedTable.getName().equalsIgnoreCase(nullToEmpty(localTable.name().getName()))) {
      return localTableIndex;
    }

    return null;
  }

  private static TableColumnIndex indexColumns(List<SystemColumnDef> columns) {
    Map<String, SystemColumnDef> byName = new LinkedHashMap<>();
    Map<Long, SystemColumnDef> byId = new LinkedHashMap<>();
    if (columns == null) {
      return new TableColumnIndex(Map.of(), Map.of());
    }
    for (SystemColumnDef column : columns) {
      if (column == null) {
        continue;
      }
      byName.put(column.name(), column);
      if (column.hasId() && column.id() > 0) {
        byId.put(column.id(), column);
      }
    }
    return new TableColumnIndex(Map.copyOf(byName), Map.copyOf(byId));
  }

  private static String namespacePrefix(NameRef tableName) {
    if (tableName == null || tableName.getPathCount() == 0) {
      return "";
    }
    return String.join(".", tableName.getPathList()).toLowerCase();
  }

  private static boolean requiresLocalColumns(ConstraintType type) {
    return type == ConstraintType.CT_PRIMARY_KEY
        || type == ConstraintType.CT_UNIQUE
        || type == ConstraintType.CT_FOREIGN_KEY
        || type == ConstraintType.CT_NOT_NULL;
  }

  private static void validateConstraintColumnRefs(
      String refsCtx,
      List<ConstraintColumnRef> refs,
      Map<String, SystemColumnDef> columnsByName,
      Map<Long, SystemColumnDef> columnsById,
      List<ValidationIssue> issues) {
    if (refs == null || refs.isEmpty()) {
      return;
    }
    Set<Integer> ordinals = new HashSet<>();
    for (int i = 0; i < refs.size(); i++) {
      ConstraintColumnRef ref = refs.get(i);
      if (ref == null) {
        continue;
      }
      String refCtx = refsCtx + "[" + i + "]";
      if (ref.getOrdinal() <= 0) {
        err(
            issues,
            Codes.Constraint.COLUMN_REF_ORDINAL_INVALID,
            refCtx,
            null,
            Integer.toString(ref.getOrdinal()));
      } else if (!ordinals.add(ref.getOrdinal())) {
        err(
            issues,
            Codes.Constraint.COLUMN_REF_ORDINAL_DUPLICATE,
            refCtx,
            null,
            Integer.toString(ref.getOrdinal()));
      }

      boolean hasId = ref.getColumnId() > 0;
      boolean hasName = !isBlank(ref.getColumnName());
      if (!hasId && !hasName) {
        err(issues, Codes.Constraint.COLUMN_REF_TARGET_REQUIRED, refCtx, null);
        continue;
      }
      if ((columnsByName == null || columnsByName.isEmpty())
          && (columnsById == null || columnsById.isEmpty())) {
        continue;
      }
      SystemColumnDef byId =
          hasId && columnsById != null ? columnsById.get(ref.getColumnId()) : null;
      SystemColumnDef byName =
          hasName && columnsByName != null ? columnsByName.get(ref.getColumnName()) : null;

      if (hasId && hasName) {
        if (byId != null && byName != null && byId.equals(byName)) {
          continue;
        }
        err(
            issues,
            Codes.Constraint.COLUMN_REF_ID_NAME_MISMATCH,
            refCtx,
            null,
            Long.toString(ref.getColumnId()),
            ref.getColumnName());
        continue;
      }
      if (byId != null || byName != null) {
        continue;
      }
      err(
          issues,
          Codes.Constraint.COLUMN_REF_UNKNOWN,
          refCtx,
          null,
          hasId ? Long.toString(ref.getColumnId()) : ref.getColumnName());
    }
  }

  private static SystemColumnDef resolveLocalColumn(
      Map<String, SystemColumnDef> columnsByName,
      Map<Long, SystemColumnDef> columnsById,
      ConstraintColumnRef ref) {
    if (ref == null) {
      return null;
    }
    if (ref.getColumnId() > 0 && columnsById.containsKey(ref.getColumnId())) {
      return columnsById.get(ref.getColumnId());
    }
    if (!isBlank(ref.getColumnName()) && columnsByName.containsKey(ref.getColumnName())) {
      return columnsByName.get(ref.getColumnName());
    }
    return null;
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
      // If we have no known types in this fragment, avoid spamming unknown-type errors.
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

  private static boolean isBlank(NameRef ref) {
    return ref == null
        || (ref.getCatalog().isBlank() && ref.getPathCount() == 0 && ref.getName().isBlank());
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  private record TableColumnIndex(
      Map<String, SystemColumnDef> byName, Map<Long, SystemColumnDef> byId) {}
}
