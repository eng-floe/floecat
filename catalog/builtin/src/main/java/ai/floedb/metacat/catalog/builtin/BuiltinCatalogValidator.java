package ai.floedb.metacat.catalog.builtin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. This
 * version is engine-neutral and does NOT enforce PostgreSQL-specific metadata like backing
 * functions for operators or state/final functions for aggregates.
 */
public final class BuiltinCatalogValidator {

  private BuiltinCatalogValidator() {}

  /** Runs validation and returns a list of human-readable errors. */
  public static List<String> validate(BuiltinCatalogData catalog) {
    List<String> errors = new ArrayList<>();
    if (catalog == null) {
      errors.add("catalog.null");
      return errors;
    }

    Set<String> typeNames = validateTypes(catalog.types(), errors);
    Set<String> functionNames = validateFunctions(catalog.functions(), typeNames, errors);
    validateOperators(catalog.operators(), typeNames, errors); // simplified
    validateCasts(catalog.casts(), typeNames, errors);
    validateCollations(catalog.collations(), errors);
    validateAggregates(catalog.aggregates(), typeNames, errors); // simplified

    return errors;
  }

  // ------------------------------------------------------------
  // Types
  // ------------------------------------------------------------

  private static Set<String> validateTypes(List<BuiltinTypeDef> types, List<String> errors) {
    if (types.isEmpty()) {
      errors.add("types.empty");
    }

    Set<String> typeNames = new HashSet<>();
    for (BuiltinTypeDef type : types) {
      String name = safeName(type.name());
      if (name.isBlank()) {
        errors.add("type.name.required");
        continue;
      }
      if (!typeNames.add(name)) {
        errors.add("type.duplicate:" + name);
      }
    }
    return typeNames;
  }

  // ------------------------------------------------------------
  // Functions
  // ------------------------------------------------------------

  private static Set<String> validateFunctions(
      List<BuiltinFunctionDef> functions, Set<String> typeNames, List<String> errors) {

    if (functions.isEmpty()) {
      errors.add("functions.empty");
    }

    Set<String> names = new HashSet<>();
    for (BuiltinFunctionDef fn : functions) {
      String name = safeName(fn.name());
      if (name.isBlank()) {
        errors.add("function.name.required");
        continue;
      }
      names.add(name);

      // return type must exist
      requireTypeExists(fn.returnType(), typeNames, errors, "function.return:" + name);

      // all arg types must exist
      for (String arg : fn.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "function.arg:" + name);
      }
    }
    return names;
  }

  // ------------------------------------------------------------
  // Operators (engine-neutral)
  // ------------------------------------------------------------

  private static void validateOperators(
      List<BuiltinOperatorDef> operators, Set<String> typeNames, List<String> errors) {

    for (BuiltinOperatorDef op : operators) {
      String name = safeName(op.name());

      // Validate operand/return type existence
      requireTypeExists(op.leftType(), typeNames, errors, "operator.left:" + name);
      requireTypeExists(op.rightType(), typeNames, errors, "operator.right:" + name);
      requireTypeExists(op.returnType(), typeNames, errors, "operator.return:" + name);

      // No function-name checks anymore (proto no longer includes it)
    }
  }

  // ------------------------------------------------------------
  // Casts
  // ------------------------------------------------------------

  private static void validateCasts(
      List<BuiltinCastDef> casts, Set<String> typeNames, List<String> errors) {
    Set<Map.Entry<String, String>> seen = new HashSet<>();

    for (BuiltinCastDef cast : casts) {
      var key = Map.entry(safeName(cast.sourceType()), safeName(cast.targetType()));
      if (!seen.add(key)) {
        errors.add("cast.duplicate:" + key);
      }

      requireTypeExists(key.getKey(), typeNames, errors, "cast.source");
      requireTypeExists(key.getValue(), typeNames, errors, "cast.target");
    }
  }

  // ------------------------------------------------------------
  // Collations
  // ------------------------------------------------------------

  private static void validateCollations(
      List<BuiltinCollationDef> collations, List<String> errors) {

    Set<String> names = new HashSet<>();

    for (BuiltinCollationDef coll : collations) {
      String name = safeName(coll.name());
      if (name.isBlank()) {
        errors.add("collation.name.required");
        continue;
      }
      if (!names.add(name)) {
        errors.add("collation.duplicate:" + name);
      }
    }
  }

  // ------------------------------------------------------------
  // Aggregates (engine-neutral)
  // ------------------------------------------------------------

  private static void validateAggregates(
      List<BuiltinAggregateDef> aggregates, Set<String> typeNames, List<String> errors) {

    for (BuiltinAggregateDef agg : aggregates) {
      String name = safeName(agg.name());

      // state type must exist
      requireTypeExists(agg.stateType(), typeNames, errors, "agg.state:" + name);

      // return type must exist
      requireTypeExists(agg.returnType(), typeNames, errors, "agg.return:" + name);

      // argument types must exist
      for (String arg : agg.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "agg.arg:" + name);
      }

      // No stateFn/finalFn validation anymore (proto removed it)
    }
  }

  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------

  private static void requireTypeExists(
      String typeName, Set<String> knownTypes, List<String> errors, String context) {

    String name = safeName(typeName);

    if (name.isBlank()) {
      errors.add(context + ".type.required");
    } else if (!knownTypes.contains(name)) {
      errors.add(context + ".type.unknown:" + name);
    }
  }

  private static String safeName(String value) {
    return value == null ? "" : value.trim();
  }
}
