package ai.floedb.metacat.catalog.builtin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. The goal
 * is to catch malformed or incomplete catalog data during build/verify time.
 */
public final class BuiltinCatalogValidator {

  private BuiltinCatalogValidator() {}

  /**
   * Runs validation and returns the list of human-readable errors. An empty list indicates a valid
   * catalog.
   */
  public static List<String> validate(BuiltinCatalogData catalog) {
    List<String> errors = new ArrayList<>();
    if (catalog == null) {
      errors.add("catalog.null");
      return errors;
    }

    if (catalog.version() == null || catalog.version().isBlank()) {
      errors.add("catalog.version.required");
    }

    Set<String> typeNames = validateTypes(catalog.types(), errors);
    Set<String> functionNames = validateFunctions(catalog.functions(), typeNames, errors);
    validateOperators(catalog.operators(), typeNames, functionNames, errors);
    validateCasts(catalog.casts(), typeNames, errors);
    validateCollations(catalog.collations(), errors);
    validateAggregates(catalog.aggregates(), typeNames, functionNames, errors);

    return errors;
  }

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
      if (!names.add(name)) {
        errors.add("function.duplicate:" + name);
      }

      requireTypeExists(fn.returnType(), typeNames, errors, "function.return:" + name);
      for (String arg : fn.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "function.arg:" + name);
      }
    }
    return names;
  }

  private static void validateOperators(
      List<BuiltinOperatorDef> operators,
      Set<String> typeNames,
      Set<String> functionNames,
      List<String> errors) {
    for (BuiltinOperatorDef op : operators) {
      String fn = safeName(op.functionName());
      if (fn.isBlank() || !functionNames.contains(fn)) {
        errors.add("operator.function.missing:" + op.name());
      }
      requireTypeExists(op.leftType(), typeNames, errors, "operator.left:" + op.name());
      requireTypeExists(op.rightType(), typeNames, errors, "operator.right:" + op.name());
    }
  }

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

  private static void validateAggregates(
      List<BuiltinAggregateDef> aggregates,
      Set<String> typeNames,
      Set<String> functionNames,
      List<String> errors) {
    for (BuiltinAggregateDef agg : aggregates) {
      String name = safeName(agg.name());
      requireTypeExists(agg.stateType(), typeNames, errors, "agg.state:" + name);
      requireTypeExists(agg.returnType(), typeNames, errors, "agg.return:" + name);
      for (String arg : agg.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "agg.arg:" + name);
      }
      requireFunctionExists(agg.stateFunction(), functionNames, errors, "agg.stateFn:" + name);
      requireFunctionExists(agg.finalFunction(), functionNames, errors, "agg.finalFn:" + name);
    }
  }

  private static void requireTypeExists(
      String typeName, Set<String> knownTypes, List<String> errors, String context) {
    String name = safeName(typeName);
    if (name.isBlank()) {
      errors.add(context + ".type.required");
    } else if (!knownTypes.contains(name)) {
      errors.add(context + ".type.unknown:" + name);
    }
  }

  private static void requireFunctionExists(
      String fnName, Set<String> knownFunctions, List<String> errors, String context) {
    String name = safeName(fnName);
    if (name.isBlank()) {
      errors.add(context + ".function.required");
    } else if (!knownFunctions.contains(name)) {
      errors.add(context + ".function.unknown:" + name);
    }
  }

  private static String safeName(String value) {
    return value == null ? "" : value.trim();
  }
}
