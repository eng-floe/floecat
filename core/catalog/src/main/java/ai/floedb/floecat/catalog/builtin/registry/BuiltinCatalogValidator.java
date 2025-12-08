package ai.floedb.floecat.catalog.builtin.registry;

import ai.floedb.floecat.catalog.builtin.def.*;
import ai.floedb.floecat.common.rpc.NameRef;
import java.util.*;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. Uses
 * NameRef everywhere.
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

    Set<NameRef> typeNames = validateTypes(catalog.types(), errors);
    validateFunctions(catalog.functions(), typeNames, errors);
    validateOperators(catalog.operators(), typeNames, errors);
    validateCasts(catalog.casts(), typeNames, errors);
    validateCollations(catalog.collations(), errors);
    validateAggregates(catalog.aggregates(), typeNames, errors);

    return errors;
  }

  // ------------------------------------------------------------
  // Types
  // ------------------------------------------------------------

  private static Set<NameRef> validateTypes(List<BuiltinTypeDef> types, List<String> errors) {
    if (types.isEmpty()) {
      errors.add("types.empty");
    }

    Set<NameRef> typeNames = new HashSet<>();
    for (BuiltinTypeDef type : types) {
      NameRef name = type.name();
      if (name == null || name.getName().isBlank()) {
        errors.add("type.name.required");
        continue;
      }
      if (!typeNames.add(name)) {
        errors.add("type.duplicate:" + formatName(name));
      }
    }
    return typeNames;
  }

  // ------------------------------------------------------------
  // Functions
  // ------------------------------------------------------------

  private static Set<NameRef> validateFunctions(
      List<BuiltinFunctionDef> functions, Set<NameRef> typeNames, List<String> errors) {

    if (functions.isEmpty()) {
      errors.add("functions.empty");
    }

    Set<NameRef> names = new HashSet<>();
    for (BuiltinFunctionDef fn : functions) {
      NameRef name = fn.name();
      if (name == null || name.getName().isBlank()) {
        errors.add("function.name.required");
        continue;
      }
      names.add(name);

      // return type must exist
      requireTypeExists(fn.returnType(), typeNames, errors, "function.return:" + formatName(name));

      // argument types must exist
      for (NameRef arg : fn.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "function.arg:" + formatName(name));
      }
    }
    return names;
  }

  // ------------------------------------------------------------
  // Operators
  // ------------------------------------------------------------

  private static void validateOperators(
      List<BuiltinOperatorDef> operators, Set<NameRef> typeNames, List<String> errors) {

    for (BuiltinOperatorDef op : operators) {
      NameRef name = op.name();

      requireTypeExists(op.leftType(), typeNames, errors, "operator.left:" + formatName(name));
      requireTypeExists(op.rightType(), typeNames, errors, "operator.right:" + formatName(name));
      requireTypeExists(op.returnType(), typeNames, errors, "operator.return:" + formatName(name));
    }
  }

  // ------------------------------------------------------------
  // Casts
  // ------------------------------------------------------------

  private static void validateCasts(
      List<BuiltinCastDef> casts, Set<NameRef> typeNames, List<String> errors) {

    Set<NameRef> names = new HashSet<>();

    for (BuiltinCastDef cast : casts) {
      NameRef name = cast.name();
      if (name == null || name.getName().isBlank()) {
        errors.add("cast.name.required");
        continue;
      }
      if (!names.add(name)) {
        errors.add("cast.duplicate:" + formatName(name));
      }

      NameRef src = cast.sourceType();
      NameRef tgt = cast.targetType();

      requireTypeExists(src, typeNames, errors, "cast.source");
      requireTypeExists(tgt, typeNames, errors, "cast.target");
    }
  }

  // ------------------------------------------------------------
  // Collations
  // ------------------------------------------------------------

  private static void validateCollations(
      List<BuiltinCollationDef> collations, List<String> errors) {

    Set<NameRef> names = new HashSet<>();

    for (BuiltinCollationDef coll : collations) {
      NameRef name = coll.name();
      if (name == null || name.getName().isBlank()) {
        errors.add("collation.name.required");
        continue;
      }
      if (!names.add(name)) {
        errors.add("collation.duplicate:" + formatName(name));
      }
    }
  }

  // ------------------------------------------------------------
  // Aggregates
  // ------------------------------------------------------------

  private static void validateAggregates(
      List<BuiltinAggregateDef> aggregates, Set<NameRef> typeNames, List<String> errors) {

    for (BuiltinAggregateDef agg : aggregates) {
      NameRef name = agg.name();

      requireTypeExists(agg.stateType(), typeNames, errors, "agg.state:" + formatName(name));
      requireTypeExists(agg.returnType(), typeNames, errors, "agg.return:" + formatName(name));

      for (NameRef arg : agg.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "agg.arg:" + formatName(name));
      }
    }
  }

  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------

  private static void requireTypeExists(
      NameRef ref, Set<NameRef> known, List<String> errors, String ctx) {

    if (ref == null || ref.getName().isBlank()) {
      errors.add(ctx + ".type.required");
    } else if (!known.contains(ref)) {
      errors.add(ctx + ".type.unknown:" + formatName(ref));
    }
  }

  private static String formatName(NameRef ref) {
    if (ref == null) return "<null>";
    if (ref.getPathList().isEmpty()) return ref.getName();
    return String.join(".", ref.getPathList()) + "." + ref.getName();
  }
}
