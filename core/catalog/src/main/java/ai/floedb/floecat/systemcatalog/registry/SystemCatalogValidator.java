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
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import java.util.*;

/**
 * Performs structural validation of builtin catalogs before they are exposed to planners. Uses
 * NameRef everywhere.
 */
public final class SystemCatalogValidator {

  private SystemCatalogValidator() {}

  /** Runs validation and returns a list of human-readable errors. */
  public static List<String> validate(SystemCatalogData catalog) {
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

    validateEngineSpecificPayloadTypes(catalog, errors);

    return errors;
  }

  // ------------------------------------------------------------
  // Types
  // ------------------------------------------------------------

  private static Set<NameRef> validateTypes(List<SystemTypeDef> types, List<String> errors) {
    if (types.isEmpty()) {
      errors.add("types.empty");
    }

    Set<NameRef> typeNames = new HashSet<>();
    for (SystemTypeDef type : types) {
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
      List<SystemFunctionDef> functions, Set<NameRef> typeNames, List<String> errors) {

    if (functions.isEmpty()) {
      errors.add("functions.empty");
    }

    Set<NameRef> names = new HashSet<>();
    for (SystemFunctionDef fn : functions) {
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
      List<SystemOperatorDef> operators, Set<NameRef> typeNames, List<String> errors) {

    for (SystemOperatorDef op : operators) {
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
      List<SystemCastDef> casts, Set<NameRef> typeNames, List<String> errors) {

    Set<NameRef> names = new HashSet<>();

    for (SystemCastDef cast : casts) {
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

  private static void validateCollations(List<SystemCollationDef> collations, List<String> errors) {

    Set<NameRef> names = new HashSet<>();

    for (SystemCollationDef coll : collations) {
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
      List<SystemAggregateDef> aggregates, Set<NameRef> typeNames, List<String> errors) {

    for (SystemAggregateDef agg : aggregates) {
      NameRef name = agg.name();

      requireTypeExists(agg.stateType(), typeNames, errors, "agg.state:" + formatName(name));
      requireTypeExists(agg.returnType(), typeNames, errors, "agg.return:" + formatName(name));

      for (NameRef arg : agg.argumentTypes()) {
        requireTypeExists(arg, typeNames, errors, "agg.arg:" + formatName(name));
      }
    }
  }

  private static void validateEngineSpecificPayloadTypes(
      SystemCatalogData catalog, List<String> errors) {
    validateEngineSpecificPayloadTypes("function", catalog.functions(), errors);
    validateEngineSpecificPayloadTypes("operator", catalog.operators(), errors);
    validateEngineSpecificPayloadTypes("type", catalog.types(), errors);
    validateEngineSpecificPayloadTypes("cast", catalog.casts(), errors);
    validateEngineSpecificPayloadTypes("collation", catalog.collations(), errors);
    validateEngineSpecificPayloadTypes("aggregate", catalog.aggregates(), errors);
    validateEngineSpecificPayloadTypes("namespace", catalog.namespaces(), errors);
    validateEngineSpecificPayloadTypes("table", catalog.tables(), errors);
    validateEngineSpecificPayloadTypes("view", catalog.views(), errors);
    validateRegistryEngineSpecificPayloads(catalog, errors);
  }

  private static void validateEngineSpecificPayloadTypes(
      String prefix, List<? extends SystemObjectDef> defs, List<String> errors) {
    for (SystemObjectDef def : defs) {
      NameRef name = def.name();
      if (name == null) {
        continue;
      }
      validateEngineSpecificRules(prefix + "." + formatName(name), def.engineSpecific(), errors);
    }
  }

  private static void validateEngineSpecificRules(
      String ctx, List<EngineSpecificRule> rules, List<String> errors) {
    if (rules == null || rules.isEmpty()) {
      return;
    }
    for (int i = 0; i < rules.size(); i++) {
      EngineSpecificRule rule = rules.get(i);
      if (rule == null) {
        continue;
      }
      if (rule.payloadType() == null || rule.payloadType().isBlank()) {
        errors.add(ctx + ".engineSpecific[" + i + "].payloadType.required");
      }
    }
  }

  private static void validateRegistryEngineSpecificPayloads(
      SystemCatalogData catalog, List<String> errors) {
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
      if (rule.payloadType() == null || rule.payloadType().isBlank()) {
        errors.add("registry.engineSpecific[" + i + "].payloadType.required");
        continue;
      }
      String key = registryRuleKey(rule);
      if (!seen.add(key)) {
        errors.add("registry.engineSpecific.duplicate:" + key);
      }
    }
  }

  private static String registryRuleKey(EngineSpecificRule rule) {
    String payloadType = rule.payloadType();
    if (payloadType == null) {
      payloadType = "";
    }
    String kind = rule.engineKind() == null ? "" : rule.engineKind();
    String min = rule.minVersion() == null ? "" : rule.minVersion();
    String max = rule.maxVersion() == null ? "" : rule.maxVersion();
    return String.join("|", payloadType, kind, min, max);
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
