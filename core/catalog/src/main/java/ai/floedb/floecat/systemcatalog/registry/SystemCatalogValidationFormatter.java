package ai.floedb.floecat.systemcatalog.registry;

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

import java.util.Map;

/** Shared formatter for validator error and warning codes. */
public final class SystemCatalogValidationFormatter {

  /** Human-readable labels for validation contexts used in suffix error codes. */
  private static final Map<String, String> CONTEXT_LABELS =
      Map.ofEntries(
          Map.entry("function.return", "Function return type"),
          Map.entry("function.arg", "Function argument"),
          Map.entry("operator.left", "Operator left operand"),
          Map.entry("operator.right", "Operator right operand"),
          Map.entry("cast.source", "Cast source type"),
          Map.entry("cast.target", "Cast target type"),
          Map.entry("agg.state", "Aggregate state type"),
          Map.entry("agg.return", "Aggregate return type"),
          Map.entry("agg.arg", "Aggregate argument"),
          Map.entry("agg.stateFn", "Aggregate state function"),
          Map.entry("agg.finalFn", "Aggregate final function"),
          Map.entry("type.element", "Type element"));

  private SystemCatalogValidationFormatter() {}

  public static String describeError(String code) {
    if (code == null) {
      return "";
    }
    switch (code) {
      case "catalog.null":
        return "Catalog payload is null";
      case "types.empty":
        return "Catalog defines no types";
      case "functions.empty":
        return "Catalog defines no functions";
      case "type.name.required":
        return "Type name is required";
      case "function.name.required":
        return "Function name is required";
      case "operator.name.required":
        return "Operator name is required";
      case "collation.name.required":
        return "Collation name is required";
      case "agg.name.required":
        return "Aggregate name is required";
    }
    if (code.startsWith("type.duplicate:")) {
      return "Duplicate type '" + suffix(code) + "'";
    }
    if (code.startsWith("type.category.required:")) {
      return "Type category is required for '" + suffix(code) + "'";
    }
    if (code.startsWith("type.category.invalid:")) {
      return "Type category must be a single uppercase letter for '" + suffix(code) + "'";
    }
    if (code.startsWith("type.element.required:")) {
      return "Array type requires an element type for '" + suffix(code) + "'";
    }
    if (code.startsWith("type.element.self:")) {
      return "Array type cannot reference itself for '" + suffix(code) + "'";
    }
    if (code.startsWith("function.duplicate:")) {
      return "Duplicate function '" + suffix(code) + "'";
    }
    if (code.startsWith("operator.duplicate:")) {
      return "Duplicate operator '" + suffix(code) + "'";
    }
    if (code.startsWith("operator.function.missing:")) {
      return "Operator references unknown function '" + suffix(code) + "'";
    }
    if (code.startsWith("collation.duplicate:")) {
      return "Duplicate collation '" + suffix(code) + "'";
    }
    if (code.startsWith("cast.name.duplicate:")) {
      return "Duplicate cast name '" + suffix(code) + "'";
    }
    if (code.startsWith("cast.duplicate:")) {
      return "Duplicate cast mapping '" + suffix(code) + "'";
    }
    if (code.startsWith("agg.duplicate:")) {
      return "Duplicate aggregate '" + suffix(code) + "'";
    }
    if (code.contains(".type.required")) {
      return contextLabel(code) + " must reference a type";
    }
    if (code.contains(".type.unknown:")) {
      return contextLabel(code) + " references unknown type '" + suffix(code) + "'";
    }
    if (code.contains(".function.required")) {
      return contextLabel(code) + " must reference a function";
    }
    if (code.contains(".function.unknown:")) {
      return contextLabel(code) + " references unknown function '" + suffix(code) + "'";
    }
    return code;
  }

  public static String describeWarning(String code) {
    return code == null ? "" : code;
  }

  private static String contextLabel(String code) {
    String prefix = code;
    int idx = code.indexOf(':');
    if (idx > 0) {
      prefix = code.substring(0, idx);
    }
    return CONTEXT_LABELS.getOrDefault(prefix, prefix);
  }

  private static String suffix(String code) {
    int idx = code.indexOf(':');
    return idx == -1 ? "" : code.substring(idx + 1);
  }
}
