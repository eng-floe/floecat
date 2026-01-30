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

package ai.floedb.floecat.systemcatalog.util;

import static ai.floedb.floecat.systemcatalog.util.NameRefUtil.canonical;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import java.util.List;

/**
 * Canonical signature helpers for system catalog objects.
 *
 * <p>Design goals:
 *
 * <ul>
 *   <li>Deterministic and stable across JVM runs
 *   <li>Readable in logs and error messages
 *   <li>These signatures feed `ResourceId` id strings and drive duplicate detection.
 * </ul>
 */
public final class SignatureUtil {

  private SignatureUtil() {}

  // ----------------------------------------------------------------------
  // Helpers
  // ----------------------------------------------------------------------

  /** Comma-joins argument type names using {@link #canonical(NameRef)}. */
  public static String args(List<NameRef> argTypes) {
    if (argTypes == null || argTypes.isEmpty()) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < argTypes.size(); i++) {
      b.append(canonical(argTypes.get(i)));
      if (i + 1 < argTypes.size()) {
        b.append(",");
      }
    }
    return b.toString();
  }

  /**
   * Returns the stable identity string for the given system object.
   *
   * <p>This string is used as the ResourceId suffix when identifiers can be overloaded. It switches
   * between a canonical name and a full signature depending on the type.
   */
  public static String identityString(SystemObjectDef def) {
    String suffix;
    if (def == null) {
      suffix = "";
    } else if (def instanceof SystemFunctionDef fn) {
      suffix = SignatureUtil.functionSignature(fn);
    } else if (def instanceof SystemOperatorDef op) {
      suffix = SignatureUtil.operatorSignature(op);
    } else if (def instanceof SystemAggregateDef agg) {
      suffix = SignatureUtil.aggregateSignature(agg);
    } else if (def instanceof SystemCastDef cast) {
      suffix = SignatureUtil.castSignature(cast);
    } else if (def instanceof SystemCollationDef collation) {
      suffix = SignatureUtil.collationSignature(collation);
    } else if (def instanceof SystemTypeDef typeName) {
      suffix = SignatureUtil.typeSignature(typeName);
    } else {
      suffix = canonical(def.name()); // Fallback to name only
    }
    return suffix;
  }

  // ----------------------------------------------------------------------
  // Functions
  // ----------------------------------------------------------------------

  /**
   * Canonical function signature (name + argument types).
   *
   * <p>Used for ResourceId suffixes and duplicate tracking; format: {@code name(arg1,arg2)}.
   */
  public static String functionSignature(SystemFunctionDef fn) {
    if (fn == null) return "";
    return canonical(fn.name()) + "(" + args(fn.argumentTypes()) + ")";
  }

  // ----------------------------------------------------------------------
  // Operators
  // ----------------------------------------------------------------------

  /**
   * Canonical operator signature (name + operand types + return type).
   *
   * <p>Format: {@code name(left,right)->returnType}. Used for identity strings and duplicate
   * checks.
   *
   * <p>Note: left may be blank for unary operators.
   */
  public static String operatorSignature(SystemOperatorDef op) {
    if (op == null) return "";
    return canonical(op.name())
        + "("
        + canonical(op.leftType())
        + ","
        + canonical(op.rightType())
        + ")->"
        + canonical(op.returnType());
  }

  // ----------------------------------------------------------------------
  // Aggregates
  // ----------------------------------------------------------------------

  /**
   * Canonical aggregate signature (name + argument types + return + state type).
   *
   * <p>Format: {@code name(arg1,arg2)->returnType[stateType]} and used for ResourceId suffixes.
   */
  public static String aggregateSignature(SystemAggregateDef agg) {
    if (agg == null) return "";
    return canonical(agg.name())
        + "("
        + args(agg.argumentTypes())
        + ")->"
        + canonical(agg.returnType())
        + "["
        + canonical(agg.stateType())
        + "]";
  }

  // ----------------------------------------------------------------------
  // Casts
  // ----------------------------------------------------------------------

  /**
   * Canonical cast signature (source + target types).
   *
   * <p>Format: {@code sourceType->targetType}. Cast method is not part of identity.
   */
  public static String castSignature(SystemCastDef cast) {
    if (cast == null) return "";
    return canonical(cast.sourceType()) + "->" + canonical(cast.targetType());
  }

  // ----------------------------------------------------------------------
  // Collations
  // ----------------------------------------------------------------------
  /**
   * Canonical collation signature.
   *
   * <p>Format: {@code name.locale}
   */
  public static String collationSignature(SystemCollationDef collation) {
    if (collation == null) return "";
    return canonical(collation.name()) + "." + collation.locale();
  }

  // ----------------------------------------------------------------------
  // Types
  // ----------------------------------------------------------------------

  /**
   * Canonical type signature.
   *
   * <p>Format: {@code name}
   */
  public static String typeSignature(SystemTypeDef typeName) {
    return canonical(typeName.name());
  }
}
