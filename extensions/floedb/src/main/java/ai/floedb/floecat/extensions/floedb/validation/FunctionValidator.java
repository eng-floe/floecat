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

package ai.floedb.floecat.extensions.floedb.validation;

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class FunctionValidator implements SectionValidator<FunctionValidationResult> {
  private final Lookup lookup;
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  FunctionValidator(
      Lookup lookup, ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.lookup = lookup;
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public FunctionValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    List<ValidationSupport.DecodedRule<FloeFunctionSpecific>> decodedRules = new ArrayList<>();
    Set<Integer> functionOids =
        verifyFunctions(catalog.functions(), lookup, scope, runContext, errors, decodedRules);
    IntervalIndex<Integer> functionIntervals =
        IntervalIndex.fromDecoded(decodedRules, dr -> dr.payload().getOid());
    return new FunctionValidationResult(functionOids, functionIntervals, errors);
  }

  private static Set<Integer> verifyFunctions(
      List<SystemFunctionDef> fns,
      Lookup lookup,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext,
      List<ValidationIssue> errors,
      List<ValidationSupport.DecodedRule<FloeFunctionSpecific>> decodedOut) {
    Set<Integer> functionOids = new LinkedHashSet<>();
    Map<Integer, String> oidIdentity = new HashMap<>();
    Map<String, List<VersionIntervals.VersionInterval>> signatureIntervals = new LinkedHashMap<>();

    for (SystemFunctionDef fn : fns) {
      String ctx = ValidationSupport.context("function", fn.name());
      if (fn.name() == null || ValidationSupport.isBlank(NameRefUtil.canonical(fn.name()))) {
        ValidationSupport.err(errors, "floe.function.name.required", ctx);
        continue;
      }
      List<ValidationSupport.DecodedRule<FloeFunctionSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext,
              scope,
              fn.engineSpecific(),
              FUNCTION,
              FloeFunctionSpecific.class,
              ctx,
              errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, FUNCTION.type(), ctx, errors);

      for (ValidationSupport.DecodedRule<FloeFunctionSpecific> decodedRule : decoded) {
        String ruleCtx = ValidationSupport.contextWithInterval(ctx, decodedRule.interval());
        FloeFunctionSpecific spec = decodedRule.payload();
        if (!spec.hasProrettype() || spec.getProrettype() <= 0) {
          ValidationSupport.err(errors, "floe.function.prorettype.required", ruleCtx);
        }
        int oid = spec.getOid();
        if (oid <= 0) {
          ValidationSupport.err(errors, "floe.function.oid.required", ruleCtx);
          continue;
        }

        validateProvolatile(spec.getProvolatile(), ruleCtx, errors);

        if (!spec.getProname().equals(fn.name().getName())) {
          ValidationSupport.err(errors, "floe.function.name.mismatch", ruleCtx, spec.getProname());
        }

        List<NameRef> args = fn.argumentTypes();
        if (spec.getProargtypesCount() != args.size()) {
          ValidationSupport.err(
              errors,
              "floe.function.argcount.mismatch",
              ruleCtx,
              spec.getProargtypesCount(),
              args.size());
        }
        if (spec.hasPronargs() && spec.getPronargs() != spec.getProargtypesCount()) {
          ValidationSupport.err(
              errors,
              "floe.function.pronargs.mismatch",
              ruleCtx,
              spec.getPronargs(),
              spec.getProargtypesCount());
        }
        if (spec.hasProargtypelen() && spec.getProargtypelen() != spec.getProargtypesCount()) {
          ValidationSupport.err(
              errors,
              "floe.function.proargtypelen.mismatch",
              ruleCtx,
              spec.getProargtypelen(),
              spec.getProargtypesCount());
        }

        if (spec.hasProvariadic() && spec.getProvariadic() != 0) {
          int variadicOid = spec.getProvariadic();
          if (!lookup.typesByOid().containsKey(variadicOid)) {
            ValidationSupport.err(
                errors, "floe.function.variadic.type.unknown", ruleCtx, variadicOid);
          }
        }

        int compareCount = Math.min(args.size(), spec.getProargtypesCount());
        for (int i = 0; i < compareCount; i++) {
          NameRef arg = args.get(i);
          String argCanonical = NameRefUtil.canonical(arg);
          TypeInfo info = lookup.find(argCanonical, decodedRule.interval());
          if (info == null) {
            ValidationSupport.err(errors, "floe.function.arg.type.unknown", ruleCtx, argCanonical);
            continue;
          }
          int expected = info.payload().getOid();
          int actual = spec.getProargtypes(i);
          if (expected != actual) {
            ValidationSupport.err(
                errors, "floe.function.arg.mismatch", ruleCtx, i, expected, actual);
          }
        }

        if (fn.returnType() != null) {
          String retCanonical = NameRefUtil.canonical(fn.returnType());
          if (!ValidationSupport.isBlank(retCanonical)) {
            TypeInfo retInfo = lookup.find(retCanonical, decodedRule.interval());
            if (retInfo == null) {
              ValidationSupport.err(
                  errors, "floe.function.return.type.unknown", ruleCtx, retCanonical);
            } else if (spec.getProrettype() > 0
                && retInfo.payload().getOid() != spec.getProrettype()) {
              ValidationSupport.err(
                  errors,
                  "floe.function.return.mismatch",
                  ruleCtx,
                  retInfo.payload().getOid(),
                  spec.getProrettype());
            }
          }
        }

        String signature = buildSignature(spec, fn.name());
        String existingIdentity = oidIdentity.get(oid);
        if (existingIdentity != null && !existingIdentity.equals(signature)) {
          ValidationSupport.err(
              errors, "floe.function.oid.reused_for_different_signature", ruleCtx, oid);
          continue;
        }
        oidIdentity.putIfAbsent(oid, signature);

        recordSignatureInterval(
            signature, decodedRule.interval(), signatureIntervals, errors, ruleCtx);

        functionOids.add(oid);
        decodedOut.add(decodedRule);
      }
    }

    return functionOids;
  }

  private static String buildSignature(FloeFunctionSpecific spec, NameRef fnName) {
    List<Integer> args = spec.getProargtypesList();
    String canonical = NameRefUtil.canonical(fnName);
    return canonical + "/" + spec.getProrettype() + "/" + args.toString();
  }

  private static void recordSignatureInterval(
      String signature,
      VersionIntervals.VersionInterval interval,
      Map<String, List<VersionIntervals.VersionInterval>> signatureIntervals,
      List<ValidationIssue> errors,
      String ctx) {
    List<VersionIntervals.VersionInterval> intervals =
        signatureIntervals.computeIfAbsent(signature, k -> new ArrayList<>());
    for (VersionIntervals.VersionInterval existing : intervals) {
      if (VersionIntervals.overlaps(existing, interval)) {
        ValidationSupport.err(
            errors,
            "floe.function.signature.duplicate",
            ctx,
            signature,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        break;
      }
    }
    intervals.add(interval);
  }

  private static void validateProvolatile(
      String provolatile, String ctx, List<ValidationIssue> errors) {
    if (provolatile == null || provolatile.isEmpty()) {
      return;
    }
    if (provolatile.length() != 1 || "isv".indexOf(provolatile.charAt(0)) < 0) {
      ValidationSupport.err(errors, "floe.function.provolatile.invalid", ctx, provolatile);
    }
  }
}
