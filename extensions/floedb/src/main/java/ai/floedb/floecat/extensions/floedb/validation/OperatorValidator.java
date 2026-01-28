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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
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

final class OperatorValidator implements SectionValidator<OperatorValidationResult> {
  private final Lookup lookup;
  private final IntervalIndex<Integer> typeIntervals;
  private final IntervalIndex<Integer> functionIntervals;
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  OperatorValidator(
      Lookup lookup,
      IntervalIndex<Integer> typeIntervals,
      IntervalIndex<Integer> functionIntervals,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext) {
    this.lookup = lookup;
    this.typeIntervals = typeIntervals;
    this.functionIntervals = functionIntervals;
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public OperatorValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    List<OperatorRow> rows = new ArrayList<>();
    List<ValidationSupport.DecodedRule<FloeOperatorSpecific>> decodedRules = new ArrayList<>();
    Map<Integer, String> oidIdentity = new HashMap<>();
    Map<String, List<VersionIntervals.VersionInterval>> signatureIntervals = new LinkedHashMap<>();

    for (SystemOperatorDef op : catalog.operators()) {
      String ctx = ValidationSupport.context("operator", op.name());
      List<ValidationSupport.DecodedRule<FloeOperatorSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, op.engineSpecific(), FloePayloads.OPERATOR, ctx, errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, FloePayloads.OPERATOR.type(), ctx, errors);
      for (ValidationSupport.DecodedRule<FloeOperatorSpecific> decodedRule : decoded) {
        rows.add(new OperatorRow(ctx, op, decodedRule));
        decodedRules.add(decodedRule);
      }
    }

    IntervalIndex<Integer> operatorIntervals =
        IntervalIndex.fromDecoded(decodedRules, dr -> dr.payload().getOid());
    Set<Integer> operatorOids = new LinkedHashSet<>(operatorIntervals.coverageByKey().keySet());

    for (OperatorRow row : rows) {
      String ruleCtx = ValidationSupport.contextWithInterval(row.ctx(), row.decoded().interval());
      FloeOperatorSpecific spec = row.decoded().payload();
      validateOprNamespace(spec, ruleCtx, errors);

      checkOperatorType(
          ruleCtx,
          "left",
          spec.getOprleft(),
          row.op().leftType(),
          row.decoded().interval(),
          errors,
          true);
      checkOperatorType(
          ruleCtx,
          "right",
          spec.getOprright(),
          row.op().rightType(),
          row.decoded().interval(),
          errors,
          false);

      int resultOid = spec.getOprresult();
      if (resultOid > 0 && !typeIntervals.covers(resultOid, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.operator.result.unknown", ruleCtx, resultOid);
      }

      int functionOid = spec.getOprcode();
      if (functionOid > 0 && !functionIntervals.covers(functionOid, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.operator.function.unknown", ruleCtx, functionOid);
      }

      int commutator = spec.getOprcom();
      if (commutator > 0 && !operatorIntervals.covers(commutator, row.decoded().interval())) {
        ValidationSupport.err(
            errors, "floe.operator.link.unknown", ruleCtx, "commutator", commutator);
      }

      int negator = spec.getOprnegate();
      if (negator > 0 && !operatorIntervals.covers(negator, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.operator.link.unknown", ruleCtx, "negator", negator);
      }

      checkFunctionReference(
          errors,
          "floe.operator.rest.unknown",
          row.decoded().interval(),
          spec.getOprrest(),
          functionIntervals,
          ruleCtx);

      checkFunctionReference(
          errors,
          "floe.operator.join.unknown",
          row.decoded().interval(),
          spec.getOprjoin(),
          functionIntervals,
          ruleCtx);

      String signature = buildSignature(row.op(), spec);
      String existing = oidIdentity.get(spec.getOid());
      if (existing != null && !existing.equals(signature)) {
        ValidationSupport.err(
            errors, "floe.operator.oid.reused_for_different_signature", ruleCtx, spec.getOid());
        continue;
      }
      oidIdentity.putIfAbsent(spec.getOid(), signature);
      recordSignatureInterval(
          signature, row.decoded().interval(), signatureIntervals, errors, ruleCtx);
    }

    return new OperatorValidationResult(operatorOids, operatorIntervals, errors);
  }

  private void checkOperatorType(
      String ctx,
      String lane,
      int oid,
      NameRef ref,
      VersionIntervals.VersionInterval interval,
      List<ValidationIssue> errors,
      boolean optional) {
    if (oid <= 0) {
      return;
    }

    String canonical = canonicalName(ref, oid, interval);
    if (ValidationSupport.isBlank(canonical)) {
      if (!optional) {
        ValidationSupport.err(errors, "floe.operator.type.missing", ctx, lane, oid);
      }
      return;
    }

    TypeInfo info = lookup.find(canonical, interval);
    if (info == null) {
      ValidationSupport.err(errors, "floe.operator.type.unknown", ctx, lane, canonical);
      return;
    }

    if (!typeIntervals.covers(oid, interval)) {
      ValidationSupport.err(errors, "floe.operator.type.unknown", ctx, lane, oid);
      return;
    }

    int expectedOid = info.payload().getOid();
    if (expectedOid != oid) {
      ValidationSupport.err(errors, "floe.operator.type.mismatch", ctx, lane, expectedOid, oid);
    }
  }

  private record OperatorRow(
      String ctx,
      SystemOperatorDef op,
      ValidationSupport.DecodedRule<FloeOperatorSpecific> decoded) {}

  private String canonicalName(NameRef ref, int oid, VersionIntervals.VersionInterval interval) {
    String canonical = ref == null ? "" : ValidationSupport.canonicalOrBlank(ref);
    if (!ValidationSupport.isBlank(canonical)) {
      return canonical;
    }
    for (TypeInfo info : lookup.byOid(oid)) {
      if (interval == null || VersionIntervals.covers(List.of(info.interval()), interval)) {
        String fallback = ValidationSupport.canonicalOrBlank(info.type().name());
        if (!ValidationSupport.isBlank(fallback)) {
          return fallback;
        }
      }
    }
    return "";
  }

  private static String buildSignature(SystemOperatorDef operator, FloeOperatorSpecific spec) {
    String canonicalName =
        operator == null || operator.name() == null ? "" : NameRefUtil.canonical(operator.name());
    if (canonicalName == null || canonicalName.isBlank()) {
      canonicalName = spec.getOprname();
    }
    return canonicalName
        + ":"
        + spec.getOprleft()
        + "/"
        + spec.getOprright()
        + "->"
        + spec.getOprresult();
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
            "floe.operator.signature.duplicate",
            ctx,
            signature,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        break;
      }
    }
    intervals.add(interval);
  }

  private static void validateOprNamespace(
      FloeOperatorSpecific spec, String ctx, List<ValidationIssue> errors) {
    if (!spec.hasOprnamespace()) {
      return;
    }
    if (spec.getOprnamespace() <= 0) {
      ValidationSupport.err(errors, "floe.operator.namespace.invalid", ctx, spec.getOprnamespace());
    }
  }

  private static void checkFunctionReference(
      List<ValidationIssue> errors,
      String code,
      VersionIntervals.VersionInterval interval,
      int oid,
      IntervalIndex<Integer> functionIntervals,
      String ctx) {
    if (oid <= 0) {
      return;
    }
    if (!functionIntervals.covers(oid, interval)) {
      ValidationSupport.err(errors, code, ctx, oid);
    }
  }
}
