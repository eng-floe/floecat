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

import ai.floedb.floecat.extensions.floedb.proto.FloeCastSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class CastValidator implements SectionValidator<SimpleValidationResult> {
  private final Lookup lookup;
  private final IntervalIndex<Integer> typeIntervals;
  private final IntervalIndex<Integer> functionIntervals;
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  CastValidator(
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
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    verifyCasts(
        catalog.casts(), lookup, typeIntervals, functionIntervals, scope, runContext, errors);
    return new SimpleValidationResult(errors);
  }

  private static void verifyCasts(
      List<SystemCastDef> casts,
      Lookup lookup,
      IntervalIndex<Integer> typeIntervals,
      IntervalIndex<Integer> functionIntervals,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext,
      List<ValidationIssue> errors) {
    List<CastRow> rows = new ArrayList<>();
    Map<String, List<VersionIntervals.VersionInterval>> signatureIntervals = new LinkedHashMap<>();

    for (SystemCastDef cast : casts) {
      String ctx = ValidationSupport.context("cast", cast.name());
      List<ValidationSupport.DecodedRule<FloeCastSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, cast.engineSpecific(), FloePayloads.CAST, ctx, errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, FloePayloads.CAST.type(), ctx, errors);
      for (ValidationSupport.DecodedRule<FloeCastSpecific> decodedRule : decoded) {
        rows.add(new CastRow(ctx, decodedRule));
      }
    }

    for (CastRow row : rows) {
      String ruleCtx = ValidationSupport.contextWithInterval(row.ctx(), row.decoded().interval());
      FloeCastSpecific spec = row.decoded().payload();
      int src = spec.getCastsource();
      int tgt = spec.getCasttarget();
      if (!typeIntervals.covers(src, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.cast.type.unknown", ruleCtx, "source", src);
      }
      if (!typeIntervals.covers(tgt, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.cast.type.unknown", ruleCtx, "target", tgt);
      }

      String ctxValue = spec.getCastcontext();
      if (!ctxValue.isEmpty()
          && !ctxValue.equals("i")
          && !ctxValue.equals("a")
          && !ctxValue.equals("e")) {
        ValidationSupport.err(errors, "floe.cast.context.invalid", ruleCtx, ctxValue);
      }

      String methodValue = spec.getCastmethod();
      if (!methodValue.isEmpty()
          && !methodValue.equals("f")
          && !methodValue.equals("b")
          && !methodValue.equals("i")) {
        ValidationSupport.err(errors, "floe.cast.method.invalid", ruleCtx, methodValue);
      }

      int funcOid = spec.getCastfunc();
      if (methodValue.equals("f")) {
        if (funcOid <= 0) {
          ValidationSupport.err(errors, "floe.cast.func.required", ruleCtx);
        }
      } else {
        if (funcOid > 0) {
          ValidationSupport.err(errors, "floe.cast.func.unexpected", ruleCtx, funcOid);
        }
      }

      if (funcOid > 0 && !functionIntervals.covers(funcOid, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.cast.func.unknown", ruleCtx, funcOid);
      }

      String signature = buildSignature(src, tgt, ctxValue);
      recordSignatureInterval(
          signature,
          row.decoded().interval(),
          signatureIntervals,
          errors,
          ruleCtx,
          "floe.cast.mapping.duplicate");
    }
  }

  private static String buildSignature(int source, int target, String context) {
    return source + ":" + target + ":" + (context == null ? "" : context);
  }

  private static void recordSignatureInterval(
      String signature,
      VersionIntervals.VersionInterval interval,
      Map<String, List<VersionIntervals.VersionInterval>> signatureIntervals,
      List<ValidationIssue> errors,
      String ctx,
      String code) {
    List<VersionIntervals.VersionInterval> intervals =
        signatureIntervals.computeIfAbsent(signature, k -> new ArrayList<>());
    for (VersionIntervals.VersionInterval existing : intervals) {
      if (VersionIntervals.overlaps(existing, interval)) {
        ValidationSupport.err(
            errors,
            code,
            ctx,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        return;
      }
    }
    intervals.add(interval);
  }

  private record CastRow(String ctx, ValidationSupport.DecodedRule<FloeCastSpecific> decoded) {}
}
