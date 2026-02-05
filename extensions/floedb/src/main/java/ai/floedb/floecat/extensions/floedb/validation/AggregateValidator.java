/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
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

import ai.floedb.floecat.extensions.floedb.proto.FloeAggregateSpecific;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class AggregateValidator implements SectionValidator<SimpleValidationResult> {
  private final Lookup lookup;
  private final IntervalIndex<Integer> typeIntervals;
  private final IntervalIndex<Integer> functionIntervals;
  private final IntervalIndex<Integer> operatorIntervals;
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  AggregateValidator(
      Lookup lookup,
      IntervalIndex<Integer> typeIntervals,
      IntervalIndex<Integer> functionIntervals,
      IntervalIndex<Integer> operatorIntervals,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext) {
    this.lookup = lookup;
    this.typeIntervals = typeIntervals;
    this.functionIntervals = functionIntervals;
    this.operatorIntervals = operatorIntervals;
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    verifyAggregates(
        catalog.aggregates(),
        lookup,
        typeIntervals,
        functionIntervals,
        operatorIntervals,
        scope,
        runContext,
        errors);
    return new SimpleValidationResult(errors);
  }

  private static void verifyAggregates(
      List<SystemAggregateDef> aggs,
      Lookup lookup,
      IntervalIndex<Integer> typeIntervals,
      IntervalIndex<Integer> functionIntervals,
      IntervalIndex<Integer> operatorIntervals,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext,
      List<ValidationIssue> errors) {
    Map<Integer, String> oidIdentity = new HashMap<>();
    List<AggregateRow> rows = new ArrayList<>();

    for (SystemAggregateDef agg : aggs) {
      String ctx = ValidationSupport.context("aggregate", agg.name());
      List<ValidationSupport.DecodedRule<FloeAggregateSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext,
              scope,
              agg.engineSpecific(),
              AGGREGATE,
              FloeAggregateSpecific.class,
              ctx,
              errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, AGGREGATE.type(), ctx, errors);
      for (ValidationSupport.DecodedRule<FloeAggregateSpecific> decodedRule : decoded) {
        rows.add(new AggregateRow(ctx, agg, decodedRule));
      }
    }

    for (AggregateRow row : rows) {
      String ruleCtx = ValidationSupport.contextWithInterval(row.ctx(), row.decoded().interval());
      FloeAggregateSpecific spec = row.decoded().payload();
      int oid = spec.getAggfnoid();
      if (oid <= 0) {
        ValidationSupport.err(errors, "floe.agg.fnoid.required", ruleCtx, oid);
        continue;
      }

      String canonical = ValidationSupport.canonicalOrBlank(row.agg().name());
      String existing = oidIdentity.get(oid);
      if (existing != null && !existing.equals(canonical)) {
        ValidationSupport.err(
            errors, "floe.aggregate.oid.reused_for_different_object", ruleCtx, oid, canonical);
        continue;
      }
      oidIdentity.putIfAbsent(oid, canonical);

      if (!functionIntervals.covers(oid, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.aggregate.fnoid.unknown", ruleCtx, oid);
      }

      checkFunctionReference(
          errors, "floe.aggregate.transfn.unknown", row, spec.getAggtransfn(), functionIntervals);
      checkFunctionReference(
          errors, "floe.aggregate.finalfn.unknown", row, spec.getAggfinalfn(), functionIntervals);
      checkFunctionReference(
          errors, "floe.aggregate.mtransfn.unknown", row, spec.getAggmtransfn(), functionIntervals);
      checkFunctionReference(
          errors,
          "floe.aggregate.minvtransfn.unknown",
          row,
          spec.getAggminvtransfn(),
          functionIntervals);
      checkFunctionReference(
          errors, "floe.aggregate.mfinalfn.unknown", row, spec.getAggmfinalfn(), functionIntervals);

      int sortOp = spec.getAggsortop();
      if (sortOp > 0
          && operatorIntervals != null
          && !operatorIntervals.covers(sortOp, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.operator.link.unknown", ruleCtx, "aggsortop", sortOp);
      }

      int transType = spec.getAggtranstype();
      if (transType > 0 && !typeIntervals.covers(transType, row.decoded().interval())) {
        ValidationSupport.err(errors, "floe.aggregate.transtype.unknown", ruleCtx, transType);
      }
    }
  }

  private static void checkFunctionReference(
      List<ValidationIssue> errors,
      String code,
      AggregateRow row,
      int oid,
      IntervalIndex<Integer> functionIntervals) {
    if (oid <= 0) {
      return;
    }
    String ruleCtx = ValidationSupport.contextWithInterval(row.ctx(), row.decoded().interval());
    if (!functionIntervals.covers(oid, row.decoded().interval())) {
      ValidationSupport.err(errors, code, ruleCtx, oid);
    }
  }

  private record AggregateRow(
      String ctx,
      SystemAggregateDef agg,
      ValidationSupport.DecodedRule<FloeAggregateSpecific> decoded) {}
}
