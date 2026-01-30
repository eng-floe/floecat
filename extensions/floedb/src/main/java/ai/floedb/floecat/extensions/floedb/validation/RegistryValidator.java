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

import ai.floedb.floecat.extensions.floedb.proto.FloeAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorClasses;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorFamilies;
import ai.floedb.floecat.extensions.floedb.proto.FloeProcedureAccessMethods;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class RegistryValidator implements SectionValidator<SimpleValidationResult> {
  private static final String CONTEXT_ACCESS_METHODS = "registry:access_methods";
  private static final String CONTEXT_OPERATOR_FAMILIES = "registry:operator_families";
  private static final String CONTEXT_OPERATOR_CLASSES = "registry:operator_classes";
  private static final String CONTEXT_OPERATOR_STRATEGIES = "registry:operator_strategies";
  private static final String CONTEXT_SUPPORT_PROCEDURES = "registry:support_procedures";

  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;
  private final IntervalIndex<Integer> typeIntervals;
  private final IntervalIndex<Integer> functionIntervals;
  private final IntervalIndex<Integer> operatorIntervals;

  RegistryValidator(
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext,
      IntervalIndex<Integer> typeIntervals,
      IntervalIndex<Integer> functionIntervals,
      IntervalIndex<Integer> operatorIntervals) {
    this.scope = scope;
    this.runContext = runContext;
    this.typeIntervals = typeIntervals;
    this.functionIntervals = functionIntervals;
    this.operatorIntervals = operatorIntervals;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();

    List<ValidationSupport.DecodedRule<FloeAccessMethods>> accessRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.ACCESS_METHODS,
            CONTEXT_ACCESS_METHODS,
            errors);
    IntervalIndex<Integer> accessIndex = processAccessMethods(accessRules, errors);

    List<ValidationSupport.DecodedRule<FloeOperatorFamilies>> familyRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.OPERATOR_FAMILIES,
            CONTEXT_OPERATOR_FAMILIES,
            errors);
    IntervalIndex<Integer> familyIndex = processOperatorFamilies(familyRules, accessIndex, errors);

    List<ValidationSupport.DecodedRule<FloeOperatorClasses>> classRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.OPERATOR_CLASSES,
            CONTEXT_OPERATOR_CLASSES,
            errors);
    IntervalIndex<Integer> classIndex = processOperatorClasses(classRules, familyIndex, errors);

    List<ValidationSupport.DecodedRule<FloeOperatorAccessMethods>> strategyRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.OPERATOR_ACCESS_METHODS,
            CONTEXT_OPERATOR_STRATEGIES,
            errors);
    processOperatorStrategies(strategyRules, familyIndex, accessIndex, classIndex, errors);

    List<ValidationSupport.DecodedRule<FloeProcedureAccessMethods>> procRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.PROCEDURE_ACCESS_METHODS,
            CONTEXT_SUPPORT_PROCEDURES,
            errors);
    processSupportProcedures(procRules, familyIndex, errors);

    return new SimpleValidationResult(errors);
  }

  private static IntervalIndex<Integer> processAccessMethods(
      List<ValidationSupport.DecodedRule<FloeAccessMethods>> rules, List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeAccessMethods> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeAccessMethods.AccessMethod method : decoded.payload().getMethodsList()) {
        int oid = method.getOid();
        if (oid <= 0) {
          ValidationSupport.err(
              errors, "floe.registry.access_method.oid.required", CONTEXT_ACCESS_METHODS);
          continue;
        }
        addInterval(
            intervals,
            oid,
            interval,
            errors,
            CONTEXT_ACCESS_METHODS,
            "floe.registry.access_method.oid.duplicate");
      }
    }
    return buildIndex(intervals);
  }

  private static IntervalIndex<Integer> processOperatorFamilies(
      List<ValidationSupport.DecodedRule<FloeOperatorFamilies>> rules,
      IntervalIndex<Integer> accessIndex,
      List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeOperatorFamilies> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeOperatorFamilies.OperatorFamily family : decoded.payload().getFamiliesList()) {
        int oid = family.getOid();
        if (oid <= 0) {
          ValidationSupport.err(
              errors, "floe.registry.operator_family.oid.required", CONTEXT_OPERATOR_FAMILIES);
          continue;
        }
        int accessMethod = family.getOpfmethod();
        if (!ensureCoverage(
            accessIndex,
            accessMethod,
            interval,
            errors,
            "floe.registry.operator_family.opfmethod.unknown",
            CONTEXT_OPERATOR_FAMILIES)) {
          continue;
        }
        addInterval(
            intervals,
            oid,
            interval,
            errors,
            CONTEXT_OPERATOR_FAMILIES,
            "floe.registry.operator_family.oid.duplicate");
      }
    }
    return buildIndex(intervals);
  }

  private IntervalIndex<Integer> processOperatorClasses(
      List<ValidationSupport.DecodedRule<FloeOperatorClasses>> rules,
      IntervalIndex<Integer> familyIndex,
      List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    Map<String, List<VersionIntervals.VersionInterval>> defaultIntervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeOperatorClasses> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeOperatorClasses.OperatorClass cls : decoded.payload().getClassesList()) {
        int oid = cls.getOid();
        if (oid <= 0) {
          ValidationSupport.err(
              errors, "floe.registry.operator_class.oid.required", CONTEXT_OPERATOR_CLASSES);
          continue;
        }
        if (!ensureCoverage(
            familyIndex,
            cls.getOpcfamily(),
            interval,
            errors,
            "floe.registry.operator_class.opcfamily.unknown",
            CONTEXT_OPERATOR_CLASSES)) {
          continue;
        }
        if (!ensureCoverage(
            typeIntervals,
            cls.getOpcintype(),
            interval,
            errors,
            "floe.registry.operator_class.opcintype.unknown",
            CONTEXT_OPERATOR_CLASSES)) {
          continue;
        }
        if (cls.getOpcdefault()) {
          String key = cls.getOpcfamily() + ":" + cls.getOpcintype();
          enforceDefaultUnique(defaultIntervals, key, interval, errors, CONTEXT_OPERATOR_CLASSES);
        }
        addInterval(
            intervals,
            oid,
            interval,
            errors,
            CONTEXT_OPERATOR_CLASSES,
            "floe.registry.operator_class.oid.duplicate");
      }
    }
    return buildIndex(intervals);
  }

  private void processOperatorStrategies(
      List<ValidationSupport.DecodedRule<FloeOperatorAccessMethods>> rules,
      IntervalIndex<Integer> familyIndex,
      IntervalIndex<Integer> accessIndex,
      IntervalIndex<Integer> classIndex,
      List<ValidationIssue> errors) {
    Map<String, List<VersionIntervals.VersionInterval>> seen = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeOperatorAccessMethods> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeOperatorAccessMethods.OperatorAccessMethod strategy :
          decoded.payload().getEntriesList()) {
        if (!ensureCoverage(
            familyIndex,
            strategy.getAmopfamily(),
            interval,
            errors,
            "floe.registry.operator_strategy.amopfamily.unknown",
            CONTEXT_OPERATOR_STRATEGIES)) {
          continue;
        }
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.amopopr.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getAmopopr(),
            operatorIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.amoplefttype.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getAmoplefttype(),
            typeIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.amoprighttype.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getAmoprighttype(),
            typeIntervals,
            interval);
        if (strategy.getAmopsortfamily() > 0) {
          ensureCoverage(
              familyIndex,
              strategy.getAmopsortfamily(),
              interval,
              errors,
              "floe.registry.operator_strategy.amopsortfamily.unknown",
              CONTEXT_OPERATOR_STRATEGIES);
        }
        String key =
            strategy.getAmopfamily()
                + ":"
                + strategy.getAmopstrategy()
                + ":"
                + strategy.getAmoplefttype()
                + ":"
                + strategy.getAmoprighttype();
        recordInterval(
            seen,
            key,
            interval,
            errors,
            CONTEXT_OPERATOR_STRATEGIES,
            "floe.registry.operator_strategy.duplicate");
      }
    }
  }

  private void processSupportProcedures(
      List<ValidationSupport.DecodedRule<FloeProcedureAccessMethods>> rules,
      IntervalIndex<Integer> familyIndex,
      List<ValidationIssue> errors) {
    Map<String, List<VersionIntervals.VersionInterval>> seen = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeProcedureAccessMethods> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeProcedureAccessMethods.ProcedureAccessMethod proc :
          decoded.payload().getEntriesList()) {
        if (!ensureCoverage(
            familyIndex,
            proc.getAmprocfamily(),
            interval,
            errors,
            "floe.registry.support_procedure.amprocfamily.unknown",
            CONTEXT_SUPPORT_PROCEDURES)) {
          continue;
        }
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.amproc.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getAmproc(),
            functionIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.amproclefttype.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getAmproclefttype(),
            typeIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.amprocrighttype.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getAmprocrighttype(),
            typeIntervals,
            interval);
        String key =
            proc.getAmprocfamily()
                + ":"
                + proc.getAmproc()
                + ":"
                + proc.getAmproclefttype()
                + ":"
                + proc.getAmprocrighttype();
        recordInterval(
            seen,
            key,
            interval,
            errors,
            CONTEXT_SUPPORT_PROCEDURES,
            "floe.registry.support_procedure.duplicate");
      }
    }
  }

  private static IntervalIndex<Integer> buildIndex(
      Map<Integer, List<VersionIntervals.VersionInterval>> intervals) {
    Map<Integer, List<VersionIntervals.VersionInterval>> coverage = new LinkedHashMap<>();
    intervals.forEach((key, value) -> coverage.put(key, VersionIntervals.union(value)));
    return new IntervalIndex<>(Map.copyOf(coverage));
  }

  private static void addInterval(
      Map<Integer, List<VersionIntervals.VersionInterval>> bucket,
      int oid,
      VersionIntervals.VersionInterval interval,
      List<ValidationIssue> errors,
      String ctx,
      String duplicateCode) {
    List<VersionIntervals.VersionInterval> intervals =
        bucket.computeIfAbsent(oid, k -> new ArrayList<>());
    for (VersionIntervals.VersionInterval existing : intervals) {
      if (VersionIntervals.overlaps(existing, interval)) {
        ValidationSupport.err(
            errors,
            duplicateCode,
            ctx,
            oid,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        return;
      }
    }
    intervals.add(interval);
  }

  private static void recordInterval(
      Map<String, List<VersionIntervals.VersionInterval>> bucket,
      String key,
      VersionIntervals.VersionInterval interval,
      List<ValidationIssue> errors,
      String ctx,
      String code) {
    List<VersionIntervals.VersionInterval> intervals =
        bucket.computeIfAbsent(key, k -> new ArrayList<>());
    for (VersionIntervals.VersionInterval existing : intervals) {
      if (VersionIntervals.overlaps(existing, interval)) {
        ValidationSupport.err(
            errors,
            code,
            ctx,
            key,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        return;
      }
    }
    intervals.add(interval);
  }

  private static boolean ensureCoverage(
      IntervalIndex<Integer> index,
      int oid,
      VersionIntervals.VersionInterval interval,
      List<ValidationIssue> errors,
      String code,
      String ctx) {
    if (oid <= 0) {
      return false;
    }
    if (index == null || !index.covers(oid, interval)) {
      ValidationSupport.err(errors, code, ctx, oid);
      return false;
    }
    return true;
  }

  private static void enforceDefaultUnique(
      Map<String, List<VersionIntervals.VersionInterval>> bucket,
      String key,
      VersionIntervals.VersionInterval interval,
      List<ValidationIssue> errors,
      String ctx) {
    List<VersionIntervals.VersionInterval> intervals =
        bucket.computeIfAbsent(key, k -> new ArrayList<>());
    for (VersionIntervals.VersionInterval existing : intervals) {
      if (VersionIntervals.overlaps(existing, interval)) {
        ValidationSupport.err(
            errors,
            "floe.registry.operator_class.default.duplicate",
            ctx,
            key,
            ValidationSupport.formatInterval(existing),
            ValidationSupport.formatInterval(interval));
        return;
      }
    }
    intervals.add(interval);
  }

  private static void checkFunctionReference(
      List<ValidationIssue> errors,
      String code,
      String ctx,
      int oid,
      IntervalIndex<Integer> index,
      VersionIntervals.VersionInterval interval) {
    if (oid <= 0) {
      return;
    }
    if (index == null || !index.covers(oid, interval)) {
      ValidationSupport.err(errors, code, ctx, oid);
    }
  }
}
