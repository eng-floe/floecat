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

import ai.floedb.floecat.extensions.floedb.proto.FloeIndexAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexOperatorClasses;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexOperatorFamilies;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexOperatorStrategies;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexSupportProcedures;
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

    List<ValidationSupport.DecodedRule<FloeIndexAccessMethods>> accessRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.INDEX_ACCESS_METHODS,
            CONTEXT_ACCESS_METHODS,
            errors);
    IntervalIndex<Integer> accessIndex = processAccessMethods(accessRules, errors);

    List<ValidationSupport.DecodedRule<FloeIndexOperatorFamilies>> familyRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.INDEX_OPERATOR_FAMILIES,
            CONTEXT_OPERATOR_FAMILIES,
            errors);
    IntervalIndex<Integer> familyIndex = processOperatorFamilies(familyRules, accessIndex, errors);

    List<ValidationSupport.DecodedRule<FloeIndexOperatorClasses>> classRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.INDEX_OPERATOR_CLASSES,
            CONTEXT_OPERATOR_CLASSES,
            errors);
    IntervalIndex<Integer> classIndex = processOperatorClasses(classRules, familyIndex, errors);

    List<ValidationSupport.DecodedRule<FloeIndexOperatorStrategies>> strategyRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.INDEX_OPERATOR_STRATEGIES,
            CONTEXT_OPERATOR_STRATEGIES,
            errors);
    processOperatorStrategies(strategyRules, familyIndex, accessIndex, classIndex, errors);

    List<ValidationSupport.DecodedRule<FloeIndexSupportProcedures>> procRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            catalog.registryEngineSpecific(),
            FloePayloads.INDEX_SUPPORT_PROCEDURES,
            CONTEXT_SUPPORT_PROCEDURES,
            errors);
    processSupportProcedures(procRules, familyIndex, errors);

    return new SimpleValidationResult(errors);
  }

  private static IntervalIndex<Integer> processAccessMethods(
      List<ValidationSupport.DecodedRule<FloeIndexAccessMethods>> rules,
      List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeIndexAccessMethods> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeIndexAccessMethods.AccessMethod method : decoded.payload().getMethodsList()) {
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
      List<ValidationSupport.DecodedRule<FloeIndexOperatorFamilies>> rules,
      IntervalIndex<Integer> accessIndex,
      List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeIndexOperatorFamilies> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeIndexOperatorFamilies.OperatorFamily family : decoded.payload().getFamiliesList()) {
        int oid = family.getOid();
        if (oid <= 0) {
          ValidationSupport.err(
              errors, "floe.registry.operator_family.oid.required", CONTEXT_OPERATOR_FAMILIES);
          continue;
        }
        int accessMethod = family.getAccessMethodOid();
        if (!ensureCoverage(
            accessIndex,
            accessMethod,
            interval,
            errors,
            "floe.registry.operator_family.access_method.unknown",
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
      List<ValidationSupport.DecodedRule<FloeIndexOperatorClasses>> rules,
      IntervalIndex<Integer> familyIndex,
      List<ValidationIssue> errors) {
    Map<Integer, List<VersionIntervals.VersionInterval>> intervals = new LinkedHashMap<>();
    Map<String, List<VersionIntervals.VersionInterval>> defaultIntervals = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeIndexOperatorClasses> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeIndexOperatorClasses.OperatorClass cls : decoded.payload().getClassesList()) {
        int oid = cls.getOid();
        if (oid <= 0) {
          ValidationSupport.err(
              errors, "floe.registry.operator_class.oid.required", CONTEXT_OPERATOR_CLASSES);
          continue;
        }
        if (!ensureCoverage(
            familyIndex,
            cls.getFamilyOid(),
            interval,
            errors,
            "floe.registry.operator_class.family.unknown",
            CONTEXT_OPERATOR_CLASSES)) {
          continue;
        }
        if (!ensureCoverage(
            typeIntervals,
            cls.getInputTypeOid(),
            interval,
            errors,
            "floe.registry.operator_class.type.unknown",
            CONTEXT_OPERATOR_CLASSES)) {
          continue;
        }
        if (cls.getIsDefault()) {
          String key = cls.getFamilyOid() + ":" + cls.getInputTypeOid();
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
      List<ValidationSupport.DecodedRule<FloeIndexOperatorStrategies>> rules,
      IntervalIndex<Integer> familyIndex,
      IntervalIndex<Integer> accessIndex,
      IntervalIndex<Integer> classIndex,
      List<ValidationIssue> errors) {
    Map<String, List<VersionIntervals.VersionInterval>> seen = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeIndexOperatorStrategies> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeIndexOperatorStrategies.OperatorStrategy strategy :
          decoded.payload().getEntriesList()) {
        if (!ensureCoverage(
            familyIndex,
            strategy.getFamilyOid(),
            interval,
            errors,
            "floe.registry.operator_strategy.family.unknown",
            CONTEXT_OPERATOR_STRATEGIES)) {
          continue;
        }
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.operator.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getOperatorOid(),
            operatorIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.left_type.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getLeftTypeOid(),
            typeIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.operator_strategy.right_type.unknown",
            CONTEXT_OPERATOR_STRATEGIES,
            strategy.getRightTypeOid(),
            typeIntervals,
            interval);
        if (strategy.getSortFamilyOid() > 0) {
          ensureCoverage(
              familyIndex,
              strategy.getSortFamilyOid(),
              interval,
              errors,
              "floe.registry.operator_strategy.sort_family.unknown",
              CONTEXT_OPERATOR_STRATEGIES);
        }
        String key =
            strategy.getFamilyOid()
                + ":"
                + strategy.getStrategy()
                + ":"
                + strategy.getLeftTypeOid()
                + ":"
                + strategy.getRightTypeOid();
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
      List<ValidationSupport.DecodedRule<FloeIndexSupportProcedures>> rules,
      IntervalIndex<Integer> familyIndex,
      List<ValidationIssue> errors) {
    Map<String, List<VersionIntervals.VersionInterval>> seen = new LinkedHashMap<>();
    for (ValidationSupport.DecodedRule<FloeIndexSupportProcedures> decoded : rules) {
      VersionIntervals.VersionInterval interval = decoded.interval();
      for (FloeIndexSupportProcedures.SupportProcedure proc : decoded.payload().getEntriesList()) {
        if (!ensureCoverage(
            familyIndex,
            proc.getFamilyOid(),
            interval,
            errors,
            "floe.registry.support_procedure.family.unknown",
            CONTEXT_SUPPORT_PROCEDURES)) {
          continue;
        }
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.function.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getFunctionOid(),
            functionIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.left_type.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getLeftTypeOid(),
            typeIntervals,
            interval);
        checkFunctionReference(
            errors,
            "floe.registry.support_procedure.right_type.unknown",
            CONTEXT_SUPPORT_PROCEDURES,
            proc.getRightTypeOid(),
            typeIntervals,
            interval);
        String key =
            proc.getFamilyOid()
                + ":"
                + proc.getProcNumber()
                + ":"
                + proc.getLeftTypeOid()
                + ":"
                + proc.getRightTypeOid();
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
