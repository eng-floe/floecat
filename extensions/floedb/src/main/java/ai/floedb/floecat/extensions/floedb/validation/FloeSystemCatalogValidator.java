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

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.List;

/** Engine-specific validation for Floe catalogs. */
public final class FloeSystemCatalogValidator {
  private FloeSystemCatalogValidator() {}

  public static List<ValidationIssue> validate(SystemCatalogData catalog, ValidationScope scope) {
    if (catalog == null) {
      return List.of();
    }
    List<ValidationIssue> issues = new ArrayList<>();
    ValidationScope effectiveScope = scope == null ? new ValidationScope("") : scope;
    ValidationSupport.ValidationRunContext runContext = ValidationSupport.newRunContext();

    TypeValidationResult typeResult =
        new TypeValidator(effectiveScope, runContext).validate(catalog);
    issues.addAll(typeResult.errors());

    Lookup lookup = typeResult.lookup();
    IntervalIndex<Integer> typeIntervals = typeResult.typeIntervals();

    FunctionValidationResult fnResult =
        new FunctionValidator(lookup, effectiveScope, runContext).validate(catalog);
    issues.addAll(fnResult.errors());
    IntervalIndex<Integer> functionIntervals = fnResult.functionIntervals();
    TypeValidator.validateFunctionReferences(lookup, functionIntervals, issues);

    OperatorValidationResult opResult =
        new OperatorValidator(lookup, typeIntervals, functionIntervals, effectiveScope, runContext)
            .validate(catalog);
    issues.addAll(opResult.errors());
    IntervalIndex<Integer> operatorIntervals = opResult.operatorIntervals();

    CollationValidationResult collResult =
        new CollationValidator(effectiveScope, runContext).validate(catalog);
    issues.addAll(collResult.errors());
    IntervalIndex<Integer> collationIntervals = collResult.collationIntervals();

    issues.addAll(
        new TypeCollationValidator(lookup, collResult.collationOids(), collationIntervals)
            .validate(catalog)
            .errors());

    issues.addAll(
        new CastValidator(lookup, typeIntervals, functionIntervals, effectiveScope, runContext)
            .validate(catalog)
            .errors());

    issues.addAll(
        new AggregateValidator(
                lookup,
                typeIntervals,
                functionIntervals,
                operatorIntervals,
                effectiveScope,
                runContext)
            .validate(catalog)
            .errors());

    issues.addAll(
        new RegistryValidator(
                effectiveScope, runContext, typeIntervals, functionIntervals, operatorIntervals)
            .validate(catalog)
            .errors());
    issues.addAll(new NamespaceValidator(effectiveScope, runContext).validate(catalog).errors());
    issues.addAll(new RelationValidator(effectiveScope, runContext).validate(catalog).errors());
    issues.addAll(
        new ColumnValidator(effectiveScope, runContext, lookup).validate(catalog).errors());
    issues.addAll(new GlobalOidValidator(effectiveScope, runContext).validate(catalog).errors());

    return List.copyOf(issues);
  }
}
