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

import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

final class TypeCollationValidator implements SectionValidator<SimpleValidationResult> {
  private final Lookup lookup;
  private final Set<Integer> collationOids;
  private final IntervalIndex<Integer> collationIntervals;

  TypeCollationValidator(
      Lookup lookup, Set<Integer> collationOids, IntervalIndex<Integer> collationIntervals) {
    this.lookup = lookup;
    this.collationOids = collationOids;
    this.collationIntervals = collationIntervals;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    verifyTypeCollations(lookup, collationOids, collationIntervals, errors);
    return new SimpleValidationResult(errors);
  }

  private static void verifyTypeCollations(
      Lookup lookup,
      Set<Integer> collationOids,
      IntervalIndex<Integer> collationIntervals,
      List<ValidationIssue> errors) {
    if (collationOids == null || collationOids.isEmpty()) {
      return;
    }
    for (TypeInfo info : lookup.all()) {
      FloeTypeSpecific spec = info.payload();
      if (spec.hasTypcollation() && spec.getTypcollation() != 0) {
        int collOid = spec.getTypcollation();
        String ctx = ValidationSupport.context("type", info.type().name());
        String ruleCtx = ValidationSupport.contextWithInterval(ctx, info.interval());
        if (!collationOids.contains(collOid)) {
          ValidationSupport.err(errors, "floe.collation.unknown", ruleCtx, collOid);
          continue;
        }
        if (!collationIntervals.covers(collOid, info.interval())) {
          ValidationSupport.err(errors, "floe.collation.unknown", ruleCtx, collOid);
        }
      }
    }
  }
}
