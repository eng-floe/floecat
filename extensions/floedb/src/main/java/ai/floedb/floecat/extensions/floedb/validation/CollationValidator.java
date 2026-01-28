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

import ai.floedb.floecat.extensions.floedb.proto.FloeCollationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class CollationValidator implements SectionValidator<CollationValidationResult> {
  private static final String CODE_LOCALE_REQUIRED = "floe.collation.locale.required";
  private static final String CODE_SIGNATURE_DUPLICATE = "floe.collation.duplicate";

  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  CollationValidator(ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public CollationValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    List<ValidationSupport.DecodedRule<FloeCollationSpecific>> decodedRules = new ArrayList<>();
    Set<Integer> collationOids =
        verifyCollations(catalog.collations(), scope, runContext, errors, decodedRules);
    IntervalIndex<Integer> collationIntervals =
        IntervalIndex.fromDecoded(decodedRules, dr -> dr.payload().getOid());
    return new CollationValidationResult(collationOids, collationIntervals, errors);
  }

  private static Set<Integer> verifyCollations(
      List<SystemCollationDef> collations,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext,
      List<ValidationIssue> errors,
      List<ValidationSupport.DecodedRule<FloeCollationSpecific>> decodedOut) {
    Map<Integer, String> oidIdentity = new HashMap<>();
    Set<Integer> seen = new HashSet<>();
    Set<String> signatures = new HashSet<>();

    for (SystemCollationDef coll : collations) {
      String ctx = ValidationSupport.context("collation", coll.name());
      List<ValidationSupport.DecodedRule<FloeCollationSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, coll.engineSpecific(), FloePayloads.COLLATION, ctx, errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, FloePayloads.COLLATION.type(), ctx, errors);

      for (ValidationSupport.DecodedRule<FloeCollationSpecific> decodedRule : decoded) {
        String ruleCtx = ValidationSupport.contextWithInterval(ctx, decodedRule.interval());
        FloeCollationSpecific spec = decodedRule.payload();
        int oid = spec.getOid();
        if (oid <= 0) {
          ValidationSupport.err(errors, "floe.collation.oid.required", ruleCtx);
          continue;
        }

        String canonical = ValidationSupport.canonicalOrBlank(coll.name());
        String locale = coll.locale();
        if (ValidationSupport.isBlank(locale)) {
          ValidationSupport.err(errors, CODE_LOCALE_REQUIRED, ruleCtx);
          continue;
        }
        String signature = collationSignature(coll);
        String existing = oidIdentity.get(oid);
        if (existing != null && !existing.equals(canonical)) {
          ValidationSupport.err(
              errors, "floe.collation.oid.reused_for_different_object", ruleCtx, oid, canonical);
          continue;
        }
        oidIdentity.putIfAbsent(oid, canonical);
        seen.add(oid);
        if (!signatures.add(signature)) {
          ValidationSupport.err(errors, CODE_SIGNATURE_DUPLICATE, ruleCtx, signature);
          continue;
        }

        if (!spec.getCollname().equals(coll.name().getName())) {
          ValidationSupport.err(
              errors, "floe.collation.name.mismatch", ruleCtx, spec.getCollname());
        }

        decodedOut.add(decodedRule);
      }
    }
    return seen;
  }

  private static String collationSignature(SystemCollationDef coll) {
    String name = ValidationSupport.canonicalOrBlank(coll.name());
    String locale = coll.locale() == null ? "" : coll.locale();
    return name + ":" + locale;
  }
}
