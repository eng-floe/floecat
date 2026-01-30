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
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.VersionIntervals;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class TypeValidator implements SectionValidator<TypeValidationResult> {
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  TypeValidator(ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public TypeValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    List<ValidationSupport.DecodedRule<FloeTypeSpecific>> decodedRules = new ArrayList<>();
    TypeIndex index = indexTypes(catalog.types(), errors, decodedRules, scope, runContext);
    IntervalIndex<Integer> typeIntervals =
        IntervalIndex.fromDecoded(decodedRules, dr -> dr.payload().getOid());
    Lookup lookup = new Lookup(index.typesByName(), index.typesByOid());
    validateArrayTypes(lookup, index, typeIntervals, errors);
    return new TypeValidationResult(lookup, typeIntervals, errors);
  }

  private static TypeIndex indexTypes(
      List<SystemTypeDef> types,
      List<ValidationIssue> errors,
      List<ValidationSupport.DecodedRule<FloeTypeSpecific>> decodedOut,
      ValidationScope scope,
      ValidationSupport.ValidationRunContext runContext) {
    Map<Integer, List<TypeInfo>> typesByOid = new HashMap<>();
    Map<String, List<TypeInfo>> typesByName = new HashMap<>();
    Map<Integer, String> oidIdentity = new HashMap<>();

    for (SystemTypeDef type : types) {
      String ctx = ValidationSupport.context("type", type.name());

      if (type.name() == null) {
        ValidationSupport.err(errors, "floe.type.name.required", ctx);
        continue;
      }
      String canonical = ValidationSupport.canonicalOrBlank(type.name());
      if (ValidationSupport.isBlank(canonical)) {
        ValidationSupport.err(errors, "floe.type.name.required", ctx);
        continue;
      }

      List<ValidationSupport.DecodedRule<FloeTypeSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, type.engineSpecific(), FloePayloads.TYPE, ctx, errors);
      if (decoded.isEmpty()) {
        continue;
      }
      ValidationSupport.detectRuleOverlaps(decoded, FloePayloads.TYPE.type(), ctx, errors);
      for (ValidationSupport.DecodedRule<FloeTypeSpecific> decodedRule : decoded) {
        FloeTypeSpecific spec = decodedRule.payload();
        int oid = spec.getOid();
        String ruleCtx = ValidationSupport.contextWithInterval(ctx, decodedRule.interval());

        if (!ValidationSupport.requirePositiveOid(errors, "floe.type.oid.required", ruleCtx, oid)) {
          continue;
        }

        String existingIdentity = oidIdentity.get(oid);
        if (existingIdentity != null && !existingIdentity.equals(canonical)) {
          ValidationSupport.err(
              errors, "floe.type.oid.reused_for_different_object", ruleCtx, oid, canonical);
          continue;
        }
        oidIdentity.putIfAbsent(oid, canonical);

        if (!spec.getTypname().equals(type.name().getName())) {
          ValidationSupport.err(errors, "floe.type.name.mismatch", ruleCtx, spec.getTypname());
        }

        if (!type.array() && spec.hasTypelem() && spec.getTypelem() != 0) {
          ValidationSupport.err(
              errors, "floe.type.element_oid.unexpected", ruleCtx, spec.getTypelem());
        }

        if (!spec.hasTypcategory()) {
          ValidationSupport.err(errors, "floe.type.typcategory.required", ruleCtx);
        }

        if (!List.of("A", "B", "C", "D", "E", "G", "I", "N", "P", "R", "S", "T", "U", "V", "X", "Z")
            .contains(spec.getTypcategory())) {
          ValidationSupport.err(
              errors, "floe.type.typcategory.invalid", ruleCtx, spec.getTypcategory());
        }

        if (!spec.hasTypispreferred()) {
          ValidationSupport.err(errors, "floe.type.typispreferred.required", ruleCtx);
        }

        decodedOut.add(decodedRule);
        TypeInfo info = new TypeInfo(type, spec, decodedRule.interval());
        typesByOid.computeIfAbsent(oid, k -> new ArrayList<>()).add(info);
        typesByName.computeIfAbsent(canonical, k -> new ArrayList<>()).add(info);
      }
    }

    return new TypeIndex(typesByName, typesByOid);
  }

  private static void validateArrayTypes(
      Lookup lookup,
      TypeIndex index,
      IntervalIndex<Integer> typeIntervals,
      List<ValidationIssue> errors) {
    for (List<TypeInfo> infos : index.typesByOid().values()) {
      for (TypeInfo info : infos) {
        if (!info.type().array()) {
          continue;
        }

        String ctx = ValidationSupport.context("type", info.type().name());
        String ruleCtx = ValidationSupport.contextWithInterval(ctx, info.interval());
        int elementOid = info.payload().getTypelem();
        if (elementOid <= 0) {
          ValidationSupport.err(errors, "floe.type.array.typelem.required", ctx);
          continue;
        }

        List<TypeInfo> elementInfos = index.typesByOid().get(elementOid);
        if (elementInfos == null || elementInfos.isEmpty()) {
          ValidationSupport.err(errors, "floe.type.array.typelem.unknown", ctx, elementOid);
          continue;
        }
        TypeInfo element = elementInfos.get(0);

        if (!typeIntervals.covers(elementOid, info.interval())) {
          ValidationSupport.err(
              errors, "floe.type.array.element_coverage.missing", ruleCtx, elementOid);
        }

        NameRef catalogElement = info.type().elementType();
        if (catalogElement != null) {
          String elementCanonical = ValidationSupport.canonicalOrBlank(catalogElement);
          if (!ValidationSupport.isBlank(elementCanonical)) {
            TypeInfo expectedElement = lookup.find(elementCanonical, info.interval());
            if (expectedElement != null) {
              int expectedElemOid = expectedElement.payload().getOid();
              if (expectedElemOid != elementOid) {
                ValidationSupport.err(
                    errors, "floe.type.array.typelem.mismatch", ctx, expectedElemOid, elementOid);
              }
            }
          }
        }

        int arrayOid = info.payload().getOid();

        if (info.payload().hasTyparray() && info.payload().getTyparray() != 0) {
          ValidationSupport.err(
              errors, "floe.type.array.typarray.nonzero", ctx, info.payload().getTyparray());
        }

        int expectedArrayOidFromElement = element.payload().getTyparray();
        if (expectedArrayOidFromElement != 0 && expectedArrayOidFromElement != arrayOid) {
          ValidationSupport.err(
              errors,
              "floe.type.array.typarray.mismatch",
              ctx,
              arrayOid,
              expectedArrayOidFromElement);
        }

        if (catalogElement != null) {
          String expectedElementCanonical = ValidationSupport.canonicalOrBlank(catalogElement);
          String resolvedElementCanonical =
              element.type().name() != null
                  ? ValidationSupport.canonicalOrBlank(element.type().name())
                  : "";
          if (!ValidationSupport.isBlank(expectedElementCanonical)
              && !ValidationSupport.isBlank(resolvedElementCanonical)
              && !expectedElementCanonical.equals(resolvedElementCanonical)) {
            ValidationSupport.err(
                errors,
                "floe.type.array.element_name.mismatch",
                ctx,
                expectedElementCanonical,
                resolvedElementCanonical);
          }
        }
      }
    }
  }

  static void validateFunctionReferences(
      Lookup lookup, IntervalIndex<Integer> functionIntervals, List<ValidationIssue> errors) {
    if (functionIntervals == null) {
      return;
    }
    for (TypeInfo info : lookup.all()) {
      String ctx = ValidationSupport.context("type", info.type().name());
      String ruleCtx = ValidationSupport.contextWithInterval(ctx, info.interval());
      FloeTypeSpecific spec = info.payload();
      checkFunctionReference(
          errors,
          "floe.type.typinput.unknown",
          ruleCtx,
          spec.getTypinput(),
          functionIntervals,
          info.interval());
      checkFunctionReference(
          errors,
          "floe.type.typoutput.unknown",
          ruleCtx,
          spec.getTypoutput(),
          functionIntervals,
          info.interval());
      checkFunctionReference(
          errors,
          "floe.type.typreceive.unknown",
          ruleCtx,
          spec.getTypreceive(),
          functionIntervals,
          info.interval());
      checkFunctionReference(
          errors,
          "floe.type.typsend.unknown",
          ruleCtx,
          spec.getTypsend(),
          functionIntervals,
          info.interval());
      checkFunctionReference(
          errors,
          "floe.type.typmodin.unknown",
          ruleCtx,
          spec.getTypmodin(),
          functionIntervals,
          info.interval());
      checkFunctionReference(
          errors,
          "floe.type.typmodout.unknown",
          ruleCtx,
          spec.getTypmodout(),
          functionIntervals,
          info.interval());
    }
  }

  private static void checkFunctionReference(
      List<ValidationIssue> errors,
      String code,
      String ctx,
      int oid,
      IntervalIndex<Integer> functionIntervals,
      VersionIntervals.VersionInterval interval) {
    if (oid <= 0) {
      return;
    }
    if (!functionIntervals.covers(oid, interval)) {
      ValidationSupport.err(errors, code, ctx, oid);
    }
  }
}
