/*
 * Copyright 2025 Yellowbrick Data, Inc.
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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeCollationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeFunctionSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexAccessMethods;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexOperatorClasses;
import ai.floedb.floecat.extensions.floedb.proto.FloeIndexOperatorFamilies;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeOperatorSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeTypeSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.*;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.List;

/** Ensures the same OID is not claimed by two different objects over overlapping intervals. */
final class GlobalOidValidator implements SectionValidator<SimpleValidationResult> {
  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  GlobalOidValidator(ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    trackNamespaces(catalog.namespaces(), errors);
    trackRelations(catalog.tables(), "relation", errors);
    trackRelations(catalog.views(), "relation", errors);
    trackTypes(catalog.types(), errors);
    trackFunctions(catalog.functions(), errors);
    trackOperators(catalog.operators(), errors);
    trackCollations(catalog.collations(), errors);
    trackRegistry(catalog.registryEngineSpecific(), errors);
    return new SimpleValidationResult(errors);
  }

  private void trackNamespaces(List<SystemNamespaceDef> namespaces, List<ValidationIssue> errors) {
    for (SystemNamespaceDef ns : namespaces) {
      String ctx = ValidationSupport.context(ResourceKind.RK_NAMESPACE.name(), ns.name());
      String identity = "namespace:" + ValidationSupport.canonicalOrBlank(ns.name());
      List<ValidationSupport.DecodedRule<FloeNamespaceSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, ns.engineSpecific(), FloePayloads.NAMESPACE, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeNamespaceSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, "namespace", identity, oid, dr.interval());
      }
    }
  }

  private void trackRelations(
      List<? extends SystemObjectDef> relations, String domain, List<ValidationIssue> errors) {
    for (SystemObjectDef entry : relations) {
      if (!(entry instanceof SystemTableDef || entry instanceof SystemViewDef)) {
        continue;
      }
      NameRef name = entry instanceof SystemTableDef t ? t.name() : ((SystemViewDef) entry).name();
      String ctx = ValidationSupport.context(domain, name);
      String identity = domain + ":" + ValidationSupport.canonicalOrBlank(name);
      List<ValidationSupport.DecodedRule<FloeRelationSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, entry.engineSpecific(), FloePayloads.RELATION, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeRelationSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, domain, identity, oid, dr.interval());
      }
    }
  }

  private void trackTypes(List<SystemTypeDef> types, List<ValidationIssue> errors) {
    for (SystemTypeDef type : types) {
      String ctx = ValidationSupport.context(ResourceKind.RK_TYPE.name(), type.name());
      String identity = "type:" + ValidationSupport.canonicalOrBlank(type.name());
      List<ValidationSupport.DecodedRule<FloeTypeSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, type.engineSpecific(), FloePayloads.TYPE, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeTypeSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, "type", identity, oid, dr.interval());
      }
    }
  }

  private void trackFunctions(List<SystemFunctionDef> functions, List<ValidationIssue> errors) {
    for (SystemFunctionDef def : functions) {
      String ctx = ValidationSupport.context(ResourceKind.RK_FUNCTION.name(), def.name());
      String identity = buildFunctionIdentity(def.name());
      List<ValidationSupport.DecodedRule<FloeFunctionSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, def.engineSpecific(), FloePayloads.FUNCTION, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeFunctionSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, "function", identity, oid, dr.interval());
      }
    }
  }

  private void trackOperators(List<SystemOperatorDef> operators, List<ValidationIssue> errors) {
    for (SystemOperatorDef def : operators) {
      String ctx = ValidationSupport.context(ResourceKind.RK_OPERATOR.name(), def.name());
      String identity = buildOperatorIdentity(def);
      List<ValidationSupport.DecodedRule<FloeOperatorSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, def.engineSpecific(), FloePayloads.OPERATOR, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeOperatorSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, "operator", identity, oid, dr.interval());
      }
    }
  }

  private void trackCollations(List<SystemCollationDef> collations, List<ValidationIssue> errors) {
    for (SystemCollationDef col : collations) {
      String ctx = ValidationSupport.context(ResourceKind.RK_COLLATION.name(), col.name());
      String identity = "collation:" + ValidationSupport.canonicalOrBlank(col.name());
      List<ValidationSupport.DecodedRule<FloeCollationSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext, scope, col.engineSpecific(), FloePayloads.COLLATION, ctx, errors);
      for (ValidationSupport.DecodedRule<FloeCollationSpecific> dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        ValidationSupport.trackGlobalOid(
            runContext, errors, ctx, "collation", identity, oid, dr.interval());
      }
    }
  }

  private void trackRegistry(List<EngineSpecificRule> rules, List<ValidationIssue> errors) {
    List<ValidationSupport.DecodedRule<FloeIndexAccessMethods>> accessRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            rules,
            FloePayloads.INDEX_ACCESS_METHODS,
            "registry:access_methods",
            errors);
    for (ValidationSupport.DecodedRule<FloeIndexAccessMethods> dr : accessRules) {
      for (FloeIndexAccessMethods.AccessMethod method : dr.payload().getMethodsList()) {
        if (method.getOid() <= 0) {
          continue;
        }
        String identity = "access_method:" + method.getName();
        ValidationSupport.trackGlobalOid(
            runContext,
            errors,
            "registry:access_methods",
            "registry:access_method",
            identity,
            method.getOid(),
            dr.interval());
      }
    }
    List<ValidationSupport.DecodedRule<FloeIndexOperatorFamilies>> familyRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            rules,
            FloePayloads.INDEX_OPERATOR_FAMILIES,
            "registry:operator_families",
            errors);
    for (ValidationSupport.DecodedRule<FloeIndexOperatorFamilies> dr : familyRules) {
      for (FloeIndexOperatorFamilies.OperatorFamily family : dr.payload().getFamiliesList()) {
        if (family.getOid() <= 0) {
          continue;
        }
        String identity = "operator_family:" + family.getAccessMethodOid() + ":" + family.getName();
        ValidationSupport.trackGlobalOid(
            runContext,
            errors,
            "registry:operator_families",
            "registry:operator_family",
            identity,
            family.getOid(),
            dr.interval());
      }
    }
    List<ValidationSupport.DecodedRule<FloeIndexOperatorClasses>> classRules =
        ValidationSupport.decodeAllPayloads(
            runContext,
            scope,
            rules,
            FloePayloads.INDEX_OPERATOR_CLASSES,
            "registry:operator_classes",
            errors);
    for (ValidationSupport.DecodedRule<FloeIndexOperatorClasses> dr : classRules) {
      for (FloeIndexOperatorClasses.OperatorClass clazz : dr.payload().getClassesList()) {
        if (clazz.getOid() <= 0) {
          continue;
        }
        String identity =
            "operator_class:"
                + clazz.getFamilyOid()
                + ":"
                + clazz.getInputTypeOid()
                + ":"
                + clazz.getAccessMethodOid();
        ValidationSupport.trackGlobalOid(
            runContext,
            errors,
            "registry:operator_classes",
            "registry:operator_class",
            identity,
            clazz.getOid(),
            dr.interval());
      }
    }
  }

  private static String buildFunctionIdentity(NameRef name) {
    return "function:" + ValidationSupport.canonicalOrBlank(name);
  }

  private static String buildOperatorIdentity(SystemOperatorDef def) {
    String name = ValidationSupport.canonicalOrBlank(def.name());
    if (def instanceof SystemOperatorDef op) {
      return "operator:"
          + name
          + "("
          + ValidationSupport.canonicalOrBlank(op.leftType())
          + ","
          + ValidationSupport.canonicalOrBlank(op.rightType())
          + ")";
    }
    return "operator:" + name;
  }
}
