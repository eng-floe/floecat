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

import static ai.floedb.floecat.extensions.floedb.utils.FloePayloads.Descriptor.*;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class RelationValidator implements SectionValidator<SimpleValidationResult> {
  private static final String CODE_OID_REQUIRED = "floe.relation.oid_required";
  private static final String CODE_NAME_REQUIRED = "floe.relation.name_required";
  private static final String CODE_NAME_MISMATCH = "floe.relation.name_mismatch";
  private static final String CODE_NAMESPACE_REQUIRED = "floe.relation.namespace_required";
  private static final String CODE_NAMESPACE_UNKNOWN = "floe.relation.namespace_unknown";
  private static final String CODE_KIND_INVALID = "floe.relation.kind_invalid";

  private static final Set<String> TABLE_KINDS = Set.of("r");
  private static final Set<String> VIEW_KINDS = Set.of("v");

  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  RelationValidator(ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    if (catalog == null) {
      return new SimpleValidationResult(errors);
    }
    Map<Integer, NameRef> namespaceOids = collectNamespaceOids(catalog, errors);
    validateRelations(catalog.tables(), "table", namespaceOids, errors);
    validateRelations(catalog.views(), "view", namespaceOids, errors);
    return new SimpleValidationResult(errors);
  }

  private Map<Integer, NameRef> collectNamespaceOids(
      SystemCatalogData catalog, List<ValidationIssue> errors) {
    Map<Integer, NameRef> namespaces = new HashMap<>();
    for (var ns : catalog.namespaces()) {
      String ctx = ValidationSupport.context(ResourceKind.RK_NAMESPACE.name(), ns.name());
      List<ValidationSupport.DecodedRule<FloeNamespaceSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext,
              scope,
              ns.engineSpecific(),
              NAMESPACE,
              FloeNamespaceSpecific.class,
              ctx,
              errors);
      for (var dr : decoded) {
        int oid = dr.payload().getOid();
        if (oid <= 0) {
          continue;
        }
        namespaces.putIfAbsent(oid, ns.name());
      }
    }
    return namespaces;
  }

  private void validateRelations(
      List<? extends SystemObjectDef> relations,
      String domain,
      Map<Integer, NameRef> namespaceOids,
      List<ValidationIssue> errors) {
    for (SystemObjectDef entry : relations) {
      NameRef name = entry instanceof SystemTableDef t ? t.name() : ((SystemViewDef) entry).name();
      String ctx = ValidationSupport.context(domain, name);
      List<ValidationSupport.DecodedRule<FloeRelationSpecific>> decoded =
          ValidationSupport.decodeAllPayloads(
              runContext,
              scope,
              entry.engineSpecific(),
              RELATION,
              FloeRelationSpecific.class,
              ctx,
              errors);
      for (var dr : decoded) {
        FloeRelationSpecific payload = dr.payload();
        if (!ValidationSupport.requirePositiveOid(
            errors, CODE_OID_REQUIRED, ctx, payload.getOid())) {
          continue;
        }
        String relname = payload.getRelname();
        if (ValidationSupport.isBlank(relname)) {
          ValidationSupport.err(errors, CODE_NAME_REQUIRED, ctx);
        } else if (!relname.equals(lastSegment(name))) {
          ValidationSupport.err(errors, CODE_NAME_MISMATCH, ctx, lastSegment(name), relname);
        }
        int namespace = payload.getRelnamespace();
        if (namespace <= 0) {
          ValidationSupport.err(errors, CODE_NAMESPACE_REQUIRED, ctx);
        } else if (!namespaceOids.containsKey(namespace)) {
          ValidationSupport.err(errors, CODE_NAMESPACE_UNKNOWN, ctx, namespace);
        }
        String relkind = payload.getRelkind();
        if (!isAllowedKind(entry, relkind)) {
          ValidationSupport.err(errors, CODE_KIND_INVALID, ctx, relkind);
        }
      }
    }
  }

  private static boolean isAllowedKind(SystemObjectDef entry, String relkind) {
    String candidate = relkind == null ? "" : relkind.toLowerCase(Locale.ROOT);
    if (entry instanceof SystemTableDef) {
      return !candidate.isBlank() && TABLE_KINDS.contains(candidate);
    }
    return !candidate.isBlank() && VIEW_KINDS.contains(candidate);
  }

  private static String lastSegment(NameRef name) {
    if (name == null) {
      return "";
    }
    String canonical = NameRefUtil.canonical(name);
    if (canonical == null || canonical.isBlank()) {
      return "";
    }
    int dot = canonical.lastIndexOf('.');
    if (dot < 0) {
      return canonical;
    }
    return canonical.substring(dot + 1);
  }
}
