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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class NamespaceValidator implements SectionValidator<SimpleValidationResult> {
  private static final String CODE_OID_REQUIRED = "floe.namespace.oid_required";
  private static final String CODE_NAME_REQUIRED = "floe.namespace.nspname_required";
  private static final String CODE_NAMESPACE_NAME_REQUIRED = "floe.namespace.name_required";
  private static final String CODE_NAMESPACE_PATH = "floe.namespace.path_not_empty";
  private static final String CODE_NAME_MISMATCH = "floe.namespace.name_mismatch";

  private final ValidationScope scope;
  private final ValidationSupport.ValidationRunContext runContext;

  NamespaceValidator(ValidationScope scope, ValidationSupport.ValidationRunContext runContext) {
    this.scope = scope;
    this.runContext = runContext;
  }

  @Override
  public SimpleValidationResult validate(SystemCatalogData catalog) {
    List<ValidationIssue> errors = new ArrayList<>();
    if (catalog != null && !catalog.namespaces().isEmpty()) {
      for (SystemNamespaceDef ns : catalog.namespaces()) {
        validateNamespace(ns, errors);
      }
    }
    return new SimpleValidationResult(errors);
  }

  private void validateNamespace(SystemNamespaceDef ns, List<ValidationIssue> errors) {
    NameRef name = ns.name();
    String ctx = ValidationSupport.context(ResourceKind.RK_NAMESPACE.name(), name);
    if (name == null || ValidationSupport.isBlank(name.getName())) {
      ValidationSupport.err(errors, CODE_NAMESPACE_NAME_REQUIRED, ctx);
      return;
    }
    if (name.getPathCount() > 0) {
      ValidationSupport.err(errors, CODE_NAMESPACE_PATH, ctx);
    }
    String normalizedName = normalizedName(name);
    List<ValidationSupport.DecodedRule<FloeNamespaceSpecific>> decoded =
        ValidationSupport.decodeAllPayloads(
            runContext, scope, ns.engineSpecific(), FloePayloads.NAMESPACE, ctx, errors);
    for (ValidationSupport.DecodedRule<FloeNamespaceSpecific> dr : decoded) {
      FloeNamespaceSpecific payload = dr.payload();
      if (!ValidationSupport.requirePositiveOid(errors, CODE_OID_REQUIRED, ctx, payload.getOid())) {
        continue;
      }
      String nspname = payload.getNspname();
      if (ValidationSupport.isBlank(nspname)) {
        ValidationSupport.err(errors, CODE_NAME_REQUIRED, ctx);
        continue;
      }
      String normalizedPayload = nspname.trim().toLowerCase(Locale.ROOT);
      if (!normalizedName.equals(normalizedPayload)) {
        ValidationSupport.err(errors, CODE_NAME_MISMATCH, ctx, name.getName(), nspname);
      }
    }
  }

  private static String normalizedName(NameRef name) {
    if (name == null) {
      return "";
    }
    String candidate = name.getName();
    return candidate == null ? "" : candidate.trim().toLowerCase(Locale.ROOT);
  }
}
