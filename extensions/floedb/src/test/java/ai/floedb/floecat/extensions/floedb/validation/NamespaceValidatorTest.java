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
import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NamespaceValidatorTest {

  @Test
  void validNamespacePasses() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespaceDef("pg_catalog", payload(42, "pg_catalog"))),
            List.of(),
            List.of(),
            List.of());

    NamespaceValidator validator =
        new NamespaceValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors()).isEmpty();
  }

  @Test
  void missingOidReported() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespaceDef("schema", payload(0, "schema"))),
            List.of(),
            List.of(),
            List.of());

    NamespaceValidator validator =
        new NamespaceValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .containsExactly("floe.namespace.oid_required");
  }

  @Test
  void namespaceNameMismatchReported() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespaceDef("schema", payload(10, "other"))),
            List.of(),
            List.of(),
            List.of());

    NamespaceValidator validator =
        new NamespaceValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .containsExactly("floe.namespace.name_mismatch");
  }

  @Test
  void namespaceNameMissingReported() {
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemNamespaceDef(
                    NameRef.getDefaultInstance(), "", List.of(payload(1, "missing")))),
            List.of(),
            List.of(),
            List.of());

    NamespaceValidator validator =
        new NamespaceValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .containsExactly("floe.namespace.name_required");
  }

  @Test
  void namespacePathDetected() {
    NameRef nameWithPath = NameRefUtil.name("extra", "pg_catalog");
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespaceDef(nameWithPath, "pg_catalog", payload(3, "pg_catalog"))),
            List.of(),
            List.of(),
            List.of());

    NamespaceValidator validator =
        new NamespaceValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .containsExactly("floe.namespace.path_not_empty");
  }

  private SystemNamespaceDef namespaceDef(String canonical, EngineSpecificRule rule) {
    return new SystemNamespaceDef(NameRefUtil.name(canonical), canonical, List.of(rule));
  }

  private SystemNamespaceDef namespaceDef(NameRef name, String display, EngineSpecificRule rule) {
    return new SystemNamespaceDef(name, display, List.of(rule));
  }

  private EngineSpecificRule payload(int oid, String name) {
    FloeNamespaceSpecific.Builder builder = FloeNamespaceSpecific.newBuilder();
    if (oid > 0) {
      builder.setOid(oid);
    }
    builder.setNspname(name);
    return new EngineSpecificRule(
        "floedb", "1.0", "", NAMESPACE.type(), builder.build().toByteArray(), Map.of());
  }
}
