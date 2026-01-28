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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.extensions.floedb.proto.FloeNamespaceSpecific;
import ai.floedb.floecat.extensions.floedb.proto.FloeRelationSpecific;
import ai.floedb.floecat.extensions.floedb.utils.FloePayloads;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RelationValidatorTest {

  @Test
  void validTableRelationPasses() {
    SystemCatalogData catalog =
        catalogWithRelation("pg_catalog.pg_class", true, 10, "pg_catalog", "r", null, true);
    RelationValidator validator =
        new RelationValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors()).isEmpty();
  }

  @Test
  void missingOidReported() {
    SystemCatalogData catalog =
        catalogWithRelation("pg_catalog.pg_class", true, 0, "pg_catalog", "r", null, true);
    RelationValidator validator =
        new RelationValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .contains("floe.relation.oid_required");
  }

  @Test
  void relnameMismatchReported() {
    SystemCatalogData catalog =
        catalogWithRelation("pg_catalog.pg_class", true, 10, "pg_catalog", "r", "wrong", true);
    RelationValidator validator =
        new RelationValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .contains("floe.relation.name_mismatch");
  }

  @Test
  void unknownNamespaceReported() {
    SystemCatalogData catalog =
        catalogWithRelation("pg_catalog.pg_class", true, 10, "missing", "r", null, false);
    RelationValidator validator =
        new RelationValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .contains("floe.relation.namespace_unknown");
  }

  @Test
  void invalidKindForViewReported() {
    SystemCatalogData catalog =
        catalogWithRelation("public.my_view", false, 11, "public", "x", null, true);
    RelationValidator validator =
        new RelationValidator(new ValidationScope("floedb"), ValidationSupport.newRunContext());
    assertThat(validator.validate(catalog).errors())
        .extracting(ValidationIssue::code)
        .contains("floe.relation.kind_invalid");
  }

  private SystemCatalogData catalogWithRelation(
      String canonicalName,
      boolean isTable,
      int oid,
      String namespace,
      String relkind,
      String relnameOverride,
      boolean includeNamespace) {
    SystemNamespaceDef ns = namespaceDef(namespace);
    EngineSpecificRule rule = relationRule(oid, namespace, relkind, relnameOverride, canonicalName);
    if (isTable) {
      SystemTableDef table =
          new SystemTableDef(
              NameRefUtil.name(canonicalName),
              "rel",
              List.of(),
              TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
              "scanner",
              List.of(rule));
      return new SystemCatalogData(
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          includeNamespace ? List.of(ns) : List.of(),
          List.of(table),
          List.of(),
          List.of());
    } else {
      SystemViewDef view =
          new SystemViewDef(
              NameRefUtil.name(canonicalName), "rel", "", "", List.of(), List.of(rule));
      return new SystemCatalogData(
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(),
          List.of(ns),
          List.of(),
          List.of(view),
          List.of());
    }
  }

  private SystemNamespaceDef namespaceDef(String canonical) {
    return new SystemNamespaceDef(
        NameRefUtil.name(canonical),
        canonical,
        List.of(namespaceRule(canonicalUuid(canonical), canonical)));
  }

  private EngineSpecificRule namespaceRule(int oid, String canonical) {
    FloeNamespaceSpecific.Builder builder = FloeNamespaceSpecific.newBuilder();
    builder.setOid(oid);
    builder.setNspname(canonical);
    return new EngineSpecificRule(
        "floedb",
        "1.0",
        "",
        FloePayloads.NAMESPACE.type(),
        builder.build().toByteArray(),
        Map.of());
  }

  private EngineSpecificRule relationRule(
      int oid, String namespace, String relkind, String relname, String canonicalName) {
    FloeRelationSpecific.Builder builder = FloeRelationSpecific.newBuilder();
    builder.setOid(oid);
    builder.setRelnamespace(canonicalUuid(namespace));
    builder.setRelkind(relkind);
    builder.setRelname(relname == null ? lastSegment(canonicalName) : relname);
    return new EngineSpecificRule(
        "floedb", "1.0", "", FloePayloads.RELATION.type(), builder.build().toByteArray(), Map.of());
  }

  private int canonicalUuid(String canonical) {
    return canonical.hashCode();
  }

  private String lastSegment(String canonical) {
    int dot = canonical.lastIndexOf('.');
    return dot < 0 ? canonical : canonical.substring(dot + 1);
  }
}
