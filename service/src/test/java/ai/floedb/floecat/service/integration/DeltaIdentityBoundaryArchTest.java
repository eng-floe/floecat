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

package ai.floedb.floecat.service.integration;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Keeps canonical Delta identity usable without either connector implementation. */
class DeltaIdentityBoundaryArchTest {
  private static final String CONNECTOR_PACKAGE = "ai.floedb.floecat.connector.";
  private static final Set<String> KNOWN_INTEGRATION_DEBT =
      Set.of(
          "ai.floedb.floecat.service.integration.CatalogOverlayReconciler"
              + " -> ai.floedb.floecat.connector.common.resolver.IcebergSchemaMapper",
          "ai.floedb.floecat.service.integration.CatalogIntegrationsImpl"
              + " -> ai.floedb.floecat.connector.common.auth.CredentialResolverSupport");
  private static final JavaClasses INTEGRATION_CLASSES =
      new ClassFileImporter()
          .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
          .importPackages("ai.floedb.floecat.service.integration");

  @Test
  void integrationCodeHasOnlyTheTwoRecordedConnectorDependencies() {
    ArchCondition<JavaClass> condition =
        new ArchCondition<>("depend only on the two recorded connector-common debts") {
          @Override
          public void check(JavaClass source, ConditionEvents events) {
            for (Dependency dependency : source.getDirectDependenciesFromSelf()) {
              String target = dependency.getTargetClass().getName();
              if (!target.startsWith(CONNECTOR_PACKAGE)) {
                continue;
              }
              String edge = source.getName() + " -> " + target;
              if (!KNOWN_INTEGRATION_DEBT.contains(edge)) {
                events.add(SimpleConditionEvent.violated(source, dependency.getDescription()));
              }
            }
          }
        };
    ArchRule rule = ArchRuleDefinition.classes().should(condition);
    rule.check(INTEGRATION_CLASSES);
  }
}
