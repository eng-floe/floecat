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

package ai.floedb.floecat.service.account;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import org.junit.jupiter.api.Test;

/**
 * Floecat ships standalone, so it may not learn where it is deployed. Which accounts it serves
 * arrives over the assignment RPC; it is never derived from the cluster, a replica count or a
 * placement hash. Anything that needs those belongs in the Floe-specific runtime extension.
 */
class DeploymentIndependenceArchTest {
  private static final JavaClasses CLASSES =
      new ClassFileImporter().importPackages("ai.floedb.floecat");

  @Test
  void floecatNeverTalksToKubernetes() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAnyPackage("io.fabric8..", "io.kubernetes..");
    rule.allowEmptyShould(true).check(CLASSES);
  }

  @Test
  void floecatNeverPlacesAccountsItself() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .should()
            .dependOnClassesThat()
            .haveSimpleNameEndingWith("JumpConsistentHash");
    rule.allowEmptyShould(true).check(CLASSES);
  }
}
