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

package ai.floedb.floecat.service.error.impl;

import com.google.rpc.Status;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import org.junit.jupiter.api.Test;

class GrpcErrorsContractArchTest {
  private static final JavaClasses CLASSES =
      new ClassFileImporter().importPackages("ai.floedb.floecat");

  @Test
  void onlyGrpcErrorsMayConstructStatusRuntimeException() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .that()
            .resideOutsideOfPackage("..service.error.impl..")
            .should()
            .callConstructor(StatusRuntimeException.class);
    rule = rule.allowEmptyShould(true);
    rule.check(CLASSES);
  }

  @Test
  void onlyGrpcErrorsMayInvokeStatusProtoToStatusRuntimeException() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .that()
            .resideOutsideOfPackage("..service.error.impl..")
            .should()
            .callMethod(StatusProto.class, "toStatusRuntimeException", Status.class);
    rule = rule.allowEmptyShould(true);
    rule.check(CLASSES);
  }

  @Test
  void onlyGrpcErrorsMayCallStatusAsRuntimeException() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .that()
            .resideOutsideOfPackage("..service.error.impl..")
            .should()
            .callMethod(io.grpc.Status.class, "asRuntimeException");
    rule = rule.allowEmptyShould(true);
    rule.check(CLASSES);
  }

  @Test
  void onlyGrpcErrorsMayCallStatusAsRuntimeExceptionWithMetadata() {
    ArchRule rule =
        ArchRuleDefinition.noClasses()
            .that()
            .resideOutsideOfPackage("..service.error.impl..")
            .should()
            .callMethod(io.grpc.Status.class, "asRuntimeException", Metadata.class);
    rule = rule.allowEmptyShould(true);
    rule.check(CLASSES);
  }
}
