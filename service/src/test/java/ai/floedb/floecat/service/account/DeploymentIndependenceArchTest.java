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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Floecat ships standalone, so it may not learn where it is deployed. Which accounts it serves
 * arrives over the assignment RPC and is never derived from the cluster, a replica count or a
 * placement hash; anything that needs those belongs in the Floe runtime extension.
 *
 * <p>Asserted on the classpath rather than through ArchUnit, whose bytecode reader silently imports
 * nothing on this JDK — see {@code GrpcErrorsContractArchTest}, which has the same problem.
 */
class DeploymentIndependenceArchTest {

  @Test
  void noKubernetesClientOnTheClasspath() {
    for (String kubernetes :
        new String[] {
          "io.fabric8.kubernetes.client.KubernetesClient", "io.kubernetes.client.openapi.ApiClient"
        }) {
      assertThrows(
          ClassNotFoundException.class,
          () -> Class.forName(kubernetes),
          () ->
              kubernetes
                  + " is on Floecat's classpath. Which accounts this process serves comes from"
                  + " the control plane, not from the cluster; put anything that needs Kubernetes in"
                  + " floecat-runtime.");
    }
  }
}
