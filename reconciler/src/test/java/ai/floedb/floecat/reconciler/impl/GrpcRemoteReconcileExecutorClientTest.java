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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.grpc.Metadata;
import org.junit.jupiter.api.Test;

class GrpcRemoteReconcileExecutorClientTest {

  @Test
  void metadataIncludesExplicitWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "authorization", () -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-1");

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-1");
  }

  @Test
  void oidcModeRequiresWorkerAuthorizationHeader() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient("authorization", java.util.Optional::empty);

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> client.metadata("corr-2"));

    assertThat(ex).hasMessageContaining("Reconcile worker authorization header is required");
  }

  @Test
  void workerAuthCanBeExplicitlyDisabled() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient("authorization", false, java.util.Optional::empty);

    Metadata metadata = client.metadata("corr-disabled");

    assertThat(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)))
        .isNull();
    assertThat(metadata.get(Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("corr-disabled");
  }

  @Test
  void workerCallsUseSessionHeaderWhenConfigured() {
    GrpcRemoteReconcileExecutorClient client =
        new GrpcRemoteReconcileExecutorClient(
            "x-floe-session", () -> java.util.Optional.of("Bearer worker-token"));

    Metadata metadata = client.metadata("corr-3");

    assertThat(metadata.keys()).contains("x-correlation-id");
    assertThat(metadata.get(Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER)))
        .isEqualTo("Bearer worker-token");
  }
}
