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

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.spi.ReconcileContext;
import io.grpc.Metadata;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GrpcReconcilerBackendMetadataTest {
  @Test
  void metadataContainsCorrelationIdAndBearerToken() {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.<String>empty(), Optional.<Duration>empty());
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build();
    ReconcileContext ctx =
        new ReconcileContext(
            "corr", principal, "svc", Instant.now(), Optional.of("  secret-token  "));

    Metadata metadata = backend.metadataForContext(ctx);

    Metadata.Key<String> correlationKey =
        Metadata.Key.of("x-correlation-id", Metadata.ASCII_STRING_MARSHALLER);
    Metadata.Key<String> authorizationKey =
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    assertThat(metadata.get(correlationKey)).isEqualTo("corr");
    assertThat(metadata.get(authorizationKey)).isEqualTo("Bearer secret-token");
  }

  @Test
  void metadataFallsBackToStaticTokenWhenContextLacksOne() {
    GrpcReconcilerBackend backend =
        new GrpcReconcilerBackend(
            Optional.<String>empty(), Optional.of("static-token"), Optional.<Duration>empty());
    PrincipalContext principal =
        PrincipalContext.newBuilder().setAccountId("acct").setCorrelationId("corr").build();
    ReconcileContext ctx =
        new ReconcileContext("corr", principal, "svc", Instant.now(), Optional.empty());

    Metadata metadata = backend.metadataForContext(ctx);

    Metadata.Key<String> authorizationKey =
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    assertThat(metadata.get(authorizationKey)).isEqualTo("Bearer static-token");
  }
}
