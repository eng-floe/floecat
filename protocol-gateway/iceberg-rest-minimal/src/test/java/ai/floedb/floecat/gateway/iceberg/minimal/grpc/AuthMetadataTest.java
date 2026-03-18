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

package ai.floedb.floecat.gateway.iceberg.minimal.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import io.grpc.Metadata;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class AuthMetadataTest {
  @Test
  void usesConfiguredAuthHeader() {
    MinimalGatewayConfig config = mock(MinimalGatewayConfig.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(config.authHeader()).thenReturn("x-floe-session");
    when(config.defaultAuthorization()).thenReturn(Optional.empty());
    when(headers.getHeaderString("x-floe-session")).thenReturn("Bearer token");

    Metadata metadata = AuthMetadata.fromHeaders(config, headers);

    assertEquals(
        "Bearer token",
        metadata.get(Metadata.Key.of("x-floe-session", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  void fallsBackToDefaultAuthorization() {
    MinimalGatewayConfig config = mock(MinimalGatewayConfig.class);
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAuthorization()).thenReturn(Optional.of("Bearer default"));

    Metadata metadata = AuthMetadata.fromHeaders(config, null);

    assertEquals(
        "Bearer default",
        metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  void omitsBlankAuthValues() {
    MinimalGatewayConfig config = mock(MinimalGatewayConfig.class);
    HttpHeaders headers = mock(HttpHeaders.class);
    when(config.authHeader()).thenReturn("authorization");
    when(config.defaultAuthorization()).thenReturn(Optional.of(" "));
    when(headers.getHeaderString("authorization")).thenReturn(" ");

    Metadata metadata = AuthMetadata.fromHeaders(config, headers);

    assertNull(metadata.get(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER)));
  }
}
