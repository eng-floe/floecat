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

package ai.floedb.floecat.gateway.iceberg.grpc;

import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import io.grpc.Metadata;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Optional;

public final class AuthMetadata {
  private AuthMetadata() {}

  public static Metadata fromHeaders(IcebergGatewayConfig config, HttpHeaders headers) {
    Metadata md = new Metadata();
    String account = header(headers, config.accountHeader());
    if ((account == null || account.isBlank()) && config.defaultAccountId() != null) {
      account = config.defaultAccountId().isBlank() ? null : config.defaultAccountId();
    }
    if (account != null && !account.isBlank()) {
      md.put(key(config.accountHeader()), account);
    }
    String auth = header(headers, config.authHeader());
    if ((auth == null || auth.isBlank()) && config.defaultAuthorization() != null) {
      auth = config.defaultAuthorization().isBlank() ? null : config.defaultAuthorization();
    }
    if (auth != null && !auth.isBlank()) {
      md.put(key(config.authHeader()), auth);
    }
    return md;
  }

  private static Metadata.Key<String> key(String name) {
    return Metadata.Key.of(name.toLowerCase(), Metadata.ASCII_STRING_MARSHALLER);
  }

  private static String header(HttpHeaders headers, String name) {
    return Optional.ofNullable(headers.getHeaderString(name)).orElse(null);
  }
}
