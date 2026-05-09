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

import io.grpc.Metadata;
import java.util.Optional;

final class ReconcileRpcAuthHeaderSupport {
  private ReconcileRpcAuthHeaderSupport() {}

  static Optional<String> resolveHeaderName(
      Optional<String> sessionHeaderName, Optional<String> authorizationHeaderName) {
    Optional<String> sessionHeader = normalize(sessionHeaderName);
    if (sessionHeader.isPresent()) {
      return sessionHeader;
    }
    return normalize(authorizationHeaderName);
  }

  static Metadata.Key<String> headerKey(String headerName) {
    return Metadata.Key.of(headerName, Metadata.ASCII_STRING_MARSHALLER);
  }

  private static Optional<String> normalize(Optional<String> value) {
    return value.map(String::trim).filter(v -> !v.isBlank());
  }
}
