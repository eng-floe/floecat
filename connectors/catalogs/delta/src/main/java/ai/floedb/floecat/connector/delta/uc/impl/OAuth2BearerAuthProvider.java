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

package ai.floedb.floecat.connector.delta.uc.impl;

import ai.floedb.floecat.connector.spi.AuthProvider;
import java.util.HashMap;
import java.util.Map;

@FunctionalInterface
interface AccessTokenProvider extends AutoCloseable {
  String accessToken();

  @Override
  default void close() {}
}

public final class OAuth2BearerAuthProvider implements AuthProvider, AutoCloseable {
  private final AccessTokenProvider tokens;

  public OAuth2BearerAuthProvider(AccessTokenProvider tokens) {
    this.tokens = tokens;
  }

  @Override
  public String scheme() {
    return "oauth2";
  }

  @Override
  public Map<String, String> apply(Map<String, String> baseProps) {
    var p = new HashMap<>(baseProps);
    p.put("rest.auth.type", "oauth");
    p.put("rest.oauth2.token", tokens.accessToken());
    return p;
  }

  @Override
  public Map<String, String> applyHeaders(Map<String, String> baseHeaders) {
    var h = new HashMap<>(baseHeaders);
    h.put("Authorization", "Bearer " + tokens.accessToken());
    return h;
  }

  @Override
  public void close() {
    try {
      tokens.close();
    } catch (Exception ignore) {
    }
  }
}
