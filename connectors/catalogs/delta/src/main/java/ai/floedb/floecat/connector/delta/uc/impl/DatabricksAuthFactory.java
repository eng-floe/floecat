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

import ai.floedb.floecat.connector.common.auth.NoAuthProvider;
import ai.floedb.floecat.connector.spi.AuthProvider;
import ai.floedb.floecat.connector.spi.ConnectorConfig;
import java.util.Map;

public final class DatabricksAuthFactory {

  public static AuthProvider from(ConnectorConfig cfg) {
    if (cfg == null || cfg.auth() == null) {
      return new NoAuthProvider();
    }
    return from(cfg.auth(), cfg.uri());
  }

  public static AuthProvider from(ConnectorConfig.Auth auth, String uri) {
    String scheme = auth.scheme() == null ? "none" : auth.scheme().toLowerCase();
    switch (scheme) {
      case "oauth2" -> {
        var props = auth.props();
        String mode = props.getOrDefault("oauth.mode", "").trim().toLowerCase();
        if (mode.isBlank() && "databricks".equals(props.getOrDefault("cli.provider", ""))) {
          mode = "cli";
        }
        if (mode.isBlank()) {
          mode = "static";
        }
        return switch (mode) {
          case "cli" ->
              new OAuth2BearerAuthProvider(
                  new DatabricksCliTokenProvider(
                      requireHost(uri),
                      props.get("cache_path"),
                      props.get("client_id"),
                      props.get("scope")));
          default -> new OAuth2BearerAuthProvider(() -> require(props, "token"));
        };
      }
      case "none" -> {
        return new NoAuthProvider();
      }
      default -> throw new IllegalArgumentException("Unsupported auth.scheme=" + scheme);
    }
  }

  private static String require(Map<String, String> m, String k) {
    String v = m.get(k);
    if (v == null || v.isBlank()) {
      throw new IllegalArgumentException("Missing auth property: " + k);
    }
    return v;
  }

  private static String requireHost(String uri) {
    if (uri == null || uri.isBlank()) {
      throw new IllegalArgumentException("Missing Databricks host");
    }
    return uri;
  }
}
