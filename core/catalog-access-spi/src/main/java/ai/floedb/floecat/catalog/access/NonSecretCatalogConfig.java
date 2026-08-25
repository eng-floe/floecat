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

package ai.floedb.floecat.catalog.access;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Validation and safe rendering for configuration that may be persisted or logged. */
final class NonSecretCatalogConfig {
  private static final Set<String> ALLOWED_TOKEN_KEYS = Set.of("token_refresh_enabled");
  private static final Set<String> FORBIDDEN_KEYS =
      Set.of(
          "authorization",
          "bearer",
          "token",
          "access_token",
          "refresh_token",
          "session_token",
          "id_token",
          "client_secret",
          "secret",
          "password",
          "access_key",
          "access_key_id",
          "secret_key",
          "secret_access_key",
          "private_key",
          "private_key_pem",
          "api_key",
          "assertion",
          "credential",
          "credentials",
          "jwt",
          "cookie",
          "signature",
          "sig");

  private NonSecretCatalogConfig() {}

  static void validateProperties(Map<String, String> properties, String fieldName) {
    for (String key : properties.keySet()) {
      if (isSecretKey(key)) {
        throw new IllegalArgumentException(fieldName + " must not contain secret key: " + key);
      }
    }
  }

  static void validateEndpoint(URI endpoint) {
    if (endpoint.getRawFragment() != null) {
      throw new IllegalArgumentException("endpoint must not contain a fragment");
    }
    String query = endpoint.getRawQuery();
    if (query == null || query.isBlank()) {
      return;
    }
    for (String parameter : query.split("[&;]")) {
      String rawKey = parameter.split("=", 2)[0];
      String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
      if (isSecretKey(key)) {
        throw new IllegalArgumentException("endpoint query must not contain secret key: " + key);
      }
    }
  }

  static String propertyKeys(Map<String, String> properties) {
    return properties.keySet().stream().sorted().collect(Collectors.joining(", ", "[", "]"));
  }

  static String safeEndpoint(URI endpoint) {
    if (endpoint.getRawQuery() == null) {
      return endpoint.toString();
    }
    return endpoint.toString().substring(0, endpoint.toString().indexOf('?')) + "?<redacted>";
  }

  static boolean isSecretKey(String key) {
    String canonical = canonicalKey(key);
    if (canonical.isEmpty()) {
      return false;
    }
    if (ALLOWED_TOKEN_KEYS.contains(canonical)) {
      return false;
    }
    if (FORBIDDEN_KEYS.contains(canonical)
        || canonical.contains("private_key")
        || canonical.startsWith("header_")) {
      return true;
    }
    String[] parts = canonical.split("_");
    return containsPhrase(parts, "authorization")
        || containsPhrase(parts, "bearer")
        || containsPhrase(parts, "token")
        || containsPhrase(parts, "secret")
        || containsPhrase(parts, "password")
        || containsPhrase(parts, "credential")
        || containsPhrase(parts, "credentials")
        || containsPhrase(parts, "assertion")
        || containsPhrase(parts, "jwt")
        || containsPhrase(parts, "cookie")
        || containsPhrase(parts, "signature")
        || containsPhrase(parts, "access", "key")
        || containsPhrase(parts, "secret", "key")
        || containsPhrase(parts, "private", "key")
        || containsPhrase(parts, "api", "key");
  }

  private static boolean containsPhrase(String[] parts, String... phrase) {
    for (int i = 0; i <= parts.length - phrase.length; i++) {
      if (Arrays.equals(Arrays.copyOfRange(parts, i, i + phrase.length), phrase)) {
        if (phrase.length == 1
            && phrase[0].equals("token")
            && i + 1 < parts.length
            && parts[i + 1].equals("type")) {
          continue;
        }
        return true;
      }
    }
    return false;
  }

  private static String canonicalKey(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    return key.trim()
        .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
        .toLowerCase(java.util.Locale.ROOT)
        .replaceAll("[^a-z0-9]+", "_")
        .replaceAll("^_+|_+$", "");
  }
}
