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

package ai.floedb.floecat.service.common;

import ai.floedb.floecat.service.error.impl.GrpcErrors;
import java.util.Map;
import java.util.Set;

public final class PersistedSecretPropertyValidator {
  private static final Set<String> FORBIDDEN_PERSISTED_SECRET_KEYS =
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
          "jwt");
  private static final Set<String> FORBIDDEN_GENERAL_METADATA_SECRET_KEYS =
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
          "credentials",
          "jwt");

  private PersistedSecretPropertyValidator() {}

  public static void validateNoSecretKeys(
      Map<String, String> values, String corr, String fieldName) {
    if (values == null || values.isEmpty()) {
      return;
    }
    for (String key : values.keySet()) {
      if (!isForbiddenPersistedSecretKey(key)) {
        continue;
      }
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", fieldName, "key", String.valueOf(key)));
    }
  }

  public static void validateNoGeneralMetadataSecretKeys(
      Map<String, String> values, String corr, String fieldName) {
    if (values == null || values.isEmpty()) {
      return;
    }
    for (String key : values.keySet()) {
      if (!isForbiddenGeneralMetadataSecretKey(key)) {
        continue;
      }
      throw GrpcErrors.invalidArgument(
          corr, null, Map.of("field", fieldName, "key", String.valueOf(key)));
    }
  }

  public static boolean isForbiddenPersistedSecretKey(String key) {
    String canonical = canonicalSecretKey(key);
    if (canonical.isEmpty()) {
      return false;
    }
    if (FORBIDDEN_PERSISTED_SECRET_KEYS.contains(canonical)) {
      return true;
    }
    if (containsForbiddenSecretPhrase(canonical)) {
      return true;
    }
    if (canonical.contains("private_key")) {
      return true;
    }
    if (canonical.endsWith("_token") && !canonical.endsWith("_token_type")) {
      return true;
    }
    if (canonical.endsWith("_secret")
        || canonical.endsWith("_password")
        || canonical.endsWith("_key")
        || canonical.endsWith("_credentials")) {
      return true;
    }
    return false;
  }

  public static boolean isForbiddenGeneralMetadataSecretKey(String key) {
    String canonical = canonicalSecretKey(key);
    if (canonical.isEmpty()) {
      return false;
    }
    return FORBIDDEN_GENERAL_METADATA_SECRET_KEYS.contains(canonical);
  }

  private static boolean containsForbiddenSecretPhrase(String canonical) {
    return containsTokenPhrase(canonical, "authorization")
        || containsTokenPhrase(canonical, "bearer")
        || containsTokenPhrase(canonical, "token")
        || containsTokenPhrase(canonical, "access", "token")
        || containsTokenPhrase(canonical, "refresh", "token")
        || containsTokenPhrase(canonical, "session", "token")
        || containsTokenPhrase(canonical, "id", "token")
        || containsTokenPhrase(canonical, "client", "secret")
        || containsTokenPhrase(canonical, "secret")
        || containsTokenPhrase(canonical, "password")
        || containsTokenPhrase(canonical, "access", "key")
        || containsTokenPhrase(canonical, "access", "key", "id")
        || containsTokenPhrase(canonical, "secret", "key")
        || containsTokenPhrase(canonical, "secret", "access", "key")
        || containsTokenPhrase(canonical, "private", "key")
        || containsTokenPhrase(canonical, "private", "key", "pem")
        || containsTokenPhrase(canonical, "api", "key")
        || containsTokenPhrase(canonical, "assertion")
        || containsTokenPhrase(canonical, "credential")
        || containsTokenPhrase(canonical, "credentials")
        || containsTokenPhrase(canonical, "jwt");
  }

  private static boolean containsTokenPhrase(String canonical, String... tokens) {
    String[] parts = canonical.split("_");
    if (parts.length < tokens.length) {
      return false;
    }
    for (int i = 0; i <= parts.length - tokens.length; i++) {
      boolean match = true;
      for (int j = 0; j < tokens.length; j++) {
        if (!parts[i + j].equals(tokens[j])) {
          match = false;
          break;
        }
      }
      if (match
          && tokens.length == 1
          && tokens[0].equals("token")
          && i + 1 < parts.length
          && parts[i + 1].equals("type")) {
        match = false;
      }
      if (match) {
        return true;
      }
    }
    return false;
  }

  public static String canonicalSecretKey(String key) {
    if (key == null || key.isBlank()) {
      return "";
    }
    var chars = new StringBuilder(key.length());
    boolean prevUnderscore = false;
    boolean prevAlphaNum = false;
    boolean prevLowerOrDigit = false;
    for (int i = 0; i < key.length(); i++) {
      char raw = key.charAt(i);
      boolean isUpper = raw >= 'A' && raw <= 'Z';
      boolean isLower = raw >= 'a' && raw <= 'z';
      boolean isDigit = raw >= '0' && raw <= '9';
      if (isUpper && prevAlphaNum && prevLowerOrDigit && !prevUnderscore) {
        chars.append('_');
        prevUnderscore = true;
      }
      char ch = Character.toLowerCase(raw);
      if ((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')) {
        chars.append(ch);
        prevUnderscore = false;
        prevAlphaNum = true;
        prevLowerOrDigit = isLower || isDigit;
      } else if (!prevUnderscore) {
        chars.append('_');
        prevUnderscore = true;
        prevAlphaNum = false;
        prevLowerOrDigit = false;
      }
    }
    int start = 0;
    int end = chars.length();
    while (start < end && chars.charAt(start) == '_') {
      start++;
    }
    while (end > start && chars.charAt(end - 1) == '_') {
      end--;
    }
    return chars.substring(start, end);
  }
}
