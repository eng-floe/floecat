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

package ai.floedb.floecat.client.cli;

import ai.floedb.floecat.connector.rpc.AuthCredentials;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class AuthCredentialParser {
  private AuthCredentialParser() {}

  static AuthCredentials buildCredentials(
      String credType, Map<String, String> credProps, Map<String, String> credHeaders) {
    boolean any =
        !nvl(credType, "").isBlank()
            || (credProps != null && !credProps.isEmpty())
            || (credHeaders != null && !credHeaders.isEmpty());
    if (!any) {
      return null;
    }
    if (credType == null || credType.isBlank()) {
      throw new IllegalArgumentException("Missing --cred-type when --cred or --cred-head is set");
    }

    Map<String, String> props =
        credProps == null ? new LinkedHashMap<>() : new LinkedHashMap<>(credProps);
    Map<String, String> headers =
        credHeaders == null ? new LinkedHashMap<>() : new LinkedHashMap<>(credHeaders);

    var b = AuthCredentials.newBuilder();
    switch (credType.toLowerCase(Locale.ROOT)) {
      case "bearer" -> {
        String token = requireCredField(props, "token");
        b.setBearer(AuthCredentials.BearerToken.newBuilder().setToken(token));
      }
      case "client" -> {
        String endpoint = requireCredField(props, "endpoint");
        String clientId = requireCredField(props, "client_id");
        String clientSecret = requireCredField(props, "client_secret");
        b.setClient(
            AuthCredentials.ClientCredentials.newBuilder()
                .setEndpoint(endpoint)
                .setClientId(clientId)
                .setClientSecret(clientSecret));
      }
      case "cli" -> {
        var cb = AuthCredentials.CliCredentials.newBuilder();
        String provider = take(props, "provider");
        if (provider != null && !provider.isBlank()) {
          cb.setProvider(provider);
        }
        b.setCli(cb);
      }
      case "aws" -> {
        String accessKeyId = requireCredField(props, "access_key_id");
        String secretAccessKey = requireCredField(props, "secret_access_key");
        var ab =
            AuthCredentials.AwsCredentials.newBuilder()
                .setAccessKeyId(accessKeyId)
                .setSecretAccessKey(secretAccessKey);
        String sessionToken = take(props, "session_token");
        if (sessionToken != null && !sessionToken.isBlank()) {
          ab.setSessionToken(sessionToken);
        }
        b.setAws(ab);
      }
      case "aws-web-identity" -> {
        String roleArn = requireCredField(props, "role_arn");
        var wb = AuthCredentials.AwsWebIdentity.newBuilder().setRoleArn(roleArn);
        String roleSessionName = take(props, "role_session_name");
        if (roleSessionName != null && !roleSessionName.isBlank()) {
          wb.setRoleSessionName(roleSessionName);
        }
        String providerId = take(props, "provider_id");
        if (providerId != null && !providerId.isBlank()) {
          wb.setProviderId(providerId);
        }
        int durationSeconds = parseIntOrZero(take(props, "duration_seconds"));
        if (durationSeconds > 0) {
          wb.setDurationSeconds(durationSeconds);
        }
        b.setAwsWebIdentity(wb);
      }
      case "aws-assume-role" -> {
        String roleArn = requireCredField(props, "role_arn");
        var ab = AuthCredentials.AwsAssumeRole.newBuilder().setRoleArn(roleArn);
        String roleSessionName = take(props, "role_session_name");
        if (roleSessionName != null && !roleSessionName.isBlank()) {
          ab.setRoleSessionName(roleSessionName);
        }
        String externalId = take(props, "external_id");
        if (externalId != null && !externalId.isBlank()) {
          ab.setExternalId(externalId);
        }
        int durationSeconds = parseIntOrZero(take(props, "duration_seconds"));
        if (durationSeconds > 0) {
          ab.setDurationSeconds(durationSeconds);
        }
        b.setAwsAssumeRole(ab);
      }
      case "token-exchange" -> {
        var base = buildTokenExchange(props);
        b.setRfc8693TokenExchange(AuthCredentials.Rfc8693TokenExchange.newBuilder().setBase(base));
      }
      case "token-exchange-entra" -> {
        var base = buildTokenExchange(props);
        b.setAzureTokenExchange(AuthCredentials.AzureTokenExchange.newBuilder().setBase(base));
      }
      case "token-exchange-gcp" -> {
        var base = buildTokenExchange(props);
        var gb = AuthCredentials.GcpTokenExchange.newBuilder().setBase(base);
        String svcEmail = take(props, "gcp.service_account_email");
        if (svcEmail != null && !svcEmail.isBlank()) {
          gb.setServiceAccountEmail(svcEmail);
        }
        String delegatedUser = take(props, "gcp.delegated_user");
        if (delegatedUser != null && !delegatedUser.isBlank()) {
          gb.setDelegatedUser(delegatedUser);
        }
        String pk = take(props, "gcp.service_account_private_key_pem");
        if (pk != null && !pk.isBlank()) {
          gb.setServiceAccountPrivateKeyPem(pk);
        }
        String kid = take(props, "gcp.service_account_private_key_id");
        if (kid != null && !kid.isBlank()) {
          gb.setServiceAccountPrivateKeyId(kid);
        }
        b.setGcpTokenExchange(gb);
      }
      default -> throw new IllegalArgumentException("Unsupported --cred-type: " + credType);
    }

    if (!props.isEmpty()) {
      b.putAllProperties(props);
    }
    if (!headers.isEmpty()) {
      b.putAllHeaders(headers);
    }
    return b.build();
  }

  private static String nvl(String value, String fallback) {
    return value == null ? fallback : value;
  }

  private static AuthCredentials.TokenExchange buildTokenExchange(Map<String, String> props) {
    var b = AuthCredentials.TokenExchange.newBuilder();
    String endpoint = take(props, "endpoint");
    if (endpoint != null && !endpoint.isBlank()) {
      b.setEndpoint(endpoint);
    }
    String subjectType = take(props, "subject_token_type");
    if (subjectType != null && !subjectType.isBlank()) {
      b.setSubjectTokenType(subjectType);
    }
    String requested = take(props, "requested_token_type");
    if (requested != null && !requested.isBlank()) {
      b.setRequestedTokenType(requested);
    }
    String audience = take(props, "audience");
    if (audience != null && !audience.isBlank()) {
      b.setAudience(audience);
    }
    String scope = take(props, "scope");
    if (scope != null && !scope.isBlank()) {
      b.setScope(scope);
    }
    String clientId = take(props, "client_id");
    if (clientId != null && !clientId.isBlank()) {
      b.setClientId(clientId);
    }
    String clientSecret = take(props, "client_secret");
    if (clientSecret != null && !clientSecret.isBlank()) {
      b.setClientSecret(clientSecret);
    }
    return b.build();
  }

  private static String requireCredField(Map<String, String> props, String key) {
    String value = take(props, key);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing --cred " + key + "=...");
    }
    return value;
  }

  private static String take(Map<String, String> props, String key) {
    if (props == null) {
      return null;
    }
    return props.remove(key);
  }

  private static int parseIntOrZero(String value) {
    if (value == null || value.isBlank()) {
      return 0;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid integer: " + value);
    }
  }
}
